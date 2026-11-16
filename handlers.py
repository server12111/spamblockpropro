import os
import html
import sqlite3
import telebot
import time
import threading
import logging
import uuid
from input_validation import bounded_number
import re
import database as db
from safe_logging import register_secret
from telegram_text import send_html, send_long, edit_html, protect_text_lengths
from purchase_store import (db_get_payment, db_register_paid_bot, db_activate_trial,
                            db_use_purchase_discount, purchase_context, db_claim_coupon, claim_ton_transfer)
from datetime import datetime
from database import subscription_days_left
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import SUPER_ADMIN, TON_WALLET, WITHDRAWAL_CHANNEL, BOT_USERNAME
from database import (DB_PATH, init_db,
                       db_add_user, db_get_owner_bots,
                       db_get_all_users, db_get_bot_users, db_get_stats,
                       db_get_active_bots_list, db_get_bot_info,
                       db_get_all_bots_for_checker,
                       db_get_bot_setting, db_set_bot_setting,
                       db_renew_bot,
                       get_price, set_price,
                       db_set_pending,
                       db_claim_pending_payment,
                       db_mark_paid, db_is_paid,
                       db_has_used_trial,
                       db_set_referral,
                       db_get_discount_count,
                       db_set_marketplace, db_is_marketplace_enabled,
                       db_get_marketplace_bots,
                       db_get_bot_earnings,
                       db_get_pending_withdrawals,
                       db_get_withdrawal, db_update_withdrawal_status,
                       db_return_bot_earnings,
                       db_set_free_coupon, db_get_free_coupon, db_mark_coupon_used,
                       db_deactivate_bot,
                       DBState)
from keyboards import (start_kb, back_to_start_kb, back_to_payment_kb, cancel_kb, close_kb, reply_kb, payment_kb, cryptobot_kb, broadcast_type_kb, super_admin_kb, _btn, start_text, buy_text, PE_FIRE, PE_DIAMOND, PE_STAR, PE_ROCKET, PE_GIFT, PE_CHECK, PE_MONEY, TE_ANNOUNCE, TE_BLOCK, TE_CALENDAR, TE_CARD, TE_CART, TE_CHAT, TE_CHECK, TE_CROSS, TE_DIAMOND, TE_DOLLAR, TE_EXCLAM, TE_FIRE, TE_FOLDER, TE_FREE, TE_GIFT, TE_GREEN, TE_HOURGLASS, TE_ID, TE_INBOX, TE_KEYBOARD, TE_LINK, TE_MONEY, TE_MOON, TE_NEW, TE_NOTE, TE_PARTY, TE_PICTURE, TE_RED, TE_REFRESH, TE_ROBOT, TE_ROCKET, TE_SEARCH, TE_SEND, TE_STAR, TE_STATS, TE_USER, TE_USERS)
from payments import (cb_create_invoice, cb_check_invoice,
                      get_ton_amount, ton_payment_link, ton_check_transfer)
from purchased_bot import launch_bot, stop_bot, running_bots, running_bot_ids, _lifecycle_lock
from forum_support import html_safe_chunks

log = logging.getLogger(__name__)

_main_rate: dict = {}
MAIN_RATE_SEC = 15
_token_locks = {}
_token_locks_guard = threading.Lock()


def _replace_owned_token(owner_id, bot_id, expected, token, me, main_bot):
    with _token_locks_guard:
        lock = _token_locks.setdefault(bot_id, threading.Lock())
    if not lock.acquire(blocking=False):
        return 'Смена токена уже выполняется'
    try:
        # Тот же RLock используют launch/stop: параллельный автозапуск
        # не вклинивается между остановкой, сменой в БД и новым запуском.
        with _lifecycle_lock:
            info = db_get_bot_info(bot_id)
            if not info or info[2] != owner_id: return 'Этот бот тебе недоступен'
            if info[0] != expected: return 'Токен уже изменён. Открой список ботов заново'
            was_running = expected in running_bots
            if not stop_bot(expected) and was_running:
                return 'Не удалось остановить бота. Попробуй ещё раз'
            snapshot = None
            try:
                snapshot = db.db_replace_bot_token(bot_id, expected, token, me.username or '',
                                                    reset_group=str(me.id) != expected.split(':', 1)[0])
                if not db.db_bot_can_run(bot_id):
                    return f'{TE_CHECK} Токен обновлён, для запуска продли подписку'
                if launch_bot(bot_id, token, info[1], main_bot):
                    return f'{TE_CHECK} Токен обновлён, бот @{me.username} запущен'
            except Exception:
                # Здесь не выводим ни исключение Telegram, ни аргументы вызова.
                pass
            try:
                stop_bot(token)
                if snapshot is not None:
                    db.db_restore_bot_token(bot_id, token, snapshot)
                if db.db_bot_can_run(bot_id):
                    launch_bot(bot_id, expected, info[1], main_bot)
            except Exception:
                log.error('Не удалось восстановить запуск бота #%s', bot_id)
            return 'Не удалось запустить бота с новым токеном, оставлен прежний'
    finally:
        lock.release()


def _is_super_admin(event):
    return event.from_user.id == SUPER_ADMIN


def _is_private_super_admin(event):
    message = getattr(event, 'message', None) or event
    return (_is_super_admin(event) and message.chat.type == 'private'
            and message.chat.id == SUPER_ADMIN)


def _restore_database(tmp_path, main_bot):
    """Ни один экземпляр прежней БД не должен работать с восстановленными ID"""
    import shutil
    import service_broadcast as svc
    backup_path = DB_PATH + f'.pre_restore_{int(time.time())}.bak'
    with _lifecycle_lock:
        previous_ids = set(running_bot_ids)
        replaced = False
        try:
            for token in list(running_bots):
                if not stop_bot(token):
                    raise RuntimeError('Не удалось остановить ботов')
            if running_bots or running_bot_ids:
                raise RuntimeError('Не удалось остановить ботов')
            # Рабочая рассылка завершает текущую отправку до замены файла
            with svc.pause_delivery(), svc._lock, db.db_restore_section():
                for timer in svc._timers.values(): timer.cancel()
                svc._timers.clear()
                db.db_close_all()
                if os.path.exists(DB_PATH): shutil.copy2(DB_PATH, backup_path)
                os.replace(tmp_path, DB_PATH)
                try:
                    init_db()
                except Exception:
                    db.db_close_all()
                    shutil.copy2(backup_path, DB_PATH)
                    raise
                replaced = True
        finally:
            # После успеха запускаем данные новой БД; после отказа — прежние
            for bid, owner, token, admin, expires in db.db_get_all_bots_for_checker():
                if (replaced or bid in previous_ids) and db.db_bot_can_run(bid):
                    launch_bot(bid, token, admin, main_bot)
    log.info('БД заменена; прежняя сохранена: %s', backup_path)

def _is_main_rate_limited(user_id: int) -> bool:
    now = time.time()
    if now - _main_rate.get(user_id, 0) < MAIN_RATE_SEC:
        return True
    _main_rate[user_id] = now
    if len(_main_rate) > 10000:  # защита от роста: чистим протухшие записи
        for k in [k for k, ts in _main_rate.items() if now - ts >= MAIN_RATE_SEC]:
            _main_rate.pop(k, None)
        if len(_main_rate) > 10000:
            _main_rate.clear()
    return False

def start_subscription_checker(main_bot):
    """Фоновый поток: проверяет подписки раз в час, шлёт уведомления."""
    def _check():
        while True:
            time.sleep(3600)
            try:
                bots = db_get_all_bots_for_checker()
                now  = datetime.now()
                for bot_id, owner_id, token, admin_id, expires_at in bots:
                    with _lifecycle_lock:
                        live = db_get_bot_info(bot_id)
                        if not live: continue
                        token = live[0]
                        expires_at = db.db_get_bot_expires(bot_id)
                        if not expires_at:
                            continue
                        try:
                            exp = datetime.fromisoformat(str(expires_at))
                        except Exception:
                            continue
                        days_left = subscription_days_left(exp, now)

                        if days_left <= -30:
                            if not db_deactivate_bot(bot_id, expected_expires=expires_at):
                                continue
                            if token in running_bots:
                                stop_bot(token)
                            log.info(f'Bot #{bot_id} deactivated (expired {abs(days_left)}d ago).')
                            continue

                        if days_left <= 0 and token in running_bots:
                            stop_bot(token)
                            log.info(f'Bot #{bot_id} subscription expired, stopped.')
                            try:
                                main_bot.send_message(owner_id,
                                    f"{TE_CROSS} <b>Подписка на вашего бота истекла!</b>\n\n"
                                    f"Чтобы возобновить — нажми «{TE_CART} Купить бота» в главном меню.",
                                    parse_mode='HTML')
                            except Exception:
                                pass

                        elif days_left == 3 and not db_get_bot_setting(bot_id, 'reminded_3d', ''):
                            db_set_bot_setting(bot_id, 'reminded_3d', '1')
                            try:
                                kb = InlineKeyboardMarkup()
                                kb.add(_btn('Продлить сейчас', icon='🔄', style='success', callback_data=f'renew_bot_{bot_id}'))
                                main_bot.send_message(owner_id,
                                    f"{TE_EXCLAM} <b>Подписка истекает через 3 дня!</b>\n\n"
                                    f"Продли через «{TE_CART} Купить бота» в главном меню или нажми кнопку ниже.",
                                    parse_mode='HTML', reply_markup=kb)
                            except Exception:
                                pass

                        elif days_left == 1 and not db_get_bot_setting(bot_id, 'reminded_1d', ''):
                            db_set_bot_setting(bot_id, 'reminded_1d', '1')
                            try:
                                kb = InlineKeyboardMarkup()
                                kb.add(_btn('Продлить срочно', icon='🔄', style='danger', callback_data=f'renew_bot_{bot_id}'))
                                main_bot.send_message(owner_id,
                                    f"{TE_EXCLAM} <b>Подписка истекает завтра!</b>\n\n"
                                    f"Срочно продли через «{TE_CART} Купить бота».",
                                    parse_mode='HTML', reply_markup=kb)
                            except Exception:
                                pass

                        if days_left > 3:
                            db_set_bot_setting(bot_id, 'reminded_3d', '')
                            db_set_bot_setting(bot_id, 'reminded_1d', '')

            except Exception as e:
                log.error(f'Subscription checker error: {e}')

    threading.Thread(target=_check, daemon=True).start()
    log.info('Subscription checker started.')


def register(bot: telebot.TeleBot):
    protect_text_lengths(bot)
    class _EH:
        def __init__(self):
            self._last_401_alert = 0
            self._last_transient_log = 0
        def handle(self, e):
            err_code = getattr(e, 'error_code', None)
            s = str(e)
            # Временные сбои Telegram/сети: infinity_polling повторит сам —
            # не спамим ERROR-трейсбеками, просто короткий WARNING раз в 5 минут
            if (err_code in (502, 503, 504) or 'Bad Gateway' in s
                    or 'Read timed out' in s or 'Connection aborted' in s
                    or 'Max retries exceeded' in s):
                now = time.time()
                if now - self._last_transient_log > 300:
                    self._last_transient_log = now
                    log.warning('Telegram API временно недоступен (%s) — '
                                'polling продолжается автоматически', s[:120])
                return True
            if '401' in s or 'Unauthorized' in s:
                log.error(f'Handler exception: {e}')
                now = time.time()
                if now - self._last_401_alert > 3600:
                    self._last_401_alert = now
                    # Токен главного бота невалиден — самим ботом это не отправить
                    # (тот же мёртвый токен), поэтому кричим в лог максимально громко.
                    log.critical(
                        '='*60 + '\n'
                        'ГЛАВНЫЙ БОТ: ТОКЕН НЕВАЛИДЕН (401 Unauthorized)!\n'
                        'Проверь BOT_TOKEN в @BotFather и обнови .env / переменные окружения.\n'
                        + '='*60
                    )
                return True
            log.error(f'Handler exception: {e}', exc_info=True)
            return True
    bot.exception_handler = _EH()

    state = DBState(0)
    token_candidates = {}
    token_flow_lock = threading.RLock()
    def _reset_flow(uid, preserve_purchase=False):
        with token_flow_lock:
            token_candidates.pop(uid, None)
        current = state.pop(uid, None)
        current = current if isinstance(current, dict) else {}
        target, via = purchase_context(current)
        clean = {}
        if via:
            clean['via_bot_id'] = via
        if preserve_purchase and target:
            clean.update(step='renewing', bot_id=target)
        if clean:
            state[uid] = clean

    def _resume_bot(bot_id):
        with _lifecycle_lock:
            info = db_get_bot_info(bot_id)
            if info and info[0] not in running_bots:
                return launch_bot(bot_id, info[0], info[1], bot)
            return bool(info)

    def _edit(text, chat_id, message_id, reply_markup=None, parse_mode='HTML'):
        try:
            return edit_html(bot, chat_id, message_id, text, reply_markup, parse_mode)
        except Exception as exc:
            log.warning('_edit failed: %s', exc)
            return None

    _bot_username_cache = [None]

    def _get_bot_username():
        if _bot_username_cache[0] is None:
            try:
                _bot_username_cache[0] = bot.get_me().username or ''
            except Exception:
                _bot_username_cache[0] = ''
        return _bot_username_cache[0]

    def _safe_send(uid, text):
        from purchased_bot import _retry_send
        return _retry_send(lambda: bot.send_message(uid, text, parse_mode='HTML'))

    def token_cancel_kb():
        kb = InlineKeyboardMarkup()
        kb.add(_btn('Отмена', icon='❌', callback_data='token_cancel'))
        return kb

    @bot.callback_query_handler(func=lambda c: c.data == 'token_cancel')
    def cancel_token_change(cb):
        _reset_flow(cb.from_user.id)
        _edit('Смена токена отменена', cb.message.chat.id, cb.message.message_id,
              reply_markup=back_to_start_kb())

    @bot.callback_query_handler(func=lambda c: c.data.startswith('change_token_'))
    def start_token_change(cb):
        try: bot_id = int(cb.data[len('change_token_'):])
        except ValueError: return
        info = db_get_bot_info(bot_id)
        if not info or info[2] != cb.from_user.id:
            bot.answer_callback_query(cb.id, 'Этот бот тебе недоступен', show_alert=True)
            return
        _reset_flow(cb.from_user.id)
        state[cb.from_user.id] = dict(step='change_bot_token', bot_id=bot_id,
                                      expires=time.time()+600, request_id=uuid.uuid4().hex[:16])
        _edit('Отправь новый токен бота из @BotFather', cb.message.chat.id,
              cb.message.message_id, reply_markup=token_cancel_kb())

    @bot.message_handler(func=lambda m: isinstance(state.get(m.from_user.id), dict)
                         and state[m.from_user.id].get('step') == 'change_bot_token'
                         and not (m.text or '').startswith('/'))
    def receive_new_token(m):
        uid = m.from_user.id
        # Удаляем даже некорректный или просроченный ввод, ничего не цитируем.
        try: bot.delete_message(m.chat.id, m.message_id)
        except Exception: pass
        token = (m.text or '').strip()
        register_secret(token)
        with token_flow_lock:
            session = state.get(uid)
            if not isinstance(session, dict) or session.get('step') != 'change_bot_token': return
            if session['expires'] <= time.time():
                _reset_flow(uid)
                bot.send_message(m.chat.id, 'Время ожидания истекло. Нажми «Сменить токен» ещё раз')
                return
            # Один вход забирает этот шаг. Второй одновременный ввод его не выполнит.
            state[uid] = dict(session, step='checking_bot_token')
        def retry(message):
            with token_flow_lock:
                current = state.get(uid)
                if isinstance(current, dict) and current.get('request_id') == session['request_id']:
                    state[uid] = session
            bot.send_message(m.chat.id, message, reply_markup=token_cancel_kb())
        info = db_get_bot_info(session['bot_id'])
        if not info or info[2] != uid:
            _reset_flow(uid); bot.send_message(m.chat.id, 'Этот бот тебе недоступен'); return
        expected = info[0]
        register_secret(expected)
        if not re.fullmatch(r'\d+:[A-Za-z0-9_-]+', token) or len(token) < 30:
            retry('Неверный формат токена. Попробуй ещё раз'); return
        if token == expected:
            retry('Этот токен уже используется этим ботом'); return
        if db.db_token_in_use(token, session['bot_id']):
            retry('Этот бот уже подключён'); return
        probe = None
        try:
            probe = telebot.TeleBot(token, threaded=False)
            me = probe.get_me()
        except Exception:
            retry('Токен недействителен. Проверь его в @BotFather'); return
        finally:
            if probe is not None:
                try: probe.stop_bot()
                except Exception: pass
        with token_flow_lock:
            current = state.get(uid)
            if not isinstance(current, dict) or current.get('request_id') != session['request_id']: return
            if session['expires'] <= time.time():
                _reset_flow(uid)
                bot.send_message(m.chat.id, 'Время ожидания истекло. Нажми «Сменить токен» ещё раз')
                return
            if str(me.id) != expected.split(':',1)[0]:
                token_candidates[uid] = dict(session, token=token, expected=expected, me=me)
                state[uid] = dict(session, step='confirm_bot_token')
                old_name = next((r[2] for r in db_get_owner_bots(uid) if r[0] == session['bot_id']), '')
                old_name = '@'+old_name if old_name else 'прежнего бота'
                kb=InlineKeyboardMarkup()
                kb.row(_btn('Сменить', icon='✔️', callback_data=f"token_confirm_{session['bot_id']}_{session['request_id']}"),
                       _btn('Отмена', icon='❌', callback_data='token_cancel'))
                bot.send_message(m.chat.id, f'Это другой бот (@{me.username}). Пользователи {old_name} не перейдут в него, группу поддержки нужно будет подключить заново', reply_markup=kb)
                return
            _reset_flow(uid)
        result = _replace_owned_token(uid, session['bot_id'], expected, token, me, bot)
        bot.send_message(m.chat.id, result, parse_mode='HTML', reply_markup=back_to_start_kb())

    @bot.callback_query_handler(func=lambda c: c.data.startswith('token_confirm_'))
    def confirm_token_change(cb):
        with token_flow_lock:
            candidate = token_candidates.get(cb.from_user.id)
            session = state.get(cb.from_user.id)
            valid = (candidate and isinstance(session,dict) and session.get('step')=='confirm_bot_token'
                     and candidate['request_id']==session.get('request_id')
                     and cb.data==f"token_confirm_{candidate['bot_id']}_{candidate['request_id']}"
                     and candidate['expires']>time.time())
            if not valid:
                if ((candidate and candidate['expires']<=time.time()) or
                    (isinstance(session,dict) and session.get('step')=='confirm_bot_token'
                     and (session.get('expires',0)<=time.time() or not candidate))):
                    _reset_flow(cb.from_user.id)
                bot.answer_callback_query(cb.id, 'Подтверждение устарело. Нажми «Сменить токен» ещё раз', show_alert=True)
                return
            _reset_flow(cb.from_user.id)
        result = _replace_owned_token(cb.from_user.id, candidate['bot_id'], candidate['expected'],
                                      candidate['token'], candidate['me'], bot)
        _edit(result, cb.message.chat.id, cb.message.message_id, reply_markup=back_to_start_kb())

    @staticmethod
    def _cap_lines(header, rows, limit=30):
        """Собирает текст списка с ограничением: сообщения Telegram ≤4096 символов,
        а списки (боты/выводы/блокировки) растут месяцами — длинный текст начнёт
        падать с 'message is too long'. Возвращает готовый текст с припиской."""
        rows = list(rows)
        shown = rows[:limit]
        text = header + '\n' + '\n'.join(shown)
        if len(rows) > limit:
            text += f"\n<i>… показаны первые {limit} из {len(rows)}</i>"
        return text

    def _after_payment_confirmed(uid, chat_id, message_id, renew_days=30,
                                 context=None):
        # Начисления уже сохранены транзакцией; здесь только запуск и ответ
        receipt = context.get('receipt', {}) if isinstance(context, dict) else {}
        referrer = receipt.get('referrer')
        if referrer:
            _safe_send(referrer,
                f"{TE_PARTY} <b>Твой реферал оплатил подписку</b>\n\n"
                f"Накоплено скидок: <b>{db_get_discount_count(referrer)}</b>\n"
                "Каждая скидка даёт 10 дней — используй её через «Купить бота»")
        if receipt.get('commission_owner'):
            _safe_send(receipt['commission_owner'],
                f"{TE_MONEY} <b>Новая комиссия</b>\n\n"
                f"Начислено: <b>{receipt['commission']} USDT</b> (50%)\n"
                "Баланс — в /admin своего бота → Мой баланс")
        target, _ = purchase_context(context if context is not None else state.get(uid))
        if target:
            _reset_flow(uid)
            started = _resume_bot(target)
            from database import db_get_bot_expires
            expires = db_get_bot_expires(target)
            text = (f"{TE_CHECK} <b>Подписка продлена на {renew_days} дней</b>\n\n"
                    f"Действует до: <b>{str(expires)[:10]}</b>")
            if not started:
                text += "\n\nНе удалось запустить бота — попробуй позже или напиши в поддержку"
            _edit(text, chat_id, message_id, reply_markup=close_kb())
        else:
            state[uid] = 'await_bot_token'
            _edit(f"{TE_CHECK} <b>Оплата получена</b>\n\n"
                  "Введи токен бота из @BotFather", chat_id, message_id,
                  reply_markup=cancel_kb())

    @bot.message_handler(commands=['start'])
    def cmd_start(m):
        uid = m.from_user.id
        _reset_flow(uid)
        log.info(f'/start from uid={uid}')
        db_add_user(0, uid)
        payload = m.text.strip().split(' ', 1)[1] if ' ' in m.text else ''

        if payload.startswith('ref_'):
            try:
                referrer_id = bounded_number(payload[4:])
                if referrer_id != uid:
                    db_set_referral(referrer_id, uid)
            except (ValueError, Exception):
                pass

        elif payload.startswith('via_'):
            try:
                via_bot_id = bounded_number(payload[4:])
                cur = state.get(uid)
                if isinstance(cur, dict):
                    cur['via_bot_id'] = via_bot_id
                    state[uid] = cur
                else:
                    state[uid] = {'via_bot_id': via_bot_id}
            except (ValueError, Exception):
                pass

        bot.send_message(m.chat.id, start_text(),
            parse_mode='HTML', reply_markup=start_kb())

    # ── /admin ───────────────────────────────────────────
    @bot.message_handler(commands=['admin'])
    def cmd_admin(m):
        if not _is_super_admin(m): return
        bot.send_message(m.chat.id, f"<b>{TE_KEYBOARD} Панель супер-админа</b>",
            parse_mode='HTML', reply_markup=super_admin_kb())

    # ── /backup ──────────────────────────────────────────
    @bot.message_handler(commands=['backup'])
    def cmd_backup(m):
        if not _is_private_super_admin(m): return
        try:
            import tempfile
            with tempfile.TemporaryDirectory(dir=db.DATA_DIR, prefix='download_') as backup_dir:
                snapshot = db.db_backup(backup_dir)
                self_send_backup(m.chat.id, snapshot)
        except Exception as e:
            log.error('backup error: %s', e)
            bot.send_message(m.chat.id, f'{TE_CROSS} Не получилось скачать копию, попробуй позже', parse_mode='HTML')

    def self_send_backup(chat_id, snapshot):
        with open(snapshot, 'rb') as f:
            bot.send_document(chat_id, f,
                caption=f"{TE_FOLDER} Резервная копия\n{TE_CALENDAR} {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                parse_mode='HTML')

    @bot.message_handler(commands=['restore_db'])
    def cmd_restore_db(m):
        if not _is_private_super_admin(m): return
        state[m.from_user.id] = 'await_db_file'
        bot.send_message(m.chat.id,
            f"<b>{TE_FOLDER} Восстановление базы данных</b>\n\n"
            "Отправь файл <code>spambots.db</code> как документ.\n"
            f"{TE_EXCLAM} Текущая база будет заменена!",
            parse_mode='HTML', reply_markup=cancel_kb())

    @bot.message_handler(content_types=['document'],
                         func=lambda m: state.get(m.from_user.id) == 'await_db_file')
    def handle_db_restore(m):
        if not _is_private_super_admin(m): return
        state.pop(m.from_user.id, None)
        doc = m.document
        # file_name у документа опционален (Telegram) — None сломал бы .endswith
        if not (doc.file_name or '').endswith('.db'):
            bot.send_message(m.chat.id, f"<b>{TE_CROSS}  Нужен файл с расширением .db</b>", parse_mode='HTML')
            return
        try:
            file_info = bot.get_file(doc.file_id)
            downloaded = bot.download_file(file_info.file_path)
            # валидация: загруженный файл должен быть корректной SQLite-БД
            tmp_path = DB_PATH + '.restore_tmp'
            with open(tmp_path, 'wb') as f:
                f.write(downloaded)
            import sqlite3 as _sq
            _chk = _sq.connect(tmp_path)
            try:
                row = _chk.execute('PRAGMA integrity_check').fetchone()[0]
                if row != 'ok':
                    raise ValueError(f'integrity_check: {row}')
                _chk.execute('SELECT COUNT(*) FROM bots')
            finally:
                _chk.close()
            db.validate_restore(tmp_path)
            _restore_database(tmp_path, bot)
            bot.send_message(m.chat.id,
                f"<b>{TE_CHECK} База данных восстановлена</b>",
                parse_mode='HTML', reply_markup=close_kb())
            log.info(f'DB restored from upload by SUPER_ADMIN, size={len(downloaded)} bytes')
        except Exception as e:
            log.error(f'DB restore error: {e}')
            bot.send_message(m.chat.id, f"<b>{TE_CROSS} Не удалось восстановить базу данных</b>", parse_mode='HTML')

    # ── Backup / Restore через кнопки ────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'backup_db')
    def backup_db_cb(cb):
        if not _is_private_super_admin(cb): return
        try:
            import tempfile
            with tempfile.TemporaryDirectory(dir=db.DATA_DIR, prefix='download_') as backup_dir:
                self_send_backup(cb.message.chat.id, db.db_backup(backup_dir))
        except Exception as e:
            bot.answer_callback_query(cb.id, 'Не получилось скачать копию, попробуй позже', show_alert=True)
            return
        bot.answer_callback_query(cb.id)

    @bot.callback_query_handler(func=lambda c: c.data == 'restore_db_btn')
    def restore_db_btn_cb(cb):
        if not _is_private_super_admin(cb): return
        state[cb.from_user.id] = 'await_db_file'
        bot.answer_callback_query(cb.id)
        bot.send_message(cb.message.chat.id,
            f"<b>{TE_FOLDER} Восстановление базы данных</b>\n\n"
            "Отправь файл <code>spambots.db</code> как документ.\n"
            f"{TE_EXCLAM} Текущая база будет заменена!",
            parse_mode='HTML', reply_markup=cancel_kb())

    # ── Навигация ────────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'back_to_start')
    def back_to_start_cb(cb):
        _reset_flow(cb.from_user.id)
        _edit(start_text(),
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=start_kb())

    @bot.callback_query_handler(func=lambda c: c.data == 'back_to_payment')
    def back_to_payment_cb(cb):
        uid = cb.from_user.id
        _reset_flow(uid, preserve_purchase=True)
        disc = db_get_discount_count(uid)
        show_trial = not db_has_used_trial(uid)
        _edit(buy_text(),
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=payment_kb(disc, show_trial))

    # ── Отправить сообщение ──────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'user_send')
    def user_send(cb):
        _edit(f"<b>{TE_CHAT}  Введи своё сообщение:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=back_to_start_kb())
        state[cb.from_user.id] = 'await_user_msg'

    @bot.callback_query_handler(func=lambda c: c.data.startswith('admin_reply_'))
    def admin_reply(cb):
        if not _is_super_admin(cb):
            return
        target = int(cb.data.split('_')[-1])
        _edit(f"<b>{TE_CHAT}  Введи ответ:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=cancel_kb())
        state[cb.from_user.id] = f'await_admin_msg_{target}'

    @bot.callback_query_handler(func=lambda c: c.data == 'cancel')
    def cancel_cb(cb):
        # состояние сбрасываем ДО удаления: сообщение может быть старше 48 ч
        # (удаление упадёт) — юзер не должен застревать в состоянии
        _reset_flow(cb.from_user.id)
        try:
            bot.delete_message(cb.message.chat.id, cb.message.message_id)
        except Exception:
            pass

    @bot.callback_query_handler(func=lambda c: c.data == 'close')
    def close_cb(cb):
        try:
            bot.delete_message(cb.message.chat.id, cb.message.message_id)
        except Exception:
            pass

    @bot.message_handler(func=lambda m: state.get(m.from_user.id) == 'await_user_msg')
    def user_text(m):
        if _is_main_rate_limited(m.from_user.id):
            bot.send_message(m.chat.id,
                f"<b>{TE_HOURGLASS} Подожди {MAIN_RATE_SEC} секунд перед следующим сообщением</b>",
                parse_mode='HTML')
            return
        if state.take(m.from_user.id, lambda v: v == 'await_user_msg') is None: return
        username = f"@{m.from_user.username}" if m.from_user.username else "нет"
        header = (f"<b>{TE_INBOX} Сообщение от:</b>\n"
                  f"{TE_USER} Имя: <b>{html.escape(m.from_user.full_name)}</b>\n"
                  f"{TE_LINK} Username: {html.escape(username)}\n"
                  f"{TE_ID} ID: <code>{m.from_user.id}</code>")
        body = html.escape(m.text or '')
        # длинные обращения: заголовок отдельно + тело кусками.
        # html_safe_chunks режет УЖЕ ЭКРАНИРОВАННЫЙ текст: сырой разрез рвёт
        # сущности (&amp; → "&am"+"p;" → 400), а наивный разрез экранированного
        # мог отдать кусок >4096 (3800 символов '<' → ~19000 экранированных)
        if len(header) + len(body) + 4 > 4000:
            try:
                bot.send_message(SUPER_ADMIN, header, parse_mode='HTML',
                                 reply_markup=reply_kb(m.from_user.id))
                for chunk in html_safe_chunks(body):
                    bot.send_message(SUPER_ADMIN, chunk, parse_mode='HTML')
            except Exception as e:
                log.warning(f'user_text delivery to SUPER_ADMIN failed: {e}')
                bot.send_message(m.chat.id,
                    f"<b>{TE_CROSS}  Не удалось доставить сообщение. Попробуй позже.</b>",
                    parse_mode='HTML', reply_markup=close_kb())
                return
        else:
            try:
                bot.send_message(SUPER_ADMIN, header + '\n\n' + f"{TE_CHAT} {body}",
                                 parse_mode='HTML', reply_markup=reply_kb(m.from_user.id))
            except Exception as e:
                log.warning(f'user_text delivery to SUPER_ADMIN failed: {e}')
                bot.send_message(m.chat.id,
                    f"<b>{TE_CROSS}  Не удалось доставить сообщение. Попробуй позже.</b>",
                    parse_mode='HTML', reply_markup=close_kb())
                return
        bot.send_message(m.chat.id, f"<b>{TE_CHECK}  Сообщение отправлено</b>",
            parse_mode='HTML', reply_markup=close_kb())

    @bot.message_handler(func=lambda m: isinstance(state.get(m.from_user.id), str)
                                        and state[m.from_user.id].startswith('await_admin_msg_'))
    def admin_text(m):
        if not _is_super_admin(m):
            _reset_flow(m.from_user.id)
            return
        captured = state.take(m.from_user.id, lambda v: isinstance(v, str) and v.startswith('await_admin_msg_'))
        if captured is None: return
        target_id = int(captured.split('_')[-1])
        try:
            send_long(bot, target_id, f"<b>{TE_INBOX} Сообщение от администратора</b>", m.text)
        except Exception as e:
            # адресат недоступен (заблокировал бота / удалил аккаунт): не молчим —
            # сообщаем и снимаем состояние, иначе админ застрянет в режиме ответа
            log.warning(f'admin_text delivery to {target_id} failed: {e}')
            bot.send_message(m.chat.id,
                f"<b>{TE_CROSS}  Не удалось доставить ответ: пользователь недоступен "
                f"(заблокировал бота или удалил аккаунт).</b>",
                parse_mode='HTML', reply_markup=close_kb())
            return
        bot.send_message(m.chat.id, f"<b>{TE_CHECK}  Ответ отправлен</b>",
            parse_mode='HTML', reply_markup=close_kb())

    # ── Купить / продлить бота ────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'buy_bot')
    def buy_bot_cb(cb):
        uid = cb.from_user.id
        _reset_flow(uid)

        # Проверяем бесплатный купон от суперадмина
        coupon_days = db_claim_coupon(uid)
        if coupon_days:
            state[uid] = {'step': 'await_bot_token_free', 'days': coupon_days}
            _edit(
                f"{TE_GIFT} <b>У тебя есть бесплатный купон на {coupon_days} дней!</b>\n\n"
                f"{TE_ROBOT} Введи токен своего бота (получи в @BotFather):",
                cb.message.chat.id, cb.message.message_id,
                parse_mode='HTML', reply_markup=cancel_kb())
            return

        # Уже оплатил — продолжаем настройку
        if db_is_paid(uid):
            state[uid] = 'await_bot_token'
            _edit(
                f"{TE_CHECK} <b>Оплата уже получена!\n\n"
                f"{TE_ROBOT} Введи токен своего бота (получи в @BotFather):</b>",
                cb.message.chat.id, cb.message.message_id,
                parse_mode='HTML', reply_markup=cancel_kb())
            return

        # Уже есть боты — показываем продление + кнопку нового
        existing = db_get_owner_bots(uid)
        if existing:
            lines = ["<b>У тебя уже есть бот(ы):</b>\n"]
            kb    = InlineKeyboardMarkup()
            for bot_id, token, username, created_at, expires_at in existing:
                if token not in running_bots and expires_at:
                    try:
                        if datetime.fromisoformat(str(expires_at)) > datetime.now():
                            _resume_bot(bot_id)
                    except (ValueError, TypeError):
                        pass
                is_running = token in running_bots
                status     = f'{TE_GREEN}' if is_running else f'{TE_RED}'
                name       = f"@{username}" if username else f"Bot #{bot_id}"
                if expires_at:
                    try:
                        exp       = datetime.fromisoformat(str(expires_at))
                        days_left = subscription_days_left(exp)
                        exp_str = f"осталось {days_left} дн" if days_left > 0 else f"{TE_CROSS} истёк"
                    except Exception:
                        exp_str = "—"
                else:
                    exp_str = "—"
                lines.append(f"{status} {name} — {exp_str}")
                kb.add(_btn(f"Продлить {name} (+30д.)", icon='🔄', style='success', callback_data=f'renew_bot_{bot_id}'))
                kb.add(_btn('Сменить токен', icon='🔄', callback_data=f'change_token_{bot_id}'))
            kb.add(_btn('Купить новый бот', icon='🛒', style='primary', callback_data='buy_new_bot'))
            kb.add(_btn('Назад', icon='🔙', callback_data='back_to_start'))
            _edit("\n".join(lines),
                cb.message.chat.id, cb.message.message_id,
                parse_mode='HTML', reply_markup=kb)
            return

        # Нет ботов — обычная покупка
        disc = db_get_discount_count(uid)
        show_trial = not db_has_used_trial(uid)
        _edit(buy_text(),
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=payment_kb(disc, show_trial))

    @bot.callback_query_handler(func=lambda c: c.data == 'buy_new_bot')
    def buy_new_bot_cb(cb):
        uid = cb.from_user.id
        _reset_flow(uid)
        disc = db_get_discount_count(uid)
        show_trial = not db_has_used_trial(uid)
        _edit(buy_text(),
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=payment_kb(disc, show_trial))

    @bot.callback_query_handler(func=lambda c: c.data.startswith('renew_bot_'))
    def renew_bot_cb(cb):
        uid = cb.from_user.id
        bot_id = int(cb.data.split('_')[-1])
        # продлить можно только свой бот (владелец или админ)
        _binfo = db_get_bot_info(bot_id)
        if not _binfo or uid not in (_binfo[2], _binfo[1]):
            bot.answer_callback_query(cb.id, "❌ Это не твой бот", show_alert=True)
            return
        cur = state.get(uid)
        new_state = {'step': 'renewing', 'bot_id': bot_id}
        if isinstance(cur, dict) and 'via_bot_id' in cur:
            new_state['via_bot_id'] = cur['via_bot_id']
        state[uid] = new_state
        disc = db_get_discount_count(uid)
        _edit(
            f"<b>{TE_REFRESH} Продление подписки на 30 дней</b>\n\n"
            f"Сумма: <b>{get_price()} USDT</b>\n\n"
            f"Выбери способ оплаты:",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=payment_kb(disc))

    # ── Реферальная ссылка ───────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'get_ref_link')
    def get_ref_link_cb(cb):
        uid = cb.from_user.id
        disc = db_get_discount_count(uid)
        username = _get_bot_username()
        ref_link = f"https://t.me/{username}?start=ref_{uid}" if username else f"start=ref_{uid}"
        bot.send_message(uid,
            f"{TE_LINK} <b>Твоя реферальная ссылка:</b>\n"
            f"<code>{ref_link}</code>\n\n"
            f"За каждого приведённого клиента, который оплатит подписку, "
            f"ты получишь 1 бесплатное продление на 10 дней.\n\n"
            f"{TE_DIAMOND} Накоплено скидок: <b>{disc}</b>",
            parse_mode='HTML', reply_markup=close_kb())
        bot.answer_callback_query(cb.id)

    # ── Пробный период ───────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'try_free')
    def try_free_cb(cb):
        uid = cb.from_user.id
        if db_is_paid(uid):
            bot.answer_callback_query(cb.id, "Сначала заверши настройку оплаченного бота", show_alert=True)
            return
        if not db_activate_trial(uid):
            bot.answer_callback_query(cb.id,
                "Пробный период уже использован", show_alert=True)
            return
        state[uid] = 'await_bot_token_trial'
        _edit(
            f"{TE_GIFT} <b>Бесплатный пробный период (3 дня) активирован!\n\n"
            f"{TE_ROBOT} Введи токен своего бота (получи в @BotFather):</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=cancel_kb())

    # ── Использовать скидку ──────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'use_discount')
    def use_discount_cb(cb):
        uid = cb.from_user.id
        context = state.get(uid)
        if not db_use_purchase_discount(uid, context):
            bot.answer_callback_query(cb.id, "Нет доступной скидки или настройка уже оплачена", show_alert=True)
            return
        _after_payment_confirmed(uid, cb.message.chat.id, cb.message.message_id,
                                 renew_days=10, context=context)

    @bot.callback_query_handler(func=lambda c: c.data == 'pay_cryptobot')
    def pay_cryptobot_cb(cb):
        context = state.get(cb.from_user.id)
        invoice = cb_create_invoice(cb.from_user.id)
        if not invoice:
            bot.answer_callback_query(cb.id, "Не удалось создать счёт — попробуй ещё раз", show_alert=True)
            return
        db_set_pending(cb.from_user.id, invoice['invoice_id'], context=context,
                       amount=invoice.get('amount'), expires_at=time.time() + 3600)
        _edit(
            f"<b>{TE_CARD} Оплата через CryptoBot</b>\n\n"
            f"Сумма: <b>{invoice.get('amount', get_price())} USDT</b>\n\n"
            f"Нажми кнопку ниже, оплати и вернись сюда — нажми «Проверить оплату».",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=cryptobot_kb(invoice['pay_url'], invoice['invoice_id']))

    @bot.callback_query_handler(func=lambda c: c.data.startswith('check_payment_'))
    def check_payment_cb(cb):
        invoice_id = int(cb.data.split('_')[-1])
        uid = cb.from_user.id
        order = db_get_payment(uid, invoice_id)
        if not order:
            bot.answer_callback_query(cb.id, "Счёт не найден", show_alert=True)
            return
        if order['settled']:
            bot.answer_callback_query(cb.id, "Оплата уже зачтена", show_alert=True)
            return
        invoice = cb_check_invoice(invoice_id)
        if invoice and invoice.get('status') == 'paid':
            amount = invoice.get('amount') or order['amount']
            if amount is None:
                bot.answer_callback_query(cb.id, "Не удалось проверить сумму оплаты — попробуй позже", show_alert=True)
                return
            if db_claim_pending_payment(uid, invoice_id, paid_amount=amount):
                context = dict(db_get_payment(uid, invoice_id), step='renewing' if order['bot_id'] else 'new')
                _after_payment_confirmed(uid, cb.message.chat.id, cb.message.message_id, context=context)
            else:
                bot.answer_callback_query(cb.id, "Оплата уже зачтена", show_alert=True)
        else:
            bot.answer_callback_query(cb.id, "Оплата не найдена — попробуй ещё раз", show_alert=True)

    @bot.callback_query_handler(func=lambda c: c.data == 'pay_ton')
    def pay_ton_cb(cb):
        cur = state.get(cb.from_user.id)
        quoted_price = get_price()
        bot.answer_callback_query(cb.id, "⏳ Получаю курс TON...")
        ton_amount = get_ton_amount(quoted_price)
        if ton_amount <= 0:
            bot.answer_callback_query(cb.id, "❌ Не удалось получить курс TON.", show_alert=True)
            return
        payment_code = f"SPB{cb.from_user.id}_{uuid.uuid4().hex[:16]}"
        link = ton_payment_link(ton_amount, payment_code)
        db_set_pending(cb.from_user.id, payment_code, context=cur, amount=quoted_price,
                       expires_at=time.time() + 7200, provider='ton', expected_ton=ton_amount)
        new_state = {'step': 'ton_pending', 'ton': ton_amount, 'code': payment_code}
        target, via = purchase_context(cur)
        if target:
            new_state.update(bot_id=target, renewing=True)
        if via:
            new_state['via_bot_id'] = via
        state[cb.from_user.id] = new_state
        kb = InlineKeyboardMarkup()
        kb.add(_btn('Открыть TON Keeper и оплатить', icon='💎', style='primary', url=link))
        kb.add(_btn('Я оплатил — проверить', icon='✔️', style='success', callback_data=f'ton_check_{payment_code}'))
        kb.add(_btn('Назад', icon='🔙', callback_data='back_to_payment'))
        _edit(
            f"<b>{TE_DIAMOND} Оплата через TON Keeper</b>\n\n"
            f"Сумма: <b>{ton_amount} TON</b> (~${get_price()})\n\n"
            f"Нажми кнопку ниже — TON Keeper откроется с уже заполненной суммой, адресом и комментарием.\n\n"
            f"Адрес (вручную):\n<code>{TON_WALLET}</code>\n\n"
            f"{TE_EXCLAM} <b>ВАЖНО: при отправке обязательно укажи комментарий:</b>\n"
            f"<code>{payment_code}</code>\n"
            f"<i>(без комментария оплата не будет засчитана)</i>\n\n"
            f"После оплаты нажми «Я оплатил» — бот проверит автоматически.\n"
            f"Проверка работает в течение <b>2 часов</b>.",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith('ton_check_'))
    def ton_check_auto_cb(cb):
        uid = cb.from_user.id
        current = state.get(uid)
        if cb.data == 'ton_check_auto':
            payment_code = current.get('code', '') if isinstance(current, dict) else ''
        else:
            payment_code = cb.data[len('ton_check_'):]
        order = db_get_payment(uid, payment_code, provider='ton')
        # Совместимость с кнопками TON, созданными до миграции
        if not order and isinstance(current, dict) and current.get('code') == payment_code and 'ton' in current:
            c = db._conn()
            try: row = c.execute('SELECT updated_at FROM states WHERE bot_id=0 AND user_id=?', (uid,)).fetchone()
            finally: c.close()
            try:
                from datetime import timezone
                deadline = datetime.fromisoformat(str(row[0])).replace(tzinfo=timezone.utc).timestamp() + 7200
            except (TypeError, ValueError):
                bot.answer_callback_query(cb.id, 'Счёт истёк, создай новый', show_alert=True)
                return
            db_set_pending(uid, payment_code, context=current, amount=get_price(),
                           provider='ton', expected_ton=current['ton'], expires_at=deadline)
            order = db_get_payment(uid, payment_code, provider='ton')
        if not order:
            bot.answer_callback_query(cb.id, "Платёж не найден", show_alert=True)
            return
        if order['settled']:
            bot.answer_callback_query(cb.id, "Оплата уже зачтена", show_alert=True)
            return
        ton_amount = order['ton']
        bot.answer_callback_query(cb.id, "Проверяю оплату")
        found = ton_check_transfer(uid, ton_amount, payment_code, return_tx=True, expires_at=order['expires_at'])
        if found:
            if claim_ton_transfer(uid, payment_code, found):
                context = dict(db_get_payment(uid, payment_code, provider='ton'),
                               step='renewing' if order['bot_id'] else 'new')
                _after_payment_confirmed(uid, cb.message.chat.id, cb.message.message_id, context=context)
            else:
                settled = db_get_payment(uid, payment_code, provider='ton')['settled']
                bot.answer_callback_query(cb.id, 'Оплата уже зачтена' if settled else 'Счёт истёк, создай новый', show_alert=True)
        else:
            link = ton_payment_link(ton_amount, payment_code)
            kb   = InlineKeyboardMarkup()
            kb.add(_btn('Открыть TON Keeper', icon='💎', style='primary', url=link))
            kb.add(_btn('Проверить снова', icon='🔄', style='primary', callback_data=f'ton_check_{payment_code}'))
            kb.add(_btn('Назад', icon='🔙', callback_data='back_to_payment'))
            _edit(
                f"<b>{TE_CROSS}  Оплата не найдена</b>\n\n"
                f"Убедись что:\n"
                f"• Отправил <b>{ton_amount} TON</b>\n"
                f"• Указал комментарий: <code>{payment_code}</code>\n"
                f"• Транзакция прошла подтверждение\n"
                f"• Оплата не старше 2 часов\n\n"
                f"Попробуй ещё раз через минуту. Если не получается — напиши прямо сюда, админ увидит.",
                cb.message.chat.id, cb.message.message_id,
                parse_mode='HTML', reply_markup=kb)

    # ── Настройка бота после оплаты ──────────────────────
    @bot.message_handler(func=lambda m: state.get(m.from_user.id) in ('await_bot_token', 'await_bot_token_trial')
                                        or (isinstance(state.get(m.from_user.id), dict)
                                            and state[m.from_user.id].get('step') == 'await_bot_token_free'))
    def get_bot_token(m):
        if m.chat.type != 'private': return
        if not db_is_paid(m.from_user.id):
            _reset_flow(m.from_user.id)
            bot.send_message(m.chat.id, "Сначала оплати настройку бота")
            return
        s, revision = state.snapshot(m.from_user.id)
        is_trial = (s == 'await_bot_token_trial')
        is_free_coupon = isinstance(s, dict) and s.get('step') == 'await_bot_token_free'
        free_days = s.get('days', 30) if is_free_coupon else None

        token_val = m.text.strip()
        if ':' not in token_val or len(token_val) < 30:
            bot.send_message(m.chat.id, f"<b>{TE_CROSS}  Неверный формат токена. Попробуй ещё раз:</b>",
                parse_mode='HTML')
            return
        register_secret(token_val)
        try:
            me = telebot.TeleBot(token_val, threaded=False).get_me()
        except Exception:
            bot.send_message(m.chat.id, f"<b>{TE_CROSS}  Токен недействителен. Проверь и введи правильный:</b>",
                parse_mode='HTML')
            return
        if not state.transition(m.from_user.id, revision, {'step': 'await_admin_id', 'token': token_val,
                                  'username': me.username or '',
                                  'trial': is_trial,
                                  'free_days': free_days}): return
        bot.send_message(m.chat.id,
            f"<b>{TE_USER} Теперь введи свой Telegram ID\n"
            "(сообщения от пользователей будут приходить именно тебе)\n\n"
            "Узнать свой ID: @userinfobot</b>",
            parse_mode='HTML', reply_markup=cancel_kb())

    @bot.message_handler(func=lambda m: isinstance(state.get(m.from_user.id), dict)
                                        and state[m.from_user.id].get('step') == 'await_admin_id')
    def get_admin_id(m):
        if m.chat.type != 'private': return
        s, revision = state.snapshot(m.from_user.id)
        if not isinstance(s, dict) or s.get('step') != 'await_admin_id': return
        try:
            admin_id_val = bounded_number(m.text)
        except ValueError:
            bot.send_message(m.chat.id, 'Неверный ID')
            return
        token_val = s['token']
        uname     = s.get('username', '')
        if not db_is_paid(m.from_user.id):
            bot.send_message(m.chat.id, "Сначала оплати настройку бота")
            return
        try:
            bot_db_id = db_register_paid_bot(m.from_user.id, token_val, admin_id_val, revision=revision)
            if bot_db_id is None: return
        except sqlite3.IntegrityError:
            # именно занятый токен (UNIQUE): возвращаем юзера к вводу, грант не тронут
            if s.get('free_days'):
                retry_state = {'step': 'await_bot_token_free', 'days': s['free_days']}
            elif s.get('trial'):
                retry_state = 'await_bot_token_trial'
            else:
                retry_state = 'await_bot_token'
            if not state.transition(m.from_user.id, revision, retry_state): return
            bot.send_message(m.chat.id,
                f"<b>{TE_CROSS} Этот бот уже подключён, введи токен другого бота</b>", parse_mode='HTML')
            return
        except Exception as e:
            # другая причина (сбой БД и т.п.) — не вводим юзера в заблуждение;
            # возвращаем состояние await_admin_id, чтобы ID можно было отправить снова
            log.warning(f'db_add_bot failed for {m.from_user.id}: {e}')
            if not state.transition(m.from_user.id, revision, s): return
            bot.send_message(m.chat.id,
                f"<b>{TE_CROSS} Не удалось подключить бота — отправь ID ещё раз через минуту</b>",
                parse_mode='HTML')
            return
        if launch_bot(bot_db_id, token_val, admin_id_val, bot):
            kb = InlineKeyboardMarkup()
            if uname:
                kb.add(_btn(f'Перейти к @{uname}', icon='🔗', style='success', url=f'https://t.me/{uname}'))
            kb.add(_btn('Закрыть', icon='❌', style='danger', callback_data='close'))
            bot.send_message(m.chat.id,
                f"{TE_ROCKET} <b>Бот успешно запущен!\n\n"
                f"Управляй через /admin в своём боте.\n"
                f"Статистика: /status в своём боте.</b>",
                parse_mode='HTML', reply_markup=kb)
            bot.send_message(SUPER_ADMIN,
                f"<b>{TE_NEW} Новый бот подключён</b>\n"
                f"Owner: <code>{m.from_user.id}</code> | Admin: <code>{admin_id_val}</code> | @{uname}",
                parse_mode='HTML')
        else:
            bot.send_message(m.chat.id,
                f"<b>{TE_CROSS} Не удалось запустить бота — открой «Купить бота» для повтора или напиши в поддержку</b>",
                parse_mode='HTML')

    # ── Рассылка ─────────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'broadcast')
    def broadcast_start(cb):
        if not _is_super_admin(cb): return
        state[cb.from_user.id] = 'bcast_any_mine'
        _edit(f'{TE_ANNOUNCE} Отправь сообщение или альбом для рассылки',
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=cancel_kb())

    @bot.callback_query_handler(func=lambda c: c.data == 'broadcast_all')
    def broadcast_all_start(cb):
        if not _is_super_admin(cb): return
        state[cb.from_user.id] = 'bcast_any_all'
        _edit(f'{TE_ANNOUNCE} Отправь сообщение или альбом для рассылки',
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=cancel_kb())

    @bot.callback_query_handler(func=lambda c: c.data.startswith(('svc_send_', 'svc_cancel_')))
    def service_confirm(cb):
        if not _is_super_admin(cb): return
        import service_broadcast as svc
        try: job_id = int(cb.data.rsplit('_', 1)[1])
        except ValueError: return
        cancel = cb.data.startswith('svc_cancel_')
        with svc._lock:
            changed = svc.confirm(bot, job_id, cb.from_user.id, cancel=cancel)
            current = svc.job(job_id)
            pending = current and current['status'] == 'draft'
            if changed:
                state.take(cb.from_user.id, lambda v: isinstance(v, dict) and v.get('job_id') == job_id)
        bot.answer_callback_query(cb.id, 'Отменено' if cancel and changed else 'Рассылка запущена'
                                  if changed else 'Подожди предпросмотр' if pending else 'Уже обработано')

    def is_service_input(m):
        if not _is_super_admin(m): return False
        value = state.get(m.from_user.id)
        return isinstance(value, str) and value.startswith('bcast_') or (
            isinstance(value, dict) and value.get('step') == 'service_broadcast'
            and m.media_group_id and value.get('album') == m.media_group_id)

    @bot.message_handler(content_types=['text','photo','video','animation','document','audio','voice',
                         'video_note','sticker','contact','location','venue','poll'], func=is_service_input)
    def service_input(m):
        import service_broadcast as svc
        # Сериализация элементов альбома включает чтение и смену состояния
        with svc._lock:
            value = state.get(m.from_user.id)
            scope = value.rsplit('_', 1)[1] if isinstance(value, str) else value['scope']
            previous = value.get('job_id') if isinstance(value, dict) else None
            jid = svc.append(bot, m.from_user.id, m.chat.id, scope, m, previous)
            state[m.from_user.id] = dict(step='service_broadcast', job_id=jid, scope=scope, album=m.media_group_id)
        if m.media_group_id:
            svc.schedule_preview(bot, jid)
        else:
            try: svc.preview(bot, jid)
            except Exception as exc:
                log.warning('service preview failed: %s', exc)
                svc.fail_preview(bot, jid)
                state[m.from_user.id] = 'bcast_any_' + scope

    @bot.callback_query_handler(func=lambda c: c.data.startswith('bcast_') and not c.data.endswith('_p'))
    def broadcast_type(cb):
        if not _is_super_admin(cb): return
        parts = cb.data.split('_')
        btype, scope = parts[1], parts[2]
        if btype == 'text':
            _edit(f"<b>{TE_NOTE} Введи текст рассылки (HTML):</b>",
                cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
            state[cb.from_user.id] = f'bcast_text_{scope}'
        else:
            _edit(f"<b>{TE_PICTURE} Отправь фото с подписью (или без):</b>",
                cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
            state[cb.from_user.id] = f'bcast_photo_{scope}'

    @bot.callback_query_handler(func=lambda c: c.data == 'stats')
    def stats_cb(cb):
        if not _is_super_admin(cb): return
        bots_count, main_users, total_users, breakdown = db_get_stats()
        revenue = round(bots_count * get_price(), 2)
        lines = []
        for i, row in enumerate(breakdown[:20], 1):
            owner, _, users_cnt, created_at = row[0], row[1], row[2], row[3]
            expires_at = row[4] if len(row) > 4 else None
            date = str(created_at)[:10] if created_at else '—'
            exp  = str(expires_at)[:10] if expires_at else '—'
            lines.append(f"  {i}. Owner <code>{owner}</code> — {users_cnt} польз. | {TE_CALENDAR} {date} | {TE_HOURGLASS} {exp}")
        if len(breakdown) > 20:
            lines.append(f"  … и ещё {len(breakdown) - 20} ботов")
        text = (f"<b>{TE_STATS} Статистика SpamBot</b>\n{'─'*28}\n"
                f"{TE_ROBOT} Подключённых ботов: <b>{bots_count}</b>\n"
                f"{TE_MONEY} Выручка за всё время: <b>~{revenue} USDT</b>\n"
                f"{'─'*28}\n"
                f"{TE_USERS} Пользователей в главном боте: <b>{main_users}</b>\n"
                f"{TE_USERS} Всего пользователей (все боты): <b>{total_users}</b>\n"
                f"{'─'*28}\n"
                f"{TE_NOTE} Боты по кол-ву пользователей:\n"
                f"{chr(10).join(lines) if lines else '  —'}")
        _edit(text, cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=close_kb())

    # ── Управление ботами ────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'manage_bots')
    def manage_bots_cb(cb):
        if not _is_super_admin(cb): return
        _show_bots_list(cb.message.chat.id, cb.message.message_id)

    def _show_bots_list(chat_id, message_id=None):
        bots_list = db_get_active_bots_list()
        if not bots_list:
            text = f"<b>{TE_ROBOT} Нет активных ботов</b>"
            if message_id:
                _edit(text, chat_id, message_id, parse_mode='HTML', reply_markup=close_kb())
            else:
                bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=close_kb())
            return
        lines = []
        kb    = InlineKeyboardMarkup()
        for row in bots_list[:30]:
            bot_id, owner_id, admin_id_val = row[0], row[1], row[2]
            created_at = row[3]
            expires_at = row[4] if len(row) > 4 else None
            info  = db_get_bot_info(bot_id)
            token = info[0] if info else None
            is_running = token in running_bots if token else False
            status     = f'{TE_GREEN}' if is_running else f'{TE_RED}'
            users_cnt  = len(db_get_bot_users(bot_id))
            date       = str(created_at)[:10] if created_at else '—'
            exp        = str(expires_at)[:10] if expires_at else '—'
            lines.append(
                f"{status} <b>Bot #{bot_id}</b> | Owner: <code>{owner_id}</code> | "
                f"{users_cnt} польз. | {TE_CALENDAR} {date} | {TE_HOURGLASS} {exp}"
            )
            kb.row(
                _btn(f"⏹ #{bot_id}", style='danger',  callback_data=f'bot_stop_{bot_id}'),
                _btn(f"#{bot_id}", icon='🔄', style='primary', callback_data=f'bot_restart_{bot_id}'),
                _btn(f"#{bot_id}", icon='❌', style='danger',  callback_data=f'bot_delete_{bot_id}'),
            )
        kb.add(_btn('Закрыть', icon='❌', style='danger', callback_data='close'))
        if len(bots_list) > 30:
            lines.append(f"<i>… показаны первые 30 из {len(bots_list)} ботов</i>")
        text = f"<b>{TE_ROBOT} Управление ботами</b>\n\n" + "\n".join(lines)
        if message_id:
            _edit(text, chat_id, message_id, parse_mode='HTML', reply_markup=kb)
        else:
            bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith('bot_stop_'))
    def bot_stop_cb(cb):
        if not _is_super_admin(cb): return
        bot_id = int(cb.data.split('_')[-1])
        token  = running_bot_ids.get(bot_id)
        if token and stop_bot(token):
            bot.answer_callback_query(cb.id, f"✅ Bot #{bot_id} остановлен", show_alert=True)
        else:
            bot.answer_callback_query(cb.id, f"⚠️ Bot #{bot_id} уже остановлен", show_alert=True)
        _show_bots_list(cb.message.chat.id, cb.message.message_id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith('bot_restart_'))
    def bot_restart_cb(cb):
        if not _is_super_admin(cb): return
        bot_id = int(cb.data.split('_')[-1])
        info   = db_get_bot_info(bot_id)
        if not info:
            bot.answer_callback_query(cb.id, "❌ Бот не найден в БД", show_alert=True)
            return
        token, admin_id_val, _, _ = info
        if token in running_bots:
            stop_bot(token)
        if launch_bot(bot_id, token, admin_id_val, bot):
            bot.answer_callback_query(cb.id, f"✅ Bot #{bot_id} перезапущен", show_alert=True)
        else:
            bot.answer_callback_query(cb.id, f"❌ Ошибка перезапуска Bot #{bot_id}", show_alert=True)
        _show_bots_list(cb.message.chat.id, cb.message.message_id)

    # ── Удаление бота (супер-админ) ───────────────────────
    @bot.callback_query_handler(func=lambda c: c.data.startswith('bot_delete_yes_'))
    def bot_delete_yes_cb(cb):
        if not _is_super_admin(cb): return
        bot_id = int(cb.data.split('_')[-1])
        info = db_get_bot_info(bot_id)
        if not info:
            bot.answer_callback_query(cb.id, "❌ Бот не найден в БД", show_alert=True)
            return
        token, _, _, _ = info
        owner_id = info[2]
        if token in running_bots:
            stop_bot(token)
        db_deactivate_bot(bot_id)
        bot.answer_callback_query(cb.id, f"🗑 Bot #{bot_id} удалён", show_alert=True)
        try:
            bot.send_message(owner_id,
                f"{TE_BLOCK} <b>Твой бот был удалён администратором платформы.</b>\n\n"
                f"Если это ошибка — свяжись с поддержкой через этого бота.",
                parse_mode='HTML')
        except Exception:
            pass
        log.info(f'Bot #{bot_id} deleted by SUPER_ADMIN')
        _show_bots_list(cb.message.chat.id, cb.message.message_id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith('bot_delete_no_'))
    def bot_delete_no_cb(cb):
        if not _is_super_admin(cb): return
        _show_bots_list(cb.message.chat.id, cb.message.message_id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith('bot_delete_'))
    def bot_delete_cb(cb):
        if not _is_super_admin(cb): return
        bot_id = int(cb.data.split('_')[-1])
        info = db_get_bot_info(bot_id)
        if not info:
            bot.answer_callback_query(cb.id, "❌ Бот не найден в БД", show_alert=True)
            return
        kb = InlineKeyboardMarkup()
        kb.row(
            _btn(f"Да, удалить Bot #{bot_id}", icon='❌', style='danger',
                 callback_data=f'bot_delete_yes_{bot_id}'),
        )
        kb.add(_btn('Отмена', icon='❌', callback_data=f'bot_delete_no_{bot_id}'))
        _edit(
            f"{TE_BLOCK} <b>Удаление Bot #{bot_id}</b>\n\n"
            f"• Бот будет остановлен и отключён\n"
            f"• Владелец получит уведомление\n"
            f"• Данные пользователей останутся в базе\n\n"
            f"<i>Удалить навсегда из списка активных?</i>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=kb)

    # ── Установить цену ───────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'set_price')
    def set_price_cb(cb):
        if not _is_super_admin(cb): return
        _edit(
            f"<b>{TE_MONEY} Текущая цена: {get_price()} USDT</b>\n\n"
            f"Введи новую цену (например: 5 или 9.99):",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=back_to_start_kb())
        state[cb.from_user.id] = 'await_new_price'

    @bot.message_handler(func=lambda m: state.get(m.from_user.id) == 'await_new_price')
    def handle_new_price(m):
        if not _is_super_admin(m): return
        try:
            new_price = bounded_number(m.text.strip().replace(',', '.'), integer=False, minimum=0)
            if new_price <= 0: raise ValueError
        except ValueError:
            bot.send_message(m.chat.id, f"<b>{TE_CROSS}  Неверный формат. Введи число, например: 10</b>",
                parse_mode='HTML')
            return
        set_price(new_price)
        state.pop(m.from_user.id, None)
        bot.send_message(m.chat.id, f"<b>{TE_CHECK}  Цена обновлена: {new_price} USDT</b>",
            parse_mode='HTML', reply_markup=close_kb())

    # ── Маркетплейс ───────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'marketplace')
    def marketplace_cb(cb):
        if not _is_super_admin(cb): return
        _show_marketplace(cb.message.chat.id, cb.message.message_id)

    def _show_marketplace(chat_id, message_id=None):
        bots_list = db_get_active_bots_list()
        if not bots_list:
            text = f"<b>{TE_CART} Нет активных ботов для маркетплейса</b>"
            kb = InlineKeyboardMarkup()
            kb.add(_btn('Назад', icon='🔙', callback_data='back_admin_panel'))
            if message_id:
                _edit(text, chat_id, message_id, parse_mode='HTML', reply_markup=kb)
            else:
                bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=kb)
            return
        lines = [f"<b>{TE_CART} Маркетплейс ботов</b>\n",
                 "Включи кнопку «Купить такого бота» для нужных ботов.\n",
                 "Владелец получает 50% с каждой покупки через его бота.\n"]
        kb = InlineKeyboardMarkup()
        for row in bots_list[:30]:
            bot_id, owner_id = row[0], row[1]
            enabled = db_is_marketplace_enabled(bot_id)
            status_icon = f'{TE_GREEN}' if enabled else f'{TE_MOON}'
            label = f"{status_icon} Bot #{bot_id} (owner {owner_id})"
            toggle_label = f'{TE_MOON} Выключить' if enabled else f'{TE_GREEN} Включить'
            kb.row(
                _btn(label, callback_data='noop'),
                _btn(toggle_label, style='danger' if enabled else 'success', callback_data=f'mp_toggle_{bot_id}'),
            )
        if len(bots_list) > 30:
            lines.append(f"<i>… показаны первые 30 из {len(bots_list)} ботов</i>")
        kb.add(_btn('Назад', icon='🔙', callback_data='back_admin_panel'))
        text = "\n".join(lines)
        if message_id:
            _edit(text, chat_id, message_id, parse_mode='HTML', reply_markup=kb)
        else:
            bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith('mp_toggle_'))
    def mp_toggle_cb(cb):
        if not _is_super_admin(cb): return
        bot_id = int(cb.data.split('_')[-1])
        enabled = db_is_marketplace_enabled(bot_id)
        db_set_marketplace(bot_id, not enabled)
        status = 'включён' if not enabled else 'выключен'
        bot.answer_callback_query(cb.id, f"✅ Маркетплейс {status} для Bot #{bot_id}", show_alert=True)
        _show_marketplace(cb.message.chat.id, cb.message.message_id)

    @bot.callback_query_handler(func=lambda c: c.data == 'back_admin_panel')
    def back_admin_panel_cb(cb):
        if not _is_super_admin(cb): return
        _edit(f"<b>{TE_KEYBOARD} Панель супер-админа</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=super_admin_kb())

    @bot.callback_query_handler(func=lambda c: c.data == 'noop')
    def noop_cb(cb):
        bot.answer_callback_query(cb.id)

    # ── Бесплатная выдача ─────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'free_give')
    def free_give_cb(cb):
        if not _is_super_admin(cb): return
        kb = InlineKeyboardMarkup()
        kb.add(_btn('Купон для нового бота',     icon='🎁', style='success', callback_data='free_coupon'))
        kb.add(_btn('Продлить существующий бот', icon='🔄', style='primary', callback_data='free_renew'))
        kb.add(_btn('Назад', icon='🔙', callback_data='back_admin_panel'))
        _edit(
            f"{TE_GIFT} <b>Бесплатная выдача</b>\n\n"
            "Выбери тип:",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data == 'free_coupon')
    def free_coupon_cb(cb):
        if not _is_super_admin(cb): return
        _edit(
            f"<b>{TE_NEW} Купон для нового бота</b>\n\n"
            "Введи в формате:\n<code>user_id дни</code>\n\n"
            "Пример: <code>123456789 30</code>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=back_to_start_kb())
        state[cb.from_user.id] = 'await_coupon_input'

    @bot.message_handler(func=lambda m: state.get(m.from_user.id) == 'await_coupon_input')
    def handle_coupon_input(m):
        if not _is_super_admin(m): return
        parts = m.text.strip().split()
        if len(parts) != 2:
            bot.send_message(m.chat.id, f"<b>{TE_CROSS}  Неверный формат. Пример: 123456789 30</b>",
                parse_mode='HTML')
            return
        try:
            target_uid = bounded_number(parts[0])
        except ValueError:
            bot.send_message(m.chat.id, 'Неверный ID')
            return
        try:
            days = bounded_number(parts[1], maximum=(datetime.max - datetime.now()).days)
            if days <= 0: raise ValueError
        except ValueError:
            bot.send_message(m.chat.id, f"<b>{TE_CROSS}  Неверные данные. user_id и дни должны быть числами > 0</b>",
                parse_mode='HTML')
            return
        state.pop(m.from_user.id, None)
        db_set_free_coupon(target_uid, days)
        try:
            bot.send_message(target_uid,
                f"{TE_GIFT} <b>Тебе выдан бесплатный купон на {days} дней!</b>\n\n"
                f"Нажми «{TE_CART} Купить бота» в главном меню — купон применится автоматически.",
                parse_mode='HTML')
        except Exception:
            pass
        bot.send_message(m.chat.id,
            f"<b>{TE_CHECK}  Купон на {days} дней выдан пользователю <code>{target_uid}</code></b>",
            parse_mode='HTML', reply_markup=close_kb())

    @bot.callback_query_handler(func=lambda c: c.data == 'free_renew')
    def free_renew_cb(cb):
        if not _is_super_admin(cb): return
        _edit(
            f"<b>{TE_REFRESH} Бесплатное продление бота</b>\n\n"
            "Введи в формате:\n<code>bot_id дни</code>\n\n"
            "Пример: <code>5 30</code>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=back_to_start_kb())
        state[cb.from_user.id] = 'await_free_renew_input'

    @bot.message_handler(func=lambda m: state.get(m.from_user.id) == 'await_free_renew_input')
    def handle_free_renew_input(m):
        if not _is_super_admin(m): return
        parts = m.text.strip().split()
        if len(parts) != 2:
            bot.send_message(m.chat.id, f"<b>{TE_CROSS}  Неверный формат. Пример: 5 30</b>",
                parse_mode='HTML')
            return
        try:
            target_bot_id = bounded_number(parts[0])
        except ValueError:
            bot.send_message(m.chat.id, 'Неверный ID')
            return
        try:
            days = bounded_number(parts[1], maximum=(datetime.max - datetime.now()).days)
            if days <= 0: raise ValueError
        except ValueError:
            bot.send_message(m.chat.id, f"<b>{TE_CROSS}  Неверные данные</b>", parse_mode='HTML')
            return
        info = db_get_bot_info(target_bot_id)
        if not info:
            bot.send_message(m.chat.id, f"<b>{TE_CROSS}  Бот #{target_bot_id} не найден</b>",
                parse_mode='HTML')
            return
        state.pop(m.from_user.id, None)
        new_exp = db_renew_bot(target_bot_id, days)
        started = _resume_bot(target_bot_id)
        if not started:
            bot.send_message(m.chat.id, "Подписка продлена, но бот не запустился — попробуй позже")
        owner_id = info[2]
        exp_str  = str(new_exp)[:10] if new_exp else '—'
        try:
            bot.send_message(owner_id,
                f"{TE_GIFT} <b>Твой бот продлён бесплатно на {days} дней!</b>\n\n"
                f"Подписка действует до: <b>{exp_str}</b>",
                parse_mode='HTML')
        except Exception:
            pass
        bot.send_message(m.chat.id,
            f"<b>{TE_CHECK}  Bot #{target_bot_id} продлён на {days} дней. До: {exp_str}</b>",
            parse_mode='HTML', reply_markup=close_kb())

    # ── Запросы на вывод (super admin) ───────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'withdrawal_list')
    def withdrawal_list_cb(cb):
        if not _is_super_admin(cb): return
        _show_withdrawal_list(cb.message.chat.id, cb.message.message_id)

    def _show_withdrawal_list(chat_id, message_id=None):
        pending = db_get_pending_withdrawals()
        if not pending:
            text = f"<b>{TE_MONEY} Нет ожидающих запросов на вывод</b>"
            kb = InlineKeyboardMarkup()
            kb.add(_btn('Назад', icon='🔙', callback_data='back_admin_panel'))
            if message_id:
                _edit(text, chat_id, message_id, parse_mode='HTML', reply_markup=kb)
            else:
                bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=kb)
            return
        text = f"<b>{TE_MONEY} Запросы на вывод ({len(pending)}):</b>\n\n"
        kb = InlineKeyboardMarkup()
        shown = 0
        for wr_id, bot_id, owner_id, ton_address, usdt_amount, created_at in pending[:30]:
            shown += 1
            date = str(created_at)[:16] if created_at else '—'
            # адрес вводит владелец бота: '<' в нём ломал бы ВЕСЬ список (400),
            # и заявки становилось бы не видно до ручной правки БД
            text += (f"#{wr_id} | Bot #{bot_id} | Owner: <code>{owner_id}</code>\n"
                     f"Адрес: <code>{html.escape(str(ton_address))}</code>\n"
                     f"Сумма: <b>{usdt_amount} USDT</b> | {date}\n\n")
            kb.row(
                _btn(f"#{wr_id}", icon='✔️', style='success', callback_data=f'wr_confirm_{wr_id}'),
                _btn(f"#{wr_id}", icon='❌', style='danger',  callback_data=f'wr_reject_{wr_id}'),
            )
        if len(pending) > 30:
            text += f"<i>… показаны первые 30 из {len(pending)}</i>"
        kb.add(_btn('Назад', icon='🔙', callback_data='back_admin_panel'))
        if message_id:
            _edit(text, chat_id, message_id, parse_mode='HTML', reply_markup=kb)
        else:
            bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith('wr_confirm_'))
    def wr_confirm_cb(cb):
        if not _is_super_admin(cb): return
        wr_id = int(cb.data.split('_')[-1])
        row   = db_get_withdrawal(wr_id)
        if not row:
            bot.answer_callback_query(cb.id, "❌ Запрос не найден", show_alert=True)
            return
        bot_id, owner_id, ton_address, usdt_amount, status = row
        if status != 'pending':
            bot.answer_callback_query(cb.id, f"⚠️ Статус уже: {status}", show_alert=True)
            return
        from database import db_claim_withdrawal
        if not db_claim_withdrawal(wr_id, 'approved'):
            bot.answer_callback_query(cb.id, f"⚠️ Заявка #{wr_id} уже обработана", show_alert=True)
            return
        try:
            send_html(bot, owner_id,
                f"{TE_CHECK} <b>Запрос на вывод #{wr_id} подтверждён!</b>\n\n"
                f"Сумма: <b>{usdt_amount} USDT</b>\n"
                f"Адрес: <code>{html.escape(str(ton_address))}</code>\n\n"
                f"Средства переведены. Спасибо!",
                parse_mode='HTML')
        except Exception:
            pass
        bot.answer_callback_query(cb.id, f"✅ Запрос #{wr_id} подтверждён", show_alert=True)
        _show_withdrawal_list(cb.message.chat.id, cb.message.message_id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith('wr_reject_'))
    def wr_reject_cb(cb):
        if not _is_super_admin(cb): return
        wr_id = int(cb.data.split('_')[-1])
        row   = db_get_withdrawal(wr_id)
        if not row:
            bot.answer_callback_query(cb.id, "❌ Запрос не найден", show_alert=True)
            return
        bot_id, owner_id, ton_address, usdt_amount, status = row
        if status != 'pending':
            bot.answer_callback_query(cb.id, f"⚠️ Статус уже: {status}", show_alert=True)
            return
        from database import db_claim_withdrawal
        if not db_claim_withdrawal(wr_id, 'rejected'):
            bot.answer_callback_query(cb.id, f"⚠️ Заявка #{wr_id} уже обработана", show_alert=True)
            return
        try:
            send_html(bot, owner_id,
                f"{TE_CROSS} <b>Запрос на вывод #{wr_id} отклонён.</b>\n\n"
                f"Сумма <b>{usdt_amount} USDT</b> возвращена на твой баланс.\n"
                f"Адрес: <code>{html.escape(str(ton_address))}</code>\n"
                f"Свяжись с поддержкой через этого бота для уточнения.",
                parse_mode='HTML')
        except Exception:
            pass
        bot.answer_callback_query(cb.id, f"❌ Запрос #{wr_id} отклонён, баланс возвращён", show_alert=True)
        _show_withdrawal_list(cb.message.chat.id, cb.message.message_id)

    # ── Catch-all: диагностика ───────────────────────────
    @bot.message_handler(func=lambda m: True)
    def catch_all(m):
        log.info(f'Catch-all msg: uid={m.from_user.id} type={m.content_type} text={repr(m.text or "")}')

    from input_validation import guard_numeric_callbacks
    guard_numeric_callbacks(bot)
