import os
import telebot
import time
import threading
import logging
from datetime import datetime
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import SUPER_ADMIN, ADMIN_USERNAME, TON_WALLET
from database import (db_add_user, db_get_owner_bots, db_add_bot,
                       db_get_all_users, db_get_bot_users, db_get_stats,
                       db_get_active_bots_list, db_get_bot_info,
                       db_get_all_bots_for_checker,
                       db_get_bot_setting, db_set_bot_setting,
                       db_renew_bot,
                       get_price, set_price,
                       db_set_pending, db_del_pending,
                       db_mark_paid, db_is_paid, db_clear_paid,
                       db_has_used_trial, db_mark_trial_used,
                       db_set_referral, db_get_referrer, db_mark_referral_paid,
                       db_get_discount_count, db_use_discount,
                       DBState)
from keyboards import (start_kb, back_to_start_kb, back_to_payment_kb, cancel_kb,
                       close_kb, reply_kb, payment_kb, cryptobot_kb,
                       broadcast_type_kb, super_admin_kb,
                       start_text, buy_text)
from payments import (cb_create_invoice, cb_check_invoice,
                      get_ton_amount, ton_payment_link, ton_check_transfer)
from purchased_bot import launch_bot, stop_bot, running_bots, running_bot_ids

log = logging.getLogger(__name__)

_main_rate: dict = {}
MAIN_RATE_SEC = 15

def _is_main_rate_limited(user_id: int) -> bool:
    now = time.time()
    if now - _main_rate.get(user_id, 0) < MAIN_RATE_SEC:
        return True
    _main_rate[user_id] = now
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
                    if not expires_at:
                        continue
                    try:
                        exp = datetime.fromisoformat(str(expires_at))
                    except Exception:
                        continue
                    days_left = (exp - now).days

                    if days_left <= 0 and token in running_bots:
                        stop_bot(token)
                        log.info(f'Bot #{bot_id} subscription expired, stopped.')
                        try:
                            main_bot.send_message(owner_id,
                                "❌ <b>Подписка на вашего бота истекла!</b>\n\n"
                                "Чтобы возобновить — нажми «🛒 Купить бота» в главном меню.",
                                parse_mode='HTML')
                        except Exception:
                            pass

                    elif days_left == 3 and not db_get_bot_setting(bot_id, 'reminded_3d', ''):
                        db_set_bot_setting(bot_id, 'reminded_3d', '1')
                        try:
                            main_bot.send_message(owner_id,
                                "⚠️ <b>Подписка истекает через 3 дня!</b>\n\n"
                                "Продли через «🛒 Купить бота» в главном меню.",
                                parse_mode='HTML')
                        except Exception:
                            pass

                    elif days_left == 1 and not db_get_bot_setting(bot_id, 'reminded_1d', ''):
                        db_set_bot_setting(bot_id, 'reminded_1d', '1')
                        try:
                            main_bot.send_message(owner_id,
                                "🚨 <b>Подписка истекает завтра!</b>\n\n"
                                "Срочно продли через «🛒 Купить бота».",
                                parse_mode='HTML')
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
    state = DBState(0)
    _bot_username_cache = [None]

    def _get_bot_username():
        if _bot_username_cache[0] is None:
            try:
                _bot_username_cache[0] = bot.get_me().username or ''
            except Exception:
                _bot_username_cache[0] = ''
        return _bot_username_cache[0]

    def _safe_send(uid, text):
        try: bot.send_message(uid, text, parse_mode='HTML'); return True
        except: return False

    def _safe_photo(uid, photo, caption):
        try: bot.send_photo(uid, photo, caption=caption, parse_mode='HTML'); return True
        except: return False

    def _broadcast_users(scope):
        if scope == 'all': return db_get_all_users()
        return db_get_bot_users(0)

    def _after_payment_confirmed(uid, chat_id, message_id):
        """Вызывается после любой успешной оплаты."""
        # Нагородження реферера при першій оплаті
        referrer = db_get_referrer(uid)
        if referrer:
            db_mark_referral_paid(uid)
            try:
                disc = db_get_discount_count(referrer)
                bot.send_message(referrer,
                    f"🎉 <b>Твой реферал оплатил подписку!</b>\n\n"
                    f"💎 Накоплено скидок: <b>{disc}</b>\n"
                    f"Каждая скидка = 1 бесплатное продление на 30 дней.\n"
                    f"Используй при следующей оплате через кнопку «🛒 Купить бота».",
                    parse_mode='HTML')
            except Exception:
                pass

        s = state.get(uid)
        if isinstance(s, dict) and s.get('step') == 'renewing':
            bot_id = s['bot_id']
            state.pop(uid, None)
            new_exp = db_renew_bot(bot_id, 30)
            info = db_get_bot_info(bot_id)
            if info:
                token, admin_id_val, _, _ = info
                if token not in running_bots:
                    launch_bot(bot_id, token, admin_id_val, bot)
            exp_str = str(new_exp)[:10] if new_exp else '—'
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton('❌ Закрыть', callback_data='close'))
            bot.edit_message_text(
                f"<b>✅ Подписка успешно продлена на 30 дней!</b>\n\n"
                f"Действует до: <b>{exp_str}</b>",
                chat_id, message_id, parse_mode='HTML', reply_markup=kb)
        else:
            db_mark_paid(uid)
            state[uid] = 'await_bot_token'
            bot.edit_message_text(
                "<b>✅ Оплата получена!\n\n"
                "🤖 Введи токен своего бота (получи в @BotFather):</b>",
                chat_id, message_id, parse_mode='HTML', reply_markup=cancel_kb())

    # ── /start ───────────────────────────────────────────
    @bot.message_handler(commands=['start'])
    def cmd_start(m):
        uid = m.from_user.id
        db_add_user(0, uid)
        # Обработка реферального deep link: /start ref_123456789
        payload = m.text.strip().split(' ', 1)[1] if ' ' in m.text else ''
        if payload.startswith('ref_'):
            try:
                referrer_id = int(payload[4:])
                if referrer_id != uid:
                    db_set_referral(referrer_id, uid)
            except (ValueError, Exception):
                pass
        bot.send_message(m.chat.id, start_text(ADMIN_USERNAME),
            parse_mode='HTML', reply_markup=start_kb())

    # ── /admin ───────────────────────────────────────────
    @bot.message_handler(commands=['admin'])
    def cmd_admin(m):
        if m.from_user.id != SUPER_ADMIN: return
        bot.send_message(m.chat.id, "<b>⚙️ Панель супер-админа</b>",
            parse_mode='HTML', reply_markup=super_admin_kb())

    # ── /backup ──────────────────────────────────────────
    @bot.message_handler(commands=['backup'])
    def cmd_backup(m):
        if m.from_user.id != SUPER_ADMIN: return
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'spambots.db')
        try:
            with open(db_path, 'rb') as f:
                bot.send_document(m.chat.id, f,
                    caption=f"💾 Backup <code>spambots.db</code>\n📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                    parse_mode='HTML')
        except Exception as e:
            log.error(f'backup error: {e}')
            bot.send_message(m.chat.id, f"<b>❌ Ошибка бэкапа: {e}</b>", parse_mode='HTML')

    # ── Навигация ────────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'back_to_start')
    def back_to_start_cb(cb):
        state.pop(cb.from_user.id, None)
        db_del_pending(cb.from_user.id)
        bot.edit_message_text(start_text(ADMIN_USERNAME),
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=start_kb())

    @bot.callback_query_handler(func=lambda c: c.data == 'back_to_payment')
    def back_to_payment_cb(cb):
        uid = cb.from_user.id
        state.pop(uid, None)
        db_del_pending(uid)
        disc = db_get_discount_count(uid)
        show_trial = not db_has_used_trial(uid)
        bot.edit_message_text(buy_text(),
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=payment_kb(disc, show_trial))

    # ── Отправить сообщение ──────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'user_send')
    def user_send(cb):
        bot.edit_message_text("<b>💬 Введи своё сообщение:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=back_to_start_kb())
        state[cb.from_user.id] = 'await_user_msg'

    @bot.callback_query_handler(func=lambda c: c.data.startswith('admin_reply_'))
    def admin_reply(cb):
        target = int(cb.data.split('_')[-1])
        bot.edit_message_text("<b>💬 Введи ответ:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=cancel_kb())
        state[cb.from_user.id] = f'await_admin_msg_{target}'

    @bot.callback_query_handler(func=lambda c: c.data == 'cancel')
    def cancel_cb(cb):
        bot.delete_message(cb.message.chat.id, cb.message.message_id)
        state.pop(cb.from_user.id, None)
        db_del_pending(cb.from_user.id)

    @bot.callback_query_handler(func=lambda c: c.data == 'close')
    def close_cb(cb):
        bot.delete_message(cb.message.chat.id, cb.message.message_id)

    @bot.message_handler(func=lambda m: state.get(m.from_user.id) == 'await_user_msg')
    def user_text(m):
        if _is_main_rate_limited(m.from_user.id):
            bot.send_message(m.chat.id,
                f"<b>⏳ Подожди {MAIN_RATE_SEC} секунд перед следующим сообщением</b>",
                parse_mode='HTML')
            return
        username = f"@{m.from_user.username}" if m.from_user.username else "нет"
        text = (f"<b>📥 Сообщение от:</b>\n"
                f"👤 Имя: <b>{m.from_user.full_name}</b>\n"
                f"🔗 Username: {username}\n"
                f"🆔 ID: <code>{m.from_user.id}</code>\n\n"
                f"💬 {m.text}")
        bot.send_message(SUPER_ADMIN, text, parse_mode='HTML', reply_markup=reply_kb(m.from_user.id))
        bot.send_message(m.chat.id, "<b>✅ Сообщение отправлено</b>",
            parse_mode='HTML', reply_markup=close_kb())
        state.pop(m.from_user.id, None)

    @bot.message_handler(func=lambda m: isinstance(state.get(m.from_user.id), str)
                                        and state[m.from_user.id].startswith('await_admin_msg_'))
    def admin_text(m):
        target_id = int(state[m.from_user.id].split('_')[-1])
        bot.send_message(target_id, f"<b>📥 Сообщение от администратора\n\n💬 {m.text}</b>",
            parse_mode='HTML')
        bot.send_message(m.chat.id, "<b>✅ Ответ отправлен</b>",
            parse_mode='HTML', reply_markup=close_kb())
        state.pop(m.from_user.id, None)

    # ── Купить / продлить бота ────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'buy_bot')
    def buy_bot_cb(cb):
        uid = cb.from_user.id
        # Уже оплатил — продолжаем настройку
        if db_is_paid(uid):
            state[uid] = 'await_bot_token'
            bot.edit_message_text(
                "<b>✅ Оплата уже получена!\n\n"
                "🤖 Введи токен своего бота (получи в @BotFather):</b>",
                cb.message.chat.id, cb.message.message_id,
                parse_mode='HTML', reply_markup=cancel_kb())
            return

        # Уже есть боты — показываем продление + кнопку нового
        existing = db_get_owner_bots(uid)
        if existing:
            lines = ["<b>У тебя уже есть бот(ы):</b>\n"]
            kb    = InlineKeyboardMarkup()
            for bot_id, token, username, created_at, expires_at in existing:
                is_running = token in running_bots
                status     = '🟢' if is_running else '🔴'
                name       = f"@{username}" if username else f"Bot #{bot_id}"
                if expires_at:
                    try:
                        exp       = datetime.fromisoformat(str(expires_at))
                        days_left = (exp - datetime.now()).days
                        exp_str   = f"истекает через {days_left}д." if days_left > 0 else "❌ истёк"
                    except Exception:
                        exp_str = "—"
                else:
                    exp_str = "—"
                lines.append(f"{status} {name} — {exp_str}")
                kb.add(InlineKeyboardButton(f"🔄 Продлить {name} (+30д.)",
                                            callback_data=f'renew_bot_{bot_id}'))
            kb.add(InlineKeyboardButton('🆕 Купить новый бот', callback_data='buy_new_bot'))
            kb.add(InlineKeyboardButton('🔙 Назад',            callback_data='back_to_start'))
            bot.edit_message_text("\n".join(lines),
                cb.message.chat.id, cb.message.message_id,
                parse_mode='HTML', reply_markup=kb)
            return

        # Нет ботов — обычная покупка
        disc = db_get_discount_count(uid)
        show_trial = not db_has_used_trial(uid)
        bot.edit_message_text(buy_text(),
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=payment_kb(disc, show_trial))

    @bot.callback_query_handler(func=lambda c: c.data == 'buy_new_bot')
    def buy_new_bot_cb(cb):
        uid = cb.from_user.id
        disc = db_get_discount_count(uid)
        show_trial = not db_has_used_trial(uid)
        bot.edit_message_text(buy_text(),
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=payment_kb(disc, show_trial))

    @bot.callback_query_handler(func=lambda c: c.data.startswith('renew_bot_'))
    def renew_bot_cb(cb):
        uid = cb.from_user.id
        bot_id = int(cb.data.split('_')[-1])
        state[uid] = {'step': 'renewing', 'bot_id': bot_id}
        disc = db_get_discount_count(uid)
        bot.edit_message_text(
            f"<b>🔄 Продление подписки на 30 дней</b>\n\n"
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
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton('❌ Закрыть', callback_data='close'))
        bot.send_message(uid,
            f"🔗 <b>Твоя реферальная ссылка:</b>\n"
            f"<code>{ref_link}</code>\n\n"
            f"За каждого приведённого клиента, который оплатит подписку, "
            f"ты получишь 1 бесплатное продление на 30 дней.\n\n"
            f"💎 Накоплено скидок: <b>{disc}</b>",
            parse_mode='HTML', reply_markup=kb)
        bot.answer_callback_query(cb.id)

    # ── Пробний период ───────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'try_free')
    def try_free_cb(cb):
        uid = cb.from_user.id
        if db_has_used_trial(uid):
            bot.answer_callback_query(cb.id,
                "❌ Ты уже использовал бесплатный пробный период.", show_alert=True)
            return
        db_mark_trial_used(uid)
        db_mark_paid(uid)
        state[uid] = 'await_bot_token_trial'
        bot.edit_message_text(
            "<b>🆓 Бесплатный пробный период (3 дня) активирован!\n\n"
            "🤖 Введи токен своего бота (получи в @BotFather):</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=cancel_kb())

    # ── Использовать скидку ──────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'use_discount')
    def use_discount_cb(cb):
        uid = cb.from_user.id
        if db_get_discount_count(uid) <= 0:
            bot.answer_callback_query(cb.id, "❌ У тебя нет скидок.", show_alert=True)
            return
        db_use_discount(uid)
        # Якщо renewal — state вже має 'renewing' step, _after_payment_confirmed обробить
        # Якщо новий бот — mark_paid і await_token
        s = state.get(uid)
        if not (isinstance(s, dict) and s.get('step') == 'renewing'):
            db_mark_paid(uid)
        _after_payment_confirmed(uid, cb.message.chat.id, cb.message.message_id)

    # ── CryptoBot ────────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'pay_cryptobot')
    def pay_cryptobot_cb(cb):
        invoice = cb_create_invoice(cb.from_user.id)
        if not invoice:
            bot.answer_callback_query(cb.id, "❌ Ошибка создания инвойса. Попробуй ещё раз.", show_alert=True)
            return
        db_set_pending(cb.from_user.id, invoice['invoice_id'])
        bot.edit_message_text(
            f"<b>💳 Оплата через CryptoBot</b>\n\n"
            f"Сумма: <b>{get_price()} USDT</b>\n\n"
            f"Нажми кнопку ниже, оплати и вернись сюда — нажми «Проверить оплату».",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=cryptobot_kb(invoice['pay_url'], invoice['invoice_id']))

    @bot.callback_query_handler(func=lambda c: c.data.startswith('check_payment_'))
    def check_payment_cb(cb):
        invoice_id = int(cb.data.split('_')[-1])
        invoice    = cb_check_invoice(invoice_id)
        if invoice and invoice.get('status') == 'paid':
            db_del_pending(cb.from_user.id)
            _after_payment_confirmed(cb.from_user.id, cb.message.chat.id, cb.message.message_id)
        else:
            bot.answer_callback_query(cb.id, "❌ Оплата не найдена. Попробуй ещё раз.", show_alert=True)

    # ── TON ──────────────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'pay_ton')
    def pay_ton_cb(cb):
        bot.answer_callback_query(cb.id, "⏳ Получаю курс TON...")
        ton_amount = get_ton_amount()
        if ton_amount <= 0:
            bot.answer_callback_query(cb.id, "❌ Не удалось получить курс TON.", show_alert=True)
            return
        payment_code = f"SPB{cb.from_user.id}"
        link = ton_payment_link(ton_amount, payment_code)
        # сохраняем ton_pending поверх renewing если есть
        cur = state.get(cb.from_user.id)
        new_state = {'step': 'ton_pending', 'ton': ton_amount, 'code': payment_code}
        if isinstance(cur, dict) and cur.get('step') == 'renewing':
            new_state['bot_id'] = cur['bot_id']
            new_state['renewing'] = True
        state[cb.from_user.id] = new_state
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton('💎 Открыть TON Keeper и оплатить', url=link))
        kb.add(InlineKeyboardButton('✅ Я оплатил — проверить',         callback_data='ton_check_auto'))
        kb.add(InlineKeyboardButton('🔙 Назад',                          callback_data='back_to_payment'))
        bot.edit_message_text(
            f"<b>💎 Оплата через TON Keeper</b>\n\n"
            f"Сумма: <b>{ton_amount} TON</b> (~${get_price()})\n\n"
            f"Нажми кнопку ниже — TON Keeper откроется с уже заполненной суммой, адресом и комментарием.\n\n"
            f"Адрес (вручную):\n<code>{TON_WALLET}</code>\n\n"
            f"⚠️ <b>ВАЖНО: при отправке обязательно укажи комментарий:</b>\n"
            f"<code>{payment_code}</code>\n"
            f"<i>(без комментария оплата не будет засчитана)</i>\n\n"
            f"После оплаты нажми «Я оплатил» — бот проверит автоматически.\n"
            f"Проверка работает в течение <b>2 часов</b>.",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data == 'ton_check_auto')
    def ton_check_auto_cb(cb):
        uid = cb.from_user.id
        s   = state.get(uid)
        if not isinstance(s, dict) or 'ton' not in s:
            bot.answer_callback_query(cb.id, "❌ Сессия истекла. Начни заново.", show_alert=True)
            return
        ton_amount   = s['ton']
        payment_code = s.get('code', f"SPB{uid}")
        bot.answer_callback_query(cb.id, "🔍 Проверяю блокчейн...")
        bot.edit_message_text("<b>🔍 Проверяю блокчейн TON...</b>",
            cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
        found = ton_check_transfer(uid, ton_amount, payment_code)
        if found:
            # восстанавливаем renewing если был
            if s.get('renewing'):
                state[uid] = {'step': 'renewing', 'bot_id': s['bot_id']}
            else:
                state.pop(uid, None)
            _after_payment_confirmed(uid, cb.message.chat.id, cb.message.message_id)
        else:
            link = ton_payment_link(ton_amount, payment_code)
            kb   = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton('💎 Открыть TON Keeper', url=link))
            kb.add(InlineKeyboardButton('🔄 Проверить снова',    callback_data='ton_check_auto'))
            kb.add(InlineKeyboardButton('🔙 Назад',              callback_data='back_to_payment'))
            bot.edit_message_text(
                f"<b>❌ Оплата не найдена</b>\n\n"
                f"Убедись что:\n"
                f"• Отправил <b>{ton_amount} TON</b>\n"
                f"• Указал комментарий: <code>{payment_code}</code>\n"
                f"• Транзакция прошла подтверждение\n"
                f"• Оплата не старше 2 часов\n\n"
                f"Попробуй ещё раз через минуту или напиши {ADMIN_USERNAME}.",
                cb.message.chat.id, cb.message.message_id,
                parse_mode='HTML', reply_markup=kb)

    # ── Настройка бота после оплаты ──────────────────────
    @bot.message_handler(func=lambda m: state.get(m.from_user.id) in ('await_bot_token', 'await_bot_token_trial'))
    def get_bot_token(m):
        is_trial = state.get(m.from_user.id) == 'await_bot_token_trial'
        token_val = m.text.strip()
        if ':' not in token_val or len(token_val) < 30:
            bot.send_message(m.chat.id, "<b>❌ Неверный формат токена. Попробуй ещё раз:</b>",
                parse_mode='HTML')
            return
        try:
            me = telebot.TeleBot(token_val).get_me()
        except Exception:
            bot.send_message(m.chat.id, "<b>❌ Токен недействителен. Проверь и введи правильный:</b>",
                parse_mode='HTML')
            return
        state[m.from_user.id] = {'step': 'await_admin_id', 'token': token_val,
                                  'username': me.username or '', 'trial': is_trial}
        bot.send_message(m.chat.id,
            "<b>👤 Теперь введи свой Telegram ID\n"
            "(сообщения от пользователей будут приходить именно тебе)\n\n"
            "Узнать свой ID: @userinfobot</b>",
            parse_mode='HTML', reply_markup=cancel_kb())

    @bot.message_handler(func=lambda m: isinstance(state.get(m.from_user.id), dict)
                                        and state[m.from_user.id].get('step') == 'await_admin_id')
    def get_admin_id(m):
        try:
            admin_id_val = int(m.text.strip())
        except ValueError:
            bot.send_message(m.chat.id, "<b>❌ ID должен быть числом. Попробуй ещё раз:</b>",
                parse_mode='HTML')
            return
        s         = state.pop(m.from_user.id)
        token_val = s['token']
        uname     = s.get('username', '')
        days      = 3 if s.get('trial') else 30
        try:
            bot_db_id = db_add_bot(m.from_user.id, token_val, admin_id_val, days=days)
        except Exception:
            bot.send_message(m.chat.id,
                "<b>❌ Этот токен уже зарегистрирован. Используй другой.</b>", parse_mode='HTML')
            return
        if launch_bot(bot_db_id, token_val, admin_id_val, bot):
            db_clear_paid(m.from_user.id)
            kb = InlineKeyboardMarkup()
            if uname:
                kb.add(InlineKeyboardButton(f'🤖 Перейти к @{uname}', url=f'https://t.me/{uname}'))
            kb.add(InlineKeyboardButton('❌ Закрыть', callback_data='close'))
            bot.send_message(m.chat.id,
                f"<b>🎉 Бот успешно запущен!\n\n"
                f"Управляй через /admin в своём боте.\n"
                f"Статистика: /status в своём боте.</b>",
                parse_mode='HTML', reply_markup=kb)
            bot.send_message(SUPER_ADMIN,
                f"<b>🆕 Новый бот подключён</b>\n"
                f"Owner: <code>{m.from_user.id}</code> | Admin: <code>{admin_id_val}</code> | @{uname}",
                parse_mode='HTML')
        else:
            bot.send_message(m.chat.id,
                f"<b>❌ Ошибка запуска. Свяжись с {ADMIN_USERNAME}.</b>", parse_mode='HTML')

    # ── Рассылка ─────────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'broadcast')
    def broadcast_start(cb):
        if cb.from_user.id != SUPER_ADMIN: return
        bot.edit_message_text("<b>📢 Выбери тип рассылки:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=broadcast_type_kb('mine'))

    @bot.callback_query_handler(func=lambda c: c.data == 'broadcast_all')
    def broadcast_all_start(cb):
        if cb.from_user.id != SUPER_ADMIN: return
        bot.edit_message_text("<b>📡 Рассылка по всем ботам. Выбери тип:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=broadcast_type_kb('all'))

    @bot.callback_query_handler(func=lambda c: c.data.startswith('bcast_') and not c.data.endswith('_p'))
    def broadcast_type(cb):
        if cb.from_user.id != SUPER_ADMIN: return
        parts = cb.data.split('_')
        btype, scope = parts[1], parts[2]
        if btype == 'text':
            bot.edit_message_text("<b>📝 Введи текст рассылки (HTML):</b>",
                cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
            state[cb.from_user.id] = f'bcast_text_{scope}'
        else:
            bot.edit_message_text("<b>🖼 Отправь фото с подписью (или без):</b>",
                cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
            state[cb.from_user.id] = f'bcast_photo_{scope}'

    @bot.message_handler(func=lambda m: isinstance(state.get(m.from_user.id), str)
                                        and state[m.from_user.id].startswith('bcast_text_'))
    def broadcast_text(m):
        scope = state.pop(m.from_user.id).split('_')[-1]
        users = _broadcast_users(scope)
        sent  = sum(1 for uid in users if _safe_send(uid, m.text))
        bot.send_message(m.chat.id, f"<b>✅ Рассылка завершена: {sent}/{len(users)}</b>",
            parse_mode='HTML', reply_markup=close_kb())

    @bot.message_handler(content_types=['photo'],
                         func=lambda m: isinstance(state.get(m.from_user.id), str)
                                        and state[m.from_user.id].startswith('bcast_photo_'))
    def broadcast_photo(m):
        scope   = state.pop(m.from_user.id).split('_')[-1]
        users   = _broadcast_users(scope)
        photo   = m.photo[-1].file_id
        caption = m.caption or ''
        sent    = sum(1 for uid in users if _safe_photo(uid, photo, caption))
        bot.send_message(m.chat.id, f"<b>✅ Рассылка завершена: {sent}/{len(users)}</b>",
            parse_mode='HTML', reply_markup=close_kb())

    # ── Статистика ────────────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'stats')
    def stats_cb(cb):
        if cb.from_user.id != SUPER_ADMIN: return
        bots_count, main_users, total_users, breakdown = db_get_stats()
        revenue = round(bots_count * get_price(), 2)
        lines = []
        for i, row in enumerate(breakdown, 1):
            owner, _, users_cnt, created_at = row[0], row[1], row[2], row[3]
            expires_at = row[4] if len(row) > 4 else None
            date = str(created_at)[:10] if created_at else '—'
            exp  = str(expires_at)[:10] if expires_at else '—'
            lines.append(f"  {i}. Owner <code>{owner}</code> — {users_cnt} польз. | 📅 {date} | ⏳ {exp}")
        text = (f"<b>📊 Статистика SpamBot</b>\n{'─'*28}\n"
                f"🤖 Подключённых ботов: <b>{bots_count}</b>\n"
                f"💰 Выручка за всё время: <b>~{revenue} USDT</b>\n"
                f"{'─'*28}\n"
                f"👥 Пользователей в главном боте: <b>{main_users}</b>\n"
                f"👥 Всего пользователей (все боты): <b>{total_users}</b>\n"
                f"{'─'*28}\n"
                f"📋 Боты по кол-ву пользователей:\n"
                f"{chr(10).join(lines) if lines else '  —'}")
        bot.edit_message_text(text, cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=close_kb())

    # ── Управление ботами ────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'manage_bots')
    def manage_bots_cb(cb):
        if cb.from_user.id != SUPER_ADMIN: return
        _show_bots_list(cb.message.chat.id, cb.message.message_id)

    def _show_bots_list(chat_id, message_id=None):
        bots_list = db_get_active_bots_list()
        if not bots_list:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton('❌ Закрыть', callback_data='close'))
            text = "<b>🤖 Нет активных ботов</b>"
            if message_id:
                bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=kb)
            else:
                bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=kb)
            return
        lines = []
        kb    = InlineKeyboardMarkup()
        for row in bots_list:
            bot_id, owner_id, admin_id_val = row[0], row[1], row[2]
            created_at = row[3]
            expires_at = row[4] if len(row) > 4 else None
            info  = db_get_bot_info(bot_id)
            token = info[0] if info else None
            is_running = token in running_bots if token else False
            status     = '🟢' if is_running else '🔴'
            users_cnt  = len(db_get_bot_users(bot_id))
            date       = str(created_at)[:10] if created_at else '—'
            exp        = str(expires_at)[:10] if expires_at else '—'
            lines.append(
                f"{status} <b>Bot #{bot_id}</b> | Owner: <code>{owner_id}</code> | "
                f"{users_cnt} польз. | 📅 {date} | ⏳ {exp}"
            )
            kb.row(
                InlineKeyboardButton(f"⏹ #{bot_id}",  callback_data=f'bot_stop_{bot_id}'),
                InlineKeyboardButton(f"🔄 #{bot_id}",  callback_data=f'bot_restart_{bot_id}'),
            )
        kb.add(InlineKeyboardButton('❌ Закрыть', callback_data='close'))
        text = "<b>🤖 Управление ботами</b>\n\n" + "\n".join(lines)
        if message_id:
            bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=kb)
        else:
            bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=kb)

    @bot.callback_query_handler(func=lambda c: c.data.startswith('bot_stop_'))
    def bot_stop_cb(cb):
        if cb.from_user.id != SUPER_ADMIN: return
        bot_id = int(cb.data.split('_')[-1])
        token  = running_bot_ids.get(bot_id)
        if token and stop_bot(token):
            bot.answer_callback_query(cb.id, f"✅ Bot #{bot_id} остановлен", show_alert=True)
        else:
            bot.answer_callback_query(cb.id, f"⚠️ Bot #{bot_id} уже остановлен", show_alert=True)
        _show_bots_list(cb.message.chat.id, cb.message.message_id)

    @bot.callback_query_handler(func=lambda c: c.data.startswith('bot_restart_'))
    def bot_restart_cb(cb):
        if cb.from_user.id != SUPER_ADMIN: return
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

    # ── Установить цену ───────────────────────────────────
    @bot.callback_query_handler(func=lambda c: c.data == 'set_price')
    def set_price_cb(cb):
        if cb.from_user.id != SUPER_ADMIN: return
        bot.edit_message_text(
            f"<b>💰 Текущая цена: {get_price()} USDT</b>\n\n"
            f"Введи новую цену (например: 5 или 9.99):",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=back_to_start_kb())
        state[cb.from_user.id] = 'await_new_price'

    @bot.message_handler(func=lambda m: state.get(m.from_user.id) == 'await_new_price')
    def handle_new_price(m):
        if m.from_user.id != SUPER_ADMIN: return
        try:
            new_price = float(m.text.strip().replace(',', '.'))
            if new_price <= 0: raise ValueError
        except ValueError:
            bot.send_message(m.chat.id, "<b>❌ Неверный формат. Введи число, например: 10</b>",
                parse_mode='HTML')
            return
        set_price(new_price)
        state.pop(m.from_user.id, None)
        bot.send_message(m.chat.id, f"<b>✅ Цена обновлена: {new_price} USDT</b>",
            parse_mode='HTML', reply_markup=close_kb())
