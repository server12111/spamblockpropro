import html
import telebot
import threading
import time
import logging
import math
import re
import json
import uuid
from input_validation import bounded_number
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING
from html.parser import HTMLParser
from datetime import timedelta, timezone
import database as db
from safe_logging import register_secret
from telegram_text import edit_html, protect_text_lengths
from datetime import datetime
from functools import wraps
from types import SimpleNamespace
from database import subscription_days_left
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from keyboards import _ICON, TE_ANNOUNCE, TE_BLOCK, TE_CALENDAR, TE_CHART, TE_CHAT, TE_CHECK, TE_CLOCK, TE_CROSS, TE_CROWN, TE_DIAMOND, TE_DOLLAR, TE_EXCLAM, TE_FIRE, TE_GIFT, TE_GREEN, TE_HOURGLASS, TE_ID, TE_INBOX, TE_KEYBOARD, TE_LINK, TE_MAIL, TE_MASKS, TE_MIC, TE_MONEY, TE_NOTE, TE_PENCIL, TE_PICTURE, TE_PLUS, TE_REFRESH, TE_ROBOT, TE_ROCKET, TE_SEND, TE_STATS, TE_USER, TE_USERS, TE_VIDEO

def _pbtn(text, icon=None, style=None, **kwargs):
    extra = {}
    if icon and icon in _ICON:
        extra['icon_custom_emoji_id'] = _ICON[icon]
    if style:
        extra['style'] = style
    return InlineKeyboardButton(text, **extra, **kwargs)
from database import (db_add_user, db_get_bot_users, db_deactivate_bot, DBState,
                      db_get_bot_info, db_get_bot_expires, db_update_bot_username,
                      db_block_user, db_unblock_user, db_is_blocked, db_get_blocked_list,
                      db_get_bot_setting, db_set_bot_setting,
                      db_get_bot_admins, db_add_bot_admin, db_remove_bot_admin,
                      db_set_primary_admin,
                      db_get_templates, db_add_template, db_del_template, db_get_template,
                      db_log_message, db_get_bot_msg_stats, db_get_last_user_messages,
                      db_get_username,
                      db_is_marketplace_enabled,
                      db_get_bot_earnings, db_deduct_bot_earnings,
                      db_create_withdrawal, db_renew_bot,
                      db_return_bot_earnings)
from keyboards import broadcast_type_kb, PE_MONEY, PE_CHECK, PE_GIFT, PE_FIRE
from config import SUPER_ADMIN, WITHDRAWAL_CHANNEL, BOT_USERNAME
from payments import get_ton_amount
from forum_support import (register_forum_support, RATE_LIMIT_SEC, rate_limited,
                           fs_refresh_pin_markup, fs_refresh_management)

log = logging.getLogger(__name__)

running_bots    = {}  # {token: TeleBot}
running_bot_ids = {}  # {db_bot_id: token}
_lifecycle_lock = threading.RLock()


def _serialize_lifecycle(fn):
    @wraps(fn)
    def call(*args, **kwargs):
        with _lifecycle_lock:
            return fn(*args, **kwargs)
    return call

def _is_rate_limited(bot_id: int, user_id: int) -> bool:
    """Единый rate limit (кэш и логика в forum_support.rate_limited)."""
    return rate_limited(bot_id, user_id, mark=True)


def balance_renewal_quote(balance, price, days=None):
    balance = Decimal(str(round(balance, 10)))
    price = Decimal(str(price))
    if price <= 0: return 0, 0.0
    if days is None:
        days = int((balance * 30 / price).to_integral_value(rounding=ROUND_FLOOR))
    cost = (price * days / 30).quantize(Decimal('0.0000000001'), rounding=ROUND_CEILING)
    return days, float(cost)

def _retry_send(send):
    """Лимит Telegram откладывает доставку, а не пропускает получателя"""
    from telegram_text import retry_telegram
    try:
        retry_telegram(send)
        return True
    except Exception:
        return False


def _safe_send(b, uid, text):
    return _retry_send(lambda: b.send_message(uid, text, parse_mode='HTML'))

def _safe_photo(b, uid, photo, caption):
    return _retry_send(lambda: b.send_photo(uid, photo, caption=caption, parse_mode='HTML'))


def _close_bot(bot):
    """Остановить приём, дождаться принятых обработчиков, затем закрыть отправителей"""
    bot.stop_polling()
    polling = getattr(bot, '_polling_thread', None)
    if polling is not None and polling is not threading.current_thread():
        polling.join()
    pool = getattr(bot, 'worker_pool', None)
    if pool is not None and any(w.is_alive() for w in pool.workers):
        if threading.current_thread() in pool.workers:
            raise RuntimeError('Остановка должна выполняться вне обработчика')
        # telebot не вызывает task_done: по одному барьеру на каждого worker
        # FIFO гарантирует завершение всех заданий перед барьерами
        workers = [w for w in pool.workers if w.is_alive()]
        release = threading.Event()
        reached = [threading.Event() for _ in workers]
        def barrier(event):
            event.set()
            release.wait()
        try:
            for event in reached: pool.put(barrier, event)
            for event in reached: event.wait()
        finally:
            release.set()
    bot.stop_bot()
    stop_albums = getattr(bot, '_stop_reply_albums', None)
    if stop_albums: stop_albums()
    bid = getattr(bot, '_db_bot_id', None)
    if bid is not None:
        from forum_support import stop_bot_albums
        stop_bot_albums(bid)
    sender = getattr(bot, '_delivery_sender', None)
    if sender is not None:
        sender.stop()
    replies = getattr(bot, '_reply_sender', None)
    if replies is not None:
        replies.stop_event.set()
        with replies.lock: pass
    if bid is not None:
        from forum_support import _mgmt_lock
        with _mgmt_lock(bid): bot._closed = True

@_serialize_lifecycle
def stop_bot(token: str) -> bool:
    bot_obj = running_bots.get(token)
    if not bot_obj:
        return False
    try:
        import service_broadcast as svc
        bid = getattr(bot_obj, '_db_bot_id', 0)
        with svc.dispatch_lock(bid):
            # Рассылка не может держать экземпляр во время его остановки
            running_bots.pop(token, None)
            removed_ids = [bid for bid, tok in running_bot_ids.items() if tok == token]
            for bid in removed_ids: running_bot_ids.pop(bid, None)
            try:
                _close_bot(bot_obj)
            except Exception:
                running_bots[token] = bot_obj
                for bid in removed_ids: running_bot_ids[bid] = token
                raise
        return True
    except Exception as e:
        log.error(f'stop_bot failed: {e}')
        return False

DEFAULT_WELCOME = (f"<b>{TE_ROBOT} Привет! Это бот обратной связи.\n\n"
                   f"{TE_CHAT} Отправь своё сообщение и администратор обязательно его прочитает.</b>")
DEFAULT_SENT = f'<b>{TE_CHECK}  Сообщение отправлено</b>'
DEFAULT_OFFHOURS = f'{TE_CLOCK} Поддержка работает с {{start}} до {{end}}\nОжидайте ответ в течение {{wait}}'
TEXTS = {'welcome': ('Приветствие', DEFAULT_WELCOME),
         'txt_sent': ('Сообщение отправлено', DEFAULT_SENT),
         'txt_offhours': ('Нерабочее время', DEFAULT_OFFHOURS)}


def validate_support_html(text, limit=4096):
    if not text or len(text.encode('utf-16-le')) // 2 > limit:
        raise ValueError(f'Текст должен содержать от 1 до {limit} символов')
    class Validator(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=False)
            self.stack = []
        def handle_starttag(self, tag, attrs):
            if tag not in ('b', 'strong', 'i', 'em', 'u', 'ins', 's', 'strike', 'del',
                           'a', 'code', 'pre', 'span', 'tg-spoiler', 'tg-emoji', 'blockquote'):
                raise ValueError('Некорректный HTML: проверь теги и их закрытие')
            self.stack.append(tag)
        def handle_endtag(self, tag):
            if not self.stack or self.stack.pop() != tag:
                raise ValueError('Некорректный HTML: проверь теги и их закрытие')
        def handle_startendtag(self, tag, attrs):
            raise ValueError('Некорректный HTML: проверь теги и их закрытие')
    parser = Validator()
    parser.feed(text)
    parser.close()
    if parser.stack:
        raise ValueError('Некорректный HTML: проверь теги и их закрытие')


def support_schedule(bot_id, now=None):
    """Интервалы начинаются в отмеченный день, ночные заканчиваются на следующий."""
    get = lambda key, default: db_get_bot_setting(bot_id, 'hours_' + key, default)
    tz = timezone(timedelta(hours=int(get('tz', '3'))))
    local = datetime.fromtimestamp(time.time() if now is None else now, tz)
    days = {int(d) for d in get('days', '0,1,2,3,4').split(',') if d}
    start, end = int(get('start', '10')), int(get('end', '19'))
    intervals = []
    for delta in range(-1, 9):
        date = local.date() + timedelta(days=delta)
        if date.weekday() in days:
            begin = datetime.combine(date, datetime.min.time(), tz) + timedelta(hours=0 if start == end else start)
            finish = begin + timedelta(hours=(end - start) % 24 or 24)
            intervals.append((begin, finish))
    enabled = get('enabled', '0') == '1'
    working = not enabled or any(a <= local < b for a, b in intervals)
    next_start = next((a for a, b in intervals if a > local), None)
    hours = math.ceil(((next_start - local).total_seconds() if next_start else 0) / 3600 + 6)
    return dict(enabled=enabled, working=working, days=days, start=f'{start:02}:00', end=f'{end:02}:00',
                tz=int(get('tz', '3')), wait=hour_words(hours))


def hour_words(n):
    word = 'час' if n % 10 == 1 and n % 100 != 11 else (
        'часа' if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14 else 'часов')
    return f'{n} {word}'


def offhours_text(bot_id, schedule):
    text = db_get_bot_setting(bot_id, 'txt_offhours', DEFAULT_OFFHOURS)
    # Только известные подстановки: скобки пользователя не интерпретируются.
    return re.sub(r'\{(start|end|wait)\}', lambda m: schedule[m[1]], text)


def normalize_phrase(text):
    return ' '.join(re.findall(r'[^\W_]+', (text or '').casefold().replace('ё', 'е'), re.UNICODE))


def parse_phrases(text):
    phrases = [s.strip() for s in re.split(r'[,\n]', text or '') if s.strip()]
    if not 1 <= len(phrases) <= 20 or any(not 2 <= len(s) <= 50 or not normalize_phrase(s) for s in phrases):
        raise ValueError('Укажи от 1 до 20 фраз по 2–50 символов через запятую или с новой строки')
    return phrases


def matching_rules(bot_id, text, schedule=None):
    schedule = schedule or support_schedule(bot_id)
    normalized = ' ' + normalize_phrase(text) + ' '
    return [r for r in db.db_auto_rules(bot_id)
            if (not r['offhours'] or (schedule['enabled'] and not schedule['working']))
            and any(' ' + normalize_phrase(s) + ' ' in normalized for s in r['phrases'])]

def make_purchased_bot(db_bot_id: int, token: str, admin_id: int, main_bot=None):
    register_secret(token)
    pbot = telebot.TeleBot(token)
    try:
        return _configure_purchased_bot(pbot, db_bot_id, token, admin_id, main_bot)
    except BaseException:
        _close_bot(pbot)
        raise


def _configure_purchased_bot(pbot, db_bot_id, token, admin_id, main_bot):
    pbot._db_bot_id = db_bot_id
    from premium_emoji import protect_premium_emoji
    protect_premium_emoji(pbot, db_bot_id)
    pstate = DBState(db_bot_id)
    protect_text_lengths(pbot)
    flow_lock = threading.RLock()
    faq_input = {}

    def _commit_input(uid, expected, save, revision=None, next_faq=None):
        """Проверка актуальности и запись не могут перемежаться с отменой ввода"""
        with flow_lock:
            if revision is not None:
                if pstate.snapshot(uid)[1] != revision: return False
            elif faq_input.get(uid) is not expected:
                return False
            save()
            if revision is not None: pstate.finish(uid, revision)
            elif next_faq is not None: faq_input[uid] = next_faq
            else: faq_input.pop(uid, None)
            return True

    def _set_flow(uid, state, revision=None):
        with flow_lock:
            if revision is not None and not pstate.transition(uid, revision, state):
                return False
            faq_input.pop(uid, None)
            if revision is None:
                pstate[uid] = state
            return True

    def _reset_flow(uid):
        with flow_lock:
            faq_input.pop(uid, None)
            state = pstate.pop(uid, None)
            return state

    def _take_flow(uid, prefix):
        # Забираем состояние ДО сетевого вызова: его завершение не может
        # стереть новый диалог, даже если заново выбран тот же адресат
        with flow_lock:
            state = pstate.take(uid, lambda v: isinstance(v, str) and v.startswith(prefix))
            if state is not None:
                faq_input.pop(uid, None)
            return state

    def _edit(text, chat_id, message_id, reply_markup=None, parse_mode='HTML'):
        try:
            return edit_html(pbot, chat_id, message_id, text, reply_markup, parse_mode)
        except Exception as exc:
            log.warning('_edit failed: %s', exc)
            return None


    # Временные сбои Telegram (502/503/504, сеть) не должны засорять логи
    # ERROR-трейсбеками: infinity_polling повторяет сам, пишем WARNING раз в 5 минут.
    # ВАЖНО: telebot вызывает exception_handler.handle(e) — нужен ОБЪЕКТ с .handle(),
    # функция сломает polling-поток ('function' object has no attribute 'handle').
    class _ChildEH:
        def __init__(self):
            self._last_transient = 0
        def handle(self, e):
            err_code = getattr(e, 'error_code', None)
            s = str(e)
            if (err_code in (502, 503, 504) or 'Bad Gateway' in s
                    or 'Read timed out' in s or 'Connection aborted' in s
                    or 'Max retries exceeded' in s):
                now = time.time()
                if now - self._last_transient > 300:
                    self._last_transient = now
                    log.warning(f'Bot #{db_bot_id}: Telegram API временно недоступен — '
                                'polling продолжается автоматически')
                return True
            if '401' in s or 'Unauthorized' in s:
                log.error(f'Bot #{db_bot_id} handler exception: {e}')
                return True
            log.error(f'Bot #{db_bot_id} handler exception: {e}', exc_info=True)
            return True
    pbot.exception_handler = _ChildEH()

    def get_welcome():
        return db_get_bot_setting(db_bot_id, 'welcome', DEFAULT_WELCOME)

    def _get_exp_str() -> str:
        raw = db_get_bot_expires(db_bot_id)
        if not raw:
            return '—'
        try:
            exp = datetime.fromisoformat(str(raw))
            days_left = subscription_days_left(exp)
            if days_left > 0:
                return f"{str(raw)[:10]} (осталось {days_left} дн)"
            else:
                return f"{str(raw)[:10]} {TE_CROSS} истекла"
        except Exception:
            return '—'

    def is_admin(user_id: int) -> bool:
        return user_id in db_get_bot_admins(db_bot_id)

    def is_primary_admin(user_id: int) -> bool:
        info = db_get_bot_info(db_bot_id)
        return bool(info and user_id == info[1])

    def _require_owner(event):
        info = db_get_bot_info(db_bot_id)
        if info and event.from_user.id == info[2]:
            return True
        _reset_flow(event.from_user.id)
        if isinstance(event, telebot.types.CallbackQuery):
            pbot.answer_callback_query(event.id, "Доступно только владельцу бота", show_alert=True)
        else:
            pbot.send_message(event.chat.id, "Доступно только владельцу бота")
        return False

    # ── Клавиатуры ─────────────────────────────────────
    def pk_start(show_admin: bool = False):
        kb = InlineKeyboardMarkup()
        kb.add(_pbtn('Отправить сообщение', icon='📤', style='primary', callback_data='p_send'))
        if db.db_faq_nodes(db_bot_id):
            kb.add(_pbtn('FAQ', icon='❓', callback_data='fq_u_0_0'))
        if show_admin:
            kb.add(_pbtn('Панель админа', icon='⌨️', style='primary', callback_data='p_open_admin'))
        if db_is_marketplace_enabled(db_bot_id) and BOT_USERNAME:
            kb.add(_pbtn('Купить такого бота', icon='💎', style='success',
                         url=f'https://t.me/{BOT_USERNAME}?start=via_{db_bot_id}'))
        return kb

    def pk_back():
        kb = InlineKeyboardMarkup()
        kb.add(_pbtn('Назад', icon='🔙', callback_data='p_cancel'))
        return kb

    def pk_reply_input_with_templates(target_id):
        kb = InlineKeyboardMarkup()
        kb.row(
            _pbtn('Назад',    icon='🔙',                callback_data=f'p_back_reply_{target_id}'),
            _pbtn('Шаблоны', icon='📝', style='primary',           callback_data=f'p_templates_{target_id}'),
        )
        return kb

    def pk_templates_list(target_id):
        templates = db_get_templates(db_bot_id)
        kb = InlineKeyboardMarkup()
        for tid, text in templates:
            preview = text[:35] + '…' if len(text) > 35 else text
            kb.add(_pbtn(preview, icon='💬', style='primary', callback_data=f'p_use_tpl_{target_id}_{tid}'))
        kb.add(_pbtn('Добавить шаблон', icon='➕', style='success', callback_data=f'p_add_tpl_{target_id}'))
        if templates:
            kb.add(_pbtn('Удалить', icon='❌',  style='danger',  callback_data=f'p_del_tpl_menu_{target_id}'))
        kb.add(_pbtn('Назад', icon='🔙', callback_data=f'p_back_reply_{target_id}'))
        return kb

    def pk_close():
        kb = InlineKeyboardMarkup()
        kb.add(_pbtn('Закрыть', icon='❌', style='danger', callback_data='p_close'))
        return kb

    def pk_back_admin():
        kb = InlineKeyboardMarkup()
        kb.add(_pbtn('Назад', icon='🔙', callback_data='p_back_admin'))
        return kb

    def pk_reply(uid):
        kb = InlineKeyboardMarkup()
        kb.row(
            _pbtn('Ответить',      icon='📤', style='primary', callback_data=f'p_reply_{uid}'),
            _pbtn('Заблокировать', icon='🚫', style='danger',  callback_data=f'p_block_{uid}'),
        )
        return kb

    def pk_admin(uid=None):
        kb = InlineKeyboardMarkup()
        kb.row(
            _pbtn('Статистика', icon='📊',  style='primary', callback_data='p_stats'),
            _pbtn('Мой баланс', icon='💰',  style='success', callback_data='p_balance'),
        )
        kb.add(_pbtn('Рассылка', icon='📣', style='primary', callback_data='p_broadcast'))
        if is_primary_admin(uid):
            kb.row(_pbtn('Тексты', icon='✏️', callback_data='ps_texts'),
                   _pbtn('Рабочее время', icon='⏰', callback_data='ps_hours'))
            kb.row(_pbtn('Автоответы', icon='🤖', callback_data='ps_rules_0'),
                   _pbtn('Шаблоны', icon='📝', callback_data='p_admin_templates'))
            kb.add(_pbtn('FAQ', icon='❓', callback_data='fq_e_0_0'))
        else:
            kb.add(_pbtn('Шаблоны', icon='📝', callback_data='p_admin_templates'))
        kb.add(_pbtn('Группа техподдержки', icon='💬', style='primary', callback_data='fs_group_menu'))
        kb.add(_pbtn('Заблокированные', icon='🚫',     style='primary', callback_data='p_blocked_list'))
        kb.add(_pbtn('Управление админами', icon='👥', style='primary', callback_data='p_admins'))
        kb.add(_pbtn('Удалить бота', icon='❌',        style='danger',  callback_data='p_delete_bot'))
        return kb

    def pk_confirm_delete():
        kb = InlineKeyboardMarkup()
        kb.row(
            _pbtn('Да, удалить', icon='✔️', style='danger',  callback_data='p_confirm_delete'),
            _pbtn('Отмена',      icon='❌', style='primary', callback_data='p_cancel_delete'),
        )
        return kb

    def settings_access(event):
        if is_primary_admin(event.from_user.id):
            return True
        _reset_flow(event.from_user.id)
        if isinstance(event, telebot.types.CallbackQuery):
            pbot.answer_callback_query(event.id, 'Доступно только главному админу', show_alert=True)
        else:
            pbot.send_message(event.chat.id, 'Доступно только главному админу')
        return False

    # Иконки служебных кнопок экранов настроек и FAQ: по точной подписи или по действию (callback),
    # чтобы подписи, введённые админом (правила, разделы FAQ), иконку не получали
    _SETTINGS_ICONS = {
        'Изменить': '✏️', 'Сбросить': '🔄', 'Назад': '🔙', 'Отмена': '❌', 'Добавить': '➕',
        'Проверить фразу': '🔍', 'Изменить фразы': '✏️', 'Изменить текст': '📝', 'Удалить': '❌',
        'Да, удалить': '❌', 'Добавить кнопку': '➕', 'Название': '✏️', 'Текст': '📝',
        'Как видит пользователь': '👤', 'Не нашёл ответ — написать': '📤', 'Главное меню': '🔙',
        'К редактору': '🔙', 'Приветствие': '💬', 'Сообщение отправлено': '✔️', 'Нерабочее время': '⏰',
        'Включено': '✔️', 'Выключено': '❌',
    }

    def _settings_icon(label, data):
        if data in ('ps_grid_start', 'ps_grid_end', 'ps_grid_tz') or data.startswith('ps_flag_'):
            return '⏰'
        return _SETTINGS_ICONS.get(label)

    def settings_show(cb, text, rows):
        kb = InlineKeyboardMarkup()
        for row in rows:
            kb.row(*[_pbtn(label, icon=_settings_icon(label, data), callback_data=data) for label, data in row])
        _edit(text, cb.message.chat.id, cb.message.message_id, kb)

    def settings_input(cb, step, prompt, back, **values):
        _set_flow(cb.from_user.id, dict(step='support_input', action=step, back=back, **values))
        settings_show(cb, prompt, [[('Отмена', back)]])

    def show_text(cb, key):
        title, default = TEXTS[key]
        current = db_get_bot_setting(db_bot_id, key, default)
        from telegram_text import html_chunks
        preview = html_chunks(current, 3500)[0]
        settings_show(cb, f'<b>{title}</b>\n\n{preview}', [
            [('Изменить', 'ps_edit_' + key), ('Сбросить', 'ps_reset_' + key)], [('Назад', 'ps_texts')]])

    def show_hours(cb):
        s = support_schedule(db_bot_id)
        rows = [[('Включено' if s['enabled'] else 'Выключено', 'ps_hours_toggle')]]
        labels = ('Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс')
        for i in range(0, 7, 4):
            rows.append([(('✅ ' if d in s['days'] else '') + labels[d], f'ps_day_{d}') for d in range(i, min(i + 4, 7))])
        rows += [[('С ' + s['start'], 'ps_grid_start'), ('До ' + s['end'], 'ps_grid_end')],
                 [(f"Часовой пояс: UTC{s['tz']:+}", 'ps_grid_tz')], [('Назад', 'p_back_admin')]]
        settings_show(cb, '<b>Рабочее время</b>\n' + ('Включено' if s['enabled'] else 'Выключено'), rows)

    def show_rules(cb, page=0):
        rules = db.db_auto_rules(db_bot_id)
        page = max(0, min(page, max(0, (len(rules) - 1) // 8)))
        rows = [[(', '.join(r['phrases'])[:40], f"ps_rule_{r['id']}")] for r in rules[page*8:page*8+8]]
        nav = []
        if page: nav.append(('◀️', f'ps_rules_{page-1}'))
        if (page+1)*8 < len(rules): nav.append(('▶️', f'ps_rules_{page+1}'))
        if nav: rows.append(nav)
        rows += [[('Добавить', 'ps_add'), ('Проверить фразу', 'ps_test')], [('Назад', 'p_back_admin')]]
        settings_show(cb, f'<b>Автоответы ({len(rules)}/30)</b>', rows)

    def show_rule(cb, rule):
        from telegram_text import html_chunks
        rid = rule['id']
        text = '<b>Фразы:</b> ' + html.escape(', '.join(rule['phrases'])) + '\n\n' + rule['text']
        settings_show(cb, html_chunks(text, 3800)[0], [
            [('Изменить фразы', f'ps_phrases_{rid}'), ('Изменить текст', f'ps_answer_{rid}')],
            [('Только в нерабочее время: ' + ('да' if rule['offhours'] else 'нет'), f'ps_flag_{rid}')],
            [('Удалить', f'ps_delete_{rid}')], [('Назад', 'ps_rules_0')]])

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('ps_') or c.data == 'p_edit_welcome')
    def support_settings_cb(cb):
        if not settings_access(cb): return
        _reset_flow(cb.from_user.id)
        data = cb.data
        if data == 'p_edit_welcome': data = 'ps_edit_welcome'
        if data == 'ps_texts':
            settings_show(cb, '<b>Тексты</b>', [[(title, 'ps_text_' + key)] for key, (title, _) in TEXTS.items()] + [[('Назад', 'p_back_admin')]])
        elif data.startswith(('ps_text_', 'ps_edit_', 'ps_reset_')):
            action, key = data[3:].split('_', 1)
            if key not in TEXTS: return
            if action == 'reset': db_set_bot_setting(db_bot_id, key, TEXTS[key][1])
            if action == 'edit':
                hint = '\nПодстановки: {start}, {end}, {wait}' if key == 'txt_offhours' else ''
                settings_input(cb, 'text', f'Введи новый текст: «{TEXTS[key][0]}». Форматирование сохраняется{hint}', 'ps_text_' + key, key=key)
            else: show_text(cb, key)
        elif data.startswith('ps_hours'):
            if data.endswith('_toggle'):
                old = db_get_bot_setting(db_bot_id, 'hours_enabled', '0')
                db_set_bot_setting(db_bot_id, 'hours_enabled', '0' if old == '1' else '1')
            show_hours(cb)
        elif data.startswith('ps_day_'):
            day = int(data[7:])
            if day not in range(7): return
            days = support_schedule(db_bot_id)['days']
            days.symmetric_difference_update({day})
            if not days:
                pbot.answer_callback_query(cb.id, 'Выбери хотя бы один рабочий день', show_alert=True)
            else: db_set_bot_setting(db_bot_id, 'hours_days', ','.join(map(str, sorted(days))))
            show_hours(cb)
        elif data.startswith('ps_grid_'):
            key = data[8:]
            if key not in ('start', 'end', 'tz'): return
            vals = list(range(-12, 15)) if key == 'tz' else list(range(24))
            buttons = [(f'UTC{v:+}' if key == 'tz' else f'{v:02}', f'ps_set_{key}_{v}') for v in vals]
            settings_show(cb, 'Выбери часовой пояс' if key == 'tz' else 'Выбери час',
                          [buttons[i:i+6] for i in range(0, len(buttons), 6)] + [[('Назад', 'ps_hours')]])
        elif data.startswith('ps_set_'):
            key, val = data[7:].rsplit('_', 1)
            if key not in ('start', 'end', 'tz'): return
            if int(val) not in (range(-12, 15) if key == 'tz' else range(24)): return
            db_set_bot_setting(db_bot_id, 'hours_' + key, val)
            show_hours(cb)
        elif data.startswith('ps_rules_'): show_rules(cb, int(data[9:]))
        elif data == 'ps_add':
            if len(db.db_auto_rules(db_bot_id)) >= 30:
                pbot.answer_callback_query(cb.id, 'Можно добавить не больше 30 правил', show_alert=True)
                return
            settings_input(cb, 'new_phrases', 'Введи фразы через запятую или с новой строки (до 20, по 2–50 символов)', 'ps_rules_0')
        elif data == 'ps_test':
            settings_input(cb, 'test', 'Введи текст для проверки автоответа', 'ps_rules_0')
        else:
            try: action, rid = data[3:].rsplit('_', 1); rid = int(rid)
            except ValueError: return
            rule = next((r for r in db.db_auto_rules(db_bot_id) if r['id'] == rid), None)
            if not rule:
                pbot.answer_callback_query(cb.id, 'Правило больше недоступно', show_alert=True)
                show_rules(cb)
                return
            if action in ('phrases', 'answer'):
                settings_input(cb, action, 'Введи фразы через запятую или с новой строки' if action == 'phrases' else 'Введи текст ответа. Форматирование сохраняется', f'ps_rule_{rid}', rid=rid)
            elif action == 'delete':
                settings_show(cb, 'Удалить правило?', [[('Да, удалить', f'ps_remove_{rid}'), ('Отмена', f'ps_rule_{rid}')]])
            elif action == 'remove':
                db.db_auto_delete(db_bot_id, rid); show_rules(cb)
            else:
                if action == 'flag':
                    rule['offhours'] = not rule['offhours']
                    db.db_auto_save(db_bot_id, rule['phrases'], rule['text'], rid, rule['offhours'])
                show_rule(cb, rule)

    @pbot.message_handler(func=lambda m: isinstance(pstate.get(m.from_user.id), dict)
                         and pstate[m.from_user.id].get('step') == 'support_input'
                         and (m.text or '').split(' ', 1)[0].split('@', 1)[0]
                         not in ('/start', '/admin', '/status', '/templates'))
    def support_settings_input(m):
        if not settings_access(m): return
        state, revision = pstate.snapshot(m.from_user.id)
        if not isinstance(state, dict) or state.get('step') != 'support_input': return
        action = state['action']
        try:
            if action == 'test':
                rules = matching_rules(db_bot_id, m.text)
                result = 'Сработало бы правило: ' + ', '.join(rules[0]['phrases']) if rules else 'Ни одно правило не подходит'
            elif action in ('new_phrases', 'phrases'):
                phrases = parse_phrases(m.text)
                if action == 'new_phrases':
                    if not _set_flow(m.from_user.id, dict(step='support_input', action='new_answer', phrases=phrases, back='ps_rules_0'), revision): return
                    kb = InlineKeyboardMarkup(); kb.add(_pbtn('Отмена', icon='❌', callback_data='ps_rules_0'))
                    pbot.send_message(m.chat.id, 'Введи текст ответа. Форматирование сохраняется', reply_markup=kb)
                    return
                rule = next((r for r in db.db_auto_rules(db_bot_id) if r['id'] == state['rid']), None)
                if not rule: raise ValueError('Правило больше недоступно')
                if not _commit_input(m.from_user.id, state, lambda: db.db_auto_save(
                        db_bot_id, phrases, rule['text'], rule['id'], rule['offhours']), revision): return
                result = 'Фразы сохранены'
            else:
                text = m.html_text or ''
                validate_support_html(text, 3600 if action == 'text' and state['key'] == 'txt_offhours' else 4000)
                if action == 'text' and state['key'] == 'txt_offhours':
                    validate_support_html(re.sub(r'\{(start|end|wait)\}',
                        lambda m: '999 часов' if m[1] == 'wait' else '23:00', text))
                pbot.send_message(m.chat.id, text, parse_mode='HTML')
                def save():
                    if action == 'text': db_set_bot_setting(db_bot_id, state['key'], text)
                    elif action == 'new_answer': db.db_auto_save(db_bot_id, state['phrases'], text)
                    else:
                        rule = next((r for r in db.db_auto_rules(db_bot_id) if r['id'] == state['rid']), None)
                        if not rule: raise ValueError('Правило больше недоступно')
                        db.db_auto_save(db_bot_id, rule['phrases'], text, rule['id'], rule['offhours'])
                if not _commit_input(m.from_user.id, state, save, revision): return
                result = 'Текст сохранён'
        except Exception as exc:
            result = str(exc) if isinstance(exc, ValueError) else 'Текст не сохранён: проверь HTML и попробуй ещё раз'
            kb = InlineKeyboardMarkup(); kb.add(_pbtn('Отмена', icon='❌', callback_data=state['back']))
            pbot.send_message(m.chat.id, result, reply_markup=kb)
            return
        pstate.finish(m.from_user.id, revision)
        kb = InlineKeyboardMarkup(); kb.add(_pbtn('Назад', icon='🔙', callback_data=state['back']))
        pbot.send_message(m.chat.id, result, reply_markup=kb)

    def after_delivery(messages, group, mappings):
        uid = messages[0].from_user.id
        if is_admin(uid) or db_is_blocked(db_bot_id, uid): return
        eligible = [m for m in messages if not (m.text or '').startswith('/')]
        if not eligible: return
        now = time.time()
        schedule = support_schedule(db_bot_id, now)
        if schedule['enabled'] and not schedule['working']:
            db.db_support_notify(db_bot_id, uid, 'offhours', offhours_text(db_bot_id, schedule), [], now, 21600)
        batches = [eligible] if messages[0].media_group_id else [[m] for m in eligible]
        for batch in batches:
            text = ' '.join(m.text or m.caption or '' for m in batch)
            for rule in matching_rules(db_bot_id, text, schedule):
                preview = html.escape(' '.join(re.sub('<[^>]+>', '', rule['text']).split())[:80])
                notice = sorted({(chat, thread, f'{TE_ROBOT} Автоответ: {preview}') for chat, mid, thread, src in mappings})
                if db.db_support_notify(db_bot_id, uid, 'rule:' + str(rule['id']), rule['text'], notice, now, 43200,
                                        album=messages[0].media_group_id):
                    break

    def pump_notifications():
        import forum_support as fs
        for row in db.db_support_notifications(db_bot_id, time.time()):
            if db_is_blocked(db_bot_id, row[1]):
                db.db_support_notification_done(db_bot_id, row)
                continue
            try:
                if row[6] == 0:
                    pbot.send_message(row[1], row[3], parse_mode='HTML')
                    db.db_support_notification_sent(db_bot_id, row, time.time())
                remaining = json.loads(row[4])
                while remaining:
                    chat, thread, text = remaining[0]
                    if thread: fs._topic_notice(pbot, chat, thread, text, strict=True)
                    else: pbot.send_message(chat, text, parse_mode='HTML')
                    remaining.pop(0)
                    db.db_support_notification_done(db_bot_id, row, remaining)
            except Exception as exc:
                params = (getattr(exc, 'result_json', None) or {}).get('parameters', {})
                delay = max(0, float(params.get('retry_after', 1))) if getattr(exc, 'error_code', None) == 429 else min(300, 2 ** min(row[5], 9))
                db.db_support_notification_retry(db_bot_id, row, time.time() + delay)
                continue
            db.db_support_notification_done(db_bot_id, row)

    pbot._after_delivery = after_delivery
    pbot._pump_notifications = pump_notifications

    def faq_back(node, mode):
        siblings = db.db_faq_nodes(db_bot_id, node['parent_id'])
        index = next((i for i, n in enumerate(siblings) if n['id'] == node['id']), 0)
        return f"fq_{mode}_{node['parent_id'] or 0}_{index // 8}"

    def faq_show(cb, node_id=0, page=0, mode='u'):
        from telegram_text import html_chunks
        node = db.db_faq_get(db_bot_id, node_id) if node_id else None
        if node_id and not node:
            pbot.answer_callback_query(cb.id, 'Раздел больше недоступен', show_alert=True)
            node_id, page = 0, 0
        children = db.db_faq_nodes(db_bot_id, node_id or None)
        pages = max(1, (len(children) + 7) // 8)
        page = max(0, min(page, pages - 1))
        text = f"<b>{html.escape(node['title'])}</b>" + ('\n\n' + node['text'] if node['text'] else '') if node else f'<b>{TE_NOTE} FAQ</b>\n\nВыбери вопрос'
        rows = [[(n['title'], f"fq_{mode}_{n['id']}_0")] for n in children[page*8:page*8+8]]
        if pages > 1:
            nav = []
            if page: nav.append(('◀️', f'fq_{mode}_{node_id}_{page-1}'))
            nav.append((f'{page+1}/{pages}', 'p_noop'))
            if page+1 < pages: nav.append(('▶️', f'fq_{mode}_{node_id}_{page+1}'))
            rows.append(nav)
        if mode == 'e':
            rows += [[('Добавить кнопку', f'fq_add_{node_id}')]]
            if node:
                rows += [[('Название', f'fq_name_{node_id}'), ('Текст', f'fq_text_{node_id}')],
                         [('⬆️', f'fq_up_{node_id}'), ('⬇️', f'fq_down_{node_id}')],
                         [('Удалить', f'fq_del_{node_id}')]]
            rows += [[('Как видит пользователь', f'fq_v_{node_id}_{page}')],
                     [('Назад', faq_back(node, mode) if node else 'p_back_admin')]]
        else:
            if not node: rows.append([('Не нашёл ответ — написать', 'p_send')])
            rows += [[('Назад', faq_back(node, mode) if node else 'p_cancel')]]
            if node: rows.append([('Главное меню', 'p_cancel')])
            if mode == 'v': rows.append([('К редактору', f'fq_e_{node_id}_{page}')])
        settings_show(cb, text, rows)

    def faq_prompt(cb, action, node_id, expected=None, **values):
        prompts = {'add': 'Введи название кнопки (1–40 символов)',
                   'name': 'Введи новое название (1–40 символов)',
                   'text': 'Введи текст раздела. Форматирование сохраняется',
                   'new_text': 'Введи текст раздела или нажми «Пропустить». Форматирование сохраняется'}
        value = dict(action=action, node_id=node_id, **values)
        if expected is not None:
            if not _commit_input(cb.from_user.id, expected, lambda: None, next_faq=value): return
        else:
            with flow_lock: faq_input[cb.from_user.id] = value
        rows = [[('Отмена', f'fq_e_{node_id}_0')]]
        if action == 'new_text': rows.insert(0, [('Пропустить', f'fq_skip_{node_id}')])
        settings_show(cb, prompts[action], rows)

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('fq_'))
    def faq_callback(cb):
        parts = cb.data.split('_')
        if len(parts) not in (3, 4): return
        action = parts[1]
        try:
            node_id = int(parts[2]); page = int(parts[3]) if len(parts) == 4 else 0
        except ValueError: return
        if action != 'u' and not settings_access(cb): return
        pending = faq_input.get(cb.from_user.id)
        _reset_flow(cb.from_user.id)
        if action in ('u', 'e', 'v'):
            faq_show(cb, node_id, page, action)
            return
        node = db.db_faq_get(db_bot_id, node_id) if node_id else None
        if node_id and not node:
            pbot.answer_callback_query(cb.id, 'Раздел больше недоступен', show_alert=True)
            faq_show(cb, mode='e')
            return
        try:
            if action in ('add', 'name', 'text'):
                if action != 'add' and not node: return
                faq_prompt(cb, action, node_id)
            elif action == 'skip':
                if not pending or pending['action'] != 'new_text' or pending['node_id'] != node_id: return
                new_id = db.db_faq_add(db_bot_id, node_id or None, pending['title'])
                faq_show(cb, new_id, mode='e')
            elif action in ('up', 'down') and node:
                db.db_faq_move(db_bot_id, node_id, -1 if action == 'up' else 1)
                faq_show(cb, node_id, mode='e')
            elif action == 'del' and node:
                count = db.db_faq_subtree(db_bot_id, node_id) - 1
                kb = InlineKeyboardMarkup()
                kb.row(_pbtn('Да, удалить', icon='❌', callback_data=f'fq_yes_{node_id}', style='danger'),
                       _pbtn('Отмена', icon='❌', callback_data=f'fq_e_{node_id}_0'))
                _edit(f"Удалить «{html.escape(node['title'])}» и всё, что внутри ({count})?", cb.message.chat.id, cb.message.message_id, kb)
            elif action == 'yes' and node:
                parent = node['parent_id'] or 0
                db.db_faq_subtree(db_bot_id, node_id, delete=True)
                faq_show(cb, parent, mode='e')
        except ValueError as exc:
            pbot.answer_callback_query(cb.id, str(exc), show_alert=True)
            faq_show(cb, node_id, mode='e')

    def faq_open_saved(m, node_id):
        sent = pbot.send_message(m.chat.id, 'Раздел сохранён')
        cb = SimpleNamespace(message=SimpleNamespace(chat=m.chat, message_id=sent.message_id))
        faq_show(cb, node_id, mode='e')

    @pbot.message_handler(func=lambda m: m.from_user.id in faq_input
                         and pstate.get(m.from_user.id) is None and not m.reply_to_message
                         and (m.text or '').split(' ', 1)[0].split('@', 1)[0]
                         not in ('/start', '/admin', '/status', '/templates'))
    def faq_save_input(m):
        if not settings_access(m): return
        state = faq_input.get(m.from_user.id)
        action, node_id = state['action'], state['node_id']
        try:
            if action in ('add', 'name'):
                title = (m.text or '').strip()
                if not 1 <= len(title) <= 40: raise ValueError('Название должно содержать от 1 до 40 символов')
                if action == 'add':
                    sent = pbot.send_message(m.chat.id, 'Название принято')
                    cb = SimpleNamespace(from_user=m.from_user, message=SimpleNamespace(chat=m.chat, message_id=sent.message_id))
                    faq_prompt(cb, 'new_text', node_id, expected=state, title=title)
                    return
                if not _commit_input(m.from_user.id, state,
                        lambda: db.db_faq_edit(db_bot_id, node_id, 'title', title)): return
            else:
                text = m.html_text or ''
                db.validate_faq_text(text)
                validate_support_html(text, db.FAQ_TEXT_LIMIT)
                from telegram_text import send_html
                send_html(pbot, m.chat.id, text)
                def save():
                    nonlocal node_id
                    if action == 'new_text': node_id = db.db_faq_add(db_bot_id, node_id or None, state['title'], text)
                    else: db.db_faq_edit(db_bot_id, node_id, 'text', text)
                if not _commit_input(m.from_user.id, state, save): return
            faq_open_saved(m, node_id)
        except Exception as exc:
            message = str(exc) if isinstance(exc, ValueError) else 'Текст не сохранён: проверь HTML и попробуй ещё раз'
            kb = InlineKeyboardMarkup(); kb.add(_pbtn('Отмена', icon='❌', callback_data=f'fq_e_{state["node_id"]}_0'))
            pbot.send_message(m.chat.id, message, reply_markup=kb)

    # ── Команды ────────────────────────────────────────
    @pbot.message_handler(commands=['start'])
    def pstart(m):
        _reset_flow(m.from_user.id)
        db_add_user(db_bot_id, m.from_user.id, m.from_user.username or '')
        w = get_welcome()
        try:
            pbot.send_message(m.chat.id, w, parse_mode='HTML',
                              reply_markup=pk_start(is_admin(m.from_user.id)))
        except Exception:
            # битый HTML в приветствии (введён админом) не должен «убивать» /start:
            # отправляем как plain text — бот всегда отвечает на /start
            pbot.send_message(m.chat.id, w,
                              reply_markup=pk_start(is_admin(m.from_user.id)))

    @pbot.message_handler(commands=['admin'])
    def padmin_cmd(m):
        if not is_admin(m.from_user.id): return
        _reset_flow(m.from_user.id)
        pbot.send_message(m.chat.id,
            f"<b>{TE_KEYBOARD} Панель админа</b>\n\n{TE_HOURGLASS} Подписка до: <b>{_get_exp_str()}</b>",
            parse_mode='HTML', reply_markup=pk_admin(m.from_user.id))

    @pbot.message_handler(commands=['status'])
    def pstatus_cmd(m):
        if not is_admin(m.from_user.id): return
        users_cnt   = len(db_get_bot_users(db_bot_id))
        blocked_cnt = len(db_get_blocked_list(db_bot_id))
        admins_cnt  = len(db_get_bot_admins(db_bot_id))
        info        = db_get_bot_info(db_bot_id)
        created_at  = str(info[3])[:10] if info else '—'
        pbot.send_message(m.chat.id,
            f"<b>{TE_STATS} Статус бота</b>\n\n"
            f"{TE_USERS} Пользователей: <b>{users_cnt}</b>\n"
            f"{TE_BLOCK} Заблокированных: <b>{blocked_cnt}</b>\n"
            f"{TE_USER} Администраторов: <b>{admins_cnt}</b>\n"
            f"{TE_CALENDAR} Дата создания: <b>{created_at}</b>\n"
            f"{TE_HOURGLASS} Подписка до: <b>{_get_exp_str()}</b>\n"
            f"{TE_GREEN} Статус: активен",
            parse_mode='HTML', reply_markup=pk_close())

    @pbot.message_handler(commands=['templates'])
    def p_templates_cmd(m):
        if not is_admin(m.from_user.id): return
        templates = db_get_templates(db_bot_id)[:50]
        if not templates:
            pbot.send_message(m.chat.id, f"<b>{TE_NOTE} Шаблоны пусты\n\nИспользуй кнопку «{TE_NOTE} Шаблоны» при ответе, чтобы добавить</b>",
                parse_mode='HTML', reply_markup=pk_close())
            return
        lines = [f"<b>{TE_NOTE} Сохранённые шаблоны:</b>\n"]
        for tid, text in templates[:50]:
            # экранируем: шаблон вводит админ, '<' в нём ломал бы весь список (400)
            lines.append(f"• [{tid}] {html.escape(str(text))}")
        pbot.send_message(m.chat.id, "\n".join(lines),
            parse_mode='HTML', reply_markup=pk_close())

    # ── Навигация ──────────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_back_admin')
    def p_back_admin_cb(cb):
        if not is_admin(cb.from_user.id): return
        _reset_flow(cb.from_user.id)
        _edit(
            f"<b>{TE_KEYBOARD} Панель админа</b>\n\n{TE_HOURGLASS} Подписка до: <b>{_get_exp_str()}</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_admin(cb.from_user.id))

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_open_admin')
    def p_open_admin_cb(cb):
        if not is_admin(cb.from_user.id): return
        _reset_flow(cb.from_user.id)
        _edit(
            f"<b>{TE_KEYBOARD} Панель админа</b>\n\n{TE_HOURGLASS} Подписка до: <b>{_get_exp_str()}</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_admin(cb.from_user.id))

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_stats')
    def p_stats_cb(cb):
        if not is_admin(cb.from_user.id): return
        s = db_get_bot_msg_stats(db_bot_id)
        _edit(
            f"<b>{TE_STATS} Статистика бота</b>\n\n"
            f"{TE_SEND} Сообщений сегодня: <b>{s['today']}</b>\n"
            f"{TE_MAIL} Сообщений за неделю: <b>{s['week']}</b>\n"
            f"{TE_USERS} Уникальных пользователей: <b>{s['total_users']}</b>\n"
            f"{TE_BLOCK} Заблокировано: <b>{s['blocked']}</b>\n\n"
            f"{TE_HOURGLASS} Подписка до: <b>{_get_exp_str()}</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back_admin())

    def pk_manage_templates():
        templates = db_get_templates(db_bot_id)[:50]
        kb = InlineKeyboardMarkup()
        for tid, text in templates:
            preview = text[:30] + '…' if len(text) > 30 else text
            kb.row(
                _pbtn(preview, icon='💬', style='primary', callback_data='p_noop'),
                _pbtn('Удалить', icon='❌', style='danger', callback_data=f'p_mgdel_tpl_{tid}'),
            )
        kb.add(_pbtn('Добавить шаблон', icon='➕', style='success', callback_data='p_mgadd_tpl'))
        kb.add(_pbtn('Назад', icon='🔙', callback_data='p_back_admin'))
        return kb, templates

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_admin_templates')
    def p_admin_templates_cb(cb):
        if not is_admin(cb.from_user.id): return
        _reset_flow(cb.from_user.id)
        kb, templates = pk_manage_templates()
        header = f"<b>{TE_NOTE} Шаблоны ответов ({len(templates)}):</b>" if templates else f"<b>{TE_NOTE} Шаблоны ответов пусты</b>"
        _edit(header, cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=kb)

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_mgadd_tpl')
    def p_mgadd_tpl_cb(cb):
        if not is_admin(cb.from_user.id): return
        _set_flow(cb.from_user.id, 'mgadd_tpl')
        kb = InlineKeyboardMarkup()
        kb.add(_pbtn('Отмена', icon='❌', style='danger', callback_data='p_admin_templates'))
        _edit(f"<b>{TE_PLUS} Введи текст нового шаблона:</b>",
            cb.message.chat.id, cb.message.message_id, parse_mode='HTML', reply_markup=kb)

    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'mgadd_tpl')
    def p_mgadd_tpl_save(m):
        if not is_admin(m.from_user.id): return
        if _take_flow(m.from_user.id, 'mgadd_tpl') is None: return
        db_add_template(db_bot_id, m.text)
        fs_refresh_management(pbot, db_bot_id)
        kb, templates = pk_manage_templates()
        pbot.send_message(m.chat.id,
            f"<b>{TE_NOTE} Шаблоны ответов ({len(templates)}):</b>",
            parse_mode='HTML', reply_markup=kb)

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_mgdel_tpl_'))
    def p_mgdel_tpl_cb(cb):
        if not is_admin(cb.from_user.id): return
        tid = int(cb.data.split('_')[-1])
        db_del_template(tid, db_bot_id)
        fs_refresh_management(pbot, db_bot_id)
        pbot.answer_callback_query(cb.id, "✅ Шаблон удалён")
        kb, templates = pk_manage_templates()
        header = f"<b>{TE_NOTE} Шаблоны ответов ({len(templates)}):</b>" if templates else f"<b>{TE_NOTE} Шаблоны ответов пусты</b>"
        try:
            _edit(header, cb.message.chat.id, cb.message.message_id,
                parse_mode='HTML', reply_markup=kb)
        except Exception:
            pass

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_noop')
    def p_noop_cb(cb):
        pbot.answer_callback_query(cb.id)

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_cancel')
    def p_cancel(cb):
        _reset_flow(cb.from_user.id)
        _edit(get_welcome(), cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_start(is_admin(cb.from_user.id)))

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_close')
    def p_close(cb):
        # сообщение может быть старше 48 ч (Telegram не даст удалить) — не роняем
        try:
            pbot.delete_message(cb.message.chat.id, cb.message.message_id)
        except Exception:
            pass

    @pbot.callback_query_handler(func=lambda c: c.data == 'cancel')
    def p_cancel_generic(cb):
        _reset_flow(cb.from_user.id)
        # состояние сбрасываем ДО удаления: упавшее delete_message
        # (сообщение старше 48 ч) не должно оставлять юзера в состоянии
        try:
            pbot.delete_message(cb.message.chat.id, cb.message.message_id)
        except Exception:
            pass

    # ── Отправить сообщение ────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_send')
    def p_user_send(cb):
        if db_is_blocked(db_bot_id, cb.from_user.id):
            pbot.answer_callback_query(cb.id, "🚫 Вы заблокированы администратором", show_alert=True)
            return
        _set_flow(cb.from_user.id, 'await_msg')
        _edit(
            f"<b>{TE_CHAT}  Отправь сообщение, фото, видео, файл или голосовое:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back())

    _seen_mg: dict = {}
    _seen_mg_lock = threading.Lock()

    def _message_rate_limit(m):
        """Решение принимается один раз и сохраняется для всего альбома"""
        mg = getattr(m, 'media_group_id', None)
        if not mg:
            return _is_rate_limited(db_bot_id, m.from_user.id), True
        key = (db_bot_id, m.from_user.id, mg)
        now = time.time()
        with _seen_mg_lock:
            for k in [k for k, (ts, _) in _seen_mg.items() if now - ts > 120]:
                _seen_mg.pop(k, None)
            if key in _seen_mg:
                return _seen_mg[key][1], False
            limited = _is_rate_limited(db_bot_id, m.from_user.id)
            _seen_mg[key] = (now, limited)
            if len(_seen_mg) > 500:
                _seen_mg.pop(next(iter(_seen_mg)))
            return limited, True

    def _user_msg_checks(m) -> bool:
        """Общие проверки для всех входящих медиа. True = продолжать."""
        db_add_user(db_bot_id, m.from_user.id, m.from_user.username or '')
        if db_is_blocked(db_bot_id, m.from_user.id):
            pbot.send_message(m.chat.id, f"<b>{TE_BLOCK}  Вы заблокированы администратором</b>",
                parse_mode='HTML')
            _reset_flow(m.from_user.id)
            return False
        limited, first = _message_rate_limit(m)
        if limited:
            if first:
                pbot.send_message(m.chat.id,
                    f"<b>{TE_HOURGLASS} Подожди {RATE_LIMIT_SEC} сек перед следующим сообщением</b>",
                    parse_mode='HTML')
            return False
        return True

    def _deliver_direct(m):
        """Проверки и durable запись; доставка выполняется отправителем.

        Один путь для p_send и обычных сообщений, в форум и админам в личку.
        """
        if not _user_msg_checks(m):
            return True   # заблокирован/лимит — юзер уже уведомлён
        from delivery_queue import get_sender
        return get_sender(pbot, db_bot_id).enqueue(m, pk_close)

    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'await_msg')
    def p_user_text(m):
        _take_flow(m.from_user.id, 'await_msg')
        _deliver_direct(m)

    # ── Медиа ──────────────────────────────────────────
    @pbot.message_handler(
        content_types=['photo', 'video', 'document', 'audio', 'voice',
                       'video_note', 'sticker', 'animation', 'contact',
                       'location', 'venue', 'poll', 'dice'],
        func=lambda m: pstate.get(m.from_user.id) == 'await_msg'
    )
    def p_user_media(m):
        _take_flow(m.from_user.id, 'await_msg')
        _deliver_direct(m)

    # ── Ответ админа ───────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_reply_'))
    def p_admin_reply(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        # старые (>48 ч) сообщения приходят как InaccessibleMessage без полей —
        # считаем их текстовыми, содержимое берём из БД при возврате
        is_media = getattr(cb.message, 'content_type', 'text') != 'text'
        _set_flow(cb.from_user.id, f'await_reply_{target}')
        if is_media:
            # Исходное медиа с подписью остаётся в чате при любом исходе ответа
            pbot.send_message(cb.message.chat.id,
                f"<b>{TE_CHAT}  Введи ответ или отправь фото:</b>",
                parse_mode='HTML', reply_markup=pk_reply_input_with_templates(target))
        else:
            _edit(f"<b>{TE_CHAT}  Введи ответ или отправь фото:</b>",
                cb.message.chat.id, cb.message.message_id,
                parse_mode='HTML', reply_markup=pk_reply_input_with_templates(target))

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_back_reply_'))
    def p_back_reply(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        _reset_flow(cb.from_user.id)

        # Строим мини-чат: последние 2 сообщения + ID и username пользователя
        username = db_get_username(db_bot_id, target)
        user_line = f"{TE_ID} ID: <code>{target}</code>  {TE_LINK} @{username}" if username else f"{TE_ID} ID: <code>{target}</code>"
        msgs = db_get_last_user_messages(db_bot_id, target, n=2)
        if msgs:
            lines = [f"<b>{TE_INBOX} Сообщения от пользователя</b>\n{user_line}\n"]
            for msg_text, msg_ts in msgs:
                ts = str(msg_ts)[:16] if msg_ts else ''
                lines.append(f"{TE_CHAT} <i>{ts}</i>\n{html.escape(str(msg_text))}")
            restore_text = '\n\n'.join(lines)
        else:
            restore_text = f"<b>{TE_INBOX} Сообщение от пользователя</b>\n{user_line}"

        try:
            _edit(restore_text,
                cb.message.chat.id, cb.message.message_id,
                parse_mode='HTML', reply_markup=pk_reply(target))
        except Exception as e:
            log.warning(f'p_back_reply edit failed: {e}')
            pbot.send_message(cb.message.chat.id, restore_text,
                parse_mode='HTML', reply_markup=pk_reply(target))

    # ── Шаблони відповідей ──────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_templates_')
                                               and not c.data.startswith('p_templates_reply_'))
    def p_show_templates(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        templates = db_get_templates(db_bot_id)[:50]
        if not templates:
            text = f"<b>{TE_NOTE} Шаблоны пусты</b>\n\nДобавь первый шаблон:"
        else:
            text = f"<b>{TE_NOTE} Шаблоны ({len(templates)}):</b>\n\nВыбери или управляй:"
        _edit(text, cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_templates_list(target))

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_use_tpl_'))
    def p_use_template(cb):
        if not is_admin(cb.from_user.id): return
        parts = cb.data.split('_')  # p_use_tpl_{target_id}_{tpl_id}
        target_id = int(parts[3])
        tpl_id    = int(parts[4])
        text = db_get_template(tpl_id, db_bot_id)
        if not text:
            pbot.answer_callback_query(cb.id, "❌ Шаблон не найден", show_alert=True)
            return
        _take_flow(cb.from_user.id, 'await_reply_')
        from reply_queue import enqueue
        status, _ = enqueue(pbot, db_bot_id, f'template:{cb.id}',
                            dict(mode='text', target=target_id, text=text, notify=cb.from_user.id))
        if status == 'pending':
            pbot.answer_callback_query(cb.id, 'Ответ сохранён, отправлю позже')

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_add_tpl_'))
    def p_add_template_start(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        _set_flow(cb.from_user.id, f'await_add_tpl_{target}')
        _edit(f"<b>{TE_PLUS} Введи текст нового шаблона:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_reply_input_with_templates(target))

    @pbot.message_handler(func=lambda m: isinstance(pstate.get(m.from_user.id), str)
                                         and pstate[m.from_user.id].startswith('await_add_tpl_'))
    def p_save_template(m):
        if not is_admin(m.from_user.id): return
        state = _take_flow(m.from_user.id, 'await_add_tpl_')
        if state is None: return
        target = int(state.split('_')[-1])
        _set_flow(m.from_user.id, f'await_reply_{target}')
        db_add_template(db_bot_id, m.text)
        fs_refresh_management(pbot, db_bot_id)
        pbot.send_message(m.chat.id, f"<b>{TE_CHECK}  Шаблон сохранён</b>",
            parse_mode='HTML', reply_markup=pk_reply_input_with_templates(target))

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_del_tpl_menu_'))
    def p_del_tpl_menu(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        templates = db_get_templates(db_bot_id)[:50]
        kb = InlineKeyboardMarkup()
        for tid, text in templates:
            preview = text[:35] + '…' if len(text) > 35 else text
            kb.add(_pbtn(preview, icon='❌', style='danger', callback_data=f'p_del_tpl_{target}_{tid}'))
        kb.add(_pbtn('Назад', icon='🔙', callback_data=f'p_templates_{target}'))
        _edit(f"<b>{TE_CROSS} Выбери шаблон для удаления:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=kb)

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_del_tpl_')
                                               and not c.data.startswith('p_del_tpl_menu_'))
    def p_del_tpl(cb):
        if not is_admin(cb.from_user.id): return
        parts = cb.data.split('_')  # p_del_tpl_{target}_{tid}
        target = int(parts[3])
        tid    = int(parts[4])
        db_del_template(tid, db_bot_id)
        fs_refresh_management(pbot, db_bot_id)
        pbot.answer_callback_query(cb.id, "✅ Шаблон видалено")
        templates = db_get_templates(db_bot_id)[:50]
        kb = InlineKeyboardMarkup()
        for t_id, text in templates:
            preview = text[:35] + '…' if len(text) > 35 else text
            kb.add(_pbtn(preview, icon='❌', style='danger', callback_data=f'p_del_tpl_{target}_{t_id}'))
        kb.add(_pbtn('Назад', icon='🔙', callback_data=f'p_templates_{target}'))
        try:
            pbot.edit_message_reply_markup(cb.message.chat.id, cb.message.message_id, reply_markup=kb)
        except Exception as e:
            log.warning(f'p_del_tpl markup edit: {e}')

    @pbot.message_handler(func=lambda m: isinstance(pstate.get(m.from_user.id), str)
                                         and pstate[m.from_user.id].startswith('await_reply_'))
    def p_admin_text(m):
        if not is_admin(m.from_user.id):
            _reset_flow(m.from_user.id)
            _deliver_direct(m)
            return
        state = _take_flow(m.from_user.id, 'await_reply_')
        if state is None: return
        target_id = int(state.split('_')[-1])
        from reply_queue import enqueue
        status, _ = enqueue(pbot, db_bot_id, f'private:{m.chat.id}:{m.message_id}',
                            dict(mode='text', target=target_id, text=m.text or '',
                                 message=m.json, notify=m.chat.id))
        if status == 'pending':
            pbot.send_message(m.chat.id, 'Ответ сохранён, отправлю позже', reply_markup=pk_close())

    reply_albums = {}
    reply_album_routes = {}
    reply_album_lock = threading.RLock()

    def flush_reply_albums(key=None):
        with reply_album_lock:
            keys = [key] if key is not None else list(reply_albums)
            batches = [reply_albums.pop(k) for k in keys if k in reply_albums]
        for batch in batches:
            batch['timer'].cancel()
            if batch.get('queue_key'):
                from reply_queue import flush_album
                flush_album(pbot, db_bot_id, batch['queue_key'])
                continue
            if not is_admin(batch['admin']):
                continue
            from telegram_text import ordered_album
            ids = ordered_album(set(batch['ids']), key=int)
            from reply_queue import enqueue
            enqueue(pbot, db_bot_id, f"album:{batch['admin']}:{ids}",
                    dict(mode='album', target=batch['target'], chat=batch['admin'], ids=ids, notify=batch['admin']))

    pbot._flush_reply_albums = flush_reply_albums

    def stop_reply_albums():
        with reply_album_lock:
            timers = [v['timer'] for v in reply_albums.values()]
            reply_albums.clear()
            reply_album_routes.clear()
            for timer in timers: timer.cancel()
        for timer in timers:
            if timer.ident is not None and timer is not threading.current_thread(): timer.join()
    pbot._stop_reply_albums = stop_reply_albums

    def persisted_album_target(m):
        if not m.media_group_id: return None
        from reply_queue import album_target
        return album_target(db_bot_id, m.chat.id, m.media_group_id)

    @pbot.message_handler(
        content_types=['photo', 'video', 'document', 'audio', 'animation', 'voice', 'video_note', 'sticker', 'contact', 'location', 'venue', 'poll'],
        func=lambda m: isinstance(pstate.get(m.from_user.id), str)
                       and pstate[m.from_user.id].startswith('await_reply_')
                       or (m.from_user.id, m.media_group_id) in reply_album_routes
                       or persisted_album_target(m) is not None
    )
    def p_admin_photo_reply(m):
        if not is_admin(m.from_user.id):
            _reset_flow(m.from_user.id)
            _deliver_direct(m)
            return
        if m.media_group_id:
            key = (m.from_user.id, m.media_group_id)
            with reply_album_lock, flow_lock:
                state = pstate.get(m.from_user.id) if key not in reply_album_routes else None
                if isinstance(state, str) and state.startswith('await_reply_'):
                    reply_album_routes[key] = (int(state.split('_')[-1]), time.time())
                else:
                    state = None
                route = reply_album_routes.get(key)
                if not route:
                    target = persisted_album_target(m)
                    if target is not None:
                        route = reply_album_routes[key] = (target, time.time())
                if not route: return
                from reply_queue import enqueue_album
                queue_key = enqueue_album(pbot, db_bot_id, m,
                    dict(mode='album', target=route[0], chat=m.chat.id, notify=m.chat.id), delay=1)
                if state:
                    _take_flow(m.from_user.id, 'await_reply_')
                batch = reply_albums.setdefault(key, dict(admin=m.from_user.id, target=route[0], ids=[]))
                batch['queue_key'] = queue_key
                if 'timer' in batch: batch['timer'].cancel()
                batch['ids'].append(m.message_id)
                batch['timer'] = threading.Timer(1, flush_reply_albums, args=(key,))
                batch['timer'].daemon = True
                batch['timer'].start()
                for old, (_, stamp) in list(reply_album_routes.items()):
                    if time.time() - stamp > 60: reply_album_routes.pop(old, None)
            return
        state = _take_flow(m.from_user.id, 'await_reply_')
        if state is None: return
        target_id = int(state.split('_')[-1])
        from reply_queue import enqueue
        enqueue(pbot, db_bot_id, f'private:{m.chat.id}:{m.message_id}',
                dict(mode='media', target=target_id, message=m.json, notify=m.chat.id))

    # ── Блокировка ─────────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_block_'))
    def p_block_user(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        db_block_user(db_bot_id, target)
        fs_refresh_pin_markup(pbot, db_bot_id, target)
        pbot.answer_callback_query(cb.id, f"🚫 Пользователь {target} заблокирован", show_alert=True)
        kb = InlineKeyboardMarkup()
        kb.row(
            _pbtn('Ответить',      icon='📤', style='primary', callback_data=f'p_reply_{target}'),
            _pbtn('Разблокировать', icon='✔️', style='success', callback_data=f'p_unblock_{target}'),
        )
        try: pbot.edit_message_reply_markup(cb.message.chat.id, cb.message.message_id, reply_markup=kb)
        except Exception as e: log.warning(f'edit_markup: {e}')

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_unblock_')
                                               and not c.data.startswith('p_unblock_list_'))
    def p_unblock_user(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        db_unblock_user(db_bot_id, target)
        fs_refresh_pin_markup(pbot, db_bot_id, target)
        pbot.answer_callback_query(cb.id, f"✅ Пользователь {target} разблокирован", show_alert=True)
        kb = InlineKeyboardMarkup()
        kb.row(
            _pbtn('Ответить',      icon='📤', style='primary', callback_data=f'p_reply_{target}'),
            _pbtn('Заблокировать', icon='🚫', style='danger',  callback_data=f'p_block_{target}'),
        )
        try: pbot.edit_message_reply_markup(cb.message.chat.id, cb.message.message_id, reply_markup=kb)
        except Exception as e: log.warning(f'edit_markup: {e}')

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_blocked_list')
    def p_blocked_list_cb(cb):
        if not is_admin(cb.from_user.id): return
        _show_blocked(cb.message.chat.id, cb.message.message_id)

    def _show_blocked(chat_id, message_id):
        blocked = db_get_blocked_list(db_bot_id)
        if not blocked:
            _edit(f"<b>{TE_USERS} Нет заблокированных пользователей</b>",
                chat_id, message_id, parse_mode='HTML', reply_markup=pk_back_admin())
            return
        kb = InlineKeyboardMarkup()
        for uid in blocked[:30]:
            kb.add(_pbtn(f"Разблокировать {uid}", icon='✔️', style='success', callback_data=f'p_unblock_list_{uid}'))
        kb.add(_pbtn('Назад', icon='🔙', callback_data='p_back_admin'))
        _edit(f"<b>{TE_BLOCK}  Заблокированные ({len(blocked)}):</b>",
            chat_id, message_id, parse_mode='HTML', reply_markup=kb)

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_unblock_list_'))
    def p_unblock_list_cb(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        db_unblock_user(db_bot_id, target)
        fs_refresh_pin_markup(pbot, db_bot_id, target)
        pbot.answer_callback_query(cb.id, "✅ Разблокирован", show_alert=True)
        _show_blocked(cb.message.chat.id, cb.message.message_id)

    # ── Управление админами ────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_admins')
    def p_admins_cb(cb):
        if not is_primary_admin(cb.from_user.id): return
        _show_admins(cb.message.chat.id, cb.message.message_id)

    def _show_admins(chat_id, message_id):
        admins = db_get_bot_admins(db_bot_id)
        info   = db_get_bot_info(db_bot_id)
        primary_id = info[1] if info else None
        kb = InlineKeyboardMarkup()
        lines = [f"<b>{TE_USER} Текущие администраторы:</b>\n"]
        for aid in admins:
            label = f"{TE_CROWN} {aid} (главный)" if aid == primary_id else f"{TE_USER} {aid}"
            lines.append(label)
            if aid != primary_id:
                kb.add(_pbtn(f"Удалить {aid}", icon='❌', style='danger', callback_data=f'p_rm_admin_{aid}'))
        kb.add(_pbtn('Добавить админа', icon='➕', style='success', callback_data='p_add_admin'))
        if primary_id and primary_id != cb_from_user_id_placeholder:
            kb.add(_pbtn('Сменить главного', icon='👑', style='primary', callback_data='p_change_primary'))
        kb.add(_pbtn('Назад', icon='🔙', callback_data='p_back_admin'))
        _edit("\n".join(lines), chat_id, message_id,
            parse_mode='HTML', reply_markup=kb)

    def p_admins_open(cb):
        _show_admins(cb.message.chat.id, cb.message.message_id)

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_rm_admin_'))
    def p_rm_admin(cb):
        if not is_primary_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        db_remove_bot_admin(db_bot_id, target)
        pbot.answer_callback_query(cb.id, f"✅ Администратор {target} удалён", show_alert=True)
        p_admins_open(cb)  # обновляем список

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_add_admin')
    def p_add_admin(cb):
        if not is_primary_admin(cb.from_user.id): return
        _set_flow(cb.from_user.id, 'p_await_new_admin')
        _edit(
            f"<b>{TE_PLUS} Введи Telegram ID нового администратора:</b>\n"
            "<i>(он должен сначала написать любому боту, чтобы Telegram знал его ID)</i>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back_admin())

    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'p_await_new_admin')
    def p_save_new_admin(m):
        if not is_primary_admin(m.from_user.id): return
        try:
            new_admin = bounded_number(m.text)
        except ValueError:
            pbot.send_message(m.chat.id, 'Неверный ID')
            return
        if _take_flow(m.from_user.id, 'p_await_new_admin') is None: return
        db_add_bot_admin(db_bot_id, new_admin)
        pbot.send_message(m.chat.id,
            f"<b>{TE_CHECK}  Администратор <code>{new_admin}</code> добавлен!</b>\n"
            f"Теперь он будет получать сообщения и может отвечать.",
            parse_mode='HTML', reply_markup=pk_close())

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_change_primary')
    def p_change_primary(cb):
        if not is_primary_admin(cb.from_user.id): return
        _set_flow(cb.from_user.id, 'p_await_primary')
        _edit(
            f"<b>{TE_REFRESH} Введи Telegram ID нового главного администратора:</b>\n"
            "<i>Главный администратор может управлять другими админами и удалить бота</i>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back_admin())

    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'p_await_primary')
    def p_save_primary(m):
        if not is_primary_admin(m.from_user.id): return
        try:
            new_primary = bounded_number(m.text)
        except ValueError:
            pbot.send_message(m.chat.id, 'Неверный ID')
            return
        if _take_flow(m.from_user.id, 'p_await_primary') is None: return
        db_set_primary_admin(db_bot_id, new_primary)
        pbot.send_message(m.chat.id,
            f"<b>{TE_CHECK}  Главный администратор изменён на <code>{new_primary}</code>!</b>",
            parse_mode='HTML', reply_markup=pk_close())

    # ── Изменить приветствие ───────────────────────────
    # ── Удалить бота ───────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_delete_bot')
    def p_delete_bot(cb):
        if not is_primary_admin(cb.from_user.id): return
        _edit(
            f"<b>{TE_EXCLAM} Ты уверен что хочешь удалить бота?\n\n"
            "• Бот остановится навсегда\n"
            "• Все данные пользователей будут удалены\n"
            "• Это действие нельзя отменить</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_confirm_delete())

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_cancel_delete')
    def p_cancel_delete(cb):
        if not is_primary_admin(cb.from_user.id): return
        _edit(f"<b>{TE_KEYBOARD} Панель админа</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_admin(cb.from_user.id))

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_confirm_delete')
    def p_confirm_delete(cb):
        if not is_primary_admin(cb.from_user.id): return
        _edit(
            f"<b>{TE_CHECK}  Бот удалён. Спасибо за использование!</b>",
            cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
        db_deactivate_bot(db_bot_id)
        def _delayed_stop():
            time.sleep(1); stop_bot(token)
        threading.Thread(target=_delayed_stop, daemon=True).start()

    # ── Баланс и вывод ─────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_balance')
    def p_balance_cb(cb):
        if not _require_owner(cb): return
        balance, total = db_get_bot_earnings(db_bot_id)
        ton_rate = get_ton_amount() if balance > 0 else 0
        from database import get_price as _get_price
        price = _get_price()
        ton_equiv = round(balance * ton_rate / price, 2) if (ton_rate and price and balance) else 0
        kb = InlineKeyboardMarkup()
        if balance > 0:
            kb.row(
                _pbtn('Вывести TON',   icon='💲', style='primary',  callback_data='p_withdraw'),
                _pbtn('Продлить бота', icon='🔄', style='success', callback_data='p_renew_balance'),
            )
        kb.add(_pbtn('Назад', icon='🔙', callback_data='p_back_admin'))
        ton_text = f"\n{TE_DIAMOND} Эквивалент: ~<b>{ton_equiv} TON</b>" if ton_equiv else ""
        _edit(
            f"{TE_MONEY} <b>Мой баланс</b>\n\n"
            f"{TE_DOLLAR} Текущий баланс: <b>{balance:.4f} USDT</b>{ton_text}\n"
            f"{TE_CHART} Всего заработано: <b>{total:.4f} USDT</b>\n\n"
            f"<i>Баланс пополняется когда пользователи покупают бота через твою кнопку «{TE_DIAMOND} Купить такого бота» (50% с каждой продажи)</i>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=kb)

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_withdraw')
    def p_withdraw_cb(cb):
        if not _require_owner(cb): return
        balance, _ = db_get_bot_earnings(db_bot_id)
        if balance <= 0:
            pbot.answer_callback_query(cb.id, "❌ Баланс равен нулю", show_alert=True)
            return
        _set_flow(cb.from_user.id, {'step': 'p_await_ton_address', 'balance': balance})
        _edit(
            f"<b>{TE_MONEY} Вывод средств</b>\n\n"
            f"Баланс для вывода: <b>{balance:.4f} USDT</b>\n\n"
            f"Введи свой TON-адрес (кошелёк для получения):",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back_admin())

    @pbot.message_handler(func=lambda m: isinstance(pstate.get(m.from_user.id), dict)
                                         and pstate[m.from_user.id].get('step') == 'p_await_ton_address')
    def p_save_ton_address(m):
        if not _require_owner(m): return
        address = m.text.strip()
        if not (32 <= len(address) <= 128):
            pbot.send_message(m.chat.id, f"<b>{TE_CROSS}  Неверный TON-адрес. Попробуй ещё раз:</b>",
                parse_mode='HTML')
            return
        s = pstate[m.from_user.id]
        s['address'] = address
        s['step'] = 'p_await_withdraw_amount'
        s['operation_key'] = uuid.uuid4().hex
        _set_flow(m.from_user.id, s)
        balance = s['balance']
        pbot.send_message(m.chat.id,
            f"<b>{TE_MONEY} Адрес принят.</b>\n\n"
            f"Введи сумму для вывода (USDT) или отправь <code>всё</code> чтобы вывести весь баланс.\n\n"
            f"Доступно: <b>{balance:.4f} USDT</b>",
            parse_mode='HTML', reply_markup=pk_back_admin())

    @pbot.message_handler(func=lambda m: isinstance(pstate.get(m.from_user.id), dict)
                                         and pstate[m.from_user.id].get('step') == 'p_await_withdraw_amount')
    def p_save_withdraw_amount(m):
        if not _require_owner(m): return
        s = pstate.get(m.from_user.id)
        if not isinstance(s, dict) or s.get('step') != 'p_await_withdraw_amount': return
        balance = s['balance']
        address = s['address']
        text = m.text.strip().lower()
        if text in ('всё', 'all', 'все', 'all'):
            amount = balance
        else:
            try:
                amount = bounded_number(text.replace(',', '.'), integer=False, minimum=0)
                if amount <= 0 or amount > balance:
                    raise ValueError
            except ValueError:
                pbot.send_message(m.chat.id,
                    f"<b>{TE_CROSS}  Неверная сумма. Максимум: {balance:.4f} USDT</b>",
                    parse_mode='HTML')
                return
        owner_id = m.from_user.id
        try:
            key = 'withdraw:' + s.get('operation_key', f'{owner_id}:{m.message_id}')
            wr_id = db.db_spend_balance(db_bot_id, owner_id, amount, key, address=address, state=s)
            if wr_id is None:
                pbot.send_message(m.chat.id, f'{TE_CROSS} Недостаточно средств или заявка уже создана', parse_mode='HTML')
                return
        except Exception as e:
            log.warning(f'withdrawal create failed bot#{db_bot_id}: {e}')
            pbot.send_message(m.chat.id,
                f'{TE_CROSS} Не удалось создать заявку, попробуй позже', parse_mode='HTML', reply_markup=pk_close())
            return
        # Отправляем в канал суперадмина через main_bot (чтобы кнопки обрабатывались в нём)
        if WITHDRAWAL_CHANNEL and main_bot:
            try:
                kb_admin = InlineKeyboardMarkup()
                kb_admin.row(
                    _pbtn(f'Подтвердить #{wr_id}', icon='✔️', style='success', callback_data=f'wr_confirm_{wr_id}'),
                    _pbtn(f'Отклонить #{wr_id}', icon='❌',   style='danger',  callback_data=f'wr_reject_{wr_id}'),
                )
                from database import get_price as _get_price
                ton_rate = get_ton_amount()
                price = _get_price()
                ton_equiv = round(amount * ton_rate / price, 2) if (ton_rate and price) else '?'
                main_bot.send_message(WITHDRAWAL_CHANNEL,
                    f"{TE_MONEY} <b>Новый запрос на вывод #{wr_id}</b>\n\n"
                    f"Bot #{db_bot_id} | Owner: <code>{owner_id}</code>\n"
                    f"Адрес: <code>{html.escape(address)}</code>\n"
                    f"Сумма: <b>{amount:.4f} USDT</b> (~{ton_equiv} TON)",
                    parse_mode='HTML', reply_markup=kb_admin)
            except Exception as e:
                log.warning(f'withdrawal channel notify failed: {e}')
        pbot.send_message(m.chat.id,
            f"{TE_CHECK} <b>Запрос на вывод создан!</b>\n\n"
            f"Сумма: <b>{amount:.4f} USDT</b>\n"
            f"Адрес: <code>{html.escape(address)}</code>\n\n"
            f"Администратор обработает запрос в ближайшее время.",
            parse_mode='HTML', reply_markup=pk_close())

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_renew_balance')
    def p_renew_balance_cb(cb):
        if not _require_owner(cb): return
        balance, _ = db_get_bot_earnings(db_bot_id)
        if balance <= 0:
            pbot.answer_callback_query(cb.id, "❌ Баланс равен нулю", show_alert=True)
            return
        from database import get_price as _get_price
        price = _get_price()
        max_days_full, _ = balance_renewal_quote(balance, price)
        kb = InlineKeyboardMarkup()
        options = []
        for days_opt in [30, 90, 180]:
            _, cost = balance_renewal_quote(balance, price, days_opt)
            if balance >= cost:
                kb.add(_pbtn(
                    f'{days_opt} дней ({cost} USDT)',
                    style='success',
                    callback_data=f'p_renew_bal_{days_opt}'))
                options.append(days_opt)
        if balance > 0 and max_days_full > 0:
            kb.add(_pbtn(
                f'На весь баланс ({max_days_full} ' + (
                    'день' if max_days_full % 10 == 1 and max_days_full % 100 != 11 else
                    'дня' if 2 <= max_days_full % 10 <= 4 and not 12 <= max_days_full % 100 <= 14 else
                    'дней') + ')',
                icon='💰',
                style='success',
                callback_data='p_renew_bal_all'))
        if not options and max_days_full == 0:
            pbot.answer_callback_query(cb.id,
                f"❌ Баланса ({balance:.4f} USDT) недостаточно даже на 30 дней ({round(price,4)} USDT)",
                show_alert=True)
            return
        kb.add(_pbtn('Назад', icon='🔙', callback_data='p_balance'))
        _edit(
            f"<b>{TE_CLOCK} Продление через баланс</b>\n\n"
            f"Баланс: <b>{balance:.4f} USDT</b>\n"
            f"Цена 30 дней: <b>{price} USDT</b>\n\n"
            f"Выбери срок:",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=kb)

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_renew_bal_'))
    @_serialize_lifecycle
    def p_renew_bal_cb(cb):
        if not _require_owner(cb): return
        from database import get_price as _get_price
        balance, _ = db_get_bot_earnings(db_bot_id)
        price = _get_price()
        arg = cb.data.split('_')[-1]
        if arg == 'all':
            days, cost = balance_renewal_quote(balance, price)
        else:
            if arg not in ('30', '90', '180'): return
            days, cost = balance_renewal_quote(balance, price, int(arg))
        if days <= 0:
            pbot.answer_callback_query(cb.id, "❌ Недостаточно средств", show_alert=True)
            return
        if balance < cost:
            pbot.answer_callback_query(cb.id, f"❌ Недостаточно баланса. Нужно: {cost:.4f} USDT", show_alert=True)
            return
        try:
            key = f'renew:{cb.message.chat.id}:{cb.message.message_id}'
            new_exp = db.db_spend_balance(db_bot_id, cb.from_user.id, cost, key, days=days)
            if new_exp is None:
                pbot.answer_callback_query(cb.id, '❌ Недостаточно средств', show_alert=True)
                return
            info = db_get_bot_info(db_bot_id)
            if info and info[0] not in running_bots:
                launch_bot(db_bot_id, info[0], info[1], main_bot)
        except Exception as e:
            log.warning(f'p_renew_bal renew failed bot#{db_bot_id}: {e}')
            pbot.answer_callback_query(cb.id,
                '❌ Не удалось продлить, попробуй позже',
                show_alert=True)
            return
        exp_str = str(new_exp)[:10] if new_exp else '—'
        _edit(
            f"{TE_CHECK} <b>Бот продлён на {days} дней!</b>\n\n"
            f"Списано: <b>{cost:.4f} USDT</b>\n"
            f"Подписка до: <b>{exp_str}</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_close())

    # ── Рассылка ───────────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_broadcast')
    def p_broadcast_start(cb):
        if not is_admin(cb.from_user.id): return
        _edit(f"<b>{TE_ANNOUNCE} Выбери тип рассылки:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=broadcast_type_kb('p'))

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('bcast_') and c.data.endswith('_p'))
    def p_broadcast_type(cb):
        if not is_admin(cb.from_user.id): return
        if 'text' in cb.data:
            _set_flow(cb.from_user.id, 'p_bcast_text')
            _edit(f"<b>{TE_NOTE} Введи текст рассылки (HTML):</b>",
                cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
        else:
            _set_flow(cb.from_user.id, 'p_bcast_photo')
            _edit(f"<b>{TE_PICTURE} Отправь фото с подписью (или без):</b>",
                cb.message.chat.id, cb.message.message_id, parse_mode='HTML')

    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'p_bcast_text')
    def p_bcast_text(m):
        if not is_admin(m.from_user.id): return
        if _take_flow(m.from_user.id, 'p_bcast_text') is None: return
        users = db_get_bot_users(db_bot_id)
        sent  = sum(1 for uid in users if _safe_send(pbot, uid, m.text))
        pbot.send_message(m.chat.id, f"<b>{TE_CHECK}  Рассылка завершена: {sent}/{len(users)}</b>",
            parse_mode='HTML', reply_markup=pk_close())

    @pbot.message_handler(content_types=['photo'],
                          func=lambda m: pstate.get(m.from_user.id) == 'p_bcast_photo')
    def p_bcast_photo(m):
        if not is_admin(m.from_user.id): return
        if _take_flow(m.from_user.id, 'p_bcast_photo') is None: return
        users   = db_get_bot_users(db_bot_id)
        photo   = m.photo[-1].file_id
        from telegram_text import message_html
        caption = message_html(m.json, caption=True)
        sent    = sum(1 for uid in users if _safe_photo(pbot, uid, photo, caption))
        pbot.send_message(m.chat.id, f"<b>{TE_CHECK}  Рассылка завершена: {sent}/{len(users)}</b>",
            parse_mode='HTML', reply_markup=pk_close())

    # ── Forum Topics: техподдержка через группу с темами ─────────
    def revoked_input(m):
        current = pstate.get(m.from_user.id)
        privileged = (isinstance(current, str) and (
            current in ('mgadd_tpl','p_bcast_text','p_bcast_photo','p_await_new_admin','p_await_primary')
            or current.startswith(('await_reply_', 'await_add_tpl_')))
            or isinstance(current, dict) and current.get('step') == 'support_input')
        if not privileged: return False
        return not is_admin(m.from_user.id)

    def release_revoked_input(m):
        _reset_flow(m.from_user.id)
        _deliver_direct(m)

    pbot.message_handlers.insert(0, dict(function=release_revoked_input,
        filters=dict(func=revoked_input, content_types=['text','photo','video','document','audio','voice','video_note','sticker','animation','contact','location','venue','poll']),
        pass_bot=False))
    # Все обработчики личной панели ограничены личным чатом одним правилом
    # Группы и темы всегда проходят через форумную маршрутизацию
    for handler in pbot.message_handlers:
        check = handler['filters'].get('func')
        handler['filters']['func'] = lambda m, check=check: (
            m.chat.type == 'private' and (check is None or check(m)))

    # Регистрируется последним: все обработчики состояний выше имеют приоритет,
    # форум-обработчики ловят только то, что не перехватили они.
    # direct_route: случайные сообщения юзеров доставляются СРАЗУ (без очереди)
    register_forum_support(pbot, db_bot_id, pstate, is_admin,
                                      confirm_kb=pk_close, direct_route=_deliver_direct)
    from input_validation import guard_numeric_callbacks
    guard_numeric_callbacks(pbot)

    return pbot


# Заглушка для замыкания (не используется в рантайме)
cb_from_user_id_placeholder = -1


@_serialize_lifecycle
def launch_bot(db_bot_id: int, token: str, admin_id: int, main_bot=None) -> bool:
    if not db.db_bot_can_run(db_bot_id): return False
    existing_token = running_bot_ids.get(db_bot_id)
    if existing_token is not None and existing_token != token:
        return False
    if token in running_bots: return True
    pbot = None
    try:
        pbot = make_purchased_bot(db_bot_id, token, admin_id, main_bot)
        me   = pbot.get_me()
        if db_get_bot_setting(db_bot_id, 'notified_401', ''):
            db_set_bot_setting(db_bot_id, 'notified_401', '')
        try:
            pbot.delete_webhook(drop_pending_updates=False)
        except Exception as e:
            log.warning(f'Bot #{db_bot_id} delete_webhook failed: {e}')
        db_update_bot_username(db_bot_id, me.username or '')
        running_bots[token]        = pbot
        running_bot_ids[db_bot_id] = token
        from delivery_queue import get_sender
        get_sender(pbot, db_bot_id).start()

        def polling_loop():
            def active():
                return (running_bots.get(token) is pbot
                        and running_bot_ids.get(db_bot_id) == token)

            while active():
                try:
                    pbot.infinity_polling(timeout=60, long_polling_timeout=5,
                                  allowed_updates=['message', 'callback_query', 'message_reaction', 'my_chat_member'])
                except Exception as e:
                    if not active():
                        break
                    log.error(f'Bot #{db_bot_id} crashed: {e}')
                    if main_bot:
                        try:
                            main_bot.send_message(SUPER_ADMIN,
                                f"{TE_EXCLAM} <b>Bot #{db_bot_id} упал!</b>\n"
                                f"Ошибка: <code>{html.escape(str(e)[:300])}</code>\n"
                                f"{TE_REFRESH} Перезапускаем через 5 сек...",
                                parse_mode='HTML')
                        except Exception:
                            pass
                    time.sleep(5)
                else:
                    # infinity_polling возвращается после остановки экземпляра
                    # Прежний токен в реестре не означает, что это тот же бот
                    break
            sender = getattr(pbot, '_delivery_sender', None)
            if sender is not None:
                sender.stop()
            log.info(f'Bot #{db_bot_id} polling stopped.')

        pbot._polling_thread = threading.Thread(target=polling_loop, daemon=True)
        pbot._polling_thread.start()
        log.info(f'Bot #{db_bot_id} (@{me.username}) launched.')
        return True
    except Exception as e:
        if pbot is not None:
            if running_bots.get(token) is pbot:
                running_bots.pop(token, None)
                if running_bot_ids.get(db_bot_id) == token:
                    running_bot_ids.pop(db_bot_id, None)
            try:
                _close_bot(pbot)
            except Exception as close_exc:
                log.warning('Bot #%s cleanup failed: %s', db_bot_id, close_exc)
        log.error(f'launch_bot #{db_bot_id} failed: {e}')
        if ('401' in str(e) or 'Unauthorized' in str(e)) and not db_get_bot_setting(db_bot_id, 'notified_401', ''):
            db_set_bot_setting(db_bot_id, 'notified_401', '1')
            if main_bot:
                try:
                    info = db_get_bot_info(db_bot_id)
                    owner_id = info[2] if info else None
                    if owner_id:
                        main_bot.send_message(owner_id,
                            f"{TE_CROSS} <b>Токен вашего бота недействителен!</b>\n\n"
                            "Похоже, токен был отозван или изменён в @BotFather. "
                            "Бот не работает, пока токен не будет обновлён.",
                            parse_mode='HTML')
                    main_bot.send_message(SUPER_ADMIN,
                        f"{TE_EXCLAM} <b>Bot #{db_bot_id} — невалидный токен (401)</b>\n"
                        f"Owner: <code>{owner_id}</code>",
                        parse_mode='HTML')
                except Exception:
                    pass
        return False
