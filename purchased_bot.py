import html
import telebot
import threading
import time
import logging
from datetime import datetime
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
                      db_get_username, db_bump_user_contact,
                      db_is_marketplace_enabled,
                      db_get_bot_earnings, db_deduct_bot_earnings,
                      db_create_withdrawal, db_renew_bot,
                      db_return_bot_earnings)
from keyboards import broadcast_type_kb, PE_MONEY, PE_CHECK, PE_GIFT, PE_FIRE
from config import SUPER_ADMIN, WITHDRAWAL_CHANNEL, BOT_USERNAME
from payments import get_ton_amount
from forum_support import (register_forum_support, RATE_LIMIT_SEC, rate_limited,
                           fs_bump_ticket, queue_random_message, html_safe_chunks)

log = logging.getLogger(__name__)

running_bots    = {}  # {token: TeleBot}
running_bot_ids = {}  # {db_bot_id: token}

def _is_rate_limited(bot_id: int, user_id: int) -> bool:
    """Единый rate limit (кэш и логика в forum_support.rate_limited)."""
    return rate_limited(bot_id, user_id, mark=True)

def _safe_send(b, uid, text):
    try: b.send_message(uid, text, parse_mode='HTML'); return True
    except: return False

def _safe_photo(b, uid, photo, caption):
    try: b.send_photo(uid, photo, caption=caption, parse_mode='HTML'); return True
    except: return False

def stop_bot(token: str) -> bool:
    bot_obj = running_bots.get(token)
    if not bot_obj:
        return False
    try:
        running_bots.pop(token, None)
        for bid, tok in list(running_bot_ids.items()):
            if tok == token:
                running_bot_ids.pop(bid, None)
                break
        bot_obj.stop_polling()
        return True
    except Exception as e:
        log.error(f'stop_bot failed: {e}')
        return False

DEFAULT_WELCOME = (f"<b>{TE_ROBOT} Привет! Это бот обратной связи.\n\n"
                   f"{TE_CHAT} Отправь своё сообщение и администратор обязательно его прочитает.</b>")

def make_purchased_bot(db_bot_id: int, token: str, admin_id: int, main_bot=None):
    pbot   = telebot.TeleBot(token)
    pstate = DBState(db_bot_id)
    preply_cache = {}  # {admin_id: {'target_id': int, 'text': str}}

    def _edit(text, chat_id, message_id, reply_markup=None, parse_mode='HTML'):
        """edit_message_text с фолбэком: Telegram не даёт править сообщения
        старше 48 часов — шлём новое с тем же содержимым."""
        try:
            return pbot.edit_message_text(text, chat_id, message_id,
                                          parse_mode=parse_mode,
                                          reply_markup=reply_markup)
        except Exception as e:
            s = str(e).lower()
            if 'not modified' in s:
                return True
            try:
                return pbot.send_message(chat_id, text,
                                         parse_mode=parse_mode,
                                         reply_markup=reply_markup)
            except Exception as e2:
                log.warning(f'_edit fallback failed: {e2}')
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

    def _send_long(chat, header, body, reply_markup=None):
        """Длинные сообщения: если header+тело не влезают в лимит Telegram (4096),
        отправляет заголовок отдельно и тело кусками. Лимит меряем по ЭКРАНИРОВАННОЙ
        длине: 3800 сырых символов с '<' превращаются в ~19000 экранированных и
        упали бы с 'message is too long'. Куски режет html_safe_chunks —
        без разрыва сущностей и с гарантированным влезанием."""
        esc = html.escape(body or '')
        if len(header) + len(esc) + 4 <= 4000:
            return pbot.send_message(chat, header + '\n\n' + esc,
                                     parse_mode='HTML', reply_markup=reply_markup)
        pbot.send_message(chat, header, parse_mode='HTML', reply_markup=reply_markup)
        for chunk in html_safe_chunks(esc):
            pbot.send_message(chat, chunk, parse_mode='HTML')
        return True

    def get_welcome():
        return db_get_bot_setting(db_bot_id, 'welcome', DEFAULT_WELCOME)

    def _get_exp_str() -> str:
        raw = db_get_bot_expires(db_bot_id)
        if not raw:
            return '—'
        try:
            exp = datetime.fromisoformat(str(raw))
            days_left = (exp - datetime.now()).days
            if days_left > 0:
                return f"{str(raw)[:10]} (осталось {days_left}д.)"
            else:
                return f"{str(raw)[:10]} {TE_CROSS} истекла"
        except Exception:
            return '—'

    def is_admin(user_id: int) -> bool:
        return user_id in db_get_bot_admins(db_bot_id)

    def is_primary_admin(user_id: int) -> bool:
        info = db_get_bot_info(db_bot_id)
        return bool(info and user_id == info[1])

    def send_to_admins(text, reply_markup=None):
        # длинные обращения: заголовок HTML отдельно + сырой текст кусками
        # по 3600, экранируя каждый кусок отдельно (кусок всегда валидный HTML)
        if len(text) > 4000:
            hdr, _, raw = text.partition('\n\n')
            send_to_admins_long(hdr, raw, reply_markup=reply_markup)
            return True
        ok = False
        for aid in db_get_bot_admins(db_bot_id):
            try:
                pbot.send_message(aid, text, parse_mode='HTML', reply_markup=reply_markup)
                ok = True
            except Exception as e:
                log.warning(f'send_to_admins bot#{db_bot_id} admin {aid}: {e}')
        return ok

    def send_to_admins_long(header, body_raw, reply_markup=None):
        esc = html.escape(body_raw or '')
        for aid in db_get_bot_admins(db_bot_id):
            try:
                pbot.send_message(aid, header, parse_mode='HTML', reply_markup=reply_markup)
                # html_safe_chunks: куски гарантированно ≤3600 ПОСЛЕ экранирования
                # и не рвут сущности (&amp; на границе давал бы 400)
                for chunk in html_safe_chunks(esc, 3600):
                    pbot.send_message(aid, chunk, parse_mode='HTML')
            except Exception as e:
                log.warning(f'send_to_admins_long bot#{db_bot_id} admin {aid}: {e}')

    def send_media_to_admins(m, header):
        """Forward any media type to all admins with user info header.
        Возвращает True, если хотя бы один админ получил сообщение
        (БЕЗ return вызывающий p_user_media считает доставку провалом и
        кладёт сообщение в очередь → двойная доставка при подтверждении).
        try на КАЖДОГО админа: сбой одному не обрывает доставку остальным.
        Подпись Telegram ограничена 1024 символами, а экранирование может
        раздуть её (заголовок + подпись с '<') — тогда подпись уходит
        отдельным текстом после медиа."""
        kb = pk_reply(m.from_user.id)
        ct = m.content_type
        cap_full = header + (f"\n\n{TE_CHAT} {html.escape(m.caption)}" if m.caption else "")
        cap_overflow = len(cap_full) > 1000 and bool(m.caption)
        cap = cap_full if len(cap_full) <= 1000 else header
        ok = False
        for aid in db_get_bot_admins(db_bot_id):
            try:
                if ct == 'photo':
                    pbot.send_photo(aid, m.photo[-1].file_id, caption=cap,
                                    parse_mode='HTML', reply_markup=kb)
                elif ct == 'video':
                    pbot.send_video(aid, m.video.file_id, caption=cap,
                                    parse_mode='HTML', reply_markup=kb)
                elif ct == 'document':
                    pbot.send_document(aid, m.document.file_id, caption=cap,
                                       parse_mode='HTML', reply_markup=kb)
                elif ct == 'audio':
                    pbot.send_audio(aid, m.audio.file_id, caption=cap,
                                    parse_mode='HTML', reply_markup=kb)
                elif ct == 'voice':
                    pbot.send_message(aid, header + f"\n{TE_MIC} Голосовое сообщение:",
                                      parse_mode='HTML', reply_markup=kb)
                    pbot.send_voice(aid, m.voice.file_id)
                elif ct == 'video_note':
                    pbot.send_message(aid, header + f"\n{TE_VIDEO} Видеосообщение:",
                                      parse_mode='HTML', reply_markup=kb)
                    pbot.send_video_note(aid, m.video_note.file_id)
                elif ct == 'sticker':
                    pbot.send_message(aid, header + f"\n{TE_MASKS} Стикер:",
                                      parse_mode='HTML', reply_markup=kb)
                    pbot.send_sticker(aid, m.sticker.file_id)
                elif ct == 'animation':
                    pbot.send_animation(aid, m.animation.file_id, caption=cap,
                                        parse_mode='HTML', reply_markup=kb)
                else:
                    # контакты, геолокация, опросы и прочее — пересылка как есть
                    pbot.forward_message(aid, m.chat.id, m.message_id)
                if cap_overflow:
                    for chunk in html_safe_chunks(html.escape(m.caption)):
                        pbot.send_message(aid, chunk, parse_mode='HTML')
                ok = True
            except Exception as e:
                log.warning(f'send_media_to_admins bot#{db_bot_id} admin {aid}: {e}')
        return ok

    def _get_file_id(msg) -> str:
        ct = msg.content_type
        if ct == 'photo':      return msg.photo[-1].file_id
        if ct == 'video':      return msg.video.file_id
        if ct == 'document':   return msg.document.file_id
        if ct == 'audio':      return msg.audio.file_id
        if ct == 'voice':      return msg.voice.file_id
        if ct == 'video_note': return msg.video_note.file_id
        if ct == 'sticker':    return msg.sticker.file_id
        return ''

    # ── Клавиатуры ─────────────────────────────────────
    def pk_start(show_admin: bool = False):
        kb = InlineKeyboardMarkup()
        kb.add(_pbtn('Отправить сообщение', icon='📤', style='primary', callback_data='p_send'))
        if show_admin:
            kb.add(_pbtn('Панель админа', style='primary', callback_data='p_open_admin'))
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
            _pbtn('Шаблоны', style='primary',           callback_data=f'p_templates_{target_id}'),
        )
        return kb

    def pk_templates_list(target_id):
        templates = db_get_templates(db_bot_id)
        kb = InlineKeyboardMarkup()
        for tid, text in templates:
            preview = text[:35] + '…' if len(text) > 35 else text
            kb.add(_pbtn(preview, icon='💬', style='primary', callback_data=f'p_use_tpl_{target_id}_{tid}'))
        kb.add(_pbtn('Добавить шаблон', style='success', callback_data=f'p_add_tpl_{target_id}'))
        if templates:
            kb.add(_pbtn('Удалить',  style='danger',  callback_data=f'p_del_tpl_menu_{target_id}'))
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

    def pk_admin():
        kb = InlineKeyboardMarkup()
        kb.row(
            _pbtn('Статистика',  style='primary', callback_data='p_stats'),
            _pbtn('Мой баланс',  style='success', callback_data='p_balance'),
        )
        kb.add(_pbtn('Рассылка', style='primary', callback_data='p_broadcast'))
        kb.row(
            _pbtn('Приветствие', style='primary', callback_data='p_edit_welcome'),
            _pbtn('Шаблоны',     style='primary', callback_data='p_admin_templates'),
        )
        kb.add(_pbtn('Группа техподдержки', icon='💬', style='primary', callback_data='fs_group_menu'))
        kb.add(_pbtn('Заблокированные',     style='primary', callback_data='p_blocked_list'))
        kb.add(_pbtn('Управление админами', style='primary', callback_data='p_admins'))
        kb.add(_pbtn('Удалить бота',        style='danger',  callback_data='p_delete_bot'))
        return kb

    def pk_confirm_delete():
        kb = InlineKeyboardMarkup()
        kb.row(
            _pbtn('Да, удалить', icon='✔️', style='danger',  callback_data='p_confirm_delete'),
            _pbtn('Отмена',      icon='❌', style='primary', callback_data='p_cancel_delete'),
        )
        return kb

    # ── Команды ────────────────────────────────────────
    @pbot.message_handler(commands=['start'])
    def pstart(m):
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
        pbot.send_message(m.chat.id,
            f"<b>{TE_KEYBOARD} Панель админа</b>\n\n{TE_HOURGLASS} Подписка до: <b>{_get_exp_str()}</b>",
            parse_mode='HTML', reply_markup=pk_admin())

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
        pstate.pop(cb.from_user.id, None)
        _edit(
            f"<b>{TE_KEYBOARD} Панель админа</b>\n\n{TE_HOURGLASS} Подписка до: <b>{_get_exp_str()}</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_admin())

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_open_admin')
    def p_open_admin_cb(cb):
        if not is_admin(cb.from_user.id): return
        _edit(
            f"<b>{TE_KEYBOARD} Панель админа</b>\n\n{TE_HOURGLASS} Подписка до: <b>{_get_exp_str()}</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_admin())

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
        kb.add(_pbtn('Добавить шаблон', style='success', callback_data='p_mgadd_tpl'))
        kb.add(_pbtn('Назад', icon='🔙', callback_data='p_back_admin'))
        return kb, templates

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_admin_templates')
    def p_admin_templates_cb(cb):
        if not is_admin(cb.from_user.id): return
        kb, templates = pk_manage_templates()
        header = f"<b>{TE_NOTE} Шаблоны ответов ({len(templates)}):</b>" if templates else f"<b>{TE_NOTE} Шаблоны ответов пусты</b>"
        _edit(header, cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=kb)

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_mgadd_tpl')
    def p_mgadd_tpl_cb(cb):
        if not is_admin(cb.from_user.id): return
        pstate[cb.from_user.id] = 'mgadd_tpl'
        kb = InlineKeyboardMarkup()
        kb.add(_pbtn('Отмена', icon='❌', style='danger', callback_data='p_admin_templates'))
        _edit(f"<b>{TE_PLUS} Введи текст нового шаблона:</b>",
            cb.message.chat.id, cb.message.message_id, parse_mode='HTML', reply_markup=kb)

    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'mgadd_tpl')
    def p_mgadd_tpl_save(m):
        if not is_admin(m.from_user.id): return
        pstate.pop(m.from_user.id, None)
        db_add_template(db_bot_id, m.text)
        kb, templates = pk_manage_templates()
        pbot.send_message(m.chat.id,
            f"<b>{TE_NOTE} Шаблоны ответов ({len(templates)}):</b>",
            parse_mode='HTML', reply_markup=kb)

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_mgdel_tpl_'))
    def p_mgdel_tpl_cb(cb):
        if not is_admin(cb.from_user.id): return
        tid = int(cb.data.split('_')[-1])
        db_del_template(tid, db_bot_id)
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
        pstate.pop(cb.from_user.id, None)
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
        # состояние сбрасываем ДО удаления: упавшее delete_message
        # (сообщение старше 48 ч) не должно оставлять юзера в состоянии
        pstate.pop(cb.from_user.id, None)
        try:
            pbot.delete_message(cb.message.chat.id, cb.message.message_id)
        except Exception:
            pass

    # ── Отправить сообщение ────────────────────────────
    # fs_route назначается в конце make_purchased_bot после регистрации
    # обработчиков форум-режима; до этого обращения идут по стандартному пути.
    fs_route = None

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_send')
    def p_user_send(cb):
        if db_is_blocked(db_bot_id, cb.from_user.id):
            pbot.answer_callback_query(cb.id, "🚫 Вы заблокированы администратором", show_alert=True)
            return
        _edit(
            f"<b>{TE_CHAT}  Отправь сообщение, фото, видео, файл или голосовое:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back())
        pstate[cb.from_user.id] = 'await_msg'

    _seen_mg: dict = {}
    _seen_mg_lock = threading.Lock()

    def _mg_first_item(m) -> bool:
        """True для любого не-альбомного сообщения и для ПЕРВОГО элемента альбома.
        Элементы одного media_group приходят отдельными апдейтами — rate limit
        применяется один раз на альбом, иначе альбом разрывается лимитом."""
        mg = getattr(m, 'media_group_id', None)
        if not mg:
            return True
        key = (db_bot_id, m.from_user.id, mg)
        now = time.time()
        with _seen_mg_lock:
            if key in _seen_mg:
                return False
            _seen_mg[key] = now
            if len(_seen_mg) > 500:
                for k in [k for k, ts in _seen_mg.items() if now - ts > 120]:
                    _seen_mg.pop(k, None)
                if len(_seen_mg) > 500:
                    _seen_mg.clear()
            return True

    def _user_msg_checks(m) -> bool:
        """Общие проверки для всех входящих медиа. True = продолжать."""
        db_add_user(db_bot_id, m.from_user.id, m.from_user.username or '')
        if db_is_blocked(db_bot_id, m.from_user.id):
            pbot.send_message(m.chat.id, f"<b>{TE_BLOCK}  Вы заблокированы администратором</b>",
                parse_mode='HTML')
            pstate.pop(m.from_user.id, None)
            return False
        if _mg_first_item(m) and _is_rate_limited(db_bot_id, m.from_user.id):
            pbot.send_message(m.chat.id,
                f"<b>{TE_HOURGLASS} Подожди {RATE_LIMIT_SEC} сек перед следующим сообщением</b>",
                parse_mode='HTML')
            return False
        return True

    def _make_header(m) -> str:
        username = f"@{m.from_user.username}" if m.from_user.username else "нет"
        return (f"<b>{TE_INBOX} Сообщение от:</b>\n"
                f"{TE_USER} Имя: <b>{html.escape(m.from_user.full_name)}</b>\n"
                f"{TE_LINK} Username: {html.escape(username)}\n"
                f"{TE_ID} ID: <code>{m.from_user.id}</code>")

    # ── Текст ──────────────────────────────────────────
    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'await_msg')
    def p_user_text(m):
        if not _user_msg_checks(m): return
        # Форум-режим: группа подключена → сообщение уходит в тему пользователя
        if fs_route and fs_route(m):
            pstate.pop(m.from_user.id, None)
            return
        db_bump_user_contact(db_bot_id, m.from_user.id)
        fs_bump_ticket(db_bot_id, m.from_user.id)   # обращение учитывается и без группы
        header = _make_header(m)
        user_body = m.text or ''
        # длинные обращения: заголовок HTML отдельно + сырой текст кусками,
        # экранируя каждый кусок отдельно (кусок всегда валидный HTML)
        if len(header) + len(html.escape(user_body)) + 10 > 4000:
            delivered = send_to_admins_long(header, user_body,
                                            reply_markup=pk_reply(m.from_user.id))
        else:
            delivered = send_to_admins(f"{header}\n\n{TE_CHAT} {html.escape(user_body)}",
                                       reply_markup=pk_reply(m.from_user.id))
        if not delivered:
            # ни один админ не получил (Telegram недоступен) — не теряем сообщение
            queue_random_message(pbot, db_bot_id, m)
            pstate.pop(m.from_user.id, None)
            return
        db_log_message(db_bot_id, m.from_user.id, m.text or '')
        pbot.send_message(m.chat.id, f"<b>{TE_CHECK}  Сообщение отправлено</b>",
            parse_mode='HTML', reply_markup=pk_close())
        pstate.pop(m.from_user.id, None)

    # ── Медиа ──────────────────────────────────────────
    @pbot.message_handler(
        content_types=['photo', 'video', 'document', 'audio', 'voice',
                       'video_note', 'sticker', 'animation', 'contact',
                       'location', 'venue', 'poll', 'dice'],
        func=lambda m: pstate.get(m.from_user.id) == 'await_msg'
    )
    def p_user_media(m):
        if not _user_msg_checks(m): return
        if fs_route and fs_route(m):
            pstate.pop(m.from_user.id, None)
            return
        db_bump_user_contact(db_bot_id, m.from_user.id)
        fs_bump_ticket(db_bot_id, m.from_user.id)
        if not send_media_to_admins(m, _make_header(m)):
            queue_random_message(pbot, db_bot_id, m)
            pstate.pop(m.from_user.id, None)
            return
        db_log_message(db_bot_id, m.from_user.id, f'[{m.content_type}]')
        pbot.send_message(m.chat.id, f"<b>{TE_CHECK}  Сообщение отправлено</b>",
            parse_mode='HTML', reply_markup=pk_close())
        pstate.pop(m.from_user.id, None)

    # ── Ответ админа ───────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_reply_'))
    def p_admin_reply(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        # Cache original message for back button restoration
        try:
            original_text = getattr(cb.message, 'html_text', None)
        except Exception:
            original_text = None
        # старые (>48 ч) сообщения приходят как InaccessibleMessage без полей —
        # считаем их текстовыми, содержимое берём из БД/кэша при ответе
        is_media = getattr(cb.message, 'content_type', 'text') != 'text'
        if is_media:
            preply_cache[cb.from_user.id] = {'target_id': target, 'text': None, 'media': True,
                                              'content_type': cb.message.content_type,
                                              'file_id': _get_file_id(cb.message),
                                              'caption': cb.message.html_caption or cb.message.caption or ''}
            try:
                pbot.delete_message(cb.message.chat.id, cb.message.message_id)
            except Exception:
                pass  # >48 ч — Telegram не даст удалить; НЕ роняем хендлер,
                      # иначе админ не попадёт в режим ответа
            pbot.send_message(cb.message.chat.id,
                f"<b>{TE_CHAT}  Введи ответ или отправь фото:</b>",
                parse_mode='HTML', reply_markup=pk_reply_input_with_templates(target))
        else:
            preply_cache[cb.from_user.id] = {'target_id': target, 'text': original_text, 'media': False}
            _edit(f"<b>{TE_CHAT}  Введи ответ или отправь фото:</b>",
                cb.message.chat.id, cb.message.message_id,
                parse_mode='HTML', reply_markup=pk_reply_input_with_templates(target))
        pstate[cb.from_user.id] = f'await_reply_{target}'

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_back_reply_'))
    def p_back_reply(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        pstate.pop(cb.from_user.id, None)
        preply_cache.pop(cb.from_user.id, None)

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
            pbot.answer_callback_query(cb.id, "❌ Шаблон не знайдено", show_alert=True)
            return
        try:
            pbot.send_message(target_id, f"<b>{TE_INBOX} Сообщение от администратора\n\n{TE_CHAT} {html.escape(text)}</b>",
                parse_mode='HTML')
        except Exception:
            pbot.answer_callback_query(cb.id, "❌ Не удалось отправить — пользователь недоступен",
                show_alert=True)
            return
        pstate.pop(cb.from_user.id, None)
        preply_cache.pop(cb.from_user.id, None)
        _edit(f"<b>{TE_CHECK}  Ответ по шаблону отправлен</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_close())

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_add_tpl_'))
    def p_add_template_start(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        _edit(f"<b>{TE_PLUS} Введи текст нового шаблона:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_reply_input_with_templates(target))
        pstate[cb.from_user.id] = f'await_add_tpl_{target}'

    @pbot.message_handler(func=lambda m: isinstance(pstate.get(m.from_user.id), str)
                                         and pstate[m.from_user.id].startswith('await_add_tpl_'))
    def p_save_template(m):
        if not is_admin(m.from_user.id): return
        target = int(pstate[m.from_user.id].split('_')[-1])
        db_add_template(db_bot_id, m.text)
        pstate[m.from_user.id] = f'await_reply_{target}'
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
        if not is_admin(m.from_user.id): return
        target_id = int(pstate[m.from_user.id].split('_')[-1])
        try:
            _send_long(target_id,
                       f"<b>{TE_INBOX} Сообщение от администратора</b>", m.text or '')
        except Exception as e:
            # адресат недоступен: сообщаем и снимаем состояние, чтобы админ
            # не застрял в режиме ответа без всякого фидбека
            log.warning(f'p_admin_text bot#{db_bot_id} → {target_id}: {e}')
            pstate.pop(m.from_user.id, None)
            preply_cache.pop(m.from_user.id, None)
            pbot.send_message(m.chat.id,
                f"<b>{TE_CROSS}  Не удалось доставить ответ: пользователь недоступен "
                f"(заблокировал бота или удалил аккаунт).</b>",
                parse_mode='HTML', reply_markup=pk_close())
            return
        pbot.send_message(m.chat.id, f"<b>{TE_CHECK}  Ответ отправлен</b>",
            parse_mode='HTML', reply_markup=pk_close())
        preply_cache.pop(m.from_user.id, None)
        pstate.pop(m.from_user.id, None)

    @pbot.message_handler(
        content_types=['photo'],
        func=lambda m: isinstance(pstate.get(m.from_user.id), str)
                       and pstate[m.from_user.id].startswith('await_reply_')
    )
    def p_admin_photo_reply(m):
        if not is_admin(m.from_user.id): return
        target_id = int(pstate[m.from_user.id].split('_')[-1])
        caption = m.caption or ''
        try:
            if len(caption) > 900:
                # подпись > лимита Telegram (1024): фото отдельно + текст отдельно
                pbot.send_photo(target_id, m.photo[-1].file_id)
                _send_long(target_id, f"<b>{TE_INBOX} Сообщение от администратора</b>", caption)
            else:
                cap_text = f"<b>{TE_INBOX} Сообщение от администратора</b>\n\n{html.escape(caption)}" if caption else f"<b>{TE_INBOX} Сообщение от администратора</b>"
                pbot.send_photo(target_id, m.photo[-1].file_id, caption=cap_text, parse_mode='HTML')
        except Exception as e:
            log.warning(f'p_admin_photo_reply bot#{db_bot_id} → {target_id}: {e}')
            pstate.pop(m.from_user.id, None)
            preply_cache.pop(m.from_user.id, None)
            pbot.send_message(m.chat.id,
                f"<b>{TE_CROSS}  Не удалось отправить фото: пользователь недоступен "
                f"(заблокировал бота или удалил аккаунт).</b>",
                parse_mode='HTML', reply_markup=pk_close())
            return
        pstate.pop(m.from_user.id, None)
        preply_cache.pop(m.from_user.id, None)
        pbot.send_message(m.chat.id, f"<b>{TE_CHECK}  Фото отправлено</b>",
            parse_mode='HTML', reply_markup=pk_close())

    # ── Блокировка ─────────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_block_'))
    def p_block_user(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        db_block_user(db_bot_id, target)
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
        kb.add(_pbtn('Добавить админа', style='success', callback_data='p_add_admin'))
        if primary_id and primary_id != cb_from_user_id_placeholder:
            kb.add(_pbtn('Сменить главного', style='primary', callback_data='p_change_primary'))
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
        _edit(
            f"<b>{TE_PLUS} Введи Telegram ID нового администратора:</b>\n"
            "<i>(он должен сначала написать любому боту, чтобы Telegram знал его ID)</i>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back_admin())
        pstate[cb.from_user.id] = 'p_await_new_admin'

    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'p_await_new_admin')
    def p_save_new_admin(m):
        if not is_primary_admin(m.from_user.id): return
        try:
            new_admin = int(m.text.strip())
        except ValueError:
            pbot.send_message(m.chat.id, f"<b>{TE_CROSS}  ID должен быть числом</b>", parse_mode='HTML')
            return
        pstate.pop(m.from_user.id, None)
        db_add_bot_admin(db_bot_id, new_admin)
        pbot.send_message(m.chat.id,
            f"<b>{TE_CHECK}  Администратор <code>{new_admin}</code> добавлен!</b>\n"
            f"Теперь он будет получать сообщения и может отвечать.",
            parse_mode='HTML', reply_markup=pk_close())

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_change_primary')
    def p_change_primary(cb):
        if not is_primary_admin(cb.from_user.id): return
        _edit(
            f"<b>{TE_REFRESH} Введи Telegram ID нового главного администратора:</b>\n"
            "<i>Главный администратор может управлять другими админами и удалить бота</i>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back_admin())
        pstate[cb.from_user.id] = 'p_await_primary'

    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'p_await_primary')
    def p_save_primary(m):
        if not is_primary_admin(m.from_user.id): return
        try:
            new_primary = int(m.text.strip())
        except ValueError:
            pbot.send_message(m.chat.id, f"<b>{TE_CROSS}  ID должен быть числом</b>", parse_mode='HTML')
            return
        pstate.pop(m.from_user.id, None)
        db_set_primary_admin(db_bot_id, new_primary)
        pbot.send_message(m.chat.id,
            f"<b>{TE_CHECK}  Главный администратор изменён на <code>{new_primary}</code>!</b>",
            parse_mode='HTML', reply_markup=pk_close())

    # ── Изменить приветствие ───────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_edit_welcome')
    def p_edit_welcome(cb):
        if not is_admin(cb.from_user.id): return
        _edit(
            f"<b>{TE_PENCIL} Введи новый текст приветствия</b>\n\n"
            "Поддерживается HTML:\n"
            "<code>&lt;b&gt;жирный&lt;/b&gt;</code>, <code>&lt;i&gt;курсив&lt;/i&gt;</code>\n\n"
            f"<b>Текущий текст:</b>\n{get_welcome()}",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back_admin())
        pstate[cb.from_user.id] = 'p_await_welcome'

    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'p_await_welcome')
    def p_save_welcome(m):
        if not is_admin(m.from_user.id): return
        pstate.pop(m.from_user.id, None)
        # валидация HTML ДО сохранения: кривой текст сделал бы /start
        # молча падающим у ВСЕХ пользователей бота
        try:
            pbot.send_message(m.chat.id,
                f"<b>{TE_CHECK}  Приветствие обновлено!</b>\n\n<b>Предпросмотр:</b>\n\n{m.text}",
                parse_mode='HTML', reply_markup=pk_close())
        except Exception:
            pbot.send_message(m.chat.id,
                f"<b>{TE_CROSS}  Приветствие НЕ сохранено: некорректный HTML "
                f"(проверь закрытие тегов &lt;b&gt;, &lt;i&gt; и т.п.). Попробуй ещё раз.</b>",
                parse_mode='HTML', reply_markup=pk_back_admin())
            return
        db_set_bot_setting(db_bot_id, 'welcome', m.text)

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
            parse_mode='HTML', reply_markup=pk_admin())

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
        if not is_admin(cb.from_user.id): return
        balance, total = db_get_bot_earnings(db_bot_id)
        ton_rate = get_ton_amount() if balance > 0 else 0
        from database import get_price as _get_price
        price = _get_price()
        ton_equiv = round(balance * ton_rate / price, 2) if (ton_rate and price and balance) else 0
        kb = InlineKeyboardMarkup()
        if balance > 0:
            kb.row(
                _pbtn('Вывести TON',   icon='💲', style='primary',  callback_data='p_withdraw'),
                _pbtn('Продлить бота', style='success', callback_data='p_renew_balance'),
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
        if not is_admin(cb.from_user.id): return
        balance, _ = db_get_bot_earnings(db_bot_id)
        if balance <= 0:
            pbot.answer_callback_query(cb.id, "❌ Баланс равен нулю", show_alert=True)
            return
        pstate[cb.from_user.id] = {'step': 'p_await_ton_address', 'balance': balance}
        _edit(
            f"<b>{TE_MONEY} Вывод средств</b>\n\n"
            f"Баланс для вывода: <b>{balance:.4f} USDT</b>\n\n"
            f"Введи свой TON-адрес (кошелёк для получения):",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back_admin())
        pstate[cb.from_user.id] = {'step': 'p_await_ton_address', 'balance': balance}

    @pbot.message_handler(func=lambda m: isinstance(pstate.get(m.from_user.id), dict)
                                         and pstate[m.from_user.id].get('step') == 'p_await_ton_address')
    def p_save_ton_address(m):
        if not is_admin(m.from_user.id): return
        address = m.text.strip()
        if len(address) < 32:
            pbot.send_message(m.chat.id, f"<b>{TE_CROSS}  Неверный TON-адрес. Попробуй ещё раз:</b>",
                parse_mode='HTML')
            return
        s = pstate[m.from_user.id]
        s['address'] = address
        s['step'] = 'p_await_withdraw_amount'
        pstate[m.from_user.id] = s
        balance = s['balance']
        pbot.send_message(m.chat.id,
            f"<b>{TE_MONEY} Адрес принят.</b>\n\n"
            f"Введи сумму для вывода (USDT) или отправь <code>всё</code> чтобы вывести весь баланс.\n\n"
            f"Доступно: <b>{balance:.4f} USDT</b>",
            parse_mode='HTML', reply_markup=pk_back_admin())

    @pbot.message_handler(func=lambda m: isinstance(pstate.get(m.from_user.id), dict)
                                         and pstate[m.from_user.id].get('step') == 'p_await_withdraw_amount')
    def p_save_withdraw_amount(m):
        if not is_admin(m.from_user.id): return
        s = pstate[m.from_user.id]
        balance = s['balance']
        address = s['address']
        text = m.text.strip().lower()
        if text in ('всё', 'all', 'все', 'all'):
            amount = balance
        else:
            try:
                amount = float(text.replace(',', '.'))
                if amount <= 0 or amount > balance:
                    raise ValueError
            except ValueError:
                pbot.send_message(m.chat.id,
                    f"<b>{TE_CROSS}  Неверная сумма. Максимум: {balance:.4f} USDT</b>",
                    parse_mode='HTML')
                return
        pstate.pop(m.from_user.id, None)
        # Атомарное списание: двойные конкурентные заявки не уйдут в минус
        from database import db_deduct_bot_earnings_atomic
        if not db_deduct_bot_earnings_atomic(db_bot_id, amount):
            pbot.send_message(m.chat.id,
                f"<b>{TE_CROSS}  Недостаточно средств (баланс изменился). Заявка не создана.</b>",
                parse_mode='HTML')
            return
        # Получаем данные владельца бота
        info = db_get_bot_info(db_bot_id)
        owner_id = info[2] if info else m.from_user.id
        # Создаём запрос
        wr_id = db_create_withdrawal(db_bot_id, owner_id, address, amount)
        # Отправляем в канал суперадмина через main_bot (чтобы кнопки обрабатывались в нём)
        if WITHDRAWAL_CHANNEL and main_bot:
            try:
                kb_admin = InlineKeyboardMarkup()
                kb_admin.row(
                    _pbtn(f'Подтвердить #{wr_id}', style='success', callback_data=f'wr_confirm_{wr_id}'),
                    _pbtn(f'Отклонить #{wr_id}',   style='danger',  callback_data=f'wr_reject_{wr_id}'),
                )
                from database import get_price as _get_price
                ton_rate = get_ton_amount()
                price = _get_price()
                ton_equiv = round(amount * ton_rate / price, 2) if (ton_rate and price) else '?'
                main_bot.send_message(WITHDRAWAL_CHANNEL,
                    f"{TE_MONEY} <b>Новый запрос на вывод #{wr_id}</b>\n\n"
                    f"Bot #{db_bot_id} | Owner: <code>{owner_id}</code>\n"
                    f"Адрес: <code>{address}</code>\n"
                    f"Сумма: <b>{amount:.4f} USDT</b> (~{ton_equiv} TON)",
                    parse_mode='HTML', reply_markup=kb_admin)
            except Exception as e:
                log.warning(f'withdrawal channel notify failed: {e}')
        pbot.send_message(m.chat.id,
            f"{TE_CHECK} <b>Запрос на вывод создан!</b>\n\n"
            f"Сумма: <b>{amount:.4f} USDT</b>\n"
            f"Адрес: <code>{address}</code>\n\n"
            f"Администратор обработает запрос в ближайшее время.",
            parse_mode='HTML', reply_markup=pk_close())

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_renew_balance')
    def p_renew_balance_cb(cb):
        if not is_admin(cb.from_user.id): return
        balance, _ = db_get_bot_earnings(db_bot_id)
        if balance <= 0:
            pbot.answer_callback_query(cb.id, "❌ Баланс равен нулю", show_alert=True)
            return
        from database import get_price as _get_price
        price = _get_price()
        max_days_full = int((balance / price) * 30) if price > 0 else 0
        kb = InlineKeyboardMarkup()
        options = []
        for days_opt in [30, 90, 180]:
            cost = round(price * days_opt / 30, 4)
            if balance >= cost:
                kb.add(_pbtn(
                    f'{days_opt} дней ({cost} USDT)',
                    style='success',
                    callback_data=f'p_renew_bal_{days_opt}'))
                options.append(days_opt)
        if balance > 0 and max_days_full > 0:
            kb.add(_pbtn(
                f'На весь баланс (~{max_days_full} дней)',
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
    def p_renew_bal_cb(cb):
        if not is_admin(cb.from_user.id): return
        from database import get_price as _get_price
        balance, _ = db_get_bot_earnings(db_bot_id)
        price = _get_price()
        arg = cb.data.split('_')[-1]
        if arg == 'all':
            days = int((balance / price) * 30) if price > 0 else 0
            cost = balance
        else:
            days = int(arg)
            cost = round(price * days / 30, 4)
        if days <= 0:
            pbot.answer_callback_query(cb.id, "❌ Недостаточно средств", show_alert=True)
            return
        if balance < cost:
            pbot.answer_callback_query(cb.id, f"❌ Недостаточно баланса. Нужно: {cost:.4f} USDT", show_alert=True)
            return
        from database import db_deduct_bot_earnings_atomic
        if not db_deduct_bot_earnings_atomic(db_bot_id, cost):
            pbot.answer_callback_query(cb.id,
                f"❌ Недостаточно средств (баланс изменился)", show_alert=True)
            return
        new_exp = db_renew_bot(db_bot_id, days)
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
            _edit(f"<b>{TE_NOTE} Введи текст рассылки (HTML):</b>",
                cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
            pstate[cb.from_user.id] = 'p_bcast_text'
        else:
            _edit(f"<b>{TE_PICTURE} Отправь фото с подписью (или без):</b>",
                cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
            pstate[cb.from_user.id] = 'p_bcast_photo'

    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'p_bcast_text')
    def p_bcast_text(m):
        if not is_admin(m.from_user.id): return
        pstate.pop(m.from_user.id, None)
        users = db_get_bot_users(db_bot_id)
        sent  = sum(1 for uid in users if _safe_send(pbot, uid, m.text))
        pbot.send_message(m.chat.id, f"<b>{TE_CHECK}  Рассылка завершена: {sent}/{len(users)}</b>",
            parse_mode='HTML', reply_markup=pk_close())

    @pbot.message_handler(content_types=['photo'],
                          func=lambda m: pstate.get(m.from_user.id) == 'p_bcast_photo')
    def p_bcast_photo(m):
        if not is_admin(m.from_user.id): return
        pstate.pop(m.from_user.id, None)
        users   = db_get_bot_users(db_bot_id)
        photo   = m.photo[-1].file_id
        caption = html.escape(m.caption or '')
        sent    = sum(1 for uid in users if _safe_photo(pbot, uid, photo, caption))
        pbot.send_message(m.chat.id, f"<b>{TE_CHECK}  Рассылка завершена: {sent}/{len(users)}</b>",
            parse_mode='HTML', reply_markup=pk_close())

    # ── Forum Topics: техподдержка через группу с темами ─────────
    # Регистрируется последним: все обработчики состояний выше имеют приоритет,
    # форум-обработчики ловят только то, что не перехватили они.
    fs_route = register_forum_support(pbot, db_bot_id, pstate, is_admin,
                                      confirm_kb=pk_close)

    return pbot


# Заглушка для замыкания (не используется в рантайме)
cb_from_user_id_placeholder = -1


def launch_bot(db_bot_id: int, token: str, admin_id: int, main_bot=None) -> bool:
    if token in running_bots: return True
    try:
        pbot = make_purchased_bot(db_bot_id, token, admin_id, main_bot)
        me   = pbot.get_me()
        if db_get_bot_setting(db_bot_id, 'notified_401', ''):
            db_set_bot_setting(db_bot_id, 'notified_401', '')
        try:
            pbot.delete_webhook(drop_pending_updates=True)
        except Exception as e:
            log.warning(f'Bot #{db_bot_id} delete_webhook failed: {e}')
        db_update_bot_username(db_bot_id, me.username or '')
        running_bots[token]        = pbot
        running_bot_ids[db_bot_id] = token

        def polling_loop():
            while running_bot_ids.get(db_bot_id) == token:
                try:
                    pbot.infinity_polling(timeout=60, long_polling_timeout=5,
                                  allowed_updates=['message', 'callback_query', 'message_reaction'])
                except Exception as e:
                    if running_bot_ids.get(db_bot_id) != token:
                        break
                    log.error(f'Bot #{db_bot_id} crashed: {e}')
                    if main_bot:
                        try:
                            main_bot.send_message(SUPER_ADMIN,
                                f"{TE_EXCLAM} <b>Bot #{db_bot_id} упал!</b>\n"
                                f"Ошибка: <code>{e}</code>\n"
                                f"{TE_REFRESH} Перезапускаем через 5 сек...",
                                parse_mode='HTML')
                        except Exception:
                            pass
                    time.sleep(5)
            log.info(f'Bot #{db_bot_id} polling stopped.')

        threading.Thread(target=polling_loop, daemon=True).start()
        log.info(f'Bot #{db_bot_id} (@{me.username}) launched.')
        return True
    except Exception as e:
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
