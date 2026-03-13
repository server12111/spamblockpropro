import telebot
import threading
import time
import logging
from datetime import datetime
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from database import (db_add_user, db_get_bot_users, db_deactivate_bot, DBState,
                      db_get_bot_info, db_get_bot_expires, db_update_bot_username,
                      db_block_user, db_unblock_user, db_is_blocked, db_get_blocked_list,
                      db_get_bot_setting, db_set_bot_setting,
                      db_get_bot_admins, db_add_bot_admin, db_remove_bot_admin,
                      db_set_primary_admin)
from keyboards import broadcast_type_kb
from config import SUPER_ADMIN

log = logging.getLogger(__name__)

running_bots    = {}  # {token: TeleBot}
running_bot_ids = {}  # {db_bot_id: token}

_rate_cache: dict = {}
RATE_LIMIT_SEC = 15

def _is_rate_limited(bot_id: int, user_id: int) -> bool:
    key = (bot_id, user_id)
    now = time.time()
    if now - _rate_cache.get(key, 0) < RATE_LIMIT_SEC:
        return True
    _rate_cache[key] = now
    return False

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

DEFAULT_WELCOME = ("<b>🤖 Привет! Это бот обратной связи.\n\n"
                   "💬 Отправь своё сообщение и администратор обязательно его прочитает.</b>")

def make_purchased_bot(db_bot_id: int, token: str, admin_id: int, main_bot=None):
    pbot   = telebot.TeleBot(token)
    pstate = DBState(db_bot_id)

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
                return f"{str(raw)[:10]} ❌ истекла"
        except Exception:
            return '—'

    def is_admin(user_id: int) -> bool:
        return user_id in db_get_bot_admins(db_bot_id)

    def is_primary_admin(user_id: int) -> bool:
        info = db_get_bot_info(db_bot_id)
        return bool(info and user_id == info[1])

    def send_to_admins(text, reply_markup=None):
        for aid in db_get_bot_admins(db_bot_id):
            try:
                pbot.send_message(aid, text, parse_mode='HTML', reply_markup=reply_markup)
            except Exception as e:
                log.warning(f'send_to_admins bot#{db_bot_id} admin {aid}: {e}')

    def send_media_to_admins(m, header):
        """Forward any media type to all admins with user info header."""
        kb = pk_reply(m.from_user.id)
        caption = header + (f"\n\n💬 {m.caption}" if m.caption else "")
        ct = m.content_type
        try:
            for aid in db_get_bot_admins(db_bot_id):
                if ct == 'photo':
                    pbot.send_photo(aid, m.photo[-1].file_id, caption=caption,
                                    parse_mode='HTML', reply_markup=kb)
                elif ct == 'video':
                    pbot.send_video(aid, m.video.file_id, caption=caption,
                                    parse_mode='HTML', reply_markup=kb)
                elif ct == 'document':
                    pbot.send_document(aid, m.document.file_id, caption=caption,
                                       parse_mode='HTML', reply_markup=kb)
                elif ct == 'audio':
                    pbot.send_audio(aid, m.audio.file_id, caption=caption,
                                    parse_mode='HTML', reply_markup=kb)
                elif ct == 'voice':
                    pbot.send_message(aid, header + "\n🎤 Голосовое сообщение:",
                                      parse_mode='HTML', reply_markup=kb)
                    pbot.send_voice(aid, m.voice.file_id)
                elif ct == 'video_note':
                    pbot.send_message(aid, header + "\n🎥 Видеосообщение:",
                                      parse_mode='HTML', reply_markup=kb)
                    pbot.send_video_note(aid, m.video_note.file_id)
                elif ct == 'sticker':
                    pbot.send_message(aid, header + "\n🎭 Стикер:",
                                      parse_mode='HTML', reply_markup=kb)
                    pbot.send_sticker(aid, m.sticker.file_id)
        except Exception as e:
            log.error(f'send_media_to_admins error: {e}')

    # ── Клавиатуры ─────────────────────────────────────
    def pk_start():
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton('📤 Отправить сообщение', callback_data='p_send'))
        return kb

    def pk_back():
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton('🔙 Назад', callback_data='p_cancel'))
        return kb

    def pk_close():
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton('❌ Закрыть', callback_data='p_close'))
        return kb

    def pk_back_admin():
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton('🔙 Назад', callback_data='p_back_admin'))
        return kb

    def pk_reply(uid):
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton('📤 Ответить',      callback_data=f'p_reply_{uid}'),
            InlineKeyboardButton('🚫 Заблокировать', callback_data=f'p_block_{uid}'),
        )
        return kb

    def pk_admin():
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton('📢 Рассылка',             callback_data='p_broadcast'))
        kb.add(InlineKeyboardButton('✏️ Изменить приветствие',  callback_data='p_edit_welcome'))
        kb.add(InlineKeyboardButton('👥 Заблокированные',       callback_data='p_blocked_list'))
        kb.add(InlineKeyboardButton('👤 Управление админами',   callback_data='p_admins'))
        kb.add(InlineKeyboardButton('🗑 Удалить бота',          callback_data='p_delete_bot'))
        return kb

    def pk_confirm_delete():
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton('✅ Да, удалить', callback_data='p_confirm_delete'),
            InlineKeyboardButton('❌ Отмена',      callback_data='p_cancel_delete'),
        )
        return kb

    # ── Команды ────────────────────────────────────────
    @pbot.message_handler(commands=['start'])
    def pstart(m):
        db_add_user(db_bot_id, m.from_user.id)
        pbot.send_message(m.chat.id, get_welcome(), parse_mode='HTML', reply_markup=pk_start())

    @pbot.message_handler(commands=['admin'])
    def padmin_cmd(m):
        if not is_admin(m.from_user.id): return
        pbot.send_message(m.chat.id,
            f"<b>⚙️ Панель админа</b>\n\n⏳ Подписка до: <b>{_get_exp_str()}</b>",
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
            f"<b>📊 Статус бота</b>\n\n"
            f"👥 Пользователей: <b>{users_cnt}</b>\n"
            f"🚫 Заблокированных: <b>{blocked_cnt}</b>\n"
            f"👤 Администраторов: <b>{admins_cnt}</b>\n"
            f"📅 Дата создания: <b>{created_at}</b>\n"
            f"⏳ Подписка до: <b>{_get_exp_str()}</b>\n"
            f"🟢 Статус: активен",
            parse_mode='HTML', reply_markup=pk_close())

    # ── Навигация ──────────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_back_admin')
    def p_back_admin_cb(cb):
        if not is_admin(cb.from_user.id): return
        pstate.pop(cb.from_user.id, None)
        pbot.edit_message_text("<b>⚙️ Панель админа</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_admin())

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_cancel')
    def p_cancel(cb):
        pstate.pop(cb.from_user.id, None)
        pbot.edit_message_text(get_welcome(), cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_start())

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_close')
    def p_close(cb):
        pbot.delete_message(cb.message.chat.id, cb.message.message_id)

    @pbot.callback_query_handler(func=lambda c: c.data == 'cancel')
    def p_cancel_generic(cb):
        pbot.delete_message(cb.message.chat.id, cb.message.message_id)
        pstate.pop(cb.from_user.id, None)

    # ── Отправить сообщение ────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_send')
    def p_user_send(cb):
        if db_is_blocked(db_bot_id, cb.from_user.id):
            pbot.answer_callback_query(cb.id, "🚫 Вы заблокированы администратором", show_alert=True)
            return
        pbot.edit_message_text(
            "<b>💬 Отправь сообщение, фото, видео, файл или голосовое:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back())
        pstate[cb.from_user.id] = 'await_msg'

    def _user_msg_checks(m) -> bool:
        """Общие проверки для всех входящих медиа. True = продолжать."""
        if db_is_blocked(db_bot_id, m.from_user.id):
            pbot.send_message(m.chat.id, "<b>🚫 Вы заблокированы администратором</b>",
                parse_mode='HTML')
            pstate.pop(m.from_user.id, None)
            return False
        if _is_rate_limited(db_bot_id, m.from_user.id):
            pbot.send_message(m.chat.id,
                f"<b>⏳ Подожди {RATE_LIMIT_SEC} секунд перед следующим сообщением</b>",
                parse_mode='HTML')
            return False
        return True

    def _make_header(m) -> str:
        username = f"@{m.from_user.username}" if m.from_user.username else "нет"
        return (f"<b>📥 Сообщение от:</b>\n"
                f"👤 Имя: <b>{m.from_user.full_name}</b>\n"
                f"🔗 Username: {username}\n"
                f"🆔 ID: <code>{m.from_user.id}</code>")

    # ── Текст ──────────────────────────────────────────
    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'await_msg')
    def p_user_text(m):
        if not _user_msg_checks(m): return
        header = _make_header(m)
        send_to_admins(f"{header}\n\n💬 {m.text}", reply_markup=pk_reply(m.from_user.id))
        pbot.send_message(m.chat.id, "<b>✅ Сообщение отправлено</b>",
            parse_mode='HTML', reply_markup=pk_close())
        pstate.pop(m.from_user.id, None)

    # ── Медиа ──────────────────────────────────────────
    @pbot.message_handler(
        content_types=['photo', 'video', 'document', 'audio', 'voice', 'video_note', 'sticker'],
        func=lambda m: pstate.get(m.from_user.id) == 'await_msg'
    )
    def p_user_media(m):
        if not _user_msg_checks(m): return
        send_media_to_admins(m, _make_header(m))
        pbot.send_message(m.chat.id, "<b>✅ Сообщение отправлено</b>",
            parse_mode='HTML', reply_markup=pk_close())
        pstate.pop(m.from_user.id, None)

    # ── Ответ админа ───────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_reply_'))
    def p_admin_reply(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        pbot.edit_message_text("<b>💬 Введи ответ:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_back())
        pstate[cb.from_user.id] = f'await_reply_{target}'

    @pbot.message_handler(func=lambda m: isinstance(pstate.get(m.from_user.id), str)
                                         and pstate[m.from_user.id].startswith('await_reply_'))
    def p_admin_text(m):
        if not is_admin(m.from_user.id): return
        target_id = int(pstate[m.from_user.id].split('_')[-1])
        pbot.send_message(target_id, f"<b>📥 Сообщение от администратора\n\n💬 {m.text}</b>",
            parse_mode='HTML')
        pbot.send_message(m.chat.id, "<b>✅ Ответ отправлен</b>",
            parse_mode='HTML', reply_markup=pk_close())
        pstate.pop(m.from_user.id, None)

    # ── Блокировка ─────────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data.startswith('p_block_'))
    def p_block_user(cb):
        if not is_admin(cb.from_user.id): return
        target = int(cb.data.split('_')[-1])
        db_block_user(db_bot_id, target)
        pbot.answer_callback_query(cb.id, f"🚫 Пользователь {target} заблокирован", show_alert=True)
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton('📤 Ответить',       callback_data=f'p_reply_{target}'),
            InlineKeyboardButton('✅ Разблокировать', callback_data=f'p_unblock_{target}'),
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
            InlineKeyboardButton('📤 Ответить',       callback_data=f'p_reply_{target}'),
            InlineKeyboardButton('🚫 Заблокировать', callback_data=f'p_block_{target}'),
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
            pbot.edit_message_text("<b>👥 Нет заблокированных пользователей</b>",
                chat_id, message_id, parse_mode='HTML', reply_markup=pk_back_admin())
            return
        kb = InlineKeyboardMarkup()
        for uid in blocked:
            kb.add(InlineKeyboardButton(f"✅ Разблокировать {uid}", callback_data=f'p_unblock_list_{uid}'))
        kb.add(InlineKeyboardButton('🔙 Назад', callback_data='p_back_admin'))
        pbot.edit_message_text(f"<b>🚫 Заблокированные ({len(blocked)}):</b>",
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
        lines = ["<b>👤 Текущие администраторы:</b>\n"]
        for aid in admins:
            label = f"👑 {aid} (главный)" if aid == primary_id else f"👤 {aid}"
            lines.append(label)
            if aid != primary_id:
                kb.add(InlineKeyboardButton(f"❌ Удалить {aid}", callback_data=f'p_rm_admin_{aid}'))
        kb.add(InlineKeyboardButton('➕ Добавить админа', callback_data='p_add_admin'))
        if primary_id and primary_id != cb_from_user_id_placeholder:
            kb.add(InlineKeyboardButton('🔄 Сменить главного', callback_data='p_change_primary'))
        kb.add(InlineKeyboardButton('🔙 Назад', callback_data='p_back_admin'))
        pbot.edit_message_text("\n".join(lines), chat_id, message_id,
            parse_mode='HTML', reply_markup=kb)

    # hack: нам нужен chat_id для _show_admins, передаём через замыкание
    _last_admins_msg = {}

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_admins')
    def p_admins_open(cb):
        if not is_primary_admin(cb.from_user.id): return
        _last_admins_msg[cb.from_user.id] = (cb.message.chat.id, cb.message.message_id)
        admins   = db_get_bot_admins(db_bot_id)
        info     = db_get_bot_info(db_bot_id)
        primary  = info[1] if info else None
        lines    = ["<b>👤 Администраторы бота:</b>\n"]
        kb       = InlineKeyboardMarkup()
        for aid in admins:
            tag = " 👑 (главный)" if aid == primary else ""
            lines.append(f"• <code>{aid}</code>{tag}")
            if aid != primary:
                kb.add(InlineKeyboardButton(f"❌ Убрать {aid}", callback_data=f'p_rm_admin_{aid}'))
        kb.add(InlineKeyboardButton('➕ Добавить',       callback_data='p_add_admin'))
        kb.add(InlineKeyboardButton('🔄 Сменить главного', callback_data='p_change_primary'))
        kb.add(InlineKeyboardButton('🔙 Назад',          callback_data='p_back_admin'))
        pbot.edit_message_text("\n".join(lines), cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=kb)

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
        pbot.edit_message_text(
            "<b>➕ Введи Telegram ID нового администратора:</b>\n"
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
            pbot.send_message(m.chat.id, "<b>❌ ID должен быть числом</b>", parse_mode='HTML')
            return
        pstate.pop(m.from_user.id, None)
        db_add_bot_admin(db_bot_id, new_admin)
        pbot.send_message(m.chat.id,
            f"<b>✅ Администратор <code>{new_admin}</code> добавлен!</b>\n"
            f"Теперь он будет получать сообщения и может отвечать.",
            parse_mode='HTML', reply_markup=pk_close())

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_change_primary')
    def p_change_primary(cb):
        if not is_primary_admin(cb.from_user.id): return
        pbot.edit_message_text(
            "<b>🔄 Введи Telegram ID нового главного администратора:</b>\n"
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
            pbot.send_message(m.chat.id, "<b>❌ ID должен быть числом</b>", parse_mode='HTML')
            return
        pstate.pop(m.from_user.id, None)
        db_set_primary_admin(db_bot_id, new_primary)
        pbot.send_message(m.chat.id,
            f"<b>✅ Главный администратор изменён на <code>{new_primary}</code>!</b>",
            parse_mode='HTML', reply_markup=pk_close())

    # ── Изменить приветствие ───────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_edit_welcome')
    def p_edit_welcome(cb):
        if not is_admin(cb.from_user.id): return
        pbot.edit_message_text(
            "<b>✏️ Введи новый текст приветствия</b>\n\n"
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
        db_set_bot_setting(db_bot_id, 'welcome', m.text)
        pbot.send_message(m.chat.id,
            f"<b>✅ Приветствие обновлено!</b>\n\n<b>Предпросмотр:</b>\n\n{m.text}",
            parse_mode='HTML', reply_markup=pk_close())

    # ── Удалить бота ───────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_delete_bot')
    def p_delete_bot(cb):
        if not is_primary_admin(cb.from_user.id): return
        pbot.edit_message_text(
            "<b>⚠️ Ты уверен что хочешь удалить бота?\n\n"
            "• Бот остановится навсегда\n"
            "• Все данные пользователей будут удалены\n"
            "• Это действие нельзя отменить</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_confirm_delete())

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_cancel_delete')
    def p_cancel_delete(cb):
        if not is_primary_admin(cb.from_user.id): return
        pbot.edit_message_text("<b>⚙️ Панель админа</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=pk_admin())

    @pbot.callback_query_handler(func=lambda c: c.data == 'p_confirm_delete')
    def p_confirm_delete(cb):
        if not is_primary_admin(cb.from_user.id): return
        pbot.edit_message_text(
            "<b>✅ Бот удалён. Спасибо за использование!</b>",
            cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
        db_deactivate_bot(db_bot_id)
        def _delayed_stop():
            time.sleep(1); stop_bot(token)
        threading.Thread(target=_delayed_stop, daemon=True).start()

    # ── Рассылка ───────────────────────────────────────
    @pbot.callback_query_handler(func=lambda c: c.data == 'p_broadcast')
    def p_broadcast_start(cb):
        if not is_admin(cb.from_user.id): return
        pbot.edit_message_text("<b>📢 Выбери тип рассылки:</b>",
            cb.message.chat.id, cb.message.message_id,
            parse_mode='HTML', reply_markup=broadcast_type_kb('p'))

    @pbot.callback_query_handler(func=lambda c: c.data.startswith('bcast_') and c.data.endswith('_p'))
    def p_broadcast_type(cb):
        if not is_admin(cb.from_user.id): return
        if 'text' in cb.data:
            pbot.edit_message_text("<b>📝 Введи текст рассылки (HTML):</b>",
                cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
            pstate[cb.from_user.id] = 'p_bcast_text'
        else:
            pbot.edit_message_text("<b>🖼 Отправь фото с подписью (или без):</b>",
                cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
            pstate[cb.from_user.id] = 'p_bcast_photo'

    @pbot.message_handler(func=lambda m: pstate.get(m.from_user.id) == 'p_bcast_text')
    def p_bcast_text(m):
        if not is_admin(m.from_user.id): return
        pstate.pop(m.from_user.id, None)
        users = db_get_bot_users(db_bot_id)
        sent  = sum(1 for uid in users if _safe_send(pbot, uid, m.text))
        pbot.send_message(m.chat.id, f"<b>✅ Рассылка завершена: {sent}/{len(users)}</b>",
            parse_mode='HTML', reply_markup=pk_close())

    @pbot.message_handler(content_types=['photo'],
                          func=lambda m: pstate.get(m.from_user.id) == 'p_bcast_photo')
    def p_bcast_photo(m):
        if not is_admin(m.from_user.id): return
        pstate.pop(m.from_user.id, None)
        users   = db_get_bot_users(db_bot_id)
        photo   = m.photo[-1].file_id
        caption = m.caption or ''
        sent    = sum(1 for uid in users if _safe_photo(pbot, uid, photo, caption))
        pbot.send_message(m.chat.id, f"<b>✅ Рассылка завершена: {sent}/{len(users)}</b>",
            parse_mode='HTML', reply_markup=pk_close())

    return pbot


# Заглушка для замыкания (не используется в рантайме)
cb_from_user_id_placeholder = -1


def launch_bot(db_bot_id: int, token: str, admin_id: int, main_bot=None) -> bool:
    if token in running_bots: return True
    try:
        pbot = make_purchased_bot(db_bot_id, token, admin_id, main_bot)
        me   = pbot.get_me()
        db_update_bot_username(db_bot_id, me.username or '')
        running_bots[token]        = pbot
        running_bot_ids[db_bot_id] = token

        def polling_loop():
            while running_bot_ids.get(db_bot_id) == token:
                try:
                    pbot.infinity_polling(timeout=60, long_polling_timeout=5)
                except Exception as e:
                    if running_bot_ids.get(db_bot_id) != token:
                        break
                    log.error(f'Bot #{db_bot_id} crashed: {e}')
                    if main_bot:
                        try:
                            main_bot.send_message(SUPER_ADMIN,
                                f"⚠️ <b>Bot #{db_bot_id} упал!</b>\n"
                                f"Ошибка: <code>{e}</code>\n"
                                f"🔄 Перезапускаем через 5 сек...",
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
        return False
