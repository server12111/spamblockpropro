# -*- coding: utf-8 -*-
"""
forum_support.py — режим техподдержки через Telegram Forum Topics для дочерних ботов.

Ключевые идеи:
- Один дочерний бот = одна привязанная группа (fs_groups). Отключение группы
  возвращает стандартный режим (личные сообщения админам).
- Один пользователь = одна тема (fs_topics). Создание сериализуется на всё время
  запроса Telegram, незавершённая заявка восстанавливается после перезапуска.
- Все сообщения пользователя копируются в его тему (copy → forward → resend),
  соответствие (chat_id, group_msg_id) → исходное сообщение хранится в fs_msgs.
- Админ отвечает стандартным «Ответить» на сообщение пользователя в теме —
  бот находит соответствие и доставляет ответ пользователю в личку.
- Случайные сообщения пользователя (без нажатия p_send) ставятся в очередь
  подтверждения (fs_pending), отправка только по кнопке.
- Еженедельная статистика — один общий планировщик (fs-weekly-stats) на все боты.
"""
import hashlib
import html
import logging
import os
import random
import re
import secrets
import sqlite3
from functools import wraps
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from telebot.apihelper import ApiTelegramException
from telebot.types import (InlineKeyboardMarkup, InlineKeyboardButton,
                           ReplyParameters, InputMediaPhoto, InputMediaVideo,
                           ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton,
                           KeyboardButtonRequestChat, ChatAdministratorRights)

from database import (_utc_now_str, db_get_topic_copies_by_src,
                      db_add_user, db_get_user_info, db_bump_user_contact,
                      db_add_username_history,
                      db_is_blocked, db_block_user, db_unblock_user, db_get_bot_admins,
                      db_get_templates, db_get_template, db_add_template, db_del_template,
                      db_get_bot_setting, db_set_bot_setting,
                      db_get_bot_info, db_log_message,
                      db_fs_count_range, db_fs_tickets_total,
                      db_fs_new_users_range, db_fs_users_total)
from config import STATS_TZ_OFFSET, RATE_LIMIT_SEC
from keyboards import (_ICON, TE_BLOCK, TE_CALENDAR, TE_CHAT, TE_CHECK, TE_CLIP, TE_CLOCK, TE_CROSS, TE_EXCLAM, TE_ID, TE_LINK, TE_NOTE, TE_SEND, TE_STATS, TE_USER)

log = logging.getLogger(__name__)

# ── Конфигурация ─────────────────────────────────────────────
TICKET_GAP_SEC = 6 * 3600                              # пауза между сообщениями, открывающая новое обращение
PENDING_MAX = 20                                       # максимум сообщений в очереди подтверждения
TOPIC_NAME_LIMIT = 128                                 # лимит Telegram на название темы

# ── Единый rate limit (используется и purchased_bot._is_rate_limited) ──
# RATE_LIMIT_SEC импортируется из config (FS_RATE_LIMIT, по умолчанию 3 сек)
_rate_cache: dict = {}
_rate_lock = threading.Lock()

def rate_limited(bot_id: int, user_id: int, mark: bool = True) -> bool:
    """Проверка лимита частоты.

    mark=True — зафиксировать время (поведение старой _is_rate_limited: вызывается
    при реальной отправке). mark=False — «мягкая» проверка без фиксации: применяется
    при подтверждении отправки, чтобы сообщение не потерялось, а просто потребовало
    повторного нажатия кнопки.
    Кэш ограничен: при переполнении устаревшие записи вычищаются (без утечек памяти).
    """
    key = (bot_id, user_id)
    now = time.time()
    with _rate_lock:
        if now - _rate_cache.get(key, 0) < RATE_LIMIT_SEC:
            return True
        if mark:
            _rate_cache[key] = now
        if len(_rate_cache) > 10000:
            for k in [k for k, ts in _rate_cache.items() if now - ts >= RATE_LIMIT_SEC]:
                _rate_cache.pop(k, None)
            if len(_rate_cache) > 10000:
                _rate_cache.clear()
    return False


# ══════════════════════════════════════════════════════════════
#  Локальный доступ к БД (таблицы fs_* создаются в database.init_db).
#  Используем общий пул database._conn: retry на disk I/O / locked,
#  thread-local кэш соединений — та же обёртка, что у всех db_* функций.
# ══════════════════════════════════════════════════════════════
from database import _conn as _pooled_conn

def _conn():
    return _pooled_conn()


# ── Группа ────────────────────────────────────────────────────
def fs_get_group(bot_id: int):
    """(bot_id, chat_id, title, thread_stats, connected_at) или None."""
    c = _conn()
    row = c.execute('SELECT bot_id, chat_id, title, thread_stats, connected_at '
                    'FROM fs_groups WHERE bot_id=?', (bot_id,)).fetchone()
    c.close(); return row

def fs_get_group_by_chat(chat_id: int):
    """(bot_id, chat_id, title) или None — группа привязана к другому боту?"""
    c = _conn()
    row = c.execute('SELECT bot_id, chat_id, title FROM fs_groups WHERE chat_id=?',
                    (chat_id,)).fetchone()
    c.close(); return row

def _group_guard(fn):
    @wraps(fn)
    def call(*args, **kwargs):
        bot_id = kwargs['bot_id'] if 'bot_id' in kwargs else args[0 if fn.__name__ in ('fs_bind_group', 'fs_unbind_group') else 1]
        with _mgmt_lock(bot_id):
            return fn(*args, **kwargs)
    return call


def _require_current_group(bot_id, row):
    current = fs_get_group(bot_id)
    if not current or current[1] != row[1]:
        from delivery_queue import RetryDelivery, current_sender
        if current_sender() is not None:
            raise RetryDelivery(0)
        raise RuntimeError('Support group changed')


@_group_guard
def fs_bind_group(bot_id: int, chat_id: int, title: str):
    """Привязка группы. Повторная привязка тем же ботом обновляет запись;
    конфликт chat_id с другим ботом → IntegrityError (защита на уровне БД)."""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        old = c.execute('SELECT chat_id FROM fs_groups WHERE bot_id=?', (bot_id,)).fetchone()
        if old and old[0] != chat_id:
            for table in ('fs_groups', 'fs_topics', 'fs_msgs', 'delivery_sources'):
                c.execute(f'DELETE FROM {table} WHERE bot_id=?', (bot_id,))
        c.execute('INSERT INTO fs_groups (bot_id, chat_id, title) VALUES (?,?,?) '
                  'ON CONFLICT(bot_id) DO UPDATE SET chat_id=excluded.chat_id, '
                  'title=excluded.title', (bot_id, chat_id, title or ''))
        c.commit()
    finally:
        c.close()

def fs_set_stats_thread(bot_id: int, thread_id: int):
    c = _conn()
    c.execute('UPDATE fs_groups SET thread_stats=? WHERE bot_id=?', (thread_id, bot_id))
    c.commit(); c.close()

@_group_guard
def fs_unbind_group(bot_id: int):
    """Отвязка группы: темы и соответствия сообщений становятся неактуальными
    (thread_id действует только внутри своей группы), пользовательские данные
    (bot_users, история username, обращения) сохраняются."""
    c = _conn()
    row = c.execute('SELECT chat_id FROM fs_groups WHERE bot_id=?', (bot_id,)).fetchone()
    c.execute('DELETE FROM fs_groups WHERE bot_id=?', (bot_id,))
    c.execute('DELETE FROM fs_topics WHERE bot_id=?', (bot_id,))
    c.execute('DELETE FROM fs_msgs WHERE bot_id=?', (bot_id,))
    c.execute('DELETE FROM delivery_sources WHERE bot_id=?', (bot_id,))
    c.commit(); c.close()
    return row[0] if row else None


# ── Служебная тема управления ──────────────────────────────────
MGMT_TEMPLATE_LIMIT = 50
MGMT_TEXT_LIMIT = 4096  # единицы UTF-16, включая символы вне BMP
_mgmt_locks = tuple(threading.RLock() for _ in range(64))


def _mgmt_lock(bot_id):
    return _mgmt_locks[bot_id % len(_mgmt_locks)]


def _mgmt_row(bot_id):
    c = _conn()
    try:
        return c.execute('SELECT chat_id, thread_mgmt, mgmt_msg_id, mgmt_wait_admin '
                         'FROM fs_groups WHERE bot_id=?', (bot_id,)).fetchone()
    finally:
        c.close()


def _mgmt_update(bot_id, gid, assignments, values=()):
    c = _conn()
    try:
        c.execute(f'UPDATE fs_groups SET {assignments} WHERE bot_id=? AND chat_id=?',
                  (*values, bot_id, gid))
        c.commit()
    finally:
        c.close()


def _mgmt_is_primary(bot_id, uid):
    info = db_get_bot_info(bot_id)
    return bool(info and uid == info[1])


def _mgmt_ensure_thread(pbot, bot_id):
    """Заявка в БД: один создатель, ожидание остальных, lease для умершего потока.

    Сетевые вызовы выполняются после commit, без удержания транзакции.
    Уникальная заявка не позволяет старому создателю стереть новую заявку.
    """
    claim = uuid.uuid4().hex
    for _ in range(100):
        row = _mgmt_row(bot_id)
        if not row or row[1]:
            return row
        gid = row[0]
        c = _conn()
        try:
            cur = c.execute('UPDATE fs_groups SET mgmt_claim=?, mgmt_claim_at=? '
                            'WHERE bot_id=? AND chat_id=? AND thread_mgmt IS NULL '
                            'AND (mgmt_claim IS NULL OR mgmt_claim_at<?)',
                            (claim, time.time(), bot_id, gid, time.time() - 300))
            c.commit()
            claimed = cur.rowcount > 0
        finally:
            c.close()
        if not claimed:
            time.sleep(0.1)
            continue
        try:
            topic = _api(pbot.create_forum_topic, gid, 'Управление')
            c = _conn()
            try:
                c.execute('UPDATE fs_groups SET thread_mgmt=?, mgmt_msg_id=NULL, '
                          'mgmt_wait_admin=NULL, mgmt_claim=NULL, mgmt_claim_at=NULL '
                          'WHERE bot_id=? AND chat_id=? AND mgmt_claim=?',
                          (topic.message_thread_id, bot_id, gid, claim))
                c.commit()
            finally:
                c.close()
            return _mgmt_row(bot_id)
        except Exception:
            c = _conn()
            try:
                c.execute('UPDATE fs_groups SET mgmt_claim=NULL, mgmt_claim_at=NULL '
                          'WHERE bot_id=? AND chat_id=? AND mgmt_claim=?',
                          (bot_id, gid, claim))
                c.commit()
            finally:
                c.close()
            raise
    raise RuntimeError('management topic creation in progress')


def _mgmt_list(bot_id):
    templates = db_get_templates(bot_id)
    text = (f'<b>{TE_NOTE} Шаблоны ответов ({len(templates)})</b>' if templates
            else f'<b>{TE_NOTE} Шаблонов пока нет</b>')
    kb = InlineKeyboardMarkup()
    for tid, body in templates[:MGMT_TEMPLATE_LIMIT]:
        preview = body[:35] + '…' if len(body) > 35 else body
        kb.add(_fbtn(preview, callback_data=f'fs_mgmt_view_{tid}'))
    kb.add(_fbtn('Добавить шаблон', icon='➕', callback_data='fs_mgmt_add'))
    return text, kb, 'HTML'


def _mgmt_render(pbot, bot_id, screen=None):
    """Правка меню; новое сообщение при удалении, новая тема при её удалении.

    Вызывается под _mgmt_lock, чтобы добавление, отмена и обновление из лички
    были последовательны и не создавали несколько закрепов в одном процессе.
    """
    for _ in range(3):
        row = _mgmt_ensure_thread(pbot, bot_id)
        if not row:
            return
        gid, thread, mid, _wait = row
        text, kb, mode = screen or _mgmt_list(bot_id)
        if mid:
            try:
                _api(pbot.edit_message_text, text, gid, mid, parse_mode=mode,
                     reply_markup=kb)
                return
            except ApiTelegramException as e:
                if 'message is not modified' in _err_low(e):
                    return
                # Старый/удалённый закреп: попробуем отправить новый в ту же тему.
        try:
            sent = _api(pbot.send_message, gid, text, parse_mode=mode,
                        message_thread_id=thread, reply_markup=kb)
        except ApiTelegramException as e:
            if not _is_topic_gone(e):
                raise
            c = _conn()
            try:
                c.execute('UPDATE fs_groups SET thread_mgmt=NULL, mgmt_msg_id=NULL, '
                          'mgmt_wait_admin=NULL WHERE bot_id=? AND chat_id=? '
                          'AND thread_mgmt=?', (bot_id, gid, thread))
                c.commit()
            finally:
                c.close()
            screen = None  # после пересоздания темы ожидание ввода отменено
            continue
        _mgmt_update(bot_id, gid, 'mgmt_msg_id=?', (sent.message_id,))
        try:
            _api(pbot.pin_chat_message, gid, sent.message_id, disable_notification=True)
        except Exception as e:
            log.warning(f'fs management pin bot#{bot_id}: {e}')
        return


def fs_refresh_management(pbot, bot_id):
    """Обновление из личной панели/при подключении/запуске — best effort."""
    try:
        with _mgmt_lock(bot_id):
            row = _mgmt_row(bot_id)
            if row:
                _mgmt_update(bot_id, row[0], 'mgmt_wait_admin=NULL')
                _mgmt_render(pbot, bot_id)
    except Exception as e:
        log.warning(f'fs management refresh bot#{bot_id}: {e}')


def _mgmt_text_units(text):
    return len(text.encode('utf-16-le')) // 2


def _mgmt_preview(text):
    if _mgmt_text_units(text) <= MGMT_TEXT_LIMIT:
        return text
    return text.encode('utf-16-le')[:(MGMT_TEXT_LIMIT - 1) * 2].decode(
        'utf-16-le', errors='ignore') + '…'


def fs_management_callback(pbot, bot_id, cb):
    with _mgmt_lock(bot_id):
        row = _mgmt_row(bot_id)
        m = cb.message
        if not (m and m.chat.type in ('group', 'supergroup') and row and row[1]
                and m.chat.id == row[0] and m.message_thread_id == row[1]
                and m.message_id == row[2] and _mgmt_is_primary(bot_id, cb.from_user.id)):
            pbot.answer_callback_query(cb.id, 'Доступно только главному админу', show_alert=True)
            return
        data = cb.data or ''
        parts = data.split('_')
        action = parts[2] if len(parts) >= 3 else ''
        screen = None
        if action in ('view', 'delete'):
            number = parts[3] if len(parts) == 4 else ''
            tid = (int(number) if number.isascii() and number.isdecimal()
                   and len(number) <= 19 and 0 < int(number) < 2**63 else None)
            body = db_get_template(tid, bot_id) if tid else None
            if body is None:
                pbot.answer_callback_query(cb.id, 'Шаблон не найден', show_alert=True)
                return
            if action == 'delete':
                db_del_template(tid, bot_id)
            else:
                kb = InlineKeyboardMarkup()
                kb.add(_fbtn('Удалить', icon='❌', style='danger', callback_data=f'fs_mgmt_delete_{tid}'))
                kb.add(_fbtn('Назад', icon='🔙', callback_data='fs_mgmt_back'))
                screen = (_mgmt_preview(body), kb, None)  # шаблон — обычный текст
        elif data == 'fs_mgmt_add':
            if len(db_get_templates(bot_id)) >= MGMT_TEMPLATE_LIMIT:
                pbot.answer_callback_query(cb.id, 'Можно сохранить не больше 50 шаблонов', show_alert=True)
                return
            kb = InlineKeyboardMarkup()
            kb.add(_fbtn('Отмена', icon='❌', callback_data='fs_mgmt_cancel'))
            screen = (f'<b>{TE_NOTE} Отправь текст шаблона сюда</b>', kb, 'HTML')
        elif data not in ('fs_mgmt_back', 'fs_mgmt_cancel'):
            pbot.answer_callback_query(cb.id, 'Доступно только главному админу', show_alert=True)
            return
        # Отмена фиксируется ДО обращения к Telegram, даже если правка меню упадёт.
        _mgmt_update(bot_id, row[0], 'mgmt_wait_admin=?',
                     (cb.from_user.id if data == 'fs_mgmt_add' else None,))
        _mgmt_render(pbot, bot_id, screen)
        pbot.answer_callback_query(cb.id)


def _mgmt_is_message(pbot, bot_id, m):
    if m.chat.type not in ('group', 'supergroup'):
        return False
    row = _mgmt_row(bot_id)
    if not row or m.chat.id != row[0]:
        return False
    if not row[1]:
        fs_refresh_management(pbot, bot_id)  # ленивая миграция старой привязки
        row = _mgmt_row(bot_id)
    return bool(row and row[1] and getattr(m, 'message_thread_id', None) == row[1])


def _mgmt_handle_message(pbot, bot_id, m):
    with _mgmt_lock(bot_id):
        row = _mgmt_row(bot_id)
        if not (row and m.chat.id == row[0] and m.message_thread_id == row[1]
                and m.from_user and row[3] == m.from_user.id
                and _mgmt_is_primary(bot_id, m.from_user.id)
                and m.content_type == 'text' and m.text):
            return
        if _mgmt_text_units(m.text) > MGMT_TEXT_LIMIT:
            _topic_notice(pbot, row[0], row[1], 'Текст шаблона должен быть не длиннее 4096 символов')
            return
        if len(db_get_templates(bot_id)) >= MGMT_TEMPLATE_LIMIT:
            _topic_notice(pbot, row[0], row[1], 'Можно сохранить не больше 50 шаблонов')
            return
        db_add_template(bot_id, m.text)
        _mgmt_update(bot_id, row[0], 'mgmt_wait_admin=NULL')
        fs_refresh_management(pbot, bot_id)
        _topic_notice(pbot, row[0], row[1], 'Шаблон сохранён')


# ── Темы ──────────────────────────────────────────────────────
def fs_get_topic(bot_id: int, user_id: int):
    """(thread_id, title, pinned_msg_id, info_hash) или None."""
    c = _conn()
    row = c.execute('SELECT thread_id, title, pinned_msg_id, info_hash '
                    'FROM fs_topics WHERE bot_id=? AND user_id=?',
                    (bot_id, user_id)).fetchone()
    c.close(); return row

def fs_get_topic_by_thread(bot_id: int, thread_id: int):
    c = _conn()
    row = c.execute('SELECT user_id FROM fs_topics WHERE bot_id=? AND thread_id=?',
                    (bot_id, thread_id)).fetchone()
    c.close(); return row[0] if row else None

def fs_claim_topic(bot_id: int, user_id: int, title: str) -> bool:
    """Атомарный захват права создания темы (защита от конкурентного создания)."""
    c = _conn()
    cur = c.execute('INSERT OR IGNORE INTO fs_topics (bot_id, user_id, thread_id, title) '
                    'VALUES (?,?,0,?)', (bot_id, user_id, title))
    c.commit(); c.close()
    return cur.rowcount > 0

def fs_set_topic_thread(bot_id: int, user_id: int, thread_id: int, title: str = None):
    c = _conn()
    if title is None:
        c.execute('UPDATE fs_topics SET thread_id=?, updated_at=? '
                  'WHERE bot_id=? AND user_id=?',
                  (thread_id, _utc_now_str(), bot_id, user_id))
    else:
        c.execute('UPDATE fs_topics SET thread_id=?, title=?, updated_at=? '
                  'WHERE bot_id=? AND user_id=?',
                  (thread_id, title, _utc_now_str(), bot_id, user_id))
    c.commit(); c.close()

def fs_delete_topic_row(bot_id: int, user_id: int):
    c = _conn()
    c.execute('DELETE FROM fs_topics WHERE bot_id=? AND user_id=?', (bot_id, user_id))
    c.commit(); c.close()

def fs_set_topic_pinned(bot_id: int, user_id: int, msg_id: int, info_hash: str):
    c = _conn()
    c.execute('UPDATE fs_topics SET pinned_msg_id=?, info_hash=?, updated_at=? '
              'WHERE bot_id=? AND user_id=?',
              (msg_id, info_hash, _utc_now_str(), bot_id, user_id))
    c.commit(); c.close()

def fs_set_topic_title(bot_id: int, user_id: int, title: str):
    c = _conn()
    c.execute('UPDATE fs_topics SET title=?, updated_at=? WHERE bot_id=? AND user_id=?',
              (title, _utc_now_str(), bot_id, user_id))
    c.commit(); c.close()


# ── Соответствие сообщений ────────────────────────────────────
def fs_map_message(bot_id: int, chat_id: int, group_msg_id: int, user_id: int,
                   thread_id: int, src_msg_id, media_group: str = ''):
    c = _conn()
    c.execute('INSERT OR REPLACE INTO fs_msgs '
              '(chat_id, group_msg_id, bot_id, user_id, thread_id, src_msg_id, media_group) '
              'VALUES (?,?,?,?,?,?,?)',
              (chat_id, group_msg_id, bot_id, user_id, thread_id,
               src_msg_id, media_group or ''))
    c.commit(); c.close()

def fs_get_mapping(chat_id: int, group_msg_id: int):
    """(bot_id, user_id, thread_id, src_msg_id) или None."""
    c = _conn()
    row = c.execute('SELECT bot_id, user_id, thread_id, src_msg_id FROM fs_msgs '
                    'WHERE chat_id=? AND group_msg_id=?',
                    (chat_id, group_msg_id)).fetchone()
    c.close(); return row


# ── Очередь подтверждения ─────────────────────────────────────
_pending_session = uuid.uuid4().hex
_pending_activity_lock = threading.Lock()
_pending_active = set()


def _recover_pending_runtime(bot_id):
    """Регистрация обработчиков освобождает прерванные, но не живые отправки."""
    with _pending_activity_lock:
        c = _conn()
        try:
            c.execute('BEGIN IMMEDIATE')
            rows = c.execute("SELECT user_id FROM fs_pending WHERE bot_id=? AND status='sending'",
                             (bot_id,)).fetchall()
            for (uid,) in rows:
                if (bot_id, uid) not in _pending_active:
                    c.execute("UPDATE fs_pending SET status='pending', claim_session=NULL "
                              'WHERE bot_id=? AND user_id=?', (bot_id, uid))
            c.commit()
        finally:
            c.close()


def fs_get_pending(bot_id: int, user_id: int):
    """(payload_json, status, prompt_msg_id) или None."""
    c = _conn()
    row = c.execute('SELECT payload, status, prompt_msg_id FROM fs_pending '
                    'WHERE bot_id=? AND user_id=?', (bot_id, user_id)).fetchone()
    c.close(); return row

def fs_append_pending(bot_id: int, user_id: int, item: dict):
    """Добавляет сообщение в очередь. Возвращает (кол-во, prompt_msg_id)
    или (None, None) если очередь полна.
    BEGIN IMMEDIATE: атомарный read-modify-write — параллельные сообщения
    одного юзера (телеграм-воркеры) не теряют элементы."""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        row = c.execute('SELECT payload, prompt_msg_id FROM fs_pending '
                        'WHERE bot_id=? AND user_id=?', (bot_id, user_id)).fetchone()
        if row:
            try:
                items = json_loads(row[0])
            except Exception:
                items = []
            if any(it['mid'] == item['mid'] for it in items):
                c.commit()
                return len(items), row[1]
            if len(items) >= PENDING_MAX:
                c.execute('ROLLBACK')
                c.close()
                return None, row[1]
            items.append(item)
            c.execute('UPDATE fs_pending SET payload=?, updated_at=? '
                      'WHERE bot_id=? AND user_id=?',
                      (json_dumps(items), _utc_now_str(), bot_id, user_id))
            cnt, prompt = len(items), row[1]
        else:
            items = [item]
            c.execute('INSERT INTO fs_pending (bot_id, user_id, payload, updated_at) '
                      'VALUES (?,?,?,?)',
                      (bot_id, user_id, json_dumps(items), _utc_now_str()))
            cnt, prompt = 1, None
        c.commit()
        return cnt, prompt
    except Exception:
        try:
            c.execute('ROLLBACK')
        except Exception:
            pass
        raise
    finally:
        c.close()

def fs_set_prompt(bot_id: int, user_id: int, msg_id: int):
    c = _conn()
    c.execute('UPDATE fs_pending SET prompt_msg_id=? WHERE bot_id=? AND user_id=?',
              (msg_id, bot_id, user_id))
    c.commit(); c.close()

def fs_claim_pending(bot_id: int, user_id: int):
    """Атомарно забирает очередь в работу (защита от повторного нажатия кнопки).
    Возвращает список элементов или None, если очередь уже обрабатывается."""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        cur = c.execute("UPDATE fs_pending SET status='sending', updated_at=?, claim_session=? "
                        "WHERE bot_id=? AND user_id=? AND (status='pending' OR "
                        "(status='sending' AND COALESCE(claim_session, '') != ?))",
                        (_utc_now_str(), _pending_session, bot_id, user_id, _pending_session))
        row = c.execute('SELECT payload FROM fs_pending WHERE bot_id=? AND user_id=?',
                        (bot_id, user_id)).fetchone() if cur.rowcount else None
        c.commit()
    finally:
        c.close()
    if row is None:
        return None
    try:
        return json_loads(row[0])
    except Exception:
        return []

def _save_pending_progress(bot_id: int, user_id: int, items: list, finish=False):
    """Сохраняет результат захваченной пачки, не затрагивая новые сообщения."""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        row = c.execute('SELECT payload FROM fs_pending WHERE bot_id=? AND user_id=? '
                        'AND claim_session=?', (bot_id, user_id, _pending_session)).fetchone()
        if not row:
            c.commit()
            return []
        progress = {it['mid']: it for it in items}
        merged = [progress.get(it['mid'], it) for it in json_loads(row[0])]
        if finish:
            merged = [it for it in merged if not it.get('done')]
        if finish and not merged:
            c.execute('DELETE FROM fs_pending WHERE bot_id=? AND user_id=?', (bot_id, user_id))
        else:
            c.execute('UPDATE fs_pending SET payload=?, status=?, updated_at=? '
                      'WHERE bot_id=? AND user_id=?',
                      (json_dumps(merged), 'pending' if finish else 'sending',
                       _utc_now_str(), bot_id, user_id))
        c.commit()
        return merged
    finally:
        c.close()


def fs_restore_pending(bot_id: int, user_id: int, items: list):
    return _save_pending_progress(bot_id, user_id, items, finish=True)

def fs_delete_pending(bot_id: int, user_id: int):
    c = _conn()
    c.execute('DELETE FROM fs_pending WHERE bot_id=? AND user_id=?', (bot_id, user_id))
    c.commit(); c.close()


# ── Обращения ─────────────────────────────────────────────────
def fs_bump_ticket(bot_id: int, user_id: int):
    """Учёт обращений: серия сообщений с паузой < TICKET_GAP_SEC = одно обращение.
    BEGIN IMMEDIATE: два одновременных сообщения не создадут дубль обращения."""
    now_dt = datetime.utcnow()
    now_s = now_dt.strftime('%Y-%m-%d %H:%M:%S')
    c = _conn()
    c.execute('BEGIN IMMEDIATE')
    row = c.execute('SELECT id, last_at FROM fs_tickets '
                    'WHERE bot_id=? AND user_id=? AND closed_at IS NULL '
                    'ORDER BY id DESC LIMIT 1', (bot_id, user_id)).fetchone()
    if row:
        last_dt = None
        try:
            last_dt = datetime.strptime(str(row[1])[:19], '%Y-%m-%d %H:%M:%S') if row[1] else None
        except ValueError:
            pass
        if last_dt and (now_dt - last_dt).total_seconds() < TICKET_GAP_SEC:
            c.execute('UPDATE fs_tickets SET msgs=msgs+1, last_at=? WHERE id=?',
                      (now_s, row[0]))
            c.commit(); c.close()
            return
        # зависшее открытое обращение закрываем и открываем новое
        c.execute('UPDATE fs_tickets SET closed_at=last_at WHERE id=? AND closed_at IS NULL',
                  (row[0],))
    c.execute('INSERT INTO fs_tickets (bot_id, user_id, opened_at, last_at, msgs) '
              'VALUES (?,?,?,?,1)', (bot_id, user_id, now_s, now_s))
    c.commit(); c.close()


# ── Ответы админов ────────────────────────────────────────────
def fs_log_admin_msg(bot_id: int, user_id: int):
    c = _conn()
    c.execute('INSERT INTO fs_admin_msgs (bot_id, user_id) VALUES (?,?)', (bot_id, user_id))
    c.commit(); c.close()


# ── Еженедельные отчёты ───────────────────────────────────────
def fs_report_get(bot_id: int, period_start: str):
    c = _conn()
    row = c.execute('SELECT status, message_id FROM fs_weekly_reports '
                    'WHERE bot_id=? AND period_start=?', (bot_id, period_start)).fetchone()
    c.close(); return row

def fs_report_mark_queued(bot_id: int, period_start: str, period_end: str):
    c = _conn()
    c.execute('INSERT OR IGNORE INTO fs_weekly_reports (bot_id, period_start, period_end, status) '
              "VALUES (?,? ,?, 'queued')", (bot_id, period_start, period_end))
    c.commit(); c.close()

def fs_report_mark_sent(bot_id: int, period_start: str, message_id: int):
    c = _conn()
    c.execute("UPDATE fs_weekly_reports SET status='sent', message_id=?, published_at=? "
              'WHERE bot_id=? AND period_start=?',
              (message_id, _utc_now_str(), bot_id, period_start))
    c.commit(); c.close()


# ── Кэш админов (TTL, ограничен числом ботов) ────────────────
def fs_is_admin(bot_id: int, user_id: int) -> bool:
    return user_id in db_get_bot_admins(bot_id)


# ══════════════════════════════════════════════════════════════
#  Telegram API: вызовы с обработкой 429 и классификация ошибок
# ══════════════════════════════════════════════════════════════
def _api(fn, *args, **kwargs):
    """Очередь сохраняет повтор; служебные вызовы ждут полный retry_after"""
    from delivery_queue import current_sender
    sender = current_sender()
    if sender is not None:
        return sender.api(fn, *args, **kwargs)
    from telegram_text import retry_telegram
    def call():
        try:
            return fn(*args, **kwargs)
        except ApiTelegramException as e:
            if 'not modified' in str(e).lower(): return True
            raise
    return retry_telegram(call)

def _err_low(e) -> str:
    return str(e).lower()

def _is_topic_gone(e) -> bool:
    s = _err_low(e)
    return ('thread not found' in s or 'topic deleted' in s
            or 'topic closed' in s or 'topic_closed' in s
            or 'thread closed' in s)

def _is_bot_blocked(e) -> bool:
    return 'bot was blocked by the user' in _err_low(e)

def _is_chat_not_found(e) -> bool:
    s = _err_low(e)
    return 'chat not found' in s or 'user deactivated' in s or 'peer id invalid' in s


# ══════════════════════════════════════════════════════════════
#  Вспомогательные функции (HTML-экранирование пользовательских данных)
# ══════════════════════════════════════════════════════════════
def _full_name(u) -> str:
    fn = (getattr(u, 'first_name', None) or '').strip()
    ln = (getattr(u, 'last_name', None) or '').strip()
    name = (fn + ' ' + ln).strip()
    return name or 'Пользователь'

def fs_topic_title(u) -> str:
    """'Имя | @username | ID'; без username поле пропускается; ID сохраняется
    даже при сокращении имени."""
    name = _full_name(u)
    uname = f"@{u.username}" if getattr(u, 'username', None) else ''
    uid = str(u.id)
    if uname:
        title = f"{name} | {uname} | {uid}"
    else:
        title = f"{name} | {uid}"
    if len(title) > TOPIC_NAME_LIMIT:
        tail = f" | {uname} | {uid}" if uname else f" | {uid}"
        avail = TOPIC_NAME_LIMIT - len(tail)
        title = name[:max(avail, 1)] + tail
    return title[:TOPIC_NAME_LIMIT]

def _fmt_dt_utc(utc_val) -> str:
    """UTC-строка из БД → 'ДД.ММ.ГГГГ ЧЧ:ММ' в часовом поясе статистики.
    None/пусто → None (показываем «Неизвестно», не подменяем датой)."""
    if not utc_val:
        return None
    try:
        s = str(utc_val).replace('T', ' ')[:19]
        dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None
    dt += timedelta(hours=STATS_TZ_OFFSET)
    return dt.strftime('%d.%m.%Y %H:%M')

def _fmt_num(n) -> str:
    return f"{n:,}".replace(',', ' ')

def fs_info_text(bot_id: int, u) -> str:
    """Закрепляемая информация о пользователе (все данные экранированы)."""
    username = f"@{u.username}" if getattr(u, 'username', None) else 'нет username'
    return (f"<b>{TE_USER} {html.escape(_full_name(u))}</b>\n"
            f"{TE_LINK} {html.escape(username)}\n"
            f"{TE_ID} <code>{u.id}</code>")


def fs_pin_markup(bot_id: int, user_id: int):
    kb = InlineKeyboardMarkup()
    blocked = db_is_blocked(bot_id, user_id)
    kb.row(
        _fbtn('Шаблоны', icon='📝', style='primary', callback_data=f'fs_pin_tpl_{user_id}'),
        _fbtn('Разблокировать' if blocked else 'Заблокировать',
              icon='✔️' if blocked else '🚫', style='success' if blocked else 'danger',
              callback_data=f'fs_pin_unblock_{user_id}' if blocked else f'fs_pin_block_{user_id}'),
    )
    return kb


def fs_refresh_pin_markup(pbot, bot_id: int, user_id: int):
    """Best effort после изменения блокировки, в том числе из личной панели.

    Хеш текста не меняем: следующая сверка также обновит старый формат закрепа,
    если блокировку нажали до его миграции.
    """
    try:
        group = fs_get_group(bot_id)
        topic = fs_get_topic(bot_id, user_id)
        if group and topic and topic[2]:
            _api(pbot.edit_message_reply_markup, group[1], topic[2],
                 reply_markup=fs_pin_markup(bot_id, user_id))
    except Exception as e:
        log.warning(f'fs pin markup bot#{bot_id} uid={user_id}: {e}')


# ══════════════════════════════════════════════════════════════
#  Управление пользователем: история username, имена, обращение
# ══════════════════════════════════════════════════════════════
def fs_touch_user(pbot, bot_id: int, u) -> bool:
    """Обновляет данные пользователя и историю username.
    Возвращает True, если username изменился по сравнению с сохранённым."""
    username = getattr(u, 'username', None) or ''
    first_name = getattr(u, 'first_name', None) or ''
    old = db_get_user_info(bot_id, u.id)
    is_new = db_add_user(bot_id, u.id, username, first_name)
    changed = bool(old and not is_new and (old.get('username') or '') != username)
    if changed and old.get('username'):
        # старый username фиксируем в истории, если он ещё не записан
        db_add_username_history(bot_id, u.id, old['username'])
    if username:
        db_add_username_history(bot_id, u.id, username)
    return changed


# ══════════════════════════════════════════════════════════════
#  Создание темы и закреплённого сообщения
# ══════════════════════════════════════════════════════════════
_topic_locks = tuple(threading.RLock() for _ in range(256))


def _topic_lock(bot_id, uid):
    return _topic_locks[hash((bot_id, uid)) % len(_topic_locks)]


def _get_or_create_topic(pbot, bot_id: int, gid: int, u, depth: int = 0) -> int:
    """Живой создатель удерживает блокировку на всё время запроса Telegram."""
    with _topic_lock(bot_id, u.id):
        return _create_topic_locked(pbot, bot_id, gid, u)


def _create_topic_locked(pbot, bot_id, gid, u):
    uid = u.id
    title = fs_topic_title(u)
    row = fs_get_topic(bot_id, uid)
    if row and row[0] > 0:
        return row[0]
    # При свободной блокировке нулевая заявка осталась от завершённого runtime
    if row:
        fs_delete_topic_row(bot_id, uid)
    if fs_claim_topic(bot_id, uid, title):
        # мы выиграли гонку — создаём тему
        try:
            t = _api(pbot.create_forum_topic, gid, title)
            fs_set_topic_thread(bot_id, uid, t.message_thread_id, title)
            return t.message_thread_id
        except Exception:
            fs_delete_topic_row(bot_id, uid)  # отпускаем заявку
            raise
    raise RuntimeError('topic claim busy')


def _ensure_pinned_info(pbot, bot_id: int, gid: int, thread: int, u, trow):
    """Создаёт или обновляет закреплённое инфо-сообщение (без повторного пина)."""
    info_text = fs_info_text(bot_id, u)
    blocked = db_is_blocked(bot_id, u.id)
    h = hashlib.md5(f'{info_text}\n{int(blocked)}'.encode('utf-8')).hexdigest()
    markup = fs_pin_markup(bot_id, u.id)
    pinned = trow[2] if trow else None
    if pinned and trow and trow[3] == h:
        return  # актуально
    if pinned:
        try:
            _api(pbot.edit_message_text, info_text, gid, pinned, parse_mode='HTML',
                 reply_markup=markup)
            fs_set_topic_pinned(bot_id, u.id, pinned, h)
            return
        except ApiTelegramException as e:
            if 'message is not modified' in _err_low(e):
                # Кнопки уже обновлены из панели/закрепа; сохраняем новый хеш.
                fs_set_topic_pinned(bot_id, u.id, pinned, h)
                return
            pass  # сообщение удалено — создадим заново ниже
        except Exception as e:
            log.warning(f'fs pin edit bot#{bot_id}: {e}')
            return
    try:
        msg = _api(pbot.send_message, gid, info_text, parse_mode='HTML',
                   message_thread_id=thread, reply_markup=markup)
        try:
            _api(pbot.pin_chat_message, gid, msg.message_id, disable_notification=True)
        except Exception as e:
            # нет прав на закрепление — сообщение всё равно отправлено
            log.warning(f'fs pin bot#{bot_id} uid={u.id}: {e}')
        fs_set_topic_pinned(bot_id, u.id, msg.message_id, h)
    except ApiTelegramException:
        raise
    except Exception as e:
        log.warning(f'fs pin send bot#{bot_id} uid={u.id}: {e}')


@_group_guard
def fs_ensure_topic_info(pbot, bot_id: int, group_row, u) -> int:
    """Тема + закреплённая информация + актуальное название темы. → thread_id"""
    _require_current_group(bot_id, group_row)
    with _topic_lock(bot_id, u.id):
        for attempt in range(2):
            thread = _get_or_create_topic(pbot, bot_id, group_row[1], u)
            try:
                return _ensure_topic_info_locked(pbot, bot_id, group_row, u, thread)
            except Exception as e:
                if attempt or not _is_topic_err(e):
                    raise
                _recover_topic(pbot, bot_id, group_row[1], u, thread)


def _ensure_topic_info_locked(pbot, bot_id, group_row, u, thread):
    gid = group_row[1]
    trow = fs_get_topic(bot_id, u.id)
    _ensure_pinned_info(pbot, bot_id, gid, thread, u, trow)
    title = fs_topic_title(u)
    if not trow or trow[1] != title:
        try:
            _api(pbot.edit_forum_topic, gid, thread, name=title)
        except Exception as e:
            log.warning(f'fs topic rename bot#{bot_id} uid={u.id}: {e}')
        fs_set_topic_title(bot_id, u.id, title)
    return thread


# ══════════════════════════════════════════════════════════════
#  Передача сообщений: copy → forward → resend
# ══════════════════════════════════════════════════════════════
def _reply_params(reply_msg_id):
    if not reply_msg_id:
        return None
    return ReplyParameters(message_id=reply_msg_id, allow_sending_without_reply=True)

def _resend_by_type(pbot, to_chat, thread, m, reply_msg_id=None, *, api=None):
    """Третий способ передачи: повторная отправка содержимого через Bot API."""
    call = api or _api
    rp = _reply_params(reply_msg_id)
    kw = {'message_thread_id': thread} if thread else {}
    ct = m.content_type
    if ct == 'text':
        return call(pbot.send_message, to_chat, m.text, entities=m.entities,
                    reply_parameters=rp, **kw).message_id
    if ct == 'photo':
        return call(pbot.send_photo, to_chat, m.photo[-1].file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'video':
        return call(pbot.send_video, to_chat, m.video.file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'document':
        return call(pbot.send_document, to_chat, m.document.file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'audio':
        return call(pbot.send_audio, to_chat, m.audio.file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'voice':
        return call(pbot.send_voice, to_chat, m.voice.file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'video_note':
        return call(pbot.send_video_note, to_chat, m.video_note.file_id, **kw).message_id
    if ct == 'sticker':
        return call(pbot.send_sticker, to_chat, m.sticker.file_id, reply_parameters=rp, **kw).message_id
    if ct == 'animation':
        return call(pbot.send_animation, to_chat, m.animation.file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'contact':
        c = m.contact
        return call(pbot.send_contact, to_chat, c.phone_number, c.first_name,
                    last_name=c.last_name, vcard=c.vcard, reply_parameters=rp, **kw).message_id
    if ct == 'location':
        return call(pbot.send_location, to_chat, m.location.latitude,
                    m.location.longitude, reply_parameters=rp, **kw).message_id
    if ct == 'venue':
        v = m.venue
        return call(pbot.send_venue, to_chat, v.location.latitude, v.location.longitude,
                    v.title, v.address, reply_parameters=rp, **kw).message_id
    if ct == 'poll' and m.poll:
        opts = [o.text for o in m.poll.options]
        return call(pbot.send_poll, to_chat, m.poll.question, opts,
                    is_anonymous=m.poll.is_anonymous, type=m.poll.type,
                    allows_multiple_answers=m.poll.allows_multiple_answers,
                    correct_option_id=m.poll.correct_option_id,
                    explanation=m.poll.explanation, explanation_entities=m.poll.explanation_entities,
                    reply_parameters=rp, **kw).message_id
    return None

def _relay_single(pbot, to_chat, from_chat, msg_id, thread=None,
                  reply_msg_id=None, src_msg=None):
    """Приоритет передачи одного сообщения: копирование → пересылка → resend.
    Возвращает id созданного сообщения или None (передать невозможно).
    «Пользователь заблокировал бота» / чат не найден — терминальные ошибки:
    все способы дадут тот же результат, поэтому бросаем сразу."""
    rp = _reply_params(reply_msg_id)
    kw = {'message_thread_id': thread} if thread else {}
    # 1. копирование (сохраняет вложения/форматирование, не раскрывает отправителя)
    try:
        return _api(pbot.copy_message, to_chat, from_chat, msg_id,
                    reply_parameters=rp, **kw).message_id
    except ApiTelegramException as e:
        if _is_topic_gone(e):
            raise  # восстановлением темы займётся обёртка
        if _is_bot_blocked(e) or _is_chat_not_found(e):
            raise  # терминальная ошибка доставки
        log.debug(f'relay copy failed: {e}')
    except Exception as e:
        log.debug(f'relay copy error: {e}')
    # 2. пересылка (может раскрыть первоначального отправителя)
    try:
        return _api(pbot.forward_message, to_chat, from_chat, msg_id, **kw).message_id
    except ApiTelegramException as e:
        if _is_topic_gone(e) or _is_bot_blocked(e) or _is_chat_not_found(e):
            raise
        log.debug(f'relay forward failed: {e}')
    except Exception as e:
        log.debug(f'relay forward error: {e}')
    # 3. повторная отправка
    if src_msg is not None:
        try:
            return _resend_by_type(pbot, to_chat, thread, src_msg, reply_msg_id)
        except Exception as e:
            log.warning(f'relay resend failed: {e}')
            if isinstance(e, ApiTelegramException) and (_is_bot_blocked(e) or _is_chat_not_found(e)):
                raise
    return None

def _rebuild_media_group(msgs):
    """Пересборка альбома из file_id (только фото/видео)."""
    media = []
    for i, m in enumerate(msgs):
        cap = m.caption if i == 0 else None
        if m.content_type == 'photo':
            media.append(InputMediaPhoto(m.photo[-1].file_id, caption=cap))
        elif m.content_type == 'video':
            media.append(InputMediaVideo(m.video.file_id, caption=cap))
        else:
            return None
    return media if len(media) > 1 else None

def _recover_topic(pbot, bot_id: int, gid: int, u, thread: int):
    """Тема закрыта/удалена: переоткрываем или пересоздаём. → новый thread_id"""
    with _topic_lock(bot_id, u.id):
        row = fs_get_topic(bot_id, u.id)
        if row and row[0] > 0 and row[0] != thread:
            return row[0]
        try:
            _api(pbot.reopen_forum_topic, gid, thread)
            return thread
        except Exception:
            pass
        from delivery_queue import current_sender
        sender = current_sender()
        if sender is not None:
            sender.forget_topic(gid, fs_topic_title(u))
        fs_delete_topic_row(bot_id, u.id)
        return _get_or_create_topic(pbot, bot_id, gid, u)

def _is_topic_err(e) -> bool:
    return isinstance(e, ApiTelegramException) and _is_topic_gone(e)

@_group_guard
def _relay_to_topic(pbot, bot_id: int, group_row, u, from_chat, msg_id,
                    thread=None, reply_msg_id=None, src_msg=None):
    """Передача одного сообщения в тему пользователя с восстановлением темы."""
    _require_current_group(bot_id, group_row)
    gid = group_row[1]
    tries = 0
    while True:
        try:
            if thread is None:
                thread = fs_ensure_topic_info(pbot, bot_id, group_row, u)
            mid = _relay_single(pbot, gid, from_chat, msg_id, thread=thread,
                                reply_msg_id=reply_msg_id, src_msg=src_msg)
            return mid, thread
        except Exception as e:
            tries += 1
            if tries >= 2 or thread is None or not _is_topic_err(e):
                raise
            _recover_topic(pbot, bot_id, gid, u, thread)
            thread = None

@_group_guard
def _relay_batch_to_topic(pbot, bot_id: int, group_row, u, from_chat, ids,
                          msgs=None, thread=None):
    """Передача альбома: copy_messages → forward_messages → пересборка
    медиагруппы → по одному. Частичный успех не запускает повторную отправку"""
    _require_current_group(bot_id, group_row)
    gid = group_row[1]
    tries = 0
    while True:
        if thread is None:
            thread = fs_ensure_topic_info(pbot, bot_id, group_row, u)
        sent = None
        topic_err = None
        try:
            res = _api(pbot.copy_messages, gid, from_chat, ids, message_thread_id=thread)
            sent = _batch_message_ids(res, ids)
        except ApiTelegramException as e:
            if _is_topic_err(e):
                topic_err = e
            else:
                log.debug(f'album copy failed: {e}')
        if sent is None and topic_err is None:
            try:
                res = _api(pbot.forward_messages, gid, from_chat, ids,
                           message_thread_id=thread)
                sent = _batch_message_ids(res, ids)
            except ApiTelegramException as e:
                if _is_topic_err(e):
                    topic_err = e
                else:
                    log.debug(f'album forward failed: {e}')
        try:
            if topic_err is not None:
                raise topic_err  # → восстановление темы и повтор
            if sent is None:
                media = _rebuild_media_group(msgs or [])
                if media:
                    mm = _api(pbot.send_media_group, gid, media, message_thread_id=thread)
                    sent = _batch_message_ids(mm, ids)
                elif msgs:
                    sent = []
                    for ms in msgs:
                        mid = _relay_single(pbot, gid, from_chat, ms.message_id,
                                            thread=thread, src_msg=ms)
                        if not mid:
                            raise RuntimeError('album item not delivered')
                        sent.append(mid)
            if sent is None:
                raise RuntimeError('album not delivered')
            return sent or [], thread
        except Exception as e:
            tries += 1
            if tries >= 2 or not _is_topic_err(e):
                raise
            _recover_topic(pbot, bot_id, gid, u, thread)
            thread = None


def _batch_message_ids(result, source_ids):
    """Telegram может пропускать сообщения, не указывая исходные ID.

    Неполный результат нельзя сопоставлять через zip или считать полной доставкой
    """
    mids = [message.message_id for message in result]
    if len(mids) > len(source_ids):
        raise RuntimeError('invalid batch delivery')
    # Успешный частичный результат окончателен: повтор создаст дубликаты
    # Соответствия исходным ID допустимы только для полного результата
    return mids



# ── Буфер альбомов (группировка media_group, без утечек) ─────
_album_buffers: dict = {}
_album_lock = threading.Lock()
ALBUM_FLUSH_SEC = 0.8
ALBUM_MAX = 10

def _buffer_album(pbot, bot_id: int, gid: int, thread: int, m, confirm_kb=None):
    key = (bot_id, m.from_user.id, m.media_group_id)
    with _album_lock:
        buf = _album_buffers.get(key)
        if buf is None:
            buf = {'ids': [], 'msgs': [], 'gid': gid, 'thread': thread,
                   'confirm_kb': confirm_kb, 'chat_id': m.chat.id}
            _album_buffers[key] = buf
        buf['ids'].append(m.message_id)
        buf['msgs'].append(m)
        if len(buf['ids']) >= ALBUM_MAX:
            t = None
        else:
            t = threading.Timer(ALBUM_FLUSH_SEC, _flush_album, args=(pbot, bot_id, key))
            t.daemon = True
            buf['timer'] = t
    if t is not None:
        t.start()
    if t is None:
        _flush_album(pbot, bot_id, key)

@_group_guard
def _flush_album(pbot, bot_id: int, key):
    with _album_lock:
        buf = _album_buffers.pop(key, None)
    if not buf:
        return
    from telegram_text import ordered_album
    buf['ids'] = ordered_album(buf['ids'], key=int)
    buf['msgs'] = ordered_album(buf['msgs'])
    uid = key[1]
    group = fs_get_group(bot_id)
    u = None
    try:
        if not group or group[1] != buf['gid']:
            raise RuntimeError('album destination changed')
        u = buf['msgs'][0].from_user
        sent, thread = _relay_batch_to_topic(
            pbot, bot_id, group, u, buf['chat_id'], buf['ids'], buf['msgs'], buf['thread'])
        for src_id, gm_id in (zip(buf['ids'], sent) if len(sent) == len(buf['ids']) else []):
            fs_map_message(bot_id, buf['gid'], gm_id, uid, thread, src_id, media_group=key[2])
        fs_bump_ticket(bot_id, uid)
        db_bump_user_contact(bot_id, uid)
        db_log_message(bot_id, uid, f'[{buf["msgs"][0].content_type} x{len(buf["ids"])}]')
        if buf.get('confirm_kb'):
            try:
                pbot.send_message(buf['chat_id'], f'<b>{TE_CHECK}  Сообщение отправлено</b>',
                                  parse_mode='HTML', reply_markup=buf['confirm_kb']())
            except Exception:
                pass
    except Exception as e:
        log.warning(f'album flush bot#{bot_id} uid={uid}: {e}')
        for message in buf['msgs']:
            try:
                queue_random_message(pbot, bot_id, message)
            except Exception as queue_error:
                log.warning(f'album queue bot#{bot_id} uid={uid}: {queue_error}')


# ══════════════════════════════════════════════════════════════
#  Маршрутизация сообщения пользователя (кнопка p_send / сценарий await_msg)
# ══════════════════════════════════════════════════════════════
def route_user_message(pbot, bot_id: int, m, confirm_kb=None) -> bool:
    """Доставка обращения пользователя.

    True  — группа подключена, сообщение сохранено для фоновой доставки;
    False — группа не подключена: вызывающий код использует стандартный режим.
    """
    group = fs_get_group(bot_id)
    if not group:
        return False
    from delivery_queue import get_sender
    return get_sender(pbot, bot_id).enqueue(m, confirm_kb)


def _notify_owner_error(pbot, bot_id: int, text: str, err):
    """Уведомление владельца о проблеме с группой (не чаще 1 раза в час)."""
    try:
        last = db_get_bot_setting(bot_id, 'fs_err_notify', '')
        if last:
            try:
                if time.time() - float(last) < 3600:
                    return
            except ValueError:
                pass
        db_set_bot_setting(bot_id, 'fs_err_notify', str(time.time()))
        info = db_get_bot_info(bot_id)
        owner = info[2] if info else None
        if owner and pbot:
            pbot.send_message(
                owner,
                f"{TE_EXCLAM} <b>Группа техподдержки</b>\n\n{text}\n\n"
                f"<code>{html.escape(str(err)[:300])}</code>\n\n"
                f"Проверь группу в /admin → {TE_CHAT} Группа техподдержки.",
                parse_mode='HTML')
    except Exception as e:
        log.warning(f'notify owner bot#{bot_id}: {e}')


# ══════════════════════════════════════════════════════════════
#  Группа: обработка сообщений (ответы админов через Reply, автоподключение)
# ══════════════════════════════════════════════════════════════
SERVICE_TYPES = {
    'new_chat_members', 'left_chat_member', 'new_chat_title', 'new_chat_photo',
    'delete_chat_photo', 'group_chat_created', 'supergroup_chat_created',
    'channel_chat_created', 'migrate_to_chat_id', 'migrate_from_chat_id',
    'pinned_message', 'invoice', 'successful_payment', 'video_chat_started',
    'video_chat_ended', 'video_chat_scheduled', 'video_chat_participants_invited',
    'message_auto_delete_timer_changed', 'forum_topic_created', 'forum_topic_closed',
    'forum_topic_reopened', 'forum_topic_edited', 'forum_topic_message_deleted',
    'general_forum_topic_hidden', 'general_forum_topic_unhidden',
    'giveaway', 'giveaway_created', 'giveaway_winners', 'giveaway_completed',
    'paid_media',
}

@_group_guard
def fs_handle_group_message(pbot, bot_id: int, m, bot_self_id: int, pstate) -> None:
    """Сообщения из привязанной группы:
    - автоматическое подключение по ссылке Telegram;
    - ЛЮБОЕ сообщение администратора в теме пользователя доставляется
      пользователю: и «Ответить» на его сообщение (с reply-контекстом),
      и обычное сообщение в теме без Reply;
    - ответ на закреплённое инфо-сообщение = внутренняя заметка (не доставляется);
    - служебные события и сообщения не-админов игнорируются."""
    if _mgmt_is_message(pbot, bot_id, m):
        _mgmt_handle_message(pbot, bot_id, m)
        return
    if m.content_type == 'migrate_to_chat_id':
        # группа была преобразована: обновляем chat_id в привязке И в
        # соответствиях сообщений, иначе fs_get_mapping по новому chat_id
        # не найдёт старые маппинги и ответы через Reply сломаются
        new_chat_id = getattr(m, 'migrate_to_chat_id', None)
        if new_chat_id:
            c = _conn()
            try:
                c.execute('BEGIN IMMEDIATE')
                changed = c.execute('UPDATE fs_groups SET chat_id=? WHERE bot_id=? AND chat_id=?',
                                    (new_chat_id, bot_id, m.chat.id)).rowcount
                if changed:
                    c.execute('UPDATE fs_msgs SET chat_id=? WHERE bot_id=? AND chat_id=?',
                              (new_chat_id, bot_id, m.chat.id))
                    c.execute('UPDATE delivery_sources SET chat_id=? WHERE bot_id=? AND chat_id=?',
                              (new_chat_id, bot_id, m.chat.id))
                c.commit()
            finally:
                c.close()
            if changed:
                log.info(f'Bot #{bot_id}: группа мигрировала → chat_id {new_chat_id}')
        return
    if m.content_type in SERVICE_TYPES:
        return
    if not m.from_user or m.from_user.id == bot_self_id:
        return  # защита от циклической обработки собственных сообщений
    uid_sender = m.from_user.id

    # ── автоматический /start от Telegram после выбора группы по ссылке ──
    txt = (m.text or '').strip()
    parts = txt.split(maxsplit=1)
    command = parts[0] if parts else ''
    name, addressed, username = command.partition('@')
    is_start = name == '/start'
    if is_start and addressed:
        if username.casefold() != (pbot.get_me().username or '').casefold():
            return  # команда адресована другому боту
    if is_start:
        pending = fs_get_connect_request(bot_id, uid_sender)
        if (pending and len(parts) == 2
                and secrets.compare_digest(pending['token'], parts[1].strip())):
            _finish_connect(pbot, bot_id, uid_sender, m.chat.id,
                            m.chat.title or '', pstate, expected_request=pending['request_id'])
        return

    group = fs_get_group(bot_id)
    if not group or m.chat.id != group[1]:
        return

    # ── сообщения администрации ──
    if not fs_is_admin(bot_id, uid_sender):
        return  # сообщения не-админов в группе не доставляются

    thread = m.message_thread_id
    target_uid = fs_get_topic_by_thread(bot_id, thread) if thread else None
    if not target_uid:
        return  # General-топик / тема статистики / неизвестная тема

    # reply-контекст: ответ на сообщение пользователя
    reply_ctx = None
    if m.reply_to_message:
        rt = m.reply_to_message
        # ответ на закреплённое инфо-сообщение — внутренняя заметка админов
        trow = fs_get_topic(bot_id, target_uid)
        if trow and trow[2] and rt.message_id == trow[2]:
            return
        mp = fs_get_mapping(m.chat.id, rt.message_id)
        if mp and mp[0] == bot_id:
            reply_ctx = mp[3]  # id исходного сообщения пользователя в личке

    if db_is_blocked(bot_id, target_uid):
        _topic_notice(pbot, m.chat.id, thread,
                      f'{TE_BLOCK} Пользователь заблокирован — ответ не доставлен.')
        return

    # ── альбом от админа: накапливаем и отправляем одной пачкой ──
    if m.media_group_id:
        _buffer_admin_album(pbot, bot_id, m.chat.id, target_uid, thread, m)
        return

    _deliver_admin_reply(pbot, bot_id, m, target_uid, thread, reply_ctx)


def _deliver_admin_reply(pbot, bot_id: int, m, target_uid: int, thread: int,
                         reply_ctx=None):
    """Общий путь обычного ответа и шаблона: доставка, ошибки, учёт ответа."""
    from delivery_queue import current_sender
    if current_sender() is None:
        from reply_queue import enqueue
        status, result = enqueue(pbot, bot_id, f'forum:{m.chat.id}:{m.message_id}:{target_uid}',
                                 dict(mode='forum', target=target_uid, message=m.json,
                                      thread=thread, reply=reply_ctx))
        return True if status == 'pending' else result
    if db_is_blocked(bot_id, target_uid):
        _topic_notice(pbot, m.chat.id, thread,
                      f'{TE_BLOCK} Пользователь заблокирован — ответ не доставлен.')
        return None
    try:
        mid = _relay_single(pbot, target_uid, m.chat.id, m.message_id,
                            reply_msg_id=reply_ctx, src_msg=m)
        if mid:
            fs_log_admin_msg(bot_id, target_uid)
            return mid
        else:
            _topic_notice(pbot, m.chat.id, thread,
                          f'{TE_EXCLAM} Не удалось доставить ответ: тип сообщения не поддерживается.')
    except ApiTelegramException as e:
        if _is_bot_blocked(e):
            _topic_notice(pbot, m.chat.id, thread,
                          f'{TE_EXCLAM} Не удалось доставить ответ: пользователь заблокировал бота.')
        elif _is_chat_not_found(e):
            _topic_notice(pbot, m.chat.id, thread,
                          f'{TE_EXCLAM} Не удалось доставить ответ: пользователь недоступен.')
        else:
            log.warning(f'admin reply bot#{bot_id} → {target_uid}: {e}')
            _topic_notice(pbot, m.chat.id, thread,
                          f'{TE_CROSS} Ошибка доставки ответа пользователю.')
    except Exception as e:
        log.warning(f'admin reply bot#{bot_id} → {target_uid}: {e}')


# ── Буфер админских альбомов (пачка → один copy_messages пользователю) ──
_admin_album_buffers: dict = {}
_admin_album_lock = threading.Lock()

def _buffer_admin_album(pbot, bot_id: int, gid: int, target_uid: int,
                        thread: int, m):
    key = (bot_id, target_uid, m.media_group_id)
    with _admin_album_lock:
        buf = _admin_album_buffers.get(key)
        if buf is None:
            buf = {'ids': [], 'msgs': [], 'gid': gid, 'thread': thread,
                   'chat_id': m.chat.id, 'reply_ctx': None}
            _admin_album_buffers[key] = buf
        buf['ids'].append(m.message_id)
        buf['msgs'].append(m)
        if buf['reply_ctx'] is None and m.reply_to_message:
            mp = fs_get_mapping(m.chat.id, m.reply_to_message.message_id)
            if mp and mp[0] == bot_id:
                buf['reply_ctx'] = mp[3]
        if len(buf['ids']) >= ALBUM_MAX:
            t = None
        else:
            t = threading.Timer(ALBUM_FLUSH_SEC, _flush_admin_album,
                                args=(pbot, bot_id, key))
            t.daemon = True
            buf['timer'] = t
    if t is not None:
        t.start()
    else:
        _flush_admin_album(pbot, bot_id, key)

@_group_guard
def _flush_admin_album(pbot, bot_id: int, key):
    with _admin_album_lock:
        buf = _admin_album_buffers.pop(key, None)
    if not buf:
        return
    if buf.get('msgs'):
        buf['ids'] = [m.message_id for m in buf['msgs'] if fs_is_admin(bot_id, m.from_user.id)]
        if not buf['ids']: return
    target_uid, thread = key[1], buf['thread']
    from telegram_text import ordered_album
    buf['ids'] = ordered_album(set(buf['ids']), key=int)
    group = fs_get_group(bot_id)
    try:
        if not group or group[1] != buf['gid']:
            log.warning(f'admin album flush bot#{bot_id}: группа изменилась — пропущено')
            return
        if db_is_blocked(bot_id, target_uid):
            _topic_notice(pbot, buf['gid'], thread,
                          f'{TE_BLOCK} Пользователь заблокирован — ответ не доставлен.')
            return
        from reply_queue import enqueue
        enqueue(pbot, bot_id, f"forum_album:{buf['gid']}:{buf['ids']}:{target_uid}",
                dict(mode='forum_album', target=target_uid, chat=buf['gid'], ids=buf['ids'],
                     notify=buf['gid'], notice_thread=thread))
        return
    except ApiTelegramException as e:
        if _is_bot_blocked(e):
            _topic_notice(pbot, buf['gid'], thread,
                          f'{TE_EXCLAM} Не удалось доставить ответ: пользователь заблокировал бота.')
        elif _is_chat_not_found(e):
            _topic_notice(pbot, buf['gid'], thread,
                          f'{TE_EXCLAM} Не удалось доставить ответ: пользователь недоступен.')
        else:
            log.warning(f'admin album bot#{bot_id} → {target_uid}: {e}')
            _topic_notice(pbot, buf['gid'], thread,
                          f'{TE_EXCLAM} Не удалось доставить альбом, отправь ещё раз')
    except Exception as e:
        log.warning(f'admin album bot#{bot_id} → {target_uid}: {e}')
        _topic_notice(pbot, buf['gid'], thread,
                      f'{TE_EXCLAM} Не удалось доставить альбом, отправь ещё раз')

@_group_guard
def fs_pin_callback(pbot, bot_id: int, cb):
    """Любое действие закрепа ограничено ботом, группой, темой и админом."""
    parts = (cb.data or '').split('_')
    action = parts[2] if len(parts) >= 3 else ''
    expected = 5 if action == 'send' else 4
    uid = tid = None
    if (action in ('tpl', 'send', 'close', 'block', 'unblock')
            and len(parts) == expected and len(cb.data.encode('utf-8')) <= 64):
        numbers = parts[3:]
        if all(n.isascii() and n.isdecimal() and len(n) <= 19
               and 0 < int(n) < 2**63 for n in numbers):
            uid = int(parts[3])
            tid = int(parts[4]) if action == 'send' else None
    m = cb.message
    group = fs_get_group(bot_id)
    topic = fs_get_topic(bot_id, uid) if uid else None
    if not (m and m.chat.type in ('group', 'supergroup')
            and group and m.chat.id == group[1]
            and topic and topic[0] > 0 and getattr(m, 'message_thread_id', None) == topic[0]
            and fs_is_admin(bot_id, cb.from_user.id)):
        pbot.answer_callback_query(cb.id, 'Недостаточно прав', show_alert=True)
        return

    if action in ('block', 'unblock'):
        (db_block_user if action == 'block' else db_unblock_user)(bot_id, uid)
        fs_refresh_pin_markup(pbot, bot_id, uid)
        text = 'Пользователь заблокирован' if action == 'block' else 'Пользователь разблокирован'
        pbot.answer_callback_query(cb.id, text, show_alert=True)
        _topic_notice(pbot, group[1], topic[0], f'{TE_BLOCK if action == "block" else TE_CHECK} {text}')
    elif action == 'close':
        _api(pbot.delete_message, group[1], m.message_id)
        pbot.answer_callback_query(cb.id)
    elif action == 'tpl':
        templates = db_get_templates(bot_id)[:50]
        if not templates:
            pbot.answer_callback_query(cb.id,
                'Шаблонов пока нет. Добавь их в теме «Управление»',
                show_alert=True)
            return
        kb = InlineKeyboardMarkup()
        for template_id, text in templates:
            preview = text[:35] + '…' if len(text) > 35 else text
            kb.add(_fbtn(preview, callback_data=f'fs_pin_send_{uid}_{template_id}'))
        kb.add(_fbtn('Закрыть', icon='❌', callback_data=f'fs_pin_close_{uid}'))
        _api(pbot.send_message, group[1], f'{TE_NOTE} Выбери шаблон',
             parse_mode='HTML', message_thread_id=topic[0], reply_markup=kb)
        pbot.answer_callback_query(cb.id)
    elif action == 'send':
        if db_is_blocked(bot_id, uid):
            pbot.answer_callback_query(cb.id, 'Пользователь заблокирован', show_alert=True)
            return
        text = db_get_template(tid, bot_id)
        if not text:
            pbot.answer_callback_query(cb.id, 'Шаблон не найден', show_alert=True)
            return
        # Сначала текст в теме: копируем его тем же путём, что ответ админа.
        sent = _api(pbot.send_message, group[1], text, parse_mode=None,
                    message_thread_id=topic[0])
        mid = _deliver_admin_reply(pbot, bot_id, sent, uid, topic[0])
        if mid is True:
            pass  # Источник сохранённого ответа нужен после retry_after
        elif mid:
            fs_map_message(bot_id, group[1], sent.message_id, uid, topic[0], mid)
            _api(pbot.delete_message, group[1], m.message_id)
        else:
            # Не оставляем недоставленный шаблон как успешный ответ.
            try:
                _api(pbot.delete_message, group[1], sent.message_id)
            except Exception as e:
                log.warning(f'fs template cleanup bot#{bot_id}: {e}')
        pbot.answer_callback_query(cb.id)


def _topic_notice(pbot, chat_id: int, thread: int, text: str, *, strict=False):
    """Служебное сообщение в теме (не попадает в fs_msgs → на него нельзя «ответить»)."""
    try:
        return pbot.send_message(chat_id, text, parse_mode='HTML', message_thread_id=thread)
    except Exception as e:
        if strict:
            raise
        log.warning(f'topic notice {chat_id}/{thread}: {e}')


# ══════════════════════════════════════════════════════════════
#  Очередь подтверждения: сбор случайных сообщений и отправка
# ══════════════════════════════════════════════════════════════
# Фиксированный набор блокировок не растёт с числом пользователей.
# Добавление в очередь и замена окна одного пользователя идут последовательно.
_queue_prompt_locks = tuple(threading.Lock() for _ in range(256))


def queue_random_message(pbot, bot_id: int, m) -> None:
    """Пользователь пишет без кнопки p_send: очередь + запрос подтверждения."""
    uid = m.from_user.id
    if db_is_blocked(bot_id, uid):
        try:
            pbot.send_message(m.chat.id, f'<b>{TE_BLOCK}  Вы заблокированы администратором</b>',
                              parse_mode='HTML')
        except Exception:
            pass
        return
    item = {'mid': m.message_id, 'type': m.content_type,
            'mg': m.media_group_id or ''}
    from database import db_delivery_rows
    if any(r['user_id'] == uid and r['message_id'] == m.message_id
           and r['status'] == 'fallback' for r in db_delivery_rows(bot_id)):
        item['delivery_queue'] = True
    lock = _queue_prompt_locks[hash((bot_id, uid)) % len(_queue_prompt_locks)]
    with lock:
        cnt, prompt_id = fs_append_pending(bot_id, uid, item)
        if cnt is None:
            try:
                pbot.send_message(m.chat.id,
                                  f'<b>{TE_NOTE} Очередь полна ({PENDING_MAX} сообщений) — '
                                  'сначала отправь её администратору.</b>', parse_mode='HTML')
            except Exception:
                pass
            return
        text = f'<b>{TE_CHAT}  Отправить сообщение администратору?</b>'
        if cnt > 1:
            text += f'\n\n{TE_NOTE} В очереди: <b>{cnt}</b> сообщений'
        try:
            msg = pbot.send_message(m.chat.id, text, parse_mode='HTML', reply_markup=_confirm_kb())
            fs_set_prompt(bot_id, uid, msg.message_id)
        except Exception as e:
            log.warning(f'queue prompt bot#{bot_id} uid={uid}: {e}')
            return
        if prompt_id:
            try:
                pbot.delete_message(m.chat.id, prompt_id)
            except Exception:
                pass



def _edit(pbot, text, chat_id, message_id, reply_markup=None, parse_mode='HTML'):
    """edit_message_text с фолбэком на новое сообщение (лимит Telegram 48 ч)."""
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

def _fbtn(text, icon=None, style=None, **kwargs):
    """Кнопка с премиум-иконкой (аналог _pbtn в purchased_bot)."""
    extra = {}
    if icon and icon in _ICON:
        extra['icon_custom_emoji_id'] = _ICON[icon]
    if style:
        extra['style'] = style
    return InlineKeyboardButton(text, **extra, **kwargs)

def _confirm_kb():
    kb = InlineKeyboardMarkup()
    kb.add(_fbtn('Отправить администратору', icon='📤', style='primary',
                 callback_data='fs_confirm_send'))
    kb.add(_fbtn('Отменить', icon='❌', style='danger', callback_data='fs_confirm_cancel'))
    return kb


def _group_pending_items(items):
    """Группировка элементов очереди по media_group (порядок сохраняется)."""
    groups = []
    cur_mg = None
    cur = []
    for it in items:
        mg = it.get('mg') or ''
        if cur and mg != cur_mg:
            groups.append((cur_mg or '', cur))
            cur = []
        cur_mg = mg
        cur.append(it)
    if cur:
        groups.append((cur_mg or '', cur))
    return groups


def _pending_reply_markup(uid):
    kb = InlineKeyboardMarkup()
    kb.add(_fbtn('Ответить', icon='📤', style='primary', callback_data=f'p_reply_{uid}'))
    return kb


def _deliver_pending_payload(pbot, bot_id: int, items: list, uid: int) -> bool:
    """Точные соответствия и сохранённый прогресс по каждому получателю

    У массовых методов Telegram нет исходных ID в результате и возможны пропуски
    Поэтому сохранённую очередь доставляем по одному сообщению
    """
    from database import db_delivery_resume
    from delivery_queue import get_sender
    if db_delivery_resume(bot_id, uid):
        for it in items:
            if it.get('delivery_queue'):
                it['done'] = 1
        get_sender(pbot, bot_id).wake.set()
    todo = [it for it in items if not it.get('done')]
    group = fs_get_group(bot_id)
    if group and todo:
        try:
            chat_user = _api(pbot.get_chat, uid)
            fs_touch_user(pbot, bot_id, chat_user)
        except Exception as e:
            log.warning(f'pending get_chat bot#{bot_id} uid={uid}: {e}')
            return False
    admins = db_get_bot_admins(bot_id) if not group else []
    for it in todo:
        try:
            if group:
                mid, thread = _relay_to_topic(pbot, bot_id, group, chat_user,
                                              uid, it['mid'], src_msg=None)
                if not mid:
                    continue
                fs_map_message(bot_id, group[1], mid, uid, thread, it['mid'],
                               media_group=it.get('mg', ''))
                it['done'] = 1
                _save_pending_progress(bot_id, uid, items)
            else:
                delivered = it.setdefault('admins', [])
                # Сохраняем ID пересылки до добавления кнопки: сбой кнопки
                # требует повторить только её, а не саму доставку
                forwarded = it.setdefault('forwarded', {})
                for aid in admins:
                    if aid in delivered:
                        continue
                    try:
                        key = str(aid)
                        if key not in forwarded:
                            sent = _api(pbot.forward_message, aid, uid, it['mid'])
                            forwarded[key] = sent.message_id
                            _save_pending_progress(bot_id, uid, items)
                        _api(pbot.edit_message_reply_markup, aid, forwarded[key],
                             reply_markup=_pending_reply_markup(uid))
                        delivered.append(aid)
                        _save_pending_progress(bot_id, uid, items)
                    except Exception as e:
                        log.warning(f'pending forward bot#{bot_id} admin {aid}: {e}')
                if admins and all(aid in delivered for aid in admins):
                    it['done'] = 1
                    _save_pending_progress(bot_id, uid, items)
            if it.get('done'):
                fs_bump_ticket(bot_id, uid)
                db_bump_user_contact(bot_id, uid)
                db_log_message(bot_id, uid, it['type'])
        except Exception as e:
            log.warning(f'pending deliver bot#{bot_id} uid={uid}: {e}')
    return all(it.get('done') for it in items)


def confirm_send(pbot, bot_id: int, cb) -> None:
    """Обработка кнопки «Отправить администратору»"""
    uid = cb.from_user.id
    if db_is_blocked(bot_id, uid):
        from database import db_delivery_resume
        db_delivery_resume(bot_id, uid, cancel=True)
        fs_delete_pending(bot_id, uid)
        pbot.answer_callback_query(cb.id, '🚫 Вы заблокированы администратором',
                                   show_alert=True)
        return
    if not fs_get_pending(bot_id, uid):
        pbot.answer_callback_query(cb.id, 'Очередь пуста или уже отправляется')
        return
    with _pending_activity_lock:
        items = None if (bot_id, uid) in _pending_active else fs_claim_pending(bot_id, uid)
        if items is not None:
            _pending_active.add((bot_id, uid))
    if items is None:
        pbot.answer_callback_query(cb.id, 'Уже отправляется')
        return
    ok = False
    try:
        ok = _deliver_pending_payload(pbot, bot_id, items, uid)
    finally:
        try:
            remaining = fs_restore_pending(bot_id, uid, items)
        finally:
            with _pending_activity_lock:
                _pending_active.discard((bot_id, uid))
    if ok:
        pbot.answer_callback_query(cb.id, '✅ Отправлено')
        # Новое окно могло появиться во время доставки захваченной пачки
        if not remaining:
            try:
                _edit(pbot, f'<b>{TE_CHECK}  Сообщение отправлено</b>',
                      cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
            except Exception:
                pass
    else:
        pbot.answer_callback_query(cb.id, '❌ Не получилось отправить, попробуй позже',
                                   show_alert=True)


def confirm_cancel(pbot, bot_id: int, cb) -> None:
    uid = cb.from_user.id
    from database import db_delivery_resume
    db_delivery_resume(bot_id, uid, cancel=True)
    fs_delete_pending(bot_id, uid)
    pbot.answer_callback_query(cb.id, 'Очередь отменена')
    try:
        _edit(pbot, f'<b>{TE_CROSS} Очередь отменена</b>',
                               cb.message.chat.id, cb.message.message_id,
                               parse_mode='HTML')
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
#  Подключение / отключение группы
# ══════════════════════════════════════════════════════════════
CONNECT_TTL = 15 * 60
CONNECT_EXPIRED = 'Подключение больше не активно — нажми «Подключить группу» ещё раз'
CONNECT_INTRO = ('Выбери группу с темами кнопкой внизу — Telegram добавит бота с нужными правами\n'
                 'Также можно добавить бота по ссылке или ввести ID группы\n\n'
                 'Подключение доступно 15 минут')
CONNECT_ID_PROMPT = ('Добавь бота в группу администратором с правами: управление темами, '
                     'закрепление сообщений, удаление сообщений\n'
                     'Потом отправь сюда ID группы (например -1001234567890)\n'
                     'Можно без минуса или @username публичной группы')
CONNECT_BAD_ID = 'Неверный ID группы — отправь -1001234567890, ID без минуса или @username публичной группы'
CONNECT_RIGHT_NAMES = (('can_manage_topics', 'управление темами'),
                       ('can_pin_messages', 'закрепление сообщений'),
                       ('can_delete_messages', 'удаление сообщений'))


def fs_get_connect_request(bot_id: int, uid=None):
    """Отдельная от прочих диалогов заявка, сохраняющаяся после перезапуска."""
    c = _conn()
    try:
        row = c.execute('SELECT user_id, token, request_id, expires_at, mode, chat_id, title '
                        'FROM fs_connect_requests WHERE bot_id=?', (bot_id,)).fetchone()
    finally:
        c.close()
    if not row or row[3] <= time.time() or (uid is not None and row[0] != uid):
        return None
    return dict(zip(('user_id', 'token', 'request_id', 'expires_at', 'mode', 'chat_id', 'title'), row))


def _clear_connect_state(pstate, uid, request_id):
    predicate = lambda st: (isinstance(st, dict) and st.get('step') == 'fs_connect'
                            and (request_id is None or st.get('request_id') == request_id))
    if hasattr(pstate, 'take'):
        pstate.take(uid, predicate)
    elif predicate(pstate.get(uid)):
        pstate.pop(uid, None)


def _connect_target(bot_id, request_id, chat_id, title):
    c = _conn()
    try:
        c.execute('UPDATE fs_connect_requests SET chat_id=?, title=? '
                  'WHERE bot_id=? AND request_id=?', (str(chat_id), title, bot_id, request_id))
        c.commit()
    finally:
        c.close()


def _connect_error(pbot, uid, text, request_id=None):
    kb = InlineKeyboardMarkup()
    if request_id is not None:
        kb.add(_fbtn('Проверить снова', icon='🔄', callback_data=f'fs_connect_retry_{request_id}'))
    else:
        kb.add(_fbtn('Подключить группу', icon='🔗', callback_data='fs_connect'))
    kb.add(_fbtn('Отмена', icon='❌', callback_data='fs_connect_cancel'))
    pbot.send_message(uid, f'{TE_CROSS} {text}', parse_mode='HTML', reply_markup=kb)


def _connect_rights():
    return ChatAdministratorRights(is_anonymous=False, can_manage_chat=True,
        can_delete_messages=True, can_manage_video_chats=False, can_restrict_members=False,
        can_promote_members=False, can_change_info=False, can_invite_users=False,
        can_pin_messages=True, can_manage_topics=True)


def _connect_reply_kb(request_id, select=True):
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    if select:
        request = KeyboardButtonRequestChat(request_id=request_id, chat_is_channel=False,
            chat_is_forum=True, user_administrator_rights=_connect_rights(),
            bot_administrator_rights=_connect_rights(), request_title=True)
        kb.add(KeyboardButton('Выбрать группу', request_chat=request,
                             icon_custom_emoji_id=_ICON['💬'], style='primary'))
    kb.add(KeyboardButton('Отмена', icon_custom_emoji_id=_ICON['❌']))
    return kb


def _parse_group_id(text):
    text = text.strip()
    if re.fullmatch(r'-?100[0-9]+', text):
        return -abs(int(text))
    if re.fullmatch(r'@[A-Za-z][A-Za-z0-9_]{3,31}', text):
        return text
    return None


def fs_verify_group(pbot, chat_id: int, uid: int, bot_self_id: int, bot_id: int = None,
                    chat=None):
    """Единая проверка прав и группы для всех способов подключения."""
    try:
        chat = chat if chat is not None else _api(pbot.get_chat, chat_id)
    except Exception:
        return False, 'Не удалось найти группу — проверь ID и добавь бота в группу'
    if getattr(chat, 'type', '') != 'supergroup' or not getattr(chat, 'is_forum', False):
        return False, 'В группе не включены темы — включи их в настройках группы'
    try:
        me = _api(pbot.get_chat_member, chat_id, bot_self_id)
    except Exception:
        return False, 'Бот не администратор в группе — добавь бота и назначь администратором'
    if getattr(me, 'status', '') != 'administrator':
        return False, 'Бот не администратор в группе — назначь бота администратором'
    missing = [label for key, label in CONNECT_RIGHT_NAMES if not getattr(me, key, False)]
    if missing:
        return False, 'Боту не хватает прав: ' + ', '.join(missing) + ' — выдай их в настройках администраторов'
    try:
        member = _api(pbot.get_chat_member, chat_id, uid)
    except Exception:
        return False, 'Не удалось проверить твои права в группе — убедись, что ты администратор, и повтори проверку'
    if getattr(member, 'status', '') not in ('creator', 'administrator'):
        return False, 'Ты не администратор этой группы — попроси владельца назначить тебя администратором'
    if bot_id is not None:
        other = fs_get_group_by_chat(chat_id)
        if other and other[0] != bot_id:
            return False, 'Группа уже подключена к другому боту — сначала отключи её в том боте'
    return True, ''


@_group_guard
def _finish_connect(pbot, bot_id: int, uid: int, chat_id, title: str, pstate,
                    expected_request=None):
    """Проверка, привязка, обе служебные темы и уведомления в группе и личке."""
    if not _can_manage_group(bot_id, uid):
        return
    pending = fs_get_connect_request(bot_id, uid)
    if expected_request is not None and (not pending or pending['request_id'] != expected_request):
        return
    request_id = pending['request_id'] if pending else None
    if request_id is not None:
        _connect_target(bot_id, request_id, chat_id, title)
    try:
        chat = _api(pbot.get_chat, chat_id)
    except Exception:
        _connect_error(pbot, uid, 'Не удалось найти группу — проверь ID и добавь бота в группу', request_id)
        return
    chat_id = chat.id
    title = getattr(chat, 'title', None) or title or 'Группа'
    if request_id is not None:
        _connect_target(bot_id, request_id, chat_id, title)
    ok, err = fs_verify_group(pbot, chat_id, uid, pbot.get_me().id, bot_id=bot_id, chat=chat)
    if not ok:
        _connect_error(pbot, uid, err, request_id)
        return
    # Запросы Telegram могут занять время: истёкшая заявка не должна привязывать группу
    if expected_request is not None:
        current = fs_get_connect_request(bot_id, uid)
        if not current or current['request_id'] != expected_request:
            _connect_error(pbot, uid, CONNECT_EXPIRED)
            return
    old = fs_get_group(bot_id)
    try:
        stats = old[3] if old and old[1] == chat_id else None
        if not stats:
            stats = _api(pbot.create_forum_topic, chat_id, '📊 Статистика').message_thread_id
    except Exception:
        _connect_error(pbot, uid, 'Не удалось создать тему «Статистика» — проверь права бота и повтори проверку', request_id)
        return
    if expected_request is not None:
        current = fs_get_connect_request(bot_id, uid)
        if not current or current['request_id'] != expected_request:
            _connect_error(pbot, uid, CONNECT_EXPIRED)
            return
    try:
        fs_bind_group(bot_id, chat_id, title)
    except sqlite3.IntegrityError:
        _connect_error(pbot, uid, 'Группа уже подключена к другому боту — сначала отключи её в том боте', request_id)
        return
    fs_set_stats_thread(bot_id, stats)
    fs_refresh_management(pbot, bot_id)
    if pending:
        c = _conn()
        try:
            c.execute('DELETE FROM fs_connect_requests WHERE bot_id=? AND request_id=?', (bot_id, request_id))
            c.commit()
        finally:
            c.close()
        _clear_connect_state(pstate, uid, request_id)
    try:
        pbot.send_message(chat_id, f'{TE_CHECK} <b>Группа техподдержки подключена</b>', parse_mode='HTML')
    except Exception:
        pass
    try:
        pbot.send_message(uid,
            f'{TE_CHECK} <b>Группа техподдержки подключена</b>\n\n'
            f'{TE_CLIP} {html.escape(title)} (<code>{chat_id}</code>)\n\n'
            'Теперь обращения пользователей создают отдельные темы\n'
            'Чтобы ответить — просто напиши в теме пользователя '
            '(или через «Ответить» на его сообщение)\n'
            f'Служебная тема «{TE_STATS} Статистика» будет получать еженедельные отчёты',
            parse_mode='HTML', reply_markup=ReplyKeyboardRemove())
    except Exception:
        pass
    log.info('Bot #%s: support group %s connected by %s', bot_id, chat_id, uid)


@_group_guard
def connect_start(pbot, bot_id: int, cb, pstate):
    uid = cb.from_user.id
    previous = fs_get_connect_request(bot_id)
    request_id = secrets.randbelow(2 ** 31 - 1) + 1
    token = secrets.token_urlsafe(24)
    c = _conn()
    try:
        c.execute('INSERT OR REPLACE INTO fs_connect_requests '
                  '(bot_id,user_id,token,request_id,expires_at) VALUES (?,?,?,?,?)',
                  (bot_id, uid, token, request_id, time.time() + CONNECT_TTL))
        c.commit()
    finally:
        c.close()
    if previous:
        _clear_connect_state(pstate, previous['user_id'], previous['request_id'])
    pstate[uid] = {'step': 'fs_connect', 'request_id': request_id}
    kb = InlineKeyboardMarkup()
    username = pbot.get_me().username
    kb.add(_fbtn('Добавить в группу', icon='🔗',
        url=f'https://t.me/{username}?startgroup={token}&admin=manage_topics+pin_messages+delete_messages'))
    kb.add(_fbtn('Ввести ID группы', icon='🪪', callback_data='fs_connect_id'))
    kb.add(_fbtn('Отмена', icon='❌', callback_data='fs_connect_cancel'))
    _edit(pbot, f'<b>{TE_LINK} Подключение группы техподдержки</b>\n\n' + CONNECT_INTRO,
          cb.message.chat.id, cb.message.message_id, parse_mode='HTML', reply_markup=kb)
    pbot.send_message(uid, f'{TE_CHAT} Нажми «Выбрать группу» на клавиатуре внизу',
                      parse_mode='HTML', reply_markup=_connect_reply_kb(request_id))


@_group_guard
def _cancel_connect(pbot, bot_id, uid, pstate):
    # Не удаляем состояние другого диалога даже при просроченной заявке
    c = _conn()
    try:
        row = c.execute('SELECT request_id FROM fs_connect_requests WHERE bot_id=? AND user_id=?',
                        (bot_id, uid)).fetchone()
        c.execute('DELETE FROM fs_connect_requests WHERE bot_id=? AND user_id=?', (bot_id, uid))
        c.commit()
    finally:
        c.close()
    _clear_connect_state(pstate, uid, row[0] if row else None)
    pbot.send_message(uid, f'{TE_CROSS} Подключение отменено', parse_mode='HTML',
                      reply_markup=ReplyKeyboardRemove())


@_group_guard
def connect_cancel(pbot, bot_id: int, cb, pstate):
    _cancel_connect(pbot, bot_id, cb.from_user.id, pstate)
    pbot.answer_callback_query(cb.id)
    menu_show(pbot, bot_id, cb)


def disconnect_confirm(pbot, bot_id: int, cb):
    kb = InlineKeyboardMarkup()
    kb.add(_fbtn('Да, отключить', icon='✔️', callback_data='fs_disconnect_yes',
                                style='danger'))
    kb.add(_fbtn('Отмена', icon='❌', callback_data='fs_group_menu'))
    try:
        _edit(pbot, 
            f'<b>{TE_CROSS}  Отключение группы техподдержки</b>\n\n'
            'Обращения снова будут приходить администраторам в личные сообщения.\n'
            'Связи пользователь→тема будут сброшены (данные пользователей сохранятся).\n\n'
            'Отключить?',
            cb.message.chat.id, cb.message.message_id, parse_mode='HTML', reply_markup=kb)
    except Exception as e:
        log.warning(f'disconnect_confirm: {e}')

def disconnect_yes(pbot, bot_id: int, cb):
    chat_id = fs_unbind_group(bot_id)
    pbot.answer_callback_query(cb.id, '✅ Группа отключена')
    try:
        _edit(pbot, 
            f'{TE_CHECK} <b>Группа (<code>{chat_id}</code>) отключена.</b>\n\n'
            'Возврат в стандартный режим: обращения приходят администраторам в ЛС.',
            cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
    except Exception:
        pass
    log.info(f'Bot #{bot_id}: support group disconnected')

def menu_text(bot_id: int) -> str:
    g = fs_get_group(bot_id)
    if g:
        connected = (f'{TE_CHAT} <b>Группа техподдержки</b>\n\n'
                     f'{TE_CLIP} {html.escape(g[2] or "Группа")} (<code>{g[1]}</code>)\n'
                     f'{TE_CALENDAR} Подключена: {_fmt_dt_utc(g[4]) or "—"}\n\n'
                     'Режим: обращения пользователей создают темы в группе.\n'
                     'Ответы: любое твоё сообщение в теме пользователя уходит ему\n'
                     '(и через «Ответить», и обычным сообщением).\n'
                     'Ответ на закреплённое инфо = внутренняя заметка.')
    else:
        connected = (f'{TE_CHAT} <b>Группа техподдержки</b>\n\n'
                     'Группа не подключена.\n'
                     'Сейчас обращения приходят администраторам в личные сообщения.\n\n'
                     'Подключив группу с темами, ты получишь:\n'
                     '• отдельную тему для каждого пользователя\n'
                     '• ответы через стандартное «Ответить»\n'
                     '• закреплённую информацию о пользователе\n'
                     '• еженедельную статистику')
    return connected

def menu_kb(bot_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    if fs_get_group(bot_id):
        kb.add(_fbtn('Сменить группу', icon='🔄', callback_data='fs_connect'))
        kb.add(_fbtn('Отключить группу', icon='❌', callback_data='fs_disconnect',
                                    style='danger'))
    else:
        kb.add(_fbtn('Подключить группу', icon='🔗', callback_data='fs_connect',
                                    style='primary'))
    kb.add(_fbtn('Назад', icon='🔙', callback_data='p_back_admin'))
    return kb

def menu_show(pbot, bot_id: int, cb):
    _edit(pbot, menu_text(bot_id), cb.message.chat.id, cb.message.message_id,
                           parse_mode='HTML', reply_markup=menu_kb(bot_id))


# ══════════════════════════════════════════════════════════════
#  Еженедельная статистика: один планировщик на все боты
# ══════════════════════════════════════════════════════════════
_sched_lock = threading.Lock()
_sched_started = False

def _tz():
    return timezone(timedelta(hours=STATS_TZ_OFFSET))

def _now_tz() -> datetime:
    return datetime.now(_tz())

def _utc_boundary(dt_tz: datetime) -> str:
    """Граница периода в tz → UTC-строка для сравнения с CURRENT_TIMESTAMP."""
    utc_dt = dt_tz - timedelta(hours=STATS_TZ_OFFSET)
    return utc_dt.strftime('%Y-%m-%d %H:%M:%S')

def _next_fire(now: datetime) -> datetime:
    """Ближайший понедельник 00:05 по времени статистики."""
    days_ahead = (7 - now.weekday()) % 7
    cand = (now + timedelta(days=days_ahead)).replace(hour=0, minute=5, second=0,
                                                      microsecond=0)
    if cand <= now:
        cand += timedelta(days=7)
    return cand

def fs_weekly_report_text(bot_id: int, start_tz: datetime, end_tz: datetime) -> str:
    from_s = _utc_boundary(start_tz)
    to_s = _utc_boundary(end_tz)
    disp_start = start_tz.strftime('%d.%m.%Y')
    disp_end = (end_tz - timedelta(days=1)).strftime('%d.%m.%Y')
    tickets_week = db_fs_count_range('fs_tickets', bot_id, from_s, to_s)
    tickets_total = db_fs_tickets_total(bot_id)
    new_users = db_fs_new_users_range(bot_id, from_s, to_s)
    users_total = db_fs_users_total(bot_id)
    msgs_user = db_fs_count_range('bot_messages', bot_id, from_s, to_s)
    msgs_admin = db_fs_count_range('fs_admin_msgs', bot_id, from_s, to_s)
    updated = _now_tz().strftime('%d.%m.%Y %H:%M')
    return (f'<b>{TE_STATS} Статистика техподдержки</b>\n\n'
            f'{TE_CALENDAR} Период: {disp_start} — {disp_end}\n\n'
            f'<b>{TE_SEND}  Обращения</b>\n'
            f'• За неделю: {_fmt_num(tickets_week)}\n'
            f'• За всё время: {_fmt_num(tickets_total)}\n'
            f'• Новых пользователей за неделю: {_fmt_num(new_users)}\n'
            f'• Всего уникальных пользователей: {_fmt_num(users_total)}\n\n'
            f'<b>{TE_CHAT}  Сообщения</b>\n'
            f'• Получено от пользователей: {_fmt_num(msgs_user)}\n'
            f'• Отправлено администраторами: {_fmt_num(msgs_admin)}\n\n'
            f'{TE_CLOCK} Последнее обновление: {updated}')

def _publish_period(pbot, bot_id: int, chat_id: int, thread_stats,
                    start_tz: datetime, end_tz: datetime) -> None:
    ps_key = start_tz.strftime('%Y-%m-%d')
    pe_key = end_tz.strftime('%Y-%m-%d')
    row = fs_report_get(bot_id, ps_key)
    if row and row[0] == 'sent':
        return
    if not row:
        fs_report_mark_queued(bot_id, ps_key, pe_key)
    text = fs_weekly_report_text(bot_id, start_tz, end_tz)
    # сетевые запросы выполняются без открытых транзакций БД
    try:
        thread = thread_stats
        try:
            msg = _api(pbot.send_message, chat_id, text, parse_mode='HTML',
                       message_thread_id=thread)
        except ApiTelegramException as e:
            if not _is_topic_gone(e):
                raise
            t = _api(pbot.create_forum_topic, chat_id, '📊 Статистика')
            fs_set_stats_thread(bot_id, t.message_thread_id)
            msg = _api(pbot.send_message, chat_id, text, parse_mode='HTML',
                       message_thread_id=t.message_thread_id)
        fs_report_mark_sent(bot_id, ps_key, getattr(msg, 'message_id', None))
        log.info(f'Bot #{bot_id}: weekly report {ps_key} published')
    except Exception as e:
        log.warning(f'weekly report bot#{bot_id} {ps_key}: {e}')  # останется queued — повтор

def publish_due_reports(pbot_map: dict) -> int:
    """Публикует отчёты за завершённые периоды (до 4 недель назад — догон простоя).
    pbot_map: {db_bot_id: TeleBot} — только запущенные боты."""
    now = _now_tz()
    this_monday = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0)
    published = 0
    c = _conn()
    try:
        rows = c.execute('SELECT bot_id, chat_id, thread_stats FROM fs_groups').fetchall()
    finally:
        c.close()
    for bot_id, chat_id, thread_stats in rows:
        pbot = pbot_map.get(bot_id)
        if pbot is None:
            continue  # бот не запущен — отчёт останется «queued» до его запуска
        for k in range(0, 4):
            end = this_monday - timedelta(weeks=k)
            start = end - timedelta(weeks=1)
            if end + timedelta(minutes=5) > now:
                continue
            before = fs_report_get(bot_id, start.strftime('%Y-%m-%d'))
            _publish_period(pbot, bot_id, chat_id, thread_stats, start, end)
            after = fs_report_get(bot_id, start.strftime('%Y-%m-%d'))
            if (not before or before[0] != 'sent') and after and after[0] == 'sent':
                published += 1
    return published

def _weekly_loop():
    from purchased_bot import running_bot_ids, running_bots
    while True:
        try:
            now = _now_tz()
            next_fire = _next_fire(now)
            rem = (next_fire - now).total_seconds()
            time.sleep(min(max(rem, 1), 1800))
            pbot_map = {}
            for db_bot_id, tok in list(running_bot_ids.items()):
                b = running_bots.get(tok)
                if b is not None:
                    pbot_map[db_bot_id] = b
            publish_due_reports(pbot_map)
        except Exception:
            log.exception('weekly scheduler loop error')
            time.sleep(120)
        # еженедельное обслуживание БД сразу после публикации отчётов
        try:
            from database import db_maintenance, db_backup, DATA_DIR
            db_maintenance()
            db_backup(os.path.join(DATA_DIR, 'backups'))
        except Exception:
            log.exception('db maintenance failed')

def start_weekly_scheduler():
    """Стартует единый фоновый поток еженедельной статистики (идемпотентно)."""
    global _sched_started
    with _sched_lock:
        if _sched_started:
            return
        _sched_started = True
    threading.Thread(target=_weekly_loop, name='fs-weekly-stats', daemon=True).start()
    log.info('Forum support: weekly stats scheduler started')




# ══════════════════════════════════════════════════════════════
#  Реакции: тема ↔ ЛС клиента
#  ОГРАНИЧЕНИЕ ПЛАТФОРМЫ: setMessageReaction работает ТОЛЬКО в
#  супергруппах (в личных чатах Telegram отвечает «message must
#  belong to a supergroup»). Поэтому:
#  - админ отреагировал в теме → клиенту в ЛС уходит УВЕДОМЛЕНИЕ
#    с эмодзи (поставить реакцию в ЛС бот не может физически);
#  - юзер отреагировал в ЛС → реакция ставится на копию сообщения
#    в супергруппе (это разрешено, бот там администратор).
# ══════════════════════════════════════════════════════════════
_reaction_sent: dict = {}   # (bot_id, uid, src_msg_id) -> последний отправленный эмодзи
_reaction_lock = threading.Lock()

def _reaction_emoji(new_reaction: list) -> str:
    """Эмодзи реакции для текста уведомления. Кастомный (premium) эмодзи
    отрендерить в тексте нечем — показываем нейтральный маркер."""
    for r in new_reaction or []:
        em = getattr(r, 'emoji', None)
        if em:
            return em
    return '❤️'

@_group_guard
def fs_handle_reaction(pbot, bot_id: int, mr, bot_self_id: int) -> None:
    """Обрабатывает message_reaction: тема → уведомление в ЛС, ЛС → реакция в теме."""
    if not mr.new_reaction:
        return  # реакция снята — зеркалировать нечего
    chat = mr.chat
    reactor = mr.user or mr.actor_chat
    if not reactor:
        return
    reactor_id = reactor.id if hasattr(reactor, 'id') else 0

    group = fs_get_group(bot_id)
    if group and chat.id == group[1]:
        # ── реакция в теме: админ реагирует на сообщение юзера ──
        if not fs_is_admin(bot_id, reactor_id):
            return
        mp = fs_get_mapping(chat.id, mr.message_id)
        if not (mp and mp[0] == bot_id):
            return
        uid, src_msg_id = mp[1], mp[3]
        if db_is_blocked(bot_id, uid):
            return
        emoji = _reaction_emoji(mr.new_reaction)
        key = (bot_id, uid, src_msg_id)
        with _reaction_lock:
            if _reaction_sent.get(key) == emoji:
                return  # дедуп: ту же реакцию на то же сообщение уже отправляли
            if len(_reaction_sent) > 5000:
                _reaction_sent.clear()
            _reaction_sent[key] = emoji
        try:
            pbot.send_message(uid,
                f'{emoji} <b>Админ отреагировал на ваше сообщение</b>',
                parse_mode='HTML')
        except Exception as e:
            log.warning(f'reaction notify DM bot#{bot_id} uid={uid}: {e}')
    elif chat.type == 'private':
        # ── реакция в ЛС: юзер реагирует на сообщение бота ──
        # реакция ставится на копию/оригинал в супергруппе (в ЛС нельзя)
        if not group:
            return
        copies = db_get_topic_copies_by_src(bot_id, reactor_id, mr.message_id)
        for chat_id, group_msg_id, _thread in copies:
            if chat_id != group[1]:
                continue
            try:
                _api(pbot.set_message_reaction, group[1],
                     message_id=group_msg_id, reaction=mr.new_reaction)
                return
            except Exception as e:
                log.warning(f'reaction mirror to topic bot#{bot_id}: {e}')

# ══════════════════════════════════════════════════════════════
#  Регистрация обработчиков дочернего бота
# ══════════════════════════════════════════════════════════════
GROUP_MSG_TYPES = [
    'text', 'audio', 'document', 'photo', 'sticker', 'video', 'video_note',
    'voice', 'contact', 'location', 'venue', 'poll', 'dice', 'animation',
    'game', 'story', 'paid_media', 'new_chat_members', 'left_chat_member',
    'new_chat_title', 'new_chat_photo', 'delete_chat_photo', 'group_chat_created',
    'supergroup_chat_created', 'channel_chat_created', 'migrate_to_chat_id',
    'migrate_from_chat_id', 'pinned_message', 'video_chat_started',
    'video_chat_ended', 'video_chat_scheduled', 'video_chat_participants_invited',
    'message_auto_delete_timer_changed', 'forum_topic_created', 'forum_topic_closed',
    'forum_topic_reopened', 'forum_topic_edited', 'forum_topic_message_deleted',
    'general_forum_topic_hidden', 'general_forum_topic_unhidden',
    'giveaway', 'giveaway_created', 'giveaway_winners', 'giveaway_completed',
]

USER_MSG_TYPES = [
    'text', 'audio', 'document', 'photo', 'sticker', 'video', 'video_note',
    'voice', 'contact', 'location', 'venue', 'poll', 'dice', 'animation', 'story',
]

def _can_manage_group(bot_id: int, uid: int) -> bool:
    """Подключать/отключать группу может владелец бота или главный админ."""
    info = db_get_bot_info(bot_id)
    return bool(info and (uid == info[2] or uid == info[1]))


def register_forum_support(pbot, bot_id: int, pstate, is_admin, confirm_kb=_confirm_kb,
                           direct_route=None):
    """Регистрирует обработчики форум-режима на дочернем боте.
    direct_route(m) — доставка случайного сообщения юзера СРАЗУ (тема/админы)
    без очереди подтверждения; очередь остаётся только анти-потерей при сбое.
    Возвращает route-функцию для маршрутизации обращений пользователя."""
    _recover_pending_runtime(bot_id)
    bot_self_id = 0
    try:
        bot_self_id = pbot.get_me().id
    except Exception as e:
        log.warning(f'fs register get_me bot#{bot_id}: {e}')

    fs_refresh_management(pbot, bot_id)

    # Только служебная тема получает приоритет над состояниями личной панели.
    @pbot.message_handler(func=lambda m: _mgmt_is_message(pbot, bot_id, m),
                          content_types=GROUP_MSG_TYPES)
    def _fs_management_msg(m):
        try:
            _mgmt_handle_message(pbot, bot_id, m)
        except Exception as e:
            log.warning(f'fs management message bot#{bot_id}: {e}')

    pbot.message_handlers.insert(0, pbot.message_handlers.pop())

    @pbot.callback_query_handler(func=lambda c: (c.data or '').startswith('fs_mgmt_'))
    def _fs_management_cb(cb):
        try:
            fs_management_callback(pbot, bot_id, cb)
        except Exception as e:
            log.warning(f'fs management action bot#{bot_id}: {e}')
            pbot.answer_callback_query(cb.id, 'Ошибка, попробуй ещё раз', show_alert=True)

    def route(m, _kb=confirm_kb):
        return route_user_message(pbot, bot_id, m, confirm_kb=_kb)

    # ── реакции: зеркалирование тема ↔ ЛС клиента ──
    @pbot.message_reaction_handler()
    def _fs_reaction_cb(mr):
        try:
            fs_handle_reaction(pbot, bot_id, mr, bot_self_id)
        except Exception as e:
            log.error(f'fs reaction bot#{bot_id}: {e}', exc_info=True)

    # ── сообщения из групп (ответы админов, автоподключение) ──
    @pbot.message_handler(func=lambda m: m.chat.type in ('group', 'supergroup'),
                          content_types=GROUP_MSG_TYPES)
    def _fs_group_msg(m):
        try:
            fs_handle_group_message(pbot, bot_id, m, bot_self_id, pstate)
        except Exception as e:
            log.error(f'fs group msg bot#{bot_id}: {e}', exc_info=True)

    # ── события подключения и ввод ID имеют приоритет над личными состояниями ──
    def _connect_guard(fn):
        @wraps(fn)
        def call(event):
            with _mgmt_lock(bot_id):
                return fn(event)
        return call

    @pbot.my_chat_member_handler()
    @_connect_guard
    def _fs_member(update):
        if (update.chat.type not in ('group', 'supergroup') or not update.from_user
                or update.new_chat_member.user.id != bot_self_id
                or update.new_chat_member.status not in ('member', 'administrator')
                or not _can_manage_group(bot_id, update.from_user.id)):
            return
        pending = fs_get_connect_request(bot_id, update.from_user.id)
        if not pending:
            return
        _finish_connect(pbot, bot_id, update.from_user.id, update.chat.id,
                        update.chat.title or '', pstate, expected_request=pending['request_id'])

    def _private_connect_input(m):
        if m.chat.type != 'private' or not m.from_user or not _can_manage_group(bot_id, m.from_user.id):
            return False
        if m.content_type == 'chat_shared':
            return True
        pending = fs_get_connect_request(bot_id, m.from_user.id)
        st = pstate.get(m.from_user.id)
        own_state = isinstance(st, dict) and st.get('step') == 'fs_connect'
        if (m.text or '').strip() == 'Отмена':
            return bool(pending or own_state)
        return bool(((pending and pending['mode'] == 'id') or own_state)
                    and not (m.text or '').startswith('/')
                    and (st is None or own_state))

    @pbot.message_handler(func=_private_connect_input, content_types=['text', 'chat_shared'])
    @_connect_guard
    def _fs_connect_private(m):
        uid = m.from_user.id
        if (m.text or '').strip() == 'Отмена':
            _cancel_connect(pbot, bot_id, uid, pstate)
            pbot.send_message(uid, menu_text(bot_id), parse_mode='HTML', reply_markup=menu_kb(bot_id))
            return
        pending = fs_get_connect_request(bot_id, uid)
        if not pending:
            _connect_error(pbot, uid, CONNECT_EXPIRED)
            return
        if m.content_type == 'chat_shared':
            shared = m.chat_shared
            if shared.request_id != pending['request_id']:
                return
            pbot.send_message(uid, f'{TE_CLOCK} Проверяю группу', parse_mode='HTML',
                              reply_markup=ReplyKeyboardRemove())
            _finish_connect(pbot, bot_id, uid, shared.chat_id, shared.title or '', pstate,
                            expected_request=pending['request_id'])
            return
        if pending['mode'] != 'id':
            return
        chat_id = _parse_group_id(m.text or '')
        if chat_id is None:
            pbot.send_message(uid, f'{TE_CROSS} {CONNECT_BAD_ID}', parse_mode='HTML',
                              reply_markup=_connect_reply_kb(pending['request_id'], select=False))
            return
        _finish_connect(pbot, bot_id, uid, chat_id, '', pstate, expected_request=pending['request_id'])

    pbot.message_handlers.insert(1, pbot.message_handlers.pop())

    # ── случайные сообщения пользователя (без кнопки p_send) ──
    # доставляются СРАЗУ (direct_route); очередь — только анти-потеря при сбое
    @pbot.message_handler(
        func=lambda m: m.chat.type == 'private'
                       and pstate.get(m.from_user.id) is None
                       and not (m.text or '').startswith('/')
                       and not is_admin(m.from_user.id),
        content_types=USER_MSG_TYPES)
    def _fs_random_user_msg(m):
        try:
            if direct_route is not None and direct_route(m):
                return
            queue_random_message(pbot, bot_id, m)
        except Exception as e:
            log.error(f'fs random msg bot#{bot_id}: {e}', exc_info=True)

    # ── действия закрепа (здесь же остальные fs_* callback дочернего бота) ──
    @pbot.callback_query_handler(func=lambda c: c.data.startswith('fs_pin_'))
    def _fs_pin_cb(cb):
        try:
            fs_pin_callback(pbot, bot_id, cb)
        except Exception as e:
            log.warning(f'fs pin action bot#{bot_id}: {e}')
            pbot.answer_callback_query(cb.id, 'Ошибка, попробуй ещё раз', show_alert=True)

    # ── меню группы ──
    @pbot.callback_query_handler(func=lambda c: c.data == 'fs_group_menu')
    def _fs_menu_cb(cb):
        if not is_admin(cb.from_user.id):
            return
        try:
            menu_show(pbot, bot_id, cb)
        except Exception as e:
            log.warning(f'fs menu bot#{bot_id}: {e}')

    @pbot.callback_query_handler(func=lambda c: c.data == 'fs_connect')
    def _fs_connect_cb(cb):
        if not _can_manage_group(bot_id, cb.from_user.id):
            pbot.answer_callback_query(cb.id,
                                       '❌ Подключать группу может владелец или '
                                       'главный админ бота', show_alert=True)
            return
        connect_start(pbot, bot_id, cb, pstate)

    @pbot.callback_query_handler(func=lambda c: c.data == 'fs_connect_id')
    @_connect_guard
    def _fs_connect_id_cb(cb):
        uid = cb.from_user.id
        if not _can_manage_group(bot_id, uid):
            return
        pending = fs_get_connect_request(bot_id, uid)
        if not pending:
            _connect_error(pbot, uid, CONNECT_EXPIRED)
            return
        c = _conn()
        try:
            c.execute("UPDATE fs_connect_requests SET mode='id' WHERE bot_id=? AND request_id=?",
                      (bot_id, pending['request_id']))
            c.commit()
        finally:
            c.close()
        pstate[uid] = {'step': 'fs_connect', 'request_id': pending['request_id']}
        pbot.answer_callback_query(cb.id)
        pbot.send_message(uid, f'{TE_ID} {CONNECT_ID_PROMPT}', parse_mode='HTML',
                          reply_markup=_connect_reply_kb(pending['request_id'], select=False))

    @pbot.callback_query_handler(func=lambda c: (c.data or '').startswith('fs_connect_retry_'))
    @_connect_guard
    def _fs_connect_retry_cb(cb):
        uid = cb.from_user.id
        if not _can_manage_group(bot_id, uid):
            return
        pending = fs_get_connect_request(bot_id, uid)
        if not pending or cb.data != f"fs_connect_retry_{pending['request_id']}":
            _connect_error(pbot, uid, CONNECT_EXPIRED)
            return
        pbot.answer_callback_query(cb.id)
        if pending['chat_id']:
            chat_id = _parse_group_id(pending['chat_id'])
            if chat_id is not None:
                _finish_connect(pbot, bot_id, uid, chat_id, pending['title'], pstate,
                                expected_request=pending['request_id'])

    @pbot.callback_query_handler(func=lambda c: c.data == 'fs_connect_cancel')
    def _fs_connect_cancel_cb(cb):
        if not _can_manage_group(bot_id, cb.from_user.id):
            return
        connect_cancel(pbot, bot_id, cb, pstate)

    @pbot.callback_query_handler(func=lambda c: c.data == 'fs_disconnect')
    def _fs_disconnect_cb(cb):
        if not _can_manage_group(bot_id, cb.from_user.id):
            pbot.answer_callback_query(cb.id,
                                       '❌ Отключать группу может владелец или '
                                       'главный админ бота', show_alert=True)
            return
        if not fs_get_group(bot_id):
            pbot.answer_callback_query(cb.id, 'Группа не подключена')
            return
        disconnect_confirm(pbot, bot_id, cb)

    @pbot.callback_query_handler(func=lambda c: c.data == 'fs_disconnect_yes')
    def _fs_disconnect_yes_cb(cb):
        if not _can_manage_group(bot_id, cb.from_user.id):
            return
        disconnect_yes(pbot, bot_id, cb)

    # ── подтверждение отправки случайных сообщений ──
    @pbot.callback_query_handler(func=lambda c: c.data == 'fs_confirm_send')
    def _fs_confirm_send_cb(cb):
        try:
            confirm_send(pbot, bot_id, cb)
        except Exception as e:
            log.error(f'fs confirm bot#{bot_id}: {e}', exc_info=True)
            try:
                pbot.answer_callback_query(cb.id, '❌ Ошибка, попробуй ещё раз',
                                           show_alert=True)
            except Exception:
                pass

    @pbot.callback_query_handler(func=lambda c: c.data == 'fs_confirm_cancel')
    def _fs_confirm_cancel_cb(cb):
        confirm_cancel(pbot, bot_id, cb)

    return route


# ── лёгкие хелперы, общие для модулей ─────────────────────────
def html_safe_chunks(escaped: str, max_len: int = 3800) -> list:
    """Режет УЖЕ экранированный текст на куски ≤ max_len.
    Две ловли одновременно:
    1) нельзя резать по «сырым» кускам и потом экранировать: &amp; может
       разорваться → Telegram отвечает 400 на битый HTML;
    2) нельзя резать уже экранированный текст наивно по длине: сущность
       (&amp; = 5 символов) может оказаться на границе — отступаем к '&'.
    Сущности длиннее max_len не существуют (максимум ~10 символов), поэтому
    отступ всегда заканчивает кусок валидной границей.
    Пустая строка → [] (иначе вызывающий отправил бы пустое сообщение → 400)."""
    escaped = escaped or ''
    if not escaped:
        return []
    if len(escaped) <= max_len:
        return [escaped]
    chunks = []
    pos = 0
    n = len(escaped)
    while pos < n:
        end = min(pos + max_len, n)
        if end < n:
            amp = escaped.rfind('&', pos, end)
            semi = escaped.rfind(';', pos, end)
            if amp > semi:      # граница попала внутрь незакрытой сущности
                end = amp
            if end <= pos:      # страховка от бесконечного цикла
                end = pos + max_len
        chunks.append(escaped[pos:end])
        pos = end
    return chunks


def json_dumps(obj) -> str:
    import json as _json
    return _json.dumps(obj, ensure_ascii=False)

def json_loads(s: str):
    import json as _json
    return _json.loads(s)
