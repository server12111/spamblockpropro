# -*- coding: utf-8 -*-
"""
forum_support.py — режим техподдержки через Telegram Forum Topics для дочерних ботов.

Ключевые идеи:
- Один дочерний бот = одна привязанная группа (fs_groups). Отключение группы
  возвращает стандартный режим (личные сообщения админам).
- Один пользователь = одна тема (fs_topics). Тема создаётся конкурентно-безопасно
  через claim-запись в БД (INSERT OR IGNORE + ожидание/захват залипшей заявки).
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
import random
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone

from telebot.apihelper import ApiTelegramException
from telebot.types import (InlineKeyboardMarkup, InlineKeyboardButton,
                           ReplyParameters, InputMediaPhoto, InputMediaVideo)

from database import (DB_PATH, _utc_now_str,
                      db_add_user, db_get_user_info, db_bump_user_contact,
                      db_add_username_history, db_get_username_history,
                      db_is_blocked, db_get_bot_admins,
                      db_get_bot_setting, db_set_bot_setting,
                      db_get_bot_info, db_log_message,
                      db_fs_count_range, db_fs_tickets_total,
                      db_fs_new_users_range, db_fs_users_total,
                      db_fs_user_tickets_total)
from config import STATS_TZ_OFFSET

log = logging.getLogger(__name__)

# ── Конфигурация ─────────────────────────────────────────────
TICKET_GAP_SEC = 6 * 3600                              # пауза между сообщениями, открывающая новое обращение
PENDING_MAX = 20                                       # максимум сообщений в очереди подтверждения
TOPIC_NAME_LIMIT = 128                                 # лимит Telegram на название темы
ADMIN_TTL = 30                                         # TTL кэша списка админов (сек)
INFO_HIST_MAX = 10                                     # сколько username показывать в инфо-сообщении

# ── Единый rate limit (используется и purchased_bot._is_rate_limited) ──
RATE_LIMIT_SEC = 15
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
#  Локальный доступ к БД (таблицы fs_* создаются в database.init_db)
# ══════════════════════════════════════════════════════════════
def _conn():
    return sqlite3.connect(DB_PATH, timeout=15)


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

def fs_bind_group(bot_id: int, chat_id: int, title: str):
    """Привязка группы. Повторная привязка тем же ботом обновляет запись;
    конфликт chat_id с другим ботом → IntegrityError (защита на уровне БД)."""
    c = _conn()
    try:
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

def fs_unbind_group(bot_id: int):
    """Отвязка группы: темы и соответствия сообщений становятся неактуальными
    (thread_id действует только внутри своей группы), пользовательские данные
    (bot_users, история username, обращения) сохраняются."""
    c = _conn()
    row = c.execute('SELECT chat_id FROM fs_groups WHERE bot_id=?', (bot_id,)).fetchone()
    c.execute('DELETE FROM fs_groups WHERE bot_id=?', (bot_id,))
    c.execute('DELETE FROM fs_topics WHERE bot_id=?', (bot_id,))
    c.execute('DELETE FROM fs_msgs WHERE bot_id=?', (bot_id,))
    c.commit(); c.close()
    return row[0] if row else None


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
def fs_get_pending(bot_id: int, user_id: int):
    """(payload_json, status, prompt_msg_id) или None."""
    c = _conn()
    row = c.execute('SELECT payload, status, prompt_msg_id FROM fs_pending '
                    'WHERE bot_id=? AND user_id=?', (bot_id, user_id)).fetchone()
    c.close(); return row

def fs_append_pending(bot_id: int, user_id: int, item: dict):
    """Добавляет сообщение в очередь. Возвращает (кол-во, prompt_msg_id)
    или (None, None) если очередь полна."""
    c = _conn()
    row = c.execute('SELECT payload, prompt_msg_id FROM fs_pending '
                    'WHERE bot_id=? AND user_id=?', (bot_id, user_id)).fetchone()
    if row:
        try:
            items = json_loads(row[0])
        except Exception:
            items = []
        if len(items) >= PENDING_MAX:
            c.close(); return None, row[1]
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
    c.commit(); c.close()
    return cnt, prompt

def fs_set_prompt(bot_id: int, user_id: int, msg_id: int):
    c = _conn()
    c.execute('UPDATE fs_pending SET prompt_msg_id=? WHERE bot_id=? AND user_id=?',
              (msg_id, bot_id, user_id))
    c.commit(); c.close()

def fs_claim_pending(bot_id: int, user_id: int):
    """Атомарно забирает очередь в работу (защита от повторного нажатия кнопки).
    Возвращает список элементов или None, если очередь уже обрабатывается."""
    c = _conn()
    cur = c.execute("UPDATE fs_pending SET status='sending', updated_at=? "
                    "WHERE bot_id=? AND user_id=? AND status='pending'",
                    (_utc_now_str(), bot_id, user_id))
    c.commit()
    row = None
    if cur.rowcount > 0:
        row = c.execute('SELECT payload FROM fs_pending WHERE bot_id=? AND user_id=?',
                        (bot_id, user_id)).fetchone()
    c.close()
    if row is None:
        return None
    try:
        return json_loads(row[0])
    except Exception:
        return []

def fs_restore_pending(bot_id: int, user_id: int, items: list):
    """Возвращает очередь в состояние pending (с отметками о доставленном)."""
    c = _conn()
    c.execute("UPDATE fs_pending SET payload=?, status='pending', updated_at=? "
              'WHERE bot_id=? AND user_id=?',
              (json_dumps(items), _utc_now_str(), bot_id, user_id))
    c.commit(); c.close()

def fs_delete_pending(bot_id: int, user_id: int):
    c = _conn()
    c.execute('DELETE FROM fs_pending WHERE bot_id=? AND user_id=?', (bot_id, user_id))
    c.commit(); c.close()


# ── Обращения ─────────────────────────────────────────────────
def fs_bump_ticket(bot_id: int, user_id: int):
    """Учёт обращений: серия сообщений с паузой < TICKET_GAP_SEC = одно обращение."""
    now_dt = datetime.utcnow()
    now_s = now_dt.strftime('%Y-%m-%d %H:%M:%S')
    c = _conn()
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
_admin_cache: dict = {}
_admin_lock = threading.Lock()

def fs_is_admin(bot_id: int, user_id: int) -> bool:
    now = time.time()
    with _admin_lock:
        ent = _admin_cache.get(bot_id)
        if ent and now - ent[0] < ADMIN_TTL:
            return user_id in ent[1]
    admins = db_get_bot_admins(bot_id)
    with _admin_lock:
        _admin_cache[bot_id] = (now, admins)
    return user_id in admins


# ══════════════════════════════════════════════════════════════
#  Telegram API: вызовы с обработкой 429 и классификация ошибок
# ══════════════════════════════════════════════════════════════
def _api(fn, *args, **kwargs):
    """Вызов Telegram API с ограниченным числом повторов при 429 Too Many Requests."""
    attempts = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except ApiTelegramException as e:
            if e.error_code == 429 and attempts < 3:
                try:
                    ra = int(e.result_json['parameters']['retry_after'])
                except Exception:
                    ra = 5
                time.sleep(min(ra + 1, 30))
                attempts += 1
                continue
            raise

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
    esc = html.escape
    uid = u.id
    info = db_get_user_info(bot_id, uid) or {}
    username = f"@{u.username}" if getattr(u, 'username', None) else 'нет'

    hist = db_get_username_history(bot_id, uid)
    if hist:
        lines = [f"• {esc('@' + h)}" for h in hist[-INFO_HIST_MAX:]]
        hist_block = '\n'.join(lines)
    else:
        hist_block = '—'

    reg = _fmt_dt_utc(info.get('registered_at')) or 'Неизвестно'
    first = _fmt_dt_utc(info.get('first_contact_at')) or 'Неизвестно'
    tickets = db_fs_user_tickets_total(bot_id, uid)
    msgs_cnt = info.get('contact_count', 0)

    return (f"<b>👤 Информация о пользователе</b>\n\n"
            f"👤 Имя: <b>{esc(_full_name(u))}</b>\n"
            f"🔗 Username: {esc(username)}\n"
            f"🆔 Telegram ID: <code>{uid}</code>\n\n"
            f"📋 Ранее известные username:\n{hist_block}\n\n"
            f"📅 Зарегистрирован в боте: {reg}\n"
            f"📩 Первое обращение: {first}\n"
            f"📊 Всего обращений: {_fmt_num(tickets)}\n"
            f"💬 Сообщений от пользователя: {_fmt_num(msgs_cnt)}")


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
def _get_or_create_topic(pbot, bot_id: int, gid: int, u, depth: int = 0) -> int:
    """Возвращает message_thread_id темы пользователя.
    Конкурентная защита: claim-запись в БД + ожидание/захват залипшей заявки."""
    uid = u.id
    title = fs_topic_title(u)
    row = fs_get_topic(bot_id, uid)
    if row and row[0] > 0:
        return row[0]
    if depth > 3:
        raise RuntimeError('topic claim loop')
    if fs_claim_topic(bot_id, uid, title):
        # мы выиграли гонку — создаём тему
        try:
            t = _api(pbot.create_forum_topic, gid, title)
            fs_set_topic_thread(bot_id, uid, t.message_thread_id, title)
            return t.message_thread_id
        except Exception:
            fs_delete_topic_row(bot_id, uid)  # отпускаем заявку
            raise
    # кто-то другой создаёт — ждём результат (без удержания БД)
    for _ in range(20):
        time.sleep(0.5)
        row = fs_get_topic(bot_id, uid)
        if row and row[0] > 0:
            return row[0]
    # залипшая заявка (поток-создатель умер) — захватываем
    row = fs_get_topic(bot_id, uid)
    if row and row[0] == 0:
        fs_delete_topic_row(bot_id, uid)
    return _get_or_create_topic(pbot, bot_id, gid, u, depth + 1)


def _ensure_pinned_info(pbot, bot_id: int, gid: int, thread: int, u, trow):
    """Создаёт или обновляет закреплённое инфо-сообщение (без повторного пина)."""
    info_text = fs_info_text(bot_id, u)
    h = hashlib.md5(info_text.encode('utf-8')).hexdigest()
    pinned = trow[2] if trow else None
    if pinned and trow and trow[3] == h:
        return  # актуально
    if pinned:
        try:
            _api(pbot.edit_message_text, info_text, gid, pinned, parse_mode='HTML')
            fs_set_topic_pinned(bot_id, u.id, pinned, h)
            return
        except ApiTelegramException:
            pass  # сообщение удалено — создадим заново ниже
        except Exception as e:
            log.warning(f'fs pin edit bot#{bot_id}: {e}')
            return
    try:
        msg = _api(pbot.send_message, gid, info_text, parse_mode='HTML',
                   message_thread_id=thread)
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


def fs_ensure_topic_info(pbot, bot_id: int, group_row, u) -> int:
    """Тема + закреплённая информация + актуальное название темы. → thread_id"""
    gid = group_row[1]
    thread = _get_or_create_topic(pbot, bot_id, gid, u)
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

def _resend_by_type(pbot, to_chat, thread, m, reply_msg_id=None):
    """Третий способ передачи: повторная отправка содержимого через Bot API."""
    rp = _reply_params(reply_msg_id)
    kw = {'message_thread_id': thread} if thread else {}
    ct = m.content_type
    if ct == 'text':
        return _api(pbot.send_message, to_chat, m.text, entities=m.entities,
                    reply_parameters=rp, **kw).message_id
    if ct == 'photo':
        return _api(pbot.send_photo, to_chat, m.photo[-1].file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'video':
        return _api(pbot.send_video, to_chat, m.video.file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'document':
        return _api(pbot.send_document, to_chat, m.document.file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'audio':
        return _api(pbot.send_audio, to_chat, m.audio.file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'voice':
        return _api(pbot.send_voice, to_chat, m.voice.file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'video_note':
        return _api(pbot.send_video_note, to_chat, m.video_note.file_id, **kw).message_id
    if ct == 'sticker':
        return _api(pbot.send_sticker, to_chat, m.sticker.file_id, reply_parameters=rp, **kw).message_id
    if ct == 'animation':
        return _api(pbot.send_animation, to_chat, m.animation.file_id, caption=m.caption,
                    caption_entities=m.caption_entities, reply_parameters=rp, **kw).message_id
    if ct == 'contact':
        c = m.contact
        return _api(pbot.send_contact, to_chat, c.phone_number, c.first_name,
                    last_name=c.last_name, reply_parameters=rp, **kw).message_id
    if ct == 'location':
        return _api(pbot.send_location, to_chat, m.location.latitude,
                    m.location.longitude, reply_parameters=rp, **kw).message_id
    if ct == 'venue':
        v = m.venue
        return _api(pbot.send_venue, to_chat, v.location.latitude, v.location.longitude,
                    v.title, v.address, reply_parameters=rp, **kw).message_id
    if ct == 'poll' and m.poll:
        opts = [o.text for o in m.poll.options]
        return _api(pbot.send_poll, to_chat, m.poll.question, opts,
                    is_anonymous=m.poll.is_anonymous, type=m.poll.type,
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
    try:
        _api(pbot.reopen_forum_topic, gid, thread)
        return thread
    except Exception:
        pass
    fs_delete_topic_row(bot_id, u.id)
    return _get_or_create_topic(pbot, bot_id, gid, u)

def _is_topic_err(e) -> bool:
    return isinstance(e, ApiTelegramException) and _is_topic_gone(e)

def _relay_to_topic(pbot, bot_id: int, group_row, u, from_chat, msg_id,
                    thread=None, reply_msg_id=None, src_msg=None):
    """Передача одного сообщения в тему пользователя с восстановлением темы."""
    gid = group_row[1]
    tries = 0
    while True:
        if thread is None:
            thread = fs_ensure_topic_info(pbot, bot_id, group_row, u)
        try:
            mid = _relay_single(pbot, gid, from_chat, msg_id, thread=thread,
                                reply_msg_id=reply_msg_id, src_msg=src_msg)
            return mid, thread
        except Exception as e:
            tries += 1
            if tries >= 2 or not _is_topic_err(e):
                raise
            thread = _recover_topic(pbot, bot_id, gid, u, thread)

def _relay_batch_to_topic(pbot, bot_id: int, group_row, u, from_chat, ids,
                          msgs=None, thread=None):
    """Передача альбома: copy_messages → forward_messages → пересборка
    медиагруппы → по одному. Возвращает список id (может быть короче ids)."""
    gid = group_row[1]
    tries = 0
    while True:
        if thread is None:
            thread = fs_ensure_topic_info(pbot, bot_id, group_row, u)
        sent = None
        topic_err = None
        try:
            res = _api(pbot.copy_messages, gid, from_chat, ids, message_thread_id=thread)
            sent = [r.message_id for r in res]
        except ApiTelegramException as e:
            if _is_topic_err(e):
                topic_err = e
            else:
                log.debug(f'album copy failed: {e}')
        if sent is None and topic_err is None:
            try:
                res = _api(pbot.forward_messages, gid, from_chat, ids,
                           message_thread_id=thread)
                sent = [r.message_id for r in res]
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
                    sent = [x.message_id for x in mm]
                elif msgs:
                    sent = []
                    for ms in msgs:
                        mid = _relay_single(pbot, gid, from_chat, ms.message_id,
                                            thread=thread, src_msg=ms)
                        if mid:
                            sent.append(mid)
            return sent or [], thread
        except Exception as e:
            tries += 1
            if tries >= 2 or not _is_topic_err(e):
                raise
            thread = _recover_topic(pbot, bot_id, gid, u, thread)



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

def _flush_album(pbot, bot_id: int, key):
    with _album_lock:
        buf = _album_buffers.pop(key, None)
    if not buf:
        return
    uid = key[1]
    group = fs_get_group(bot_id)
    u = None
    try:
        if not group or group[1] != buf['gid']:
            log.warning(f'album flush bot#{bot_id}: группа изменилась/отвязана — пропущено')
            return
        u = buf['msgs'][0].from_user
        sent, thread = _relay_batch_to_topic(
            pbot, bot_id, group, u, buf['chat_id'], buf['ids'], buf['msgs'], buf['thread'])
        for src_id, gm_id in zip(buf['ids'], sent):
            fs_map_message(bot_id, buf['gid'], gm_id, uid, thread, src_id, media_group=key[2])
        fs_bump_ticket(bot_id, uid)
        db_bump_user_contact(bot_id, uid)
        db_log_message(bot_id, uid, f'[{buf["msgs"][0].content_type} x{len(buf["ids"])}]')
        if buf.get('confirm_kb'):
            try:
                pbot.send_message(buf['chat_id'], '<b>✅ Сообщение отправлено</b>',
                                  parse_mode='HTML', reply_markup=buf['confirm_kb']())
            except Exception:
                pass
    except Exception as e:
        log.warning(f'album flush bot#{bot_id} uid={uid}: {e}')
        try:
            pbot.send_message(buf['chat_id'],
                              '<b>❌ Не удалось отправить сообщение. Попробуй позже.</b>',
                              parse_mode='HTML')
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════
#  Маршрутизация сообщения пользователя (кнопка p_send / сценарий await_msg)
# ══════════════════════════════════════════════════════════════
def route_user_message(pbot, bot_id: int, m, confirm_kb=None) -> bool:
    """Доставка обращения пользователя.

    True  — группа подключена, сообщение ушло в тему (или пользователю показана
            ошибка; повторная отправка админам не выполняется);
    False — группа не подключена: вызывающий код использует стандартный режим.
    """
    group = fs_get_group(bot_id)
    if not group:
        return False
    uid = m.from_user.id
    fs_touch_user(pbot, bot_id, m.from_user)
    if m.media_group_id:
        # альбом: темы создаются по первому элементу, доставка группой
        try:
            thread = fs_ensure_topic_info(pbot, bot_id, group, m.from_user)
        except Exception as e:
            log.warning(f'route album topic bot#{bot_id} uid={uid}: {e}')
            _notify_owner_error(pbot, bot_id,
                                'Не удалось создать тему в группе техподдержки', e)
            try:
                pbot.send_message(m.chat.id,
                                  '<b>❌ Не удалось доставить сообщение в техподдержку. '
                                  'Попробуй позже.</b>', parse_mode='HTML')
            except Exception:
                pass
            return True
        _buffer_album(pbot, bot_id, group[1], thread, m, confirm_kb)
        return True
    try:
        mid, thread = _relay_to_topic(pbot, bot_id, group, m.from_user,
                                      m.chat.id, m.message_id, src_msg=m)
        if not mid:
            raise RuntimeError('не удалось передать сообщение (copy/forward/resend)')
        fs_map_message(bot_id, group[1], mid, uid, thread, m.message_id, '')
        fs_bump_ticket(bot_id, uid)
        db_bump_user_contact(bot_id, uid)
        db_log_message(bot_id, uid, m.text or f'[{m.content_type}]')
        if confirm_kb:
            try:
                pbot.send_message(m.chat.id, '<b>✅ Сообщение отправлено</b>',
                                  parse_mode='HTML', reply_markup=confirm_kb())
            except Exception:
                pass
        return True
    except Exception as e:
        log.warning(f'route user msg bot#{bot_id} uid={uid}: {e}')
        if _is_bot_blocked(e) or _is_chat_not_found(e):
            # группа недоступна боту (удалена/потеряны права) — уведомляем владельца
            _notify_owner_error(pbot, bot_id,
                                'Бот не может писать в группу техподдержки', e)
        try:
            pbot.send_message(m.chat.id,
                              '<b>❌ Не удалось доставить сообщение в техподдержку. '
                              'Попробуй позже.</b>', parse_mode='HTML')
        except Exception:
            pass
        return True


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
                f"⚠️ <b>Группа техподдержки</b>\n\n{text}\n\n"
                f"<code>{html.escape(str(err)[:300])}</code>\n\n"
                f"Проверь группу в /admin → 💬 Группа техподдержки.",
                parse_mode='HTML')
    except Exception as e:
        log.warning(f'notify owner bot#{bot_id}: {e}')


# ══════════════════════════════════════════════════════════════
#  Группа: обработка сообщений (ответы админов через Reply, код подключения)
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

def fs_handle_group_message(pbot, bot_id: int, m, bot_self_id: int, pstate) -> None:
    """Сообщения из привязанной группы:
    - код подтверждения подключения;
    - ЛЮБОЕ сообщение администратора в теме пользователя доставляется
      пользователю: и «Ответить» на его сообщение (с reply-контекстом),
      и обычное сообщение в теме без Reply;
    - ответ на закреплённое инфо-сообщение = внутренняя заметка (не доставляется);
    - служебные события и сообщения не-админов игнорируются."""
    if m.content_type in SERVICE_TYPES:
        return
    if not m.from_user or m.from_user.id == bot_self_id:
        return  # защита от циклической обработки собственных сообщений
    group = fs_get_group(bot_id)
    if not group or m.chat.id != group[1]:
        return
    uid_sender = m.from_user.id

    # ── код подтверждения подключения группы ──
    txt = (m.text or '').strip()
    if txt and len(txt) <= 24:
        st = pstate.get(uid_sender)
        if isinstance(st, dict) and st.get('step') == 'fs_connect' and st.get('code') == txt:
            _finish_connect(pbot, bot_id, uid_sender, m.chat.id,
                            m.chat.title or '', pstate)
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
                      '🚫 Пользователь заблокирован — ответ не доставлен.')
        return

    # ── альбом от админа: накапливаем и отправляем одной пачкой ──
    if m.media_group_id:
        _buffer_admin_album(pbot, bot_id, m.chat.id, target_uid, thread, m)
        return

    try:
        mid = _relay_single(pbot, target_uid, m.chat.id, m.message_id,
                            reply_msg_id=reply_ctx, src_msg=m)
        if mid:
            fs_log_admin_msg(bot_id, target_uid)
        else:
            _topic_notice(pbot, m.chat.id, thread,
                          '⚠️ Не удалось доставить ответ: тип сообщения не поддерживается.')
    except ApiTelegramException as e:
        if _is_bot_blocked(e):
            _topic_notice(pbot, m.chat.id, thread,
                          '⚠️ Не удалось доставить ответ: пользователь заблокировал бота.')
        elif _is_chat_not_found(e):
            _topic_notice(pbot, m.chat.id, thread,
                          '⚠️ Не удалось доставить ответ: пользователь недоступен.')
        else:
            log.warning(f'admin reply bot#{bot_id} → {target_uid}: {e}')
            _topic_notice(pbot, m.chat.id, thread,
                          '❌ Ошибка доставки ответа пользователю.')
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

def _flush_admin_album(pbot, bot_id: int, key):
    with _admin_album_lock:
        buf = _admin_album_buffers.pop(key, None)
    if not buf:
        return
    target_uid, thread = key[1], buf['thread']
    group = fs_get_group(bot_id)
    try:
        if not group or group[1] != buf['gid']:
            log.warning(f'admin album flush bot#{bot_id}: группа изменилась — пропущено')
            return
        if db_is_blocked(bot_id, target_uid):
            _topic_notice(pbot, buf['gid'], thread,
                          '🚫 Пользователь заблокирован — ответ не доставлен.')
            return
        _api(pbot.copy_messages, target_uid, buf['gid'], buf['ids'])
        fs_log_admin_msg(bot_id, target_uid)
    except ApiTelegramException as e:
        if _is_bot_blocked(e):
            _topic_notice(pbot, buf['gid'], thread,
                          '⚠️ Не удалось доставить ответ: пользователь заблокировал бота.')
        elif _is_chat_not_found(e):
            _topic_notice(pbot, buf['gid'], thread,
                          '⚠️ Не удалось доставить ответ: пользователь недоступен.')
        else:
            log.warning(f'admin album bot#{bot_id} → {target_uid}: {e}')
    except Exception as e:
        log.warning(f'admin album bot#{bot_id} → {target_uid}: {e}')

def _topic_notice(pbot, chat_id: int, thread: int, text: str):
    """Служебное сообщение в теме (не попадает в fs_msgs → на него нельзя «ответить»)."""
    try:
        pbot.send_message(chat_id, text, parse_mode='HTML', message_thread_id=thread)
    except Exception as e:
        log.warning(f'topic notice {chat_id}/{thread}: {e}')


# ══════════════════════════════════════════════════════════════
#  Очередь подтверждения: сбор случайных сообщений и отправка
# ══════════════════════════════════════════════════════════════
def queue_random_message(pbot, bot_id: int, m) -> None:
    """Пользователь пишет без кнопки p_send: очередь + запрос подтверждения."""
    uid = m.from_user.id
    if db_is_blocked(bot_id, uid):
        try:
            pbot.send_message(m.chat.id, '<b>🚫 Вы заблокированы администратором</b>',
                              parse_mode='HTML')
        except Exception:
            pass
        return
    item = {'mid': m.message_id, 'type': m.content_type,
            'mg': m.media_group_id or ''}
    cnt, prompt_id = fs_append_pending(bot_id, uid, item)
    if cnt is None:
        try:
            pbot.send_message(m.chat.id,
                              f'<b>📝 Очередь полна ({PENDING_MAX} сообщений) — '
                              'сначала отправь её администратору.</b>', parse_mode='HTML')
        except Exception:
            pass
        return
    text = '<b>💬 Отправить сообщение администратору?</b>'
    if cnt > 1:
        text += f'\n\n📝 В очереди: <b>{cnt}</b> сообщений'
    kb = _confirm_kb()
    if prompt_id:
        try:
            pbot.edit_message_text(text, m.chat.id, prompt_id,
                                   parse_mode='HTML', reply_markup=kb)
            return
        except ApiTelegramException as e:
            if 'not modified' in _err_low(e):
                return  # промпт уже актуален
            # промпт удалён — отправим новый ниже
        except Exception:
            pass
    try:
        msg = pbot.send_message(m.chat.id, text, parse_mode='HTML', reply_markup=kb)
        fs_set_prompt(bot_id, uid, msg.message_id)
    except Exception as e:
        log.warning(f'queue prompt bot#{bot_id} uid={uid}: {e}')


def _confirm_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('📩 Отправить администратору',
                                callback_data='fs_confirm_send'))
    kb.add(InlineKeyboardButton('❌ Отменить', callback_data='fs_confirm_cancel'))
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


def _deliver_pending_payload(pbot, bot_id: int, items: list, uid: int) -> bool:
    """Доставляет очередь подтверждения: в тему группы или админам в личку (forward).
    Возвращает True, если всё доставлено; помечает доставленное в items['done']."""
    todo = [it for it in items if not it.get('done')]
    if not todo:
        return True
    group = fs_get_group(bot_id)
    all_ok = True
    if group:
        # нужен объект пользователя для темы/инфо — берём из Telegram
        try:
            chat_user = _api(pbot.get_chat, uid)
        except Exception as e:
            log.warning(f'pending get_chat bot#{bot_id} uid={uid}: {e}')
            return False
        fs_touch_user(pbot, bot_id, chat_user)
        for mg, grp in _group_pending_items(todo):
            ids = [it['mid'] for it in grp]
            if any(it.get('done') for it in grp):
                continue
            try:
                if len(ids) == 1:
                    mid, thread = _relay_to_topic(pbot, bot_id, group, chat_user,
                                                  uid, ids[0], src_msg=None)
                    sent = [mid] if mid else []
                else:
                    sent, thread = _relay_batch_to_topic(pbot, bot_id, group,
                                                         chat_user, uid, ids)
            except Exception as e:
                log.warning(f'pending deliver bot#{bot_id} uid={uid}: {e}')
                all_ok = False
                continue
            if not sent:
                all_ok = False
                continue
            for src_id, gm_id in zip(ids, sent):
                fs_map_message(bot_id, group[1], gm_id, uid, thread, src_id,
                               media_group=mg)
            for it in grp:
                it['done'] = 1
            fs_bump_ticket(bot_id, uid)
            db_bump_user_contact(bot_id, uid)
            db_log_message(bot_id, uid,
                           grp[0]['type'] if len(grp) == 1
                           else f"[{grp[0]['type']} x{len(grp)}]")
    else:
        # стандартный режим: пересылаем админам (сохраняет отправителя)
        from database import db_get_bot_admins as _adm
        admins = _adm(bot_id)
        for mg, grp in _group_pending_items(todo):
            ids = [it['mid'] for it in grp]
            ok_all = True
            for aid in admins:
                try:
                    if len(ids) == 1:
                        _api(pbot.forward_message, aid, uid, ids[0])
                    else:
                        _api(pbot.forward_messages, aid, uid, ids)
                except Exception as e:
                    log.warning(f'pending forward bot#{bot_id} admin {aid}: {e}')
                    ok_all = False
            if ok_all:
                for it in grp:
                    it['done'] = 1
                db_log_message(bot_id, uid,
                               grp[0]['type'] if len(grp) == 1
                               else f"[{grp[0]['type']} x{len(grp)}]")
            else:
                all_ok = False
    return all_ok


def confirm_send(pbot, bot_id: int, cb) -> None:
    """Обработка кнопки «Отправить администратору»."""
    uid = cb.from_user.id
    row = fs_get_pending(bot_id, uid)
    if not row or row[1] != 'pending':
        pbot.answer_callback_query(cb.id, 'Очередь пуста или уже отправляется')
        return
    # rate limit «мягко»: при лимите очередь сохраняется, ничего не теряется
    if rate_limited(bot_id, uid, mark=False):
        pbot.answer_callback_query(
            cb.id, f'⏳ Подожди {RATE_LIMIT_SEC} сек и нажми кнопку ещё раз — '
            'сообщения не потеряются', show_alert=True)
        return
    items = fs_claim_pending(bot_id, uid)
    if items is None:
        pbot.answer_callback_query(cb.id, 'Уже отправляется')
        return
    rate_limited(bot_id, uid, mark=True)
    ok = _deliver_pending_payload(pbot, bot_id, items, uid)
    if ok:
        fs_delete_pending(bot_id, uid)
        pbot.answer_callback_query(cb.id, '✅ Отправлено')
        try:
            pbot.edit_message_text('<b>✅ Сообщение отправлено</b>',
                                   cb.message.chat.id, cb.message.message_id,
                                   parse_mode='HTML')
        except Exception:
            pass
    else:
        fs_restore_pending(bot_id, uid, items)
        pbot.answer_callback_query(cb.id,
                                   '❌ Не удалось отправить. Попробуй ещё раз '
                                   'через минуту.', show_alert=True)

def confirm_cancel(pbot, bot_id: int, cb) -> None:
    uid = cb.from_user.id
    fs_delete_pending(bot_id, uid)
    pbot.answer_callback_query(cb.id, 'Очередь отменена')
    try:
        pbot.edit_message_text('<b>🗑 Очередь отменена</b>',
                               cb.message.chat.id, cb.message.message_id,
                               parse_mode='HTML')
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
#  Подключение / отключение группы
# ══════════════════════════════════════════════════════════════
def _gen_code() -> str:
    alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
    return ''.join(random.choices(alphabet, k=6))

def fs_verify_group(pbot, chat_id: int, uid: int, bot_self_id: int, bot_id: int = None):
    """Проверки подключения. Возвращает (ok, текст_ошибки_html)."""
    try:
        chat = _api(pbot.get_chat, chat_id)
    except Exception:
        return False, ('❌ Не удалось получить информацию о чате. '
                       'Убедись, что бот добавлен в группу.')
    if getattr(chat, 'type', '') != 'supergroup':
        return False, ('❌ Группа должна быть <b>супергруппой</b>. '
                       'Обычную группу нужно преобразовать (добавь любую тему/создай новую).')
    if not getattr(chat, 'is_forum', False):
        return False, ('❌ В группе не включены <b>темы</b>.\n'
                       'Включи: настройки группы → «Темы» → Включить.')
    try:
        me = _api(pbot.get_chat_member, chat_id, bot_self_id)
    except Exception:
        return False, '❌ Бот не состоит в этой группе.'
    if getattr(me, 'status', '') != 'administrator':
        return False, '❌ Назначь бота <b>администратором</b> группы.'
    if not (getattr(me, 'can_manage_topics', False) and getattr(me, 'can_pin_messages', False)):
        return False, ('❌ У бота нет нужных прав.\nВыдай: '
                       '<b>управление темами</b> + <b>закрепление сообщений</b>.')
    try:
        member = _api(pbot.get_chat_member, chat_id, uid)
    except Exception:
        return False, '❌ Не удалось проверить твои права в группе.'
    if getattr(member, 'status', '') not in ('creator', 'administrator'):
        return False, ('❌ Подключать группу может только её '
                       '<b>владелец или администратор</b>.')
    if bot_id is not None:
        other = fs_get_group_by_chat(chat_id)
        if other and other[0] != bot_id:
            return False, ('❌ Эта группа уже подключена к другому боту '
                           f'(Bot #{other[0]}).')
    return True, ''

def _finish_connect(pbot, bot_id: int, uid: int, chat_id: int, title: str, pstate):
    """Проверка + привязка группы + создание темы статистики."""
    try:
        bot_self_id = pbot.get_me().id
    except Exception:
        bot_self_id = 0
    ok, err = fs_verify_group(pbot, chat_id, uid, bot_self_id, bot_id=bot_id)
    if not ok:
        try:
            pbot.send_message(uid, err, parse_mode='HTML')
        except Exception:
            pass
        return
    old = fs_get_group(bot_id)
    if old and old[1] != chat_id:
        fs_unbind_group(bot_id)  # смена группы: старые темы/соответствия сбрасываются
    try:
        fs_bind_group(bot_id, chat_id, title)
    except sqlite3.IntegrityError:
        try:
            pbot.send_message(uid, '❌ Эта группа уже подключена к другому боту.',
                              parse_mode='HTML')
        except Exception:
            pass
        return
    # тема статистики
    try:
        t = _api(pbot.create_forum_topic, chat_id, '📊 Статистика')
        fs_set_stats_thread(bot_id, t.message_thread_id)
    except Exception as e:
        fs_unbind_group(bot_id)
        log.warning(f'stats topic create bot#{bot_id}: {e}')
        try:
            pbot.send_message(uid,
                              '❌ Группа подключена, но не удалось создать тему '
                              '«📊 Статистика». Проверь право бота '
                              '«управление темами» и попробуй ещё раз.',
                              parse_mode='HTML')
        except Exception:
            pass
        return
    pstate.pop(uid, None)
    try:
        pbot.send_message(
            uid,
            f'✅ <b>Группа техподдержки подключена!</b>\n\n'
            f'📌 {html.escape(title or "Группа")} (<code>{chat_id}</code>)\n\n'
            'Теперь обращения пользователей создают отдельные темы.\n'
            'Чтобы ответить — просто напиши в теме пользователя '
            '(или через «Ответить» на его сообщение).\n'
            'Служебная тема «📊 Статистика» будет получать еженедельные отчёты.',
            parse_mode='HTML')
    except Exception:
        pass
    log.info(f'Bot #{bot_id}: support group {chat_id} connected by {uid}')

def connect_start(pbot, bot_id: int, cb, pstate):
    uid = cb.from_user.id
    code = _gen_code()
    pstate[uid] = {'step': 'fs_connect', 'code': code}
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('❌ Отмена', callback_data='fs_connect_cancel'))
    try:
        pbot.edit_message_text(
            '<b>🔗 Подключение группы техподдержки</b>\n\n'
            '1️⃣ Добавь бота в свою группу с темами и назначь администратором с правами:\n'
            '   • Управление темами\n'
            '   • Закрепление сообщений\n'
            '   • Удаление сообщений\n\n'
            '2️⃣ Подтверди, что группа твоя, одним из способов:\n'
            '   • Перешли в этот чат любое сообщение из группы\n'
            '   • Или отправь в группу этот код (со своего аккаунта):\n'
            f'   <code>{code}</code>\n\n'
            '⚠️ Группа должна быть супергруппой с включёнными темами.\n'
            '💡 Если бот не видит код в группе — отключи Privacy Mode '
            'у бота в @BotFather (/setprivacy → Disable) и попробуй снова.\n'
            'Один бот = одна группа. Подключённая группа включит форум-режим.',
            cb.message.chat.id, cb.message.message_id, parse_mode='HTML',
            reply_markup=kb)
    except Exception as e:
        log.warning(f'connect_start edit: {e}')

def connect_cancel(pbot, bot_id: int, cb, pstate):
    pstate.pop(cb.from_user.id, None)
    try:
        pbot.answer_callback_query(cb.id)
    except Exception:
        pass
    try:
        menu_show(pbot, bot_id, cb)
    except Exception as e:
        log.warning(f'connect_cancel menu bot#{bot_id}: {e}')

def disconnect_confirm(pbot, bot_id: int, cb):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('✅ Да, отключить', callback_data='fs_disconnect_yes',
                                style='danger'))
    kb.add(InlineKeyboardButton('❌ Отмена', callback_data='fs_group_menu'))
    try:
        pbot.edit_message_text(
            '<b>❌ Отключение группы техподдержки</b>\n\n'
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
        pbot.edit_message_text(
            f'✅ <b>Группа (<code>{chat_id}</code>) отключена.</b>\n\n'
            'Возврат в стандартный режим: обращения приходят администраторам в ЛС.',
            cb.message.chat.id, cb.message.message_id, parse_mode='HTML')
    except Exception:
        pass
    log.info(f'Bot #{bot_id}: support group disconnected')

def menu_text(bot_id: int) -> str:
    g = fs_get_group(bot_id)
    if g:
        connected = (f'💬 <b>Группа техподдержки</b>\n\n'
                     f'📌 {html.escape(g[2] or "Группа")} (<code>{g[1]}</code>)\n'
                     f'📅 Подключена: {_fmt_dt_utc(g[4]) or "—"}\n\n'
                     'Режим: обращения пользователей создают темы в группе.\n'
                     'Ответы: любое твоё сообщение в теме пользователя уходит ему\n'
                     '(и через «Ответить», и обычным сообщением).\n'
                     'Ответ на закреплённое инфо = внутренняя заметка.')
    else:
        connected = ('💬 <b>Группа техподдержки</b>\n\n'
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
        kb.add(InlineKeyboardButton('🔄 Сменить группу', callback_data='fs_connect'))
        kb.add(InlineKeyboardButton('❌ Отключить группу', callback_data='fs_disconnect',
                                    style='danger'))
    else:
        kb.add(InlineKeyboardButton('🔗 Подключить группу', callback_data='fs_connect',
                                    style='primary'))
    kb.add(InlineKeyboardButton('🔙 Назад', callback_data='p_back_admin'))
    return kb

def menu_show(pbot, bot_id: int, cb):
    pbot.edit_message_text(menu_text(bot_id), cb.message.chat.id, cb.message.message_id,
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
    return (f'<b>📊 Статистика техподдержки</b>\n\n'
            f'📅 Период: {disp_start} — {disp_end}\n\n'
            f'<b>📩 Обращения</b>\n'
            f'• За неделю: {_fmt_num(tickets_week)}\n'
            f'• За всё время: {_fmt_num(tickets_total)}\n'
            f'• Новых пользователей за неделю: {_fmt_num(new_users)}\n'
            f'• Всего уникальных пользователей: {_fmt_num(users_total)}\n\n'
            f'<b>💬 Сообщения</b>\n'
            f'• Получено от пользователей: {_fmt_num(msgs_user)}\n'
            f'• Отправлено администраторами: {_fmt_num(msgs_admin)}\n\n'
            f'🕒 Последнее обновление: {updated}')

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


def register_forum_support(pbot, bot_id: int, pstate, is_admin, confirm_kb=_confirm_kb):
    """Регистрирует обработчики форум-режима на дочернем боте.
    Возвращает route-функцию для маршрутизации обращений пользователя."""
    bot_self_id = 0
    try:
        bot_self_id = pbot.get_me().id
    except Exception as e:
        log.warning(f'fs register get_me bot#{bot_id}: {e}')

    def route(m, _kb=confirm_kb):
        return route_user_message(pbot, bot_id, m, confirm_kb=_kb)

    # ── сообщения из групп (ответы админов, код подключения) ──
    @pbot.message_handler(func=lambda m: m.chat.type in ('group', 'supergroup'),
                          content_types=GROUP_MSG_TYPES)
    def _fs_group_msg(m):
        try:
            fs_handle_group_message(pbot, bot_id, m, bot_self_id, pstate)
        except Exception as e:
            log.error(f'fs group msg bot#{bot_id}: {e}', exc_info=True)

    # ── личные сообщения в процессе подключения группы ──
    @pbot.message_handler(
        func=lambda m: m.chat.type == 'private'
                       and isinstance(pstate.get(m.from_user.id), dict)
                       and pstate[m.from_user.id].get('step') == 'fs_connect',
        content_types=USER_MSG_TYPES)
    def _fs_connect_private(m):
        try:
            st = pstate.get(m.from_user.id)
            if not isinstance(st, dict):
                return
            fwd_chat = m.forward_from_chat
            if fwd_chat is None:
                fo = getattr(m, 'forward_origin', None)
                if fo is not None:
                    fwd_chat = getattr(fo, 'chat', None)
            if fwd_chat is not None and getattr(fwd_chat, 'type', '') in ('group', 'supergroup'):
                _finish_connect(pbot, bot_id, m.from_user.id, fwd_chat.id,
                                fwd_chat.title or '', pstate)
            elif m.text and m.text.strip().upper().endswith(st.get('code', '@@')):
                try:
                    pbot.send_message(m.chat.id,
                                      '❌ Код нужно отправить <b>в саму группу</b>, '
                                      'а не сюда.', parse_mode='HTML')
                except Exception:
                    pass
        except Exception as e:
            log.error(f'fs connect private bot#{bot_id}: {e}', exc_info=True)

    # ── случайные сообщения пользователя (без кнопки p_send) ──
    @pbot.message_handler(
        func=lambda m: m.chat.type == 'private'
                       and pstate.get(m.from_user.id) is None
                       and not (m.text or '').startswith('/')
                       and not is_admin(m.from_user.id),
        content_types=USER_MSG_TYPES)
    def _fs_random_user_msg(m):
        try:
            queue_random_message(pbot, bot_id, m)
        except Exception as e:
            log.error(f'fs random msg bot#{bot_id}: {e}', exc_info=True)

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


# ── лёгкие JSON-хелперы (чтобы не тянуть json в сигнатурах выше) ──
def json_dumps(obj) -> str:
    import json as _json
    return _json.dumps(obj, ensure_ascii=False)

def json_loads(s: str):
    import json as _json
    return _json.loads(s)
