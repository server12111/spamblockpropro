"""Объявления сервиса: снимок адресатов, файлы и результаты хранятся в БД"""
import io
import json
import logging
import threading
import time
from datetime import datetime
from telebot import types as t
import database as db
from keyboards import TE_ANNOUNCE, TE_CHECK, TE_CROSS, _btn
from premium_emoji import custom_emoji_error, plain_emoji
from telegram_text import ordered_album, retry_telegram

log = logging.getLogger(__name__)
_lock = threading.RLock()
_worker_lock = threading.Lock()
_workers = {}
_timers = {}
_supervisor = None


def query(sql, values=(), *, write=False):
    c = db._conn()
    try:
        cur = c.execute(sql, values)
        if write:
            result = cur.lastrowid
            c.commit()
            return result
        return cur.fetchall()
    finally:
        c.close()


def job(job_id):
    rows = query('SELECT creator,chat_id,scope,status,payload FROM service_broadcasts WHERE id=?', (job_id,))
    if not rows: return None
    row = rows[0]
    return dict(creator=row[0], chat_id=row[1], scope=row[2], status=row[3], payload=json.loads(row[4]))


def destinations(main_bot, scope):
    import purchased_bot as pb
    bots = {0: main_bot}
    if scope == 'all':
        for bid, owner, token, admin, expires in db.db_get_all_bots_for_checker():
            if expires and datetime.fromisoformat(str(expires)) <= datetime.now(): continue
            bot = pb.running_bots.get(pb.running_bot_ids.get(bid))
            if bot is not None: bots[bid] = bot
    return bots


def recipients(main_bot, scope):
    users = {}
    for bid in sorted(destinations(main_bot, scope)):
        for uid in sorted(db.db_get_bot_users(bid)):
            if uid not in users and not db.db_is_blocked(bid, uid): users[uid] = bid
    return users


def _source(m):
    kind = m.content_type
    obj = getattr(m, kind, None)
    return (obj[-1] if kind == 'photo' else obj) if kind in (
        'photo','video','animation','document','audio','voice','video_note','sticker') else None


def append(main_bot, creator, chat_id, scope, m, job_id=None):
    """Альбом собирается до подтверждения; каждое входящее сообщение сохраняется сразу"""
    with _lock:
        current = job(job_id) if job_id else None
        if not current or current['status'] != 'draft':
            job_id = query('INSERT INTO service_broadcasts(creator,chat_id,scope,updated_at) VALUES(?,?,?,?)',
                           (creator, chat_id, scope, time.time()), write=True)
            current = job(job_id)
        payload = current['payload']
        if any(v['message_id'] == m.message_id for v in payload): return job_id
        if len(payload) >= 10: raise ValueError('В альбоме может быть не больше 10 сообщений')
        payload.append(m.json)
        payload = ordered_album(payload, key=lambda v: v['message_id'])
        query('UPDATE service_broadcasts SET payload=?,updated_at=? WHERE id=?',
              (json.dumps(payload, ensure_ascii=False), time.time(), job_id), write=True)
        return job_id


def prepare_files(main_bot, job_id):
    for raw in job(job_id)['payload']:
        source = _source(t.Message.de_json(raw))
        if not source: continue
        fid = source.file_id
        if query('SELECT 1 FROM service_files WHERE job_id=? AND source_id=?', (job_id, fid)): continue
        file = main_bot.get_file(fid)
        data = main_bot.download_file(file.file_path)
        query('INSERT OR IGNORE INTO service_files(job_id,source_id,name,data) VALUES(?,?,?,?)',
              (job_id, fid, getattr(source, 'file_name', None) or file.file_path.rsplit('/', 1)[-1], data), write=True)


def _file(job_id, bid, source):
    if bid == 0: return source.file_id
    rows = query('SELECT file_id FROM service_file_ids WHERE job_id=? AND bot_id=? AND source_id=?',
                 (job_id, bid, source.file_id))
    if rows: return rows[0][0]
    rows = query('SELECT name,data FROM service_files WHERE job_id=? AND source_id=?', (job_id, source.file_id))
    if not rows: raise RuntimeError('Файл ещё не подготовлен')
    stream = io.BytesIO(rows[0][1]); stream.name = rows[0][0]
    return stream


def _remember(job_id, bid, message, sent):
    source = _source(message)
    actual = _source(sent)
    if bid and source and actual:
        query('INSERT OR REPLACE INTO service_file_ids(job_id,bot_id,source_id,file_id) VALUES(?,?,?,?)',
              (job_id, bid, source.file_id, actual.file_id), write=True)


def send(bot, job_id, bid, uid):
    messages = [t.Message.de_json(raw) for raw in job(job_id)['payload']]
    def deliver():
        if len(messages) > 1:
            media = []
            for m in messages:
                cls = getattr(t, 'InputMedia' + m.content_type.capitalize())
                options = dict(caption=m.caption, caption_entities=m.caption_entities, parse_mode=None)
                if m.content_type in ('photo', 'video'):
                    options.update(has_spoiler=getattr(m, 'has_media_spoiler', None),
                                   show_caption_above_media=getattr(m, 'show_caption_above_media', None))
                media.append(cls(_file(job_id, bid, _source(m)), **options))
            results = bot.send_media_group(uid, media)
            if len(results) != len(messages): raise RuntimeError('Неполный альбом')
            for m, result in zip(messages, results): _remember(job_id, bid, m, result)
            return
        m = messages[0]
        if m.content_type == 'text':
            bot.send_message(uid, m.text, entities=m.entities, parse_mode=None,
                             link_preview_options=getattr(m, 'link_preview_options', None))
            return
        source = _source(m)
        if source:
            options = {}
            if m.content_type not in ('sticker', 'video_note'):
                options.update(caption=m.caption, caption_entities=m.caption_entities, parse_mode=None)
            if m.content_type in ('photo','video','animation'):
                options.update(has_spoiler=getattr(m, 'has_media_spoiler', None),
                               show_caption_above_media=getattr(m, 'show_caption_above_media', None))
            sent = getattr(bot, 'send_' + m.content_type)(uid, _file(job_id, bid, source), **options)
            _remember(job_id, bid, m, sent)
            return
        # ID сообщений принадлежат истории конкретного бота
        # Типы без файлов восстанавливаем из сохранённого содержимого
        from forum_support import _resend_by_type
        if _resend_by_type(bot, uid, None, m, api=lambda fn, *a, **k: fn(*a, **k)) is None:
            raise ValueError('Этот тип сообщения недоступен для рассылки')
    # У купленных ботов общий premium guard; главный бот тоже может получить отказ
    try:
        deliver()
    except Exception as exc:
        if not custom_emoji_error(exc): raise
        messages[:] = [plain_emoji(m) for m in messages]
        deliver()


def preview(main_bot, job_id):
    with _lock:
        current = job(job_id)
        if not current or current['status'] != 'draft': return
        prepare_files(main_bot, job_id)
        users = recipients(main_bot, current['scope'])
        c = db._conn()
        try:
            c.execute('BEGIN IMMEDIATE')
            c.execute('DELETE FROM service_recipients WHERE job_id=?', (job_id,))
            c.executemany('INSERT INTO service_recipients(job_id,user_id,bot_id) VALUES(?,?,?)',
                          [(job_id, uid, bid) for uid, bid in users.items()])
            c.commit()
        finally: c.close()
        retry_telegram(lambda: send(main_bot, job_id, 0, current['chat_id']))
        kb = t.InlineKeyboardMarkup()
        kb.row(_btn('Отправить', icon='📤', style='success', callback_data=f'svc_send_{job_id}'),
               _btn('Отмена', icon='❌', callback_data=f'svc_cancel_{job_id}'))
        retry_telegram(lambda: main_bot.send_message(current['chat_id'],
            f'{TE_ANNOUNCE} Получат {len(users)} человек через {len(set(users.values()))} ботов',
            parse_mode='HTML', reply_markup=kb))
        query("UPDATE service_broadcasts SET status='preview' WHERE id=?", (job_id,), write=True)


def schedule_preview(main_bot, job_id):
    with _lock:
        if job_id in _timers: _timers.pop(job_id).cancel()
        def run():
            try: preview(main_bot, job_id)
            except Exception:
                log.exception('Не удалось подготовить рассылку #%s', job_id)
                fail_preview(main_bot, job_id)
            finally:
                with _lock:
                    if _timers.get(job_id) is threading.current_thread(): _timers.pop(job_id, None)
        timer = threading.Timer(1, run)
        timer.daemon = True
        _timers[job_id] = timer
        timer.start()


def fail_preview(main_bot, job_id):
    # Неподходящий файл не должен запускать бесконечные предпросмотры и уведомления
    query("UPDATE service_broadcasts SET status='failed' WHERE id=? AND status='draft'", (job_id,), write=True)
    retry_telegram(lambda: main_bot.send_message(job(job_id)['chat_id'],
        f'{TE_CROSS} Не удалось подготовить рассылку, отправь ещё раз', parse_mode='HTML'))


def confirm(main_bot, job_id, creator, cancel=False, start=True):
    c = db._conn()
    try:
        cur = c.execute("UPDATE service_broadcasts SET status=? WHERE id=? AND creator=? AND status='preview'",
                        ('cancelled' if cancel else 'queued', job_id, creator))
        c.commit()
        changed = bool(cur.rowcount)
    finally: c.close()
    if changed and not cancel and start: resume(main_bot)
    return changed


def pump(main_bot, bid, *, limit=1000):
    """Один рабочий поток на бота; уже доставленные адресаты не выбираются снова"""
    count = 0
    for jid, uid, next_at in query(
            "SELECT r.job_id,r.user_id,r.next_at FROM service_recipients r JOIN service_broadcasts b ON b.id=r.job_id "
            "WHERE r.bot_id=? AND r.status='pending' AND b.status='queued' ORDER BY r.job_id,r.user_id", (bid,)):
        if count >= limit: break
        if next_at > time.time(): continue
        current = job(jid)
        bot = destinations(main_bot, current['scope']).get(bid)
        try:
            if bot is None or db.db_is_blocked(bid, uid):
                raise ValueError('Адресат недоступен')
            send(bot, jid, bid, uid)
            state = 'sent'
        except Exception as exc:
            code = getattr(exc, 'error_code', None)
            if code == 429 or code is None and not isinstance(exc, ValueError) or code and code >= 500:
                delay = max(1, float((getattr(exc, 'result_json', {}) or {}).get('parameters', {}).get('retry_after', 5)))
                query("UPDATE service_recipients SET next_at=MAX(next_at,?) WHERE bot_id=? AND status='pending'",
                      (time.time() + delay, bid), write=True)
                break  # 429 останавливает весь бот до retry_after
            state = 'failed'
        query('UPDATE service_recipients SET status=? WHERE job_id=? AND user_id=?', (state, jid, uid), write=True)
        count += 1
        time.sleep(0.04)
    finish(main_bot)
    return count


def finish(main_bot):
    for jid, chat in query("SELECT id,chat_id FROM service_broadcasts b WHERE status IN ('queued','done') AND notified=0 "
                           "AND NOT EXISTS(SELECT 1 FROM service_recipients r WHERE r.job_id=b.id AND r.status='pending')"):
        with _lock:
            if query('SELECT notified FROM service_broadcasts WHERE id=?', (jid,))[0][0]: continue
            counts = dict(query('SELECT status,COUNT(*) FROM service_recipients WHERE job_id=? GROUP BY status', (jid,)))
            sent, failed = counts.get('sent', 0), counts.get('failed', 0)
            retry_telegram(lambda: main_bot.send_message(chat, f'{TE_CHECK} Рассылка завершена\nОтправлено: {sent}\nНе доставлено: {failed}\nВсего: {sent+failed}', parse_mode='HTML'))
            query("UPDATE service_broadcasts SET status='done',notified=1 WHERE id=?", (jid,), write=True)


def resume(main_bot):
    for jid, in query("SELECT id FROM service_broadcasts WHERE status='draft'"):
        with _lock:
            if jid not in _timers or not _timers[jid].is_alive(): schedule_preview(main_bot, jid)
    for bid, in query("SELECT DISTINCT r.bot_id FROM service_recipients r JOIN service_broadcasts b ON b.id=r.job_id "
                      "WHERE b.status='queued' AND r.status='pending'"):
        with _worker_lock:
            if bid in _workers and _workers[bid].is_alive(): continue
            def run(bid=bid):
                try:
                    while query("SELECT 1 FROM service_recipients r JOIN service_broadcasts b ON b.id=r.job_id "
                                "WHERE b.status='queued' AND r.status='pending' AND r.bot_id=? LIMIT 1", (bid,)):
                        pump(main_bot, bid)
                        time.sleep(1)
                except Exception:
                    log.exception('Рабочий поток рассылки бота #%s остановлен', bid)
                finally:
                    with _worker_lock: _workers.pop(bid, None)
            worker = threading.Thread(target=run, name=f'service-broadcast-{bid}', daemon=True)
            _workers[bid] = worker
            worker.start()
    finish(main_bot)


def start(main_bot):
    """Возобновляет прогресс и поднимает рабочие потоки после временного сбоя"""
    global _supervisor
    with _lock:
        if _supervisor is not None and _supervisor.is_alive(): return
        def supervise():
            while True:
                try: resume(main_bot)
                except Exception: log.exception('Не удалось возобновить рассылки')
                time.sleep(1)
        _supervisor = threading.Thread(target=supervise, name='service-broadcast-supervisor', daemon=True)
        _supervisor.start()
