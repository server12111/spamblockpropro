"""Ответы администратора: сохранение до API, квитанции частей и отложенные повторы"""
import json
import hashlib
import threading
import time
from telebot.types import Message
import database as db
import delivery_queue as dq
from telegram_text import html_chunks, message_html
from keyboards import TE_INBOX, TE_CHECK, TE_EXCLAM

_guard = threading.RLock()


class PartialReply(Exception):
    pass


def _sender(bot, bot_id):
    with _guard:
        sender = getattr(bot, '_reply_sender', None)
        if sender is None:
            sender = bot._reply_sender = ReplySender(bot, bot_id)
        return sender


def _album_prefix(mode, chat, media_group):
    return 'album:' + hashlib.sha256(json.dumps([mode, chat, media_group]).encode()).hexdigest() + ':'


def album_target(bot_id, chat, media_group):
    """Маршрут следующих частей личного альбома переживает потерю pstate"""
    c = db._conn()
    try:
        row = c.execute('SELECT payload FROM reply_queue WHERE bot_id=? AND source_key LIKE ? '
                        'ORDER BY id DESC LIMIT 1',
                        (bot_id, _album_prefix('album', chat, media_group) + '%')).fetchone()
        return json.loads(row[0])['target'] if row else None
    finally: c.close()


def enqueue_album(bot, bot_id, message, payload, delay=.8):
    """Каждая часть сначала сохраняется; таймер только ускоряет отправку"""
    prefix = _album_prefix(payload['mode'], message.chat.id, message.media_group_id)
    sender = _sender(bot, bot_id)
    with sender.lock:
        c = db._conn()
        try:
            c.execute('BEGIN IMMEDIATE')
            rows = c.execute('SELECT source_key,payload,progress,status,next_at,attempts FROM reply_queue '
                             'WHERE bot_id=? AND source_key LIKE ? ORDER BY id DESC',
                             (bot_id, prefix + '%')).fetchall()
            for key, raw, _, _, _, _ in rows:
                if message.message_id in json.loads(raw)['ids']:
                    return key
            key = prefix + str(message.message_id)
            data = dict(payload, ids=[], authors={})
            retry_at = 0
            if rows:
                old_key, raw, progress, status, next_at, attempts = rows[0]
                # После успешной части API состав пачки неизменен
                if status == 'pending' and not json.loads(progress).get('ops'):
                    key, data = old_key, json.loads(raw)
                    retry_at = next_at if attempts else 0
            data['ids'] = sorted(data['ids'] + [message.message_id])
            data['authors'][str(message.message_id)] = message.from_user.id
            ready = max(retry_at, time.time() + (delay if len(data['ids']) < 10 else 0))
            c.execute('INSERT INTO reply_queue(bot_id,source_key,payload,next_at) VALUES(?,?,?,?) '
                      'ON CONFLICT(bot_id,source_key) DO UPDATE SET payload=excluded.payload,next_at=excluded.next_at',
                      (bot_id, key, json.dumps(data, ensure_ascii=False), ready))
            c.commit()
            return key
        finally: c.close()


def flush_album(bot, bot_id, key):
    sender = _sender(bot, bot_id)
    with sender.lock:
        c = db._conn()
        try:
            # Таймер сбора не обходит retry_after после попытки API
            c.execute("UPDATE reply_queue SET next_at=0 WHERE bot_id=? AND source_key=? "
                      "AND status='pending' AND attempts=0", (bot_id, key))
            c.commit()
        finally: c.close()
    pump(bot, bot_id)


def pending(bot_id):
    c = db._conn()
    try:
        return c.execute("SELECT id FROM reply_queue WHERE bot_id=? AND status='pending'", (bot_id,)).fetchall()
    finally: c.close()


def enqueue(bot, bot_id, key, payload):
    c = db._conn()
    try:
        c.execute('INSERT OR IGNORE INTO reply_queue(bot_id,source_key,payload) VALUES(?,?,?)',
                  (bot_id, key, json.dumps(payload, ensure_ascii=False)))
        c.commit()
        row_id = c.execute('SELECT id FROM reply_queue WHERE bot_id=? AND source_key=?', (bot_id,key)).fetchone()[0]
    finally: c.close()
    pump(bot, bot_id)
    c = db._conn()
    try:
        state, progress = c.execute('SELECT status,progress FROM reply_queue WHERE id=?', (row_id,)).fetchone()
        return state, json.loads(progress).get('result')
    finally: c.close()


class ReplySender(dq.DeliverySender):
    def _save(self):
        c = db._conn()
        try:
            c.execute('UPDATE reply_queue SET progress=? WHERE id=?',
                      (json.dumps(self.progress, ensure_ascii=False), self.job_id))
            c.commit()
        finally: c.close()

    def deliver(self, payload):
        import forum_support as fs
        target = payload['target']
        if db.db_is_blocked(self.bot_id, target):
            raise ValueError('Пользователь заблокирован')
        mode = payload['mode']
        if mode == 'text':
            import html
            result = None
            body = message_html(payload['message']) if payload.get('message') else html.escape(payload['text'])
            for part in html_chunks(f'<b>{TE_INBOX} Сообщение от администратора</b>\n\n' + body,4096):
                result = self.api(self.bot.send_message,target,part,parse_mode='HTML').message_id
            return result
        if mode in ('album', 'forum_album'):
            if mode == 'forum_album':
                group = fs.fs_get_group(self.bot_id)
                if not group or group[1] != payload['chat']:
                    raise ValueError('Группа изменилась')
            ids = payload['ids']
            if payload.get('authors'):
                # Состав разрешённых частей фиксируем перед первым API
                if not self.progress.get('ops'):
                    self.progress['album_ids'] = [mid for mid in ids
                        if fs.fs_is_admin(self.bot_id, payload['authors'][str(mid)])]
                    self._save()
                ids = self.progress['album_ids']
                if not ids: raise ValueError('Доступ отозван')
            result = self.api(self.bot.copy_messages,target,payload['chat'],ids)
            if len(result) != len(ids):
                raise PartialReply()
            if mode == 'forum_album': fs.fs_log_admin_msg(self.bot_id,target)
            return result[0].message_id if result else None
        m = Message.de_json(payload['message'])
        if mode == 'forum':
            return fs._deliver_admin_reply(self.bot,self.bot_id,m,target,payload['thread'],payload.get('reply'))
        if m.content_type == 'photo':
            header=f'<b>{TE_INBOX} Сообщение от администратора</b>'
            body=message_html(m.json, caption=True)
            caption=header+('\n\n'+body if body else '')
            overflow=len(caption.encode('utf-16-le'))//2>1024
            result=self.api(self.bot.send_photo,target,m.photo[-1].file_id,
                            caption=header if overflow else caption,parse_mode='HTML').message_id
            if overflow:
                for part in html_chunks(body,4096):
                    self.api(self.bot.send_message,target,part,parse_mode='HTML')
            return result
        return fs._relay_single(self.bot,target,m.chat.id,m.message_id,src_msg=m)


def pump(bot, bot_id):
    import forum_support as fs
    sender = _sender(bot, bot_id)
    # Один поток доставки на экземпляр, общая блокировка защищает его квитанции
    # Порядок блокировок общий с приёмом и таймером форумного альбома
    with fs._mgmt_lock(bot_id), sender.lock:
        c=db._conn()
        try:
            rows=c.execute("SELECT id,payload,progress,attempts,next_at FROM reply_queue "
                           "WHERE bot_id=? AND status='pending' ORDER BY id",(bot_id,)).fetchall()
        finally: c.close()
        previous=dq.current_sender()
        try:
            dq._context.sender=sender
            for row_id, raw, progress, attempts, next_at in rows:
                if sender.stop_event.is_set(): break
                if next_at>time.time(): continue
                payload=json.loads(raw)
                sender.job_id=row_id
                sender.rows=[dict(attempts=attempts)]
                sender.progress=json.loads(progress)
                sender.operation_counts={}
                delay=0
                try:
                    result=sender.deliver(payload)
                    sender.progress['result']=result
                    status='delivered' if result else 'failed'
                except dq.RetryDelivery as retry:
                    status='pending';delay=retry.delay
                except dq.StopDelivery:
                    break
                except PartialReply:
                    status='partial'
                except (dq.DeletedSource,dq.BrokenDestination,ValueError):
                    status='failed'
                except Exception as exc:
                    code=getattr(exc,'error_code',None)
                    if code in (400,401,403): status='failed'
                    else: status='pending';delay=min(300,2**min(attempts,9))
                c=db._conn()
                try:
                    c.execute('UPDATE reply_queue SET status=?,progress=?,next_at=?,attempts=attempts+1 WHERE id=?',
                              (status,json.dumps(sender.progress),time.time()+delay,row_id))
                    c.commit()
                finally: c.close()
                if payload.get('notify') and status!='pending':
                    try:
                        text = (f'{TE_CHECK} Ответ отправлен' if status=='delivered' else
                                f'{TE_EXCLAM} Не удалось доставить альбом полностью' if status=='partial' else
                                f'{TE_EXCLAM} Не удалось доставить ответ')
                        bot.send_message(payload['notify'],text,parse_mode='HTML',
                                         message_thread_id=payload.get('notice_thread'))
                    except Exception: pass
        finally: dq._context.sender=previous
