"""Доставка купленных ботов: SQLite, один отправитель, без ожиданий в handlers.

Telegram не предоставляет idempotency key. Квитанция каждого успешного вызова
пишется немедленно: при аварии повториться может только последний вызов между
ответом Telegram и commit. Состав пачки с квитанцией больше не меняется.
"""
import hashlib
import html
import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from telebot.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from database import (db_delivery_rows, db_delivery_enqueue, db_delivery_update,
                      db_delivery_limit, db_delivery_finish, db_delivery_cleanup,
                      db_delivery_fallback,
                      db_get_bot_admins, db_get_bot_setting)
from keyboards import TE_CHECK, TE_CHAT, TE_INBOX, TE_USER, TE_LINK, TE_ID, _ICON
from telegram_text import html_chunks

log = logging.getLogger(__name__)
_context = threading.local()
_senders_lock = threading.Lock()


class RetryDelivery(BaseException):
    def __init__(self, delay, chat=None):
        self.delay, self.chat = delay, chat


class DeletedSource(BaseException):
    pass


class BrokenDestination(BaseException):
    pass


class StopDelivery(BaseException):
    pass


def current_sender():
    return getattr(_context, 'sender', None)


def _json(value):
    if hasattr(value, 'to_dict'):
        return value.to_dict()
    raise TypeError(type(value).__name__)


def _units(text):
    return len(text.encode('utf-16-le')) // 2


def _operation_key(name, args, kwargs):
    return hashlib.sha256(json.dumps([name, args, kwargs], default=_json,
                                    sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _count(n):
    return 'сообщение' if n % 10 == 1 and n % 100 != 11 else (
        'сообщения' if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14 else 'сообщений')


def _reply_kb(uid):
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton('Ответить', callback_data=f'p_reply_{uid}', style='primary',
                               icon_custom_emoji_id=_ICON.get('📤')),
           InlineKeyboardButton('Заблокировать', callback_data=f'p_block_{uid}', style='danger',
                                icon_custom_emoji_id=_ICON.get('🚫')))
    return kb


def get_sender(pbot, bot_id):
    with _senders_lock:
        sender = getattr(pbot, '_delivery_sender', None)
        if sender is None:
            sender = DeliverySender(pbot, bot_id)
            pbot._delivery_sender = sender
    return sender


def pump_delivery(pbot, bot_id=None, *, limit=1000):
    """Явная прокачка для тестов: только готовые записи, без sleep и обхода retry_after."""
    sender = getattr(pbot, '_delivery_sender', None)
    if sender is None and bot_id is not None:
        sender = get_sender(pbot, bot_id)
    return sender.pump(limit) if sender else 0


class DeliverySender:
    def __init__(self, pbot, bot_id):
        self.bot, self.bot_id = pbot, bot_id
        self.stop_event = threading.Event()
        self.wake = threading.Event()
        self.lock = threading.RLock()
        self.admission_lock = threading.Lock()
        self.thread = None
        self.progress, self.rows = {}, []
        self.last_cleanup = 0
        self.payload_phase = False
        self.fallback_recovered = False
        # При аварии подтверждение пользователю могло не завершиться.
        abandoned = [r['id'] for r in db_delivery_rows(bot_id) if r['status'] == 'accepting']
        if abandoned:
            db_delivery_update(abandoned, status='pending')

    def enqueue(self, m, confirm_kb=None):
        raw = dict(m.json)
        raw['from'] = m.from_user.to_dict()
        raw['text'] = m.text
        raw['caption'] = m.caption
        # Альбомы собираются без Timer: единственный отправитель ждёт тишину
        # 0.9 секунды только для media_group; текст никогда специально не ждёт.
        ready = time.time() + (0.9 if m.media_group_id else 0)
        with self.admission_lock:
            added = db_delivery_enqueue(self.bot_id, m, json.dumps(raw, ensure_ascii=False), ready)
            if added and m.media_group_id:
                album = [r['id'] for r in db_delivery_rows(self.bot_id)
                         if r['user_id'] == m.from_user.id and r['media_group_id'] == m.media_group_id]
                db_delivery_update(album, ready_at=ready)
            if added:
                try:
                    self.bot.send_message(m.chat.id,
                        db_get_bot_setting(self.bot_id, 'txt_sent', f'<b>{TE_CHECK}  Сообщение отправлено</b>'),
                        parse_mode='HTML', reply_markup=confirm_kb() if confirm_kb else None)
                except Exception:
                    pass
            if added:
                db_delivery_update([added], status='pending')
        self.wake.set()
        return True

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, name=f'delivery-{self.bot_id}', daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.wake.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join()

    def _loop(self):
        while not self.stop_event.is_set():
            try:
                self.wake.clear()
                self.pump(100)
                # Повторы назначены абсолютным временем в БД. Event будит при
                # новом сообщении/stop; никакой sleep(retry_after) и потока на юзера.
                rows = db_delivery_rows(self.bot_id)
                now = time.time()
                heads = {}
                for row in rows:
                    heads.setdefault(row['user_id'], row)
                delay = min([max(0.05, max(r['next_at'], r['ready_at']) - now)
                             for r in heads.values() if r['status'] in ('pending', 'working')] or [1])
                self.wake.wait(min(delay, 1))
            except Exception:
                log.warning('Отправитель бота #%s повторит операцию после локального сбоя', self.bot_id)
                self.wake.wait(1)

    def _save(self):
        db_delivery_update([r['id'] for r in self.rows],
                           progress=json.dumps(self.progress, ensure_ascii=False))

    def forget_topic(self, gid, title):
        # Удалённая тема уже не является результатом успешной операции создания.
        self.progress.setdefault('ops', {}).pop(_operation_key('create_forum_topic', (gid, title), {}), None)
        self._save()

    def api(self, fn, *args, **kwargs):
        """_api делегирует сюда: повторы выходят сквозь старые fallback try/except."""
        if self.stop_event.is_set():
            raise StopDelivery()
        names = ('send_message', 'copy_message', 'forward_message', 'copy_messages',
                 'forward_messages', 'send_photo', 'send_video', 'send_document',
                 'send_audio', 'send_voice', 'send_video_note', 'send_sticker',
                 'send_animation', 'send_contact', 'send_location', 'send_venue',
                 'send_poll', 'send_media_group', 'create_forum_topic')
        service_names = ('pin_chat_message', 'edit_forum_topic', 'edit_message_text',
                         'reopen_forum_topic', 'edit_message_reply_markup')
        name = next((n for n in names + service_names if getattr(self.bot, n, None) == fn),
                    getattr(fn, '__name__', 'api'))
        chat = args[0] if args and isinstance(args[0], int) else kwargs.get('chat_id')
        if chat is None and name == 'edit_message_text' and len(args) > 1:
            chat = args[1]
        if chat is not None:
            until = db_delivery_limit(self.bot_id, chat)
            if until > time.time():
                raise RetryDelivery(until - time.time(), chat)
        key = _operation_key(name, args, kwargs)
        receipt = self.progress.setdefault('ops', {}).get(key)
        if receipt is not None:
            return ([SimpleNamespace(message_id=i) for i in receipt] if isinstance(receipt, list)
                    else SimpleNamespace(**receipt))
        try:
            result = fn(*args, **kwargs)
        except Exception as exc:
            code = getattr(exc, 'error_code', None)
            low = str(exc).lower()
            if code == 429:
                parameters = (getattr(exc, 'result_json', None) or {}).get('parameters', {})
                delay = max(0, float(parameters.get('retry_after', 1)))
                if chat is not None:
                    db_delivery_limit(self.bot_id, chat, time.time() + delay)
                raise RetryDelivery(delay, chat)
            if code is None or code >= 500 or code in (408, 409):
                raise RetryDelivery(min(300, 2 ** min(self.rows[0]['attempts'], 9)))
            if 'message to copy not found' in low or 'message to forward not found' in low:
                raise DeletedSource()
            if name not in ('pin_chat_message', 'edit_forum_topic') and chat is not None and chat < 0 and (code in (401, 403) or any(
                    s in low for s in ('chat not found', 'not enough rights', 'bot is not a member',
                                      'have no rights', 'bot was kicked'))):
                raise BrokenDestination('Бот не может писать в группу техподдержки')
            raise
        if name in names:
            if isinstance(result, (list, tuple)):
                receipt = [r.message_id for r in result]
            elif hasattr(result, 'message_thread_id') and name == 'create_forum_topic':
                receipt = {'message_thread_id': result.message_thread_id}
            else:
                receipt = {'message_id': result.message_id}
            self.progress['ops'][key] = receipt
            if self.payload_phase:
                self.progress['payload_sent'] = True
                self.progress['sealed'] = True
            self._save()  # первым действием после успешного API — durable receipt
        return result

    def _header(self, m):
        u = m.from_user
        return (f'<b>{TE_INBOX} Сообщение от:</b>\n{TE_USER} Имя: <b>{html.escape(u.full_name)}</b>\n'
                f'{TE_LINK} Username: {html.escape("@" + u.username if u.username else "нет")}\n'
                f'{TE_ID} ID: <code>{u.id}</code>')

    def _body(self, rows):
        if len(rows) == 1:
            return f'{TE_CHAT} ' + html.escape(json.loads(rows[0]['payload']).get('text') or '')
        try:
            offset = int(db_get_bot_setting(self.bot_id, 'hours_tz', '3'))
            tz = timezone(timedelta(hours=offset))
        except (ValueError, OverflowError):
            tz = timezone(timedelta(hours=3))
        parts = [f'{TE_CHAT} <b>{len(rows)} {_count(len(rows))}</b>']
        for row in rows:
            raw = json.loads(row['payload'])
            stamp = datetime.fromtimestamp(raw.get('date') or row['created_at'], tz).strftime('%H:%M')
            parts.append(f'<i>{stamp}</i>\n' + html.escape(raw.get('text') or ''))
        return '\n\n'.join(parts)

    def _batch(self, first, all_rows):
        progress = json.loads(first['progress'])
        if progress.get('sealed'):
            ids = progress['ids']
            return [r for r in all_rows if r['id'] in ids], progress
        user_rows = [r for r in all_rows if r['user_id'] == first['user_id'] and r['id'] >= first['id']]
        rows = [first]
        group = __import__('forum_support').fs_get_group(self.bot_id)
        header = '' if group else self._header(Message.de_json(first['payload'])) + '\n\n'
        for row in user_rows[1:]:
            if row['status'] not in ('pending', 'working'):
                break
            if first['media_group_id']:
                if row['media_group_id'] != first['media_group_id']:
                    break
            elif first['kind'] == 'text' and row['kind'] == 'text' and not row['media_group_id']:
                if _units(header + self._body(rows + [row])) > 4096:
                    break
            else:
                break
            rows.append(row)
        progress['ids'] = [r['id'] for r in rows]
        return rows, progress

    def _seal(self):
        self.progress['sealed'] = True
        self._save()

    def _deliver(self):
        import forum_support as fs
        rows = self.rows
        messages = [Message.de_json(r['payload']) for r in rows]
        m = messages[0]
        uid = m.from_user.id
        fs.fs_touch_user(self.bot, self.bot_id, m.from_user)
        group = fs.fs_get_group(self.bot_id)
        sources = [r['message_id'] for r in rows]
        mappings = []
        if group:
            self.payload_phase = False
            thread = fs.fs_ensure_topic_info(self.bot, self.bot_id, group, m.from_user)
            self.payload_phase = True
            if len(rows) > 1 and not rows[0]['media_group_id']:
                self._seal()
                for attempt in range(2):
                    try:
                        mid = self.api(self.bot.send_message, group[1], self._body(rows),
                                       parse_mode='HTML', message_thread_id=thread).message_id
                        break
                    except Exception as exc:
                        if attempt or not fs._is_topic_err(exc):
                            raise
                        self.payload_phase = False
                        fs._recover_topic(self.bot, self.bot_id, group[1], m.from_user, thread)
                        thread = fs.fs_ensure_topic_info(self.bot, self.bot_id, group, m.from_user)
                        self.payload_phase = True
                mappings.append((group[1], mid, thread, sources))
            elif rows[0]['media_group_id'] and len(rows) > 1:
                self._seal()
                mids, thread = fs._relay_batch_to_topic(self.bot, self.bot_id, group,
                    m.from_user, m.chat.id, sources, msgs=messages, thread=thread)
                mappings.extend((group[1], mid, thread, [src]) for mid, src in zip(mids, sources))
            else:
                self._seal()
                mid, thread = fs._relay_to_topic(self.bot, self.bot_id, group, m.from_user,
                    m.chat.id, m.message_id, src_msg=m, thread=thread)
                if not mid:
                    raise BrokenDestination('Не удалось доставить обращение в группу техподдержки')
                mappings.append((group[1], mid, thread, sources))
        else:
            self.payload_phase = True
            self._seal()
            failures = self.progress.setdefault('unavailable', [])
            for aid in db_get_bot_admins(self.bot_id):
                if aid in failures:
                    continue
                try:
                    mids = self._private(aid, messages)
                    if rows[0]['media_group_id'] and len(rows) > 1:
                        mappings.extend((aid, mid, 0, [src]) for mid, src in zip(mids, sources))
                    else:
                        mappings.extend((aid, mid, 0, sources) for mid in mids)
                except Exception as exc:
                    if getattr(exc, 'error_code', None) not in (400, 401, 403):
                        raise
                    failures.append(aid)
                    self._save()
            if not mappings:
                raise BrokenDestination('Все администраторы недоступны')
        db_delivery_finish(rows, mappings)
        # Следующие функции (рабочее время/автоответы) подключаются сюда:
        # обращение уже доставлено и зафиксировано до любого ответа пользователю.
        after = getattr(self.bot, '_after_delivery', None)
        if after:
            try:
                after(messages, group, mappings)
            except Exception:
                # Служебный ответ никогда не возвращает доставленное обращение
                # в pending и не создаёт повторной доставки администраторам.
                log.warning('Не удалось подготовить автоответ бота #%s', self.bot_id)

    def _private(self, aid, messages):
        import forum_support as fs
        m = messages[0]
        header = self._header(m)
        kb = _reply_kb(m.from_user.id)
        mids = []
        if m.content_type == 'text':
            for chunk in html_chunks(header + '\n\n' + self._body(self.rows), 4096):
                mids.append(self.api(self.bot.send_message, aid, chunk,
                                     parse_mode='HTML', reply_markup=kb).message_id)
        elif len(messages) > 1:
            # Заголовок и кнопки сохраняются и при пересылке альбома целиком.
            self.api(self.bot.send_message, aid, header, parse_mode='HTML', reply_markup=kb)
            mids = fs._batch_message_ids(self.api(self.bot.copy_messages, aid, m.chat.id,
                [ms.message_id for ms in messages]), [ms.message_id for ms in messages])
        elif m.content_type in ('photo', 'video', 'document', 'audio', 'animation'):
            obj = getattr(m, m.content_type)
            file_id = obj[-1].file_id if m.content_type == 'photo' else obj.file_id
            cap = header + ('\n\n' + TE_CHAT + ' ' + html.escape(m.caption) if m.caption else '')
            overflow = _units(cap) > 1024
            mid = self.api(getattr(self.bot, 'send_' + m.content_type), aid, file_id,
                caption=header if overflow else cap, parse_mode='HTML', reply_markup=kb).message_id
            mids.append(mid)
            if overflow:
                for chunk in html_chunks(html.escape(m.caption), 4096):
                    self.api(self.bot.send_message, aid, chunk, parse_mode='HTML')
        else:
            self.api(self.bot.send_message, aid, header, parse_mode='HTML', reply_markup=kb)
            if m.content_type in ('voice', 'video_note', 'sticker'):
                obj = getattr(m, m.content_type)
                mid = self.api(getattr(self.bot, 'send_' + m.content_type), aid, obj.file_id).message_id
            else:
                mid = self.api(self.bot.forward_message, aid, m.chat.id, m.message_id).message_id
            if not mid:
                raise BrokenDestination('Не удалось доставить обращение администратору')
            mids.append(mid)
        return mids

    def pump(self, limit=1000):
        import forum_support as fs
        with self.lock:
            if not self.fallback_recovered:
                # Авария могла случиться между commit страховки и показом окна.
                recovered = set()
                for row in db_delivery_rows(self.bot_id):
                    if row['status'] != 'fallback' or row['user_id'] in recovered:
                        continue
                    recovered.add(row['user_id'])
                    pending = fs.fs_get_pending(self.bot_id, row['user_id'])
                    if not pending or not pending[2]:
                        fs.queue_random_message(self.bot, self.bot_id, Message.de_json(row['payload']))
                self.fallback_recovered = True
            previous = current_sender()
            _context.sender = self
            processed = 0
            try:
                notifications = getattr(self.bot, '_pump_notifications', None)
                if notifications:
                    notifications()
                if time.time() - self.last_cleanup > 3600:
                    db_delivery_cleanup(self.bot_id)
                    self.last_cleanup = time.time()
                for _ in range(limit):
                    if self.stop_event.is_set():
                        break
                    all_rows = db_delivery_rows(self.bot_id)
                    seen = set()
                    first = None
                    for row in all_rows:
                        if row['user_id'] in seen:
                            continue
                        seen.add(row['user_id'])
                        if row['status'] in ('pending', 'working') and max(row['next_at'], row['ready_at']) <= time.time():
                            first = row
                            break
                    if first is None:
                        break
                    self.rows, self.progress = self._batch(first, all_rows)
                    self.payload_phase = False
                    ids = [r['id'] for r in self.rows]
                    db_delivery_update(ids, status='working')
                    try:
                        self._deliver()
                        processed += len(ids)
                        if notifications:
                            notifications()
                    except RetryDelivery as retry:
                        # Если payload ещё не доставляли, разрешаем склейку с
                        # поступившими за retry_after текстами. Квитанции служебных
                        # вызовов (тема/закреп) при этом сохраняются.
                        if not self.progress.get('payload_sent'):
                            self.progress['sealed'] = False
                        db_delivery_update(ids, status='pending',
                            next_at=time.time() + retry.delay, attempts=first['attempts'] + 1,
                            progress=json.dumps(self.progress, ensure_ascii=False))
                    except DeletedSource:
                        db_delivery_update(ids, status='deleted', finished_at=time.time())
                        processed += len(ids)
                    except BrokenDestination as exc:
                        # Сохраняем исходную durable запись. Очередь-страховка
                        # становится единственным способом её отправить, без дубля.
                        db_delivery_fallback(self.rows)
                        _context.sender = None
                        fs._notify_owner_error(self.bot, self.bot_id, str(exc), 'Проверь доступ бота и права отправки')
                        fs.queue_random_message(self.bot, self.bot_id, Message.de_json(self.rows[0]['payload']))
                        processed += len(ids)
                        _context.sender = self
                    except StopDelivery:
                        break
                    except Exception:
                        # Локальный/неизвестный сбой также не уничтожает запись.
                        log.warning('Повтор доставки бота #%s, запись #%s', self.bot_id, first['id'])
                        db_delivery_update(ids, next_at=time.time() + min(300, 2 ** min(first['attempts'], 9)),
                                           attempts=first['attempts'] + 1)
                return processed
            finally:
                _context.sender = previous
