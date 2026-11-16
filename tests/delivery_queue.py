"""Устойчивая доставка: настоящие handlers/SQLite, только Telegram и часы подменены."""
import os
import sys
import tempfile
import threading
import time
import json
import traceback
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
(ROOT / '.test_tmp').mkdir(exist_ok=True)
os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='delivery_', dir=ROOT / '.test_tmp')
sys.modules['dotenv'] = SimpleNamespace(load_dotenv=lambda *a, **k: None)

import telebot
from telebot import types as t
from telebot.apihelper import ApiTelegramException
import database as db
import forum_support as fs
import purchased_bot as pb
import delivery_queue as dq

API, SENT, BOTS = [], [], []
MID = 10000
FAULT = None
NOW = [time.time()]
CLOCK = SimpleNamespace(time=lambda: NOW[0])
OK, FAIL = [], []


def check(name, condition):
    (OK if condition else FAIL).append(name)
    print(('[OK] ' if condition else '[FAIL] ') + name)


def error(code, description, retry=0):
    return ApiTelegramException('test', None, dict(ok=False, error_code=code,
        description=description, parameters={'retry_after': retry}))


def message(uid, mid, text='текст', **extra):
    raw = dict(message_id=mid, date=int(NOW[0]),
        chat=dict(id=uid, type='private' if uid > 0 else 'supergroup'),
        **{'from': dict(id=uid if uid > 0 else 777, is_bot=False, first_name='Имя', username='user')})
    if text is not None:
        raw['text'] = text
    raw.update(extra)
    return t.Message.de_json(raw)


def photo(uid, mid, album=None):
    return message(uid, mid, None, media_group_id=album, caption='подпись', photo=[
        dict(file_id='photo', file_unique_id='u', width=1, height=1)])


def callback(uid, data):
    return t.CallbackQuery('cb', t.User(uid, False, 'Имя'), data=data,
                           chat_instance='ci', json_string={}, message=message(uid, 999))


def dispatch(bot, m):
    for h in bot.message_handlers:
        if bot._test_message_handler(h, m):
            return h['function'](m)


def click(bot, uid, data):
    cb = callback(uid, data)
    for h in bot.callback_query_handlers:
        if h['filters']['func'](cb):
            return h['function'](cb)


def fake_api(name):
    def call(self, *args, **kw):
        global MID
        API.append((name, args, kw, NOW[0]))
        if FAULT:
            FAULT(name, args, kw)
        if name == 'get_me':
            return t.User(999, True, 'Бот', username='test_bot')
        if name == 'get_chat':
            return t.Chat(args[0], 'private', first_name='Имя')
        if name in ('send_message', 'copy_message', 'forward_message', 'send_photo'):
            if name == 'send_message':
                assert len(args[1].encode('utf-16-le')) // 2 <= 4096
            MID += 1
            SENT.append((name, args, kw, MID))
            return SimpleNamespace(message_id=MID)
        if name in ('copy_messages', 'forward_messages', 'send_media_group'):
            ids = args[2] if name != 'send_media_group' else args[1]
            result = []
            for _ in ids:
                MID += 1
                result.append(t.MessageID(MID))
            SENT.append((name, args, kw, [m.message_id for m in result]))
            return result
        if name == 'create_forum_topic':
            MID += 1
            return t.ForumTopic(MID, args[1], 1)
        return True
    return call


def fresh(group=True, owner=777):
    bid = db.db_add_bot(owner, str(100000 + len(BOTS)) + ':TEST', owner)
    bot = pb.make_purchased_bot(bid, str(100000 + len(BOTS)) + ':TEST', owner)
    BOTS.append(bot)
    if group:
        fs.fs_bind_group(bid, -10000 - bid, 'Поддержка')
    return bid, bot, dq.get_sender(bot, bid)


def drain(bot, bid):
    return dq.pump_delivery(bot, bid)


def run():
    global FAULT
    db.init_db()
    bid, bot, sender = fresh()
    gid = fs.fs_get_group(bid)[1]
    count = [0]
    def limited(name, a, k):
        if a and a[0] == gid and name in ('create_forum_topic', 'copy_message', 'send_message') and count[0] < 4:
            count[0] += 1
            raise error(429, 'retry', 40)
    FAULT = limited
    start = len(SENT)
    with patch.object(fs.time, 'sleep', side_effect=AssertionError('handler slept')):
        for i in range(100):
            dispatch(bot, message(1000 + i % 30, i + 1, f'номер {i}'))
    acks = [s for s in SENT[start:] if s[0] == 'send_message' and s[1][0] > 0]
    check('100 подтверждений до доставки; handler не спал', len(acks) == 100 and
          not any(s[1][0] == gid for s in SENT[start:]))
    drain(bot, bid)
    for _ in range(3):
        NOW[0] += 39.99
        before = count[0]
        drain(bot, bid)
        check('429: раньше retry_after запросов нет', count[0] == before)
        NOW[0] += .01
        drain(bot, bid)
    before = len(API)
    dispatch(bot, message(5000, 1, '/start'))
    click(bot, 5000, 'p_send')
    check('/start и кнопки отвечают во время 429', any(c[0] == 'send_message' for c in API[before:])
          and any(c[0] == 'edit_message_text' for c in API[before:]))
    check('после четырёх 429 нет окна страховки', not fs.fs_get_pending(bid, 1000))
    NOW[0] += 40
    drain(bot, bid)
    rows = db.db_delivery_rows(bid, active=False)
    check('100 сообщений 30 пользователей доставлены', len(rows) == 100 and all(r['status'] == 'delivered' for r in rows))
    bodies = [s for s in SENT[start:] if s[0] == 'send_message' and s[1][0] == gid and '<i>' in s[1][1]]
    ordered = True
    for uid in range(1000, 1030):
        expected = [i for i in range(100) if i % 30 == uid - 1000]
        body = next(s[1][1] for s in bodies if s[2]['message_thread_id'] == fs.fs_get_topic(bid, uid)[0])
        ordered &= [body.index(f'номер {i}') for i in expected] == sorted(body.index(f'номер {i}') for i in expected)
    check('порядок каждого пользователя и учёт каждого обращения', ordered and
          sum(db.db_get_user_info(bid, u)['contact_count'] for u in range(1000, 1030)) == 100)
    FAULT = None

    bid, bot, sender = fresh()
    gid = fs.fs_get_group(bid)[1]
    sender.enqueue(message(6000, 1, '<первый>'))
    def text429(name, a, k):
        if name == 'copy_message' and a[0] == gid:
            raise error(429, 'retry', 100)
    FAULT = text429
    drain(bot, bid)
    for i in range(2, 6):
        sender.enqueue(message(6000, i, f'текст {i}'))
    FAULT = None
    NOW[0] += 100
    start = len(SENT)
    drain(bot, bid)
    merged = [s for s in SENT[start:] if s[0] == 'send_message' and s[1][0] == gid]
    check('пять текстов за 429 склеены, пять времён, HTML экранирован', len(merged) == 1 and
          merged[0][1][1].count('<i>') == 5 and '<b>5 сообщений</b>' in merged[0][1][1]
          and '&lt;первый&gt;' in merged[0][1][1])
    merged_id = merged[0][3]
    mapping = fs.fs_get_mapping(gid, merged_id)
    check('соответствия для всех исходных, основной Reply на последний', mapping[3] == 5 and all(
        any(r[1] == merged_id for r in db.db_get_topic_copies_by_src(bid, 6000, i)) for i in range(1, 6)))
    reply = message(gid, 100, 'Ответ', message_thread_id=mapping[2], reply_to_message=dict(
        message_id=merged_id, date=0, chat=dict(id=gid, type='supergroup'), text='склейка'))
    fs.fs_handle_group_message(bot, bid, reply, 999, {})
    check('Reply на склейку доставлен с цитатой последнего', any(s[0] == 'copy_message' and
        s[1][0] == 6000 and s[2]['reply_parameters'].message_id == 5 for s in SENT))
    start = len(SENT)
    sender.enqueue(message(6001, 1)); drain(bot, bid)
    sender.enqueue(message(6001, 2)); drain(bot, bid)
    check('без нагрузки тексты уходят отдельно', len([s for s in SENT[start:] if s[0] == 'copy_message' and s[1][0] == gid]) == 2)
    for m in (message(6002, 1), message(6002, 2), photo(6002, 3), message(6002, 4), message(6002, 5)):
        sender.enqueue(m)
    start = len(SENT)
    drain(bot, bid)
    deliveries = [s for s in SENT[start:] if s[1][0] == gid and
                  (s[0] == 'copy_message' or (s[0] == 'send_message' and '<i>' in s[1][1]))]
    check('медиа разрывает склейку', [s[0] for s in deliveries] == ['send_message', 'copy_message', 'send_message'])
    for i in range(3):
        sender.enqueue(message(6003, i + 1, 'x' * 1900))
    start = len(SENT)
    drain(bot, bid)
    check('длинные склейки разбиты по 4096', len([s for s in SENT[start:] if s[1][0] == gid and
        (s[0] == 'copy_message' or s[0] == 'send_message' and '<i>' in s[1][1])]) == 2)
    sender.enqueue(photo(6004, 1, 'album')); sender.enqueue(photo(6004, 2, 'album'))
    drain(bot, bid)
    check('альбом не отправляется до завершения сбора', not any(s[0] == 'copy_messages' and s[1][1] == 6004 for s in SENT))
    NOW[0] += 1
    drain(bot, bid)
    check('альбом доставлен целиком с отдельными соответствиями', any(s[0] == 'copy_messages' and s[1][1:] == (6004, [1, 2]) for s in SENT)
          and all(db.db_get_topic_copies_by_src(bid, 6004, i) for i in (1, 2)))

    bid, bot, sender = fresh(group=False)
    db.db_add_bot_admin(bid, 778)
    def fail_second(name, a, k):
        if name == 'send_message' and a[0] == 778:
            raise ConnectionError('offline')
    FAULT = fail_second
    for i in range(3): sender.enqueue(message(6100, i + 1, f'часть {i}'))
    start = len(SENT)
    drain(bot, bid)
    check('квитанция первого админа сохранена до повтора', len([s for s in SENT[start:] if s[1][0] == 777]) == 1)
    # Новый экземпляр имитирует перезапуск: никаких объектов старого sender.
    bot2 = pb.make_purchased_bot(bid, '199999:TEST', 777)
    BOTS.append(bot2)
    FAULT = None
    NOW[0] += 10
    drain(bot2, bid)
    check('перезапуск working: нет дубля первому, второй получил, все отмечены',
        len([s for s in SENT[start:] if s[1][0] == 777]) == 1 and len([s for s in SENT[start:] if s[1][0] == 778]) == 1
        and not db.db_delivery_rows(bid))
    private = next(s for s in SENT[start:] if s[1][0] == 778)
    check('личный режим: заголовок пользователя и кнопки у склейки', 'ID:' in private[1][1] and
          '<b>3 сообщения</b>' in private[1][1] and len(private[2]['reply_markup'].keyboard[0]) == 2)

    bid, bot, sender = fresh()
    gid = fs.fs_get_group(bid)[1]
    for uid in (6110, 6111, 6112): sender.enqueue(message(uid, 1))
    drain_count = dq.pump_delivery(bot, bid, limit=1)
    remaining = db.db_delivery_rows(bid)
    db.db_delivery_update([remaining[0]['id']], status='working')
    db.db_delivery_update([remaining[1]['id']], status='accepting')
    bot2 = pb.make_purchased_bot(bid, '177777:TEST', 777)
    BOTS.append(bot2)
    start = len(SENT)
    drain(bot2, bid)
    check('перезапуск в середине: pending/working/accepting восстановлены без дублей', drain_count == 1 and
          not db.db_delivery_rows(bid) and len([s for s in SENT[start:] if s[0] == 'copy_message']) == 2
          and not any(s[0] == 'copy_message' and s[1][1] == 6110 for s in SENT[start:]))

    bid, bot, sender = fresh(group=False)
    db.db_add_bot_admin(bid, 778)
    sender.enqueue(message(6120, 1))
    def no_admins(name, a, k):
        if name == 'send_message' and a[0] in (777, 778) and 'Сообщение от:' in a[1]:
            raise error(403, 'bot was blocked by the user')
    FAULT = no_admins
    drain(bot, bid)
    check('все админы недоступны: durable запись и страховка сохранены',
          db.db_delivery_rows(bid)[0]['status'] == 'fallback' and fs.fs_get_pending(bid, 6120) is not None)
    FAULT = None
    fs.confirm_send(bot, bid, callback(6120, 'fs_confirm_send'))
    drain(bot, bid)
    check('после восстановления админов страховка доставлена ровно по разу', not db.db_delivery_rows(bid))

    bid, bot, sender = fresh()
    sender.enqueue(message(6130, 1))
    db.db_delivery_fallback(db.db_delivery_rows(bid))
    bot2 = pb.make_purchased_bot(bid, '166666:TEST', 777)
    BOTS.append(bot2)
    start = len(SENT)
    drain(bot2, bid)
    check('перезапуск между фиксацией страховки и окном восстанавливает окно', any(
        s[0] == 'send_message' and s[1][0] == 6130 and 'Отправить сообщение администратору?' in s[1][1]
        for s in SENT[start:]))

    bid, bot, sender = fresh()
    sender.enqueue(message(6140, 1))
    def offline(name, a, k):
        if name == 'copy_message' and a[1] == 6140: raise ConnectionError('offline')
    FAULT = offline
    pauses = []
    for _ in range(12):
        drain(bot, bid)
        row = db.db_delivery_rows(bid)[0]
        pauses.append(round(row['next_at'] - NOW[0]))
        NOW[0] = row['next_at']
    check('сеть: нарастающая пауза ограничена пятью минутами', pauses[:4] == [1, 2, 4, 8]
          and pauses[-3:] == [300, 300, 300])
    FAULT = None
    drain(bot, bid)
    check('доставка восстанавливается после длительного сетевого сбоя', not db.db_delivery_rows(bid))

    bid, bot, sender = fresh()
    sender.enqueue(message(6150, 1))
    def cannot_pin(name, a, k):
        if name == 'pin_chat_message': raise error(400, 'not enough rights to pin a message')
    FAULT = cannot_pin
    drain(bot, bid)
    check('отсутствие права закреплять не считается поломкой доставки', not db.db_delivery_rows(bid)
          and not fs.fs_get_pending(bid, 6150))
    FAULT = None

    bid, bot, sender = fresh()
    gid = fs.fs_get_group(bid)[1]
    sender.enqueue(message(6200, 1)); sender.enqueue(message(6201, 1))
    def temporary(name, a, k):
        if name == 'copy_message' and a[1] == 6200: raise ConnectionError('offline')
    FAULT = temporary
    drain(bot, bid)
    rows = db.db_delivery_rows(bid, False)
    check('временная ошибка одного пользователя не держит других', [r['status'] for r in rows] == ['working', 'delivered']
          or [r['status'] for r in rows] == ['pending', 'delivered'])
    check('временная ошибка не открывает страховку', not fs.fs_get_pending(bid, 6200))
    def deleted(name, a, k):
        if name == 'copy_message' and a[1] == 6200: raise error(400, 'message to copy not found')
    FAULT = deleted
    NOW[0] += 10
    drain(bot, bid)
    check('удалённый оригинал пропущен', db.db_delivery_rows(bid, False)[0]['status'] == 'deleted')
    FAULT = None
    sender.enqueue(message(6202, 1))
    def broken(name, a, k):
        if a and a[0] == gid: raise error(403, 'bot was kicked')
    FAULT = broken
    start = len(SENT)
    drain(bot, bid)
    check('поломка группы: владелец уведомлён, окно новым сообщением, payload сохранён',
        any(s[1][0] == 777 and 'Группа техподдержки' in s[1][1] for s in SENT[start:]) and
        any(s[1][0] == 6202 and 'Отправить сообщение администратору?' in s[1][1] for s in SENT[start:]) and
        any(r['status'] == 'fallback' and json.loads(r['payload'])['text'] for r in db.db_delivery_rows(bid)))
    FAULT = None
    fs.confirm_send(bot, bid, callback(6202, 'fs_confirm_send'))
    drain(bot, bid)
    check('страховка повторно ставит durable payload в отправитель', not db.db_delivery_rows(bid))
    old = db.db_delivery_rows(bid, False)[1]
    db.db_delivery_update([old['id']], finished_at=NOW[0] - 8 * 86400)
    db.db_delivery_cleanup(bid)
    check('очистка доставленного старше семи дней', old['id'] not in [r['id'] for r in db.db_delivery_rows(bid, False)])

    bid, bot, sender = fresh()
    gid = fs.fs_get_group(bid)[1]
    entered, release = threading.Event(), threading.Event()
    def slow(name, a, k):
        if name == 'copy_message' and a[0] == gid:
            entered.set()
            assert release.wait(5)
            raise error(429, 'retry', 100000)
    FAULT = slow
    sender.enqueue(message(6300, 1))
    sender.start()
    try:
        check('фоновый поток начал доставку', entered.wait(3))
        # handler должен завершиться даже когда sender застрял внутри API.
        handler = threading.Thread(target=lambda: dispatch(bot, message(6301, 1)))
        handler.start(); handler.join(.5)
        check('новое сообщение принято при занятом сетевом API', not handler.is_alive())
    finally:
        release.set()
    token = '188888:TEST'
    pb.running_bots[token] = bot
    pb.running_bot_ids[bid] = token
    check('stop_bot прерывает даже огромный retry_after и завершает поток', pb.stop_bot(token) and not sender.thread.is_alive())
    FAULT = None

    bid, bot, sender = fresh(group=False)
    token = '155555:TEST'
    polling_release = threading.Event()
    results = []
    with patch.object(pb, 'make_purchased_bot', return_value=bot) as factory, \
         patch.object(bot, 'delete_webhook', return_value=True), \
         patch.object(bot, 'infinity_polling', side_effect=lambda **kw: polling_release.wait(5)):
        launches = [threading.Thread(target=lambda: results.append(pb.launch_bot(bid, token, 777))) for _ in range(2)]
        try:
            for th in launches: th.start()
            for th in launches: th.join(3)
            check('параллельный launch создаёт одного отправителя', results == [True, True] and
                  factory.call_count == 1 and sender.thread.is_alive())
            check('второй токен не запускает второй поток того же bot_id', not pb.launch_bot(bid, '144444:TEST', 777)
                  and factory.call_count == 1)
        finally:
            polling_release.set()
            pb.stop_bot(token)
    check('после stop_bot отправитель запущенного бота завершён', not sender.thread.is_alive())


if __name__ == '__main__':
    originals = {}
    for name in ('get_me', 'get_chat', 'send_message', 'copy_message', 'copy_messages', 'forward_message',
                 'forward_messages', 'send_media_group', 'send_photo', 'edit_message_text', 'edit_forum_topic',
                 'create_forum_topic', 'pin_chat_message', 'delete_message', 'answer_callback_query'):
        originals[name] = getattr(telebot.TeleBot, name)
        setattr(telebot.TeleBot, name, fake_api(name))
    try:
        with patch.object(dq, 'time', CLOCK), patch.object(db, 'time', CLOCK), patch.object(fs, 'RATE_LIMIT_SEC', 0):
            run()
    except Exception:
        traceback.print_exc()
        check('нет необработанных исключений', False)
    finally:
        for b in BOTS:
            pb._close_bot(b)
        for name, value in originals.items(): setattr(telebot.TeleBot, name, value)
    print(f'ИТОГО: {len(OK)} OK, {len(FAIL)} FAIL')
    sys.exit(bool(FAIL))
