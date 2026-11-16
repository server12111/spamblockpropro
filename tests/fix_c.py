# -*- coding: utf-8 -*-
"""Регрессии форума и очереди: временная БД и Telegram без сети"""
import atexit
import importlib
import logging
import os
from pathlib import Path
import shutil
import sys
import tempfile
import threading
import types
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DATA = tempfile.mkdtemp(prefix='fix_c_', dir=ROOT / 'tests')
os.environ.update(DATA_DIR=DATA, PYTHON_DOTENV_DISABLED='1', SUPER_ADMIN='900',
                  BOT_TOKEN='123456:' + 'A' * 35, CRYPTOBOT_TKN='fake', WEBHOOK_URL='')
sys.modules['dotenv'] = types.SimpleNamespace(load_dotenv=lambda *a, **k: False)
import telebot
from telebot import types as t
from telebot.apihelper import ApiTelegramException
import database as db
import forum_support as fs

logging.disable(logging.CRITICAL)
db.init_db()
OK = FAIL = 0
SEQ = 1000
BOTS = []


def msg(uid=701, mid=1, text='текст', chat=None, **fields):
    data = dict(message_id=mid, date=0,
                **{'from': dict(id=uid, is_bot=False, first_name='Юзер'),
                   'chat': dict(id=chat or uid, type='supergroup' if chat else 'private', title='Группа')})
    if text is not None:
        data['text'] = text
    data.update(fields)
    return t.Message.de_json(data)


def err(text):
    return ApiTelegramException('test', None, dict(ok=False, error_code=400, description=text))


class Bot:
    def __init__(self):
        self.calls = []
        self.fail = {}

    def __getattr__(self, name):
        def call(*args, **kw):
            global SEQ
            self.calls.append((name, args, kw))
            if name in self.fail:
                return self.fail[name](*args, **kw)
            SEQ += 1
            if name == 'get_me':
                return t.User(999, True, 'Бот')
            if name == 'get_chat':
                return types.SimpleNamespace(id=args[0], first_name='Юзер', last_name='', username='',
                                             type='supergroup', is_forum=True)
            if name == 'get_chat_member':
                return types.SimpleNamespace(status='administrator', can_manage_topics=True, can_pin_messages=True)
            if name == 'create_forum_topic':
                return types.SimpleNamespace(message_thread_id=SEQ)
            if name in ('copy_messages', 'forward_messages'):
                return [types.SimpleNamespace(message_id=SEQ + i) for i, _ in enumerate(args[2])]
            return msg(mid=SEQ)
        return call


def fresh(group=True):
    global SEQ
    SEQ += 10
    bid = db.db_add_bot(SEQ, str(SEQ) + ':FAKE', SEQ)
    bot = Bot()
    if group:
        fs.fs_bind_group(bid, -bid, 'Группа')
        fs.fs_claim_topic(bid, 701, 'Юзер')
        fs.fs_set_topic_thread(bid, 701, 77)
    return bid, bot


def item(mid, mg=''):
    return dict(mid=mid, type='text', mg=mg)


def cb(uid=701):
    return types.SimpleNamespace(id='cb', from_user=t.User(uid, False, 'Юзер'), message=msg(uid, 99))


def payload(bid):
    row = fs.fs_get_pending(bid, 701)
    return fs.json_loads(row[0]) if row else []


def check(name, fn):
    global OK, FAIL
    try:
        fn()
    except Exception as exc:
        FAIL += 1
        import traceback
        line = traceback.extract_tb(exc.__traceback__)[-1].lineno
        print(f'[FAIL] {name} ({type(exc).__name__}, строка {line})')
    else:
        OK += 1
        print('[OK] ' + name)


def migration():
    bid, bot = fresh()
    fs.fs_map_message(bid, -bid, 50, 701, 77, 1)
    fs.fs_handle_group_message(bot, bid, msg(chat=-99999, text=None, migrate_to_chat_id=-88888), 999, {})
    assert fs.fs_get_group(bid)[1] == -bid and fs.fs_get_mapping(-bid, 50)
    fs.fs_handle_group_message(bot, bid, msg(chat=-bid, text=None, migrate_to_chat_id=-77777), 999, {})
    assert fs.fs_get_group(bid)[1] == -77777 and fs.fs_get_mapping(-77777, 50)


def concurrent_append(failure=False, reregister=False):
    bid, bot = fresh()
    fs.fs_append_pending(bid, 701, item(1))
    entered, release = threading.Event(), threading.Event()
    def copy(*a, **kw):
        entered.set()
        assert release.wait(5)
        if failure:
            raise err('chat not found')
        return types.SimpleNamespace(message_id=456)
    bot.fail['copy_message'] = copy
    worker = threading.Thread(target=fs.confirm_send, args=(bot, bid, cb()))
    worker.start()
    try:
        assert entered.wait(5)
        if reregister:
            runtime = telebot.TeleBot('123456:' + 'A' * 35, threaded=False)
            with patch.object(fs, 'fs_refresh_management'):
                fs.register_forum_support(runtime, bid, {}, lambda u: False)
            assert fs.fs_get_pending(bid, 701)[1] == 'sending'
            assert fs.fs_claim_pending(bid, 701) is None
        fs.fs_append_pending(bid, 701, item(2))
    finally:
        release.set()
        worker.join(5)
    assert [it['mid'] for it in payload(bid)] == ([1, 2] if failure else [2])


def destination():
    bid, bot = fresh()
    fs.fs_append_pending(bid, 701, item(1))
    bot.fail['copy_message'] = lambda *a, **kw: (_ for _ in ()).throw(err('chat not found'))
    fs.confirm_send(bot, bid, cb())
    assert payload(bid) and not payload(bid)[0].get('done')
    assert not any('✅ Отправлено' in str(a) for n, a, k in bot.calls)


def skipped():
    bid, bot = fresh()
    fs.fs_append_pending(bid, 701, item(1))
    fs.fs_append_pending(bid, 701, item(2))
    bot.fail['copy_messages'] = lambda *a, **k: [types.SimpleNamespace(message_id=222)]
    def copy(to, src, mid, **kw):
        if mid == 1:
            raise err('quiz poll cannot be copied')
        return types.SimpleNamespace(message_id=222)
    bot.fail['copy_message'] = copy
    bot.fail['forward_message'] = lambda to, src, mid, **k: types.SimpleNamespace(message_id=111 if mid == 1 else 222)
    fs.confirm_send(bot, bid, cb())
    assert fs.fs_get_mapping(-bid, 111)[3] == 1
    assert fs.fs_get_mapping(-bid, 222)[3] == 2
    assert not payload(bid)


def album_failure():
    bid, bot = fresh()
    key = (bid, 701, 'album')
    messages = [msg(mid=i, media_group_id='album') for i in (1, 2)]
    fs._album_buffers[key] = dict(ids=[1, 2], msgs=messages, gid=-bid, thread=77, chat_id=701)
    bot.fail['copy_messages'] = lambda *a, **k: (_ for _ in ()).throw(ConnectionError('offline'))
    fs._flush_album(bot, bid, key)
    assert [it['mid'] for it in payload(bid)] == [1, 2]


def restart():
    bid, bot = fresh()
    fs.fs_append_pending(bid, 701, item(1))
    assert fs.fs_claim_pending(bid, 701)
    importlib.reload(fs)  # новый runtime с той же сохранённой БД
    fs.confirm_send(bot, bid, cb())
    assert not fs.fs_get_pending(bid, 701)


def reregister():
    bid, bot = fresh()
    fs.fs_append_pending(bid, 701, item(1))
    fs.fs_claim_pending(bid, 701)
    runtime = telebot.TeleBot('123456:' + 'A' * 35, threaded=False)
    with patch.object(fs, 'fs_refresh_management'):
        fs.register_forum_support(runtime, bid, {}, lambda u: False)
    fs.confirm_send(bot, bid, cb())
    assert not payload(bid)


def connect():
    bid, bot = fresh(False)
    owner = db.db_get_bot_info(bid)[2]
    for gid in (-90001, -90002):
        state = {}
        fs.connect_start(bot, bid, cb(owner), state)
        code = state[owner]['code']
        fs.fs_handle_group_message(bot, bid, msg(owner, text=code, chat=gid), 999, state)
        assert fs.fs_get_group(bid)[1] == gid and owner not in state


def deleted_topic():
    bid, bot = fresh()
    def send(*a, **kw):
        if kw.get('message_thread_id') == 77:
            raise err('message thread not found')
        return msg(mid=333)
    bot.fail['send_message'] = send
    bot.fail['reopen_forum_topic'] = lambda *a, **k: (_ for _ in ()).throw(err('message thread not found'))
    fs.fs_append_pending(bid, 701, item(1))
    fs.confirm_send(bot, bid, cb())
    assert not payload(bid) and fs.fs_get_topic(bid, 701)[0] != 77


def slow_topic():
    bid, bot = fresh(False)
    entered, release = threading.Event(), threading.Event()
    results, errors = [], []
    def create(*a, **kw):
        entered.set()
        assert release.wait(14)
        return types.SimpleNamespace(message_thread_id=333)
    bot.fail['create_forum_topic'] = create
    def run():
        try:
            results.append(fs._get_or_create_topic(bot, bid, -bid, msg().from_user))
        except Exception as exc:
            errors.append(type(exc).__name__)
    first = threading.Thread(target=run)
    second = threading.Thread(target=run)
    first.start()
    assert entered.wait(3)
    second.start()
    threading.Event().wait(10.8)
    release.set()
    first.join(4)
    second.join(4)
    assert not errors and results == [333, 333]
    assert len([1 for n, a, k in bot.calls if n == 'create_forum_topic']) == 1


def duplicate_direct():
    import purchased_bot as pb
    bid, fake = fresh(False)
    owner = db.db_get_bot_info(bid)[2]
    bot = pb.make_purchased_bot(bid, '123456:' + 'A' * 35, owner)
    bot.threaded = False
    BOTS.append(bot)
    def send(chat, *a, **kw):
        if chat == owner:
            raise err('bot was blocked by the user')
        return msg(mid=888)
    bot.send_message = send
    bot.process_new_messages([msg(mid=42)])
    from delivery_queue import pump_delivery
    pump_delivery(bot, bid)
    assert [it['mid'] for it in payload(bid)] == [42]


def recipients():
    bid, bot = fresh(False)
    owner = db.db_get_bot_info(bid)[2]
    db.db_add_bot_admin(bid, 888)
    def forward(aid, *a, **kw):
        if aid == 888:
            raise err('bot was blocked by the user')
        return msg(mid=444)
    bot.fail['forward_message'] = forward
    fs.fs_append_pending(bid, 701, item(1))
    for _ in range(2):
        fs._rate_cache.clear()
        fs.confirm_send(bot, bid, cb())
    assert len([1 for n, a, k in bot.calls if n == 'forward_message' and a[0] == owner]) == 1
    assert payload(bid)


def reply_button():
    import purchased_bot as pb
    bid, fake = fresh(False)
    owner = db.db_get_bot_info(bid)[2]
    bot = pb.make_purchased_bot(bid, '123456:' + 'A' * 35, owner)
    bot.threaded = False
    BOTS.append(bot)
    calls = []
    bot.forward_message = lambda *a, **k: msg(mid=555)
    bot.send_message = lambda *a, **k: (calls.append((a, k)) or msg(mid=666))
    bot.edit_message_reply_markup = lambda *a, **k: calls.append((a, k))
    bot.edit_message_text = lambda *a, **k: msg(mid=777)
    bot.answer_callback_query = lambda *a, **k: True
    fs.fs_append_pending(bid, 701, item(1))
    fs.confirm_send(bot, bid, cb())
    buttons = [b for a, k in calls if a[0] == owner and k.get('reply_markup')
               for row in k['reply_markup'].keyboard for b in row]
    assert any(b.callback_data == 'p_reply_701' for b in buttons)
    callback = t.CallbackQuery('reply', t.User(owner, False, 'Владелец'), data='p_reply_701',
                               chat_instance='ci', json_string={}, message=msg(owner))
    bot.process_new_callback_query([callback])
    bot.process_new_messages([msg(owner, text='Ответ клиенту')])
    assert any(a[0] == 701 and 'Ответ клиенту' in str(a) for a, k in calls)


def restart_progress():
    bid, bot = fresh(False)
    owner = db.db_get_bot_info(bid)[2]
    db.db_add_bot_admin(bid, 888)
    fs.fs_append_pending(bid, 701, item(1))
    fs.fs_append_pending(bid, 701, item(2))
    def interrupt(aid, src, mid, **kw):
        if aid == 888:
            raise SystemExit('имитация остановки процесса')
        return msg(mid=500 + mid)
    bot.fail['forward_message'] = interrupt
    try:
        fs._deliver_pending_payload(bot, bid, fs.fs_claim_pending(bid, 701), 701)
    except SystemExit:
        pass
    saved = payload(bid)
    assert owner in saved[0]['admins'] and saved[0]['forwarded'][str(owner)] == 501
    importlib.reload(fs)
    bot.fail.clear()
    fs.confirm_send(bot, bid, cb())
    assert not payload(bid)
    assert len([1 for n, a, k in bot.calls if n == 'forward_message' and a[0] == owner and a[2] == 1]) == 1


def incomplete_album():
    bid, bot = fresh()
    key = (bid, 701, 'partial')
    fs._album_buffers[key] = dict(ids=[1, 2], msgs=[msg(mid=i, media_group_id='partial') for i in (1, 2)],
                                  gid=-bid, thread=77, chat_id=701)
    bot.fail['copy_messages'] = lambda *a, **kw: [types.SimpleNamespace(message_id=222)]
    fs._flush_album(bot, bid, key)
    assert [it['mid'] for it in payload(bid)] == [1, 2]
    assert fs.fs_get_mapping(-bid, 222) is None


def admin_incomplete_album():
    bid, bot = fresh()
    key = (bid, 701, 'admin_partial')
    fs._admin_album_buffers[key] = dict(ids=[1, 2], gid=-bid, thread=77)
    bot.fail['copy_messages'] = lambda *a, **kw: [types.SimpleNamespace(message_id=222)]
    fs._flush_admin_album(bot, bid, key)
    assert any(n == 'send_message' and 'Не удалось доставить альбом' in str(a) for n, a, k in bot.calls)


def markup_retry():
    bid, bot = fresh(False)
    fs.fs_append_pending(bid, 701, item(1))
    bot.fail['edit_message_reply_markup'] = lambda *a, **kw: (_ for _ in ()).throw(ConnectionError('offline'))
    fs.confirm_send(bot, bid, cb())
    assert payload(bid) and payload(bid)[0]['forwarded']
    bot.fail.clear()
    fs._rate_cache.clear()
    fs.confirm_send(bot, bid, cb())
    assert not payload(bid)
    assert len([1 for n, a, k in bot.calls if n == 'forward_message']) == 1


def cleanup():
    for bot in BOTS:
        bot.worker_pool.close()
    db.db_close_all()
    logging.shutdown()
    shutil.rmtree(DATA)


atexit.register(cleanup)
with patch.object(telebot.TeleBot, 'get_me', return_value=t.User(999, True, 'Бот')):
    check('Миграция только привязанной группы', migration)
    check('Добавление во время успешной отправки', concurrent_append)
    check('Добавление во время неудачной отправки', lambda: concurrent_append(True))
    check('Регистрация не перехватывает живую отправку', lambda: concurrent_append(reregister=True))
    check('Недоступная группа сохраняет очередь', destination)
    check('Пропуск copyMessages не искажает соответствия', skipped)
    check('Сетевой сбой альбома сохраняет очередь', album_failure)
    check('Перезапуск освобождает захваченную очередь', restart)
    check('Повторная регистрация освобождает прерванный захват', reregister)
    check('Код подключает первую и новую группу', connect)
    check('Удалённая тема восстанавливается', deleted_topic)
    check('Долгое создание не порождает две темы', slow_topic)
    check('Сбой прямой доставки не дублирует очередь', duplicate_direct)
    check('Повтор только недоступному администратору', recipients)
    check('Из очереди можно ответить клиенту кнопкой', reply_button)
    check('Прогресс получателей переживает остановку процесса', restart_progress)
    check('Неполный альбом остаётся без неверных соответствий', incomplete_album)
    check('Неполный ответ альбомом не считается успехом', admin_incomplete_album)
    check('Сбой кнопки не повторяет пересылку', markup_retry)
print(f'ИТОГО: {OK} OK, {FAIL} FAIL')
sys.exit(bool(FAIL))
