# -*- coding: utf-8 -*-
"""Подключение группы: настоящие обработчики, временная БД, Telegram без сети."""
import importlib
import logging
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import types
from unittest.mock import patch

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DATA = tempfile.mkdtemp(prefix='connect_group_', dir=ROOT / 'tests')
os.environ.update(DATA_DIR=DATA, PYTHON_DOTENV_DISABLED='1', SUPER_ADMIN='900',
                  BOT_TOKEN='123456:FAKE', CRYPTOBOT_TKN='fake', WEBHOOK_URL='')
sys.modules['dotenv'] = types.SimpleNamespace(load_dotenv=lambda *a, **k: False)

import telebot
from telebot import types as t
import database as db
import forum_support as fs
import purchased_bot as pb

logging.disable(logging.CRITICAL)
db.init_db()
OK = FAIL = 0
OWNER, PRIMARY, STRANGER, SELF, GID = 701, 702, 703, 999, -1009001
INVALID = 'Код не подошёл. Получи новый в боте: «Группа техподдержки» → «Подключить группу»'
RIGHTS = 'Сделай бота администратором с правом управлять темами и отправь код ещё раз'
TOPICS = 'Включи темы в настройках группы и отправь код ещё раз'
CALLS = []
STATUS, MANAGE, FORUM = 'administrator', True, True
SEQ = 100


def message(text='текст', uid=OWNER, gid=None, **fields):
    data = dict(message_id=1, date=0, text=text,
                **{'from': dict(id=uid, is_bot=False, first_name='Владелец'),
                   'chat': dict(id=gid or uid, type='supergroup' if gid else 'private', title='Поддержка')})
    data.update(fields)
    return t.Message.de_json(data)


def callback(data='fs_connect', uid=OWNER):
    return t.CallbackQuery.de_json(dict(id='cb', chat_instance='ci', data=data,
        **{'from': dict(id=uid, is_bot=False, first_name='Владелец'),
           'message': dict(message_id=1, date=0, text='Меню',
                           chat=dict(id=uid, type='private'))}))


def api(name):
    def call(self, *args, **kwargs):
        global SEQ
        CALLS.append((name, args, kwargs))
        SEQ += 1
        if name == 'get_me':
            return t.User(SELF, True, 'Бот', username='support_bot')
        if name == 'get_chat':
            return types.SimpleNamespace(id=args[0], type='supergroup', is_forum=FORUM,
                                         first_name='Владелец', last_name='', username='')
        if name == 'get_chat_member':
            return types.SimpleNamespace(status=STATUS if args[1] == SELF else 'creator',
                                         can_manage_topics=MANAGE, can_pin_messages=True)
        if name == 'create_forum_topic':
            return types.SimpleNamespace(message_thread_id=SEQ)
        return message()
    return call


def runtime(bid, full=False, state=None):
    if full:
        bot = pb.make_purchased_bot(bid, '123456:FAKE', PRIMARY)
        bot.threaded = False
        bot.worker_pool.close()
    else:
        bot = telebot.TeleBot('123456:FAKE', threaded=False)
        fs.register_forum_support(bot, bid, {} if state is None else state,
                                  lambda uid: uid in (OWNER, PRIMARY))
    return bot


def fresh(full=False, state=None):
    global STATUS, MANAGE, FORUM, GID
    STATUS, MANAGE, FORUM = 'administrator', True, True
    bid = db.db_add_bot(OWNER, f'{SEQ}:FAKE', PRIMARY)
    GID = -1009001 - bid
    return bid, runtime(bid, full, state)


def issue(bot, uid=OWNER):
    CALLS.clear()
    bot.process_new_callback_query([callback(uid=uid)])
    text = next(args[0] for name, args, kw in CALLS if name == 'edit_message_text')
    # Читаем выданный код из сообщения, а не из реализации хранилища.
    return re.search(r'<code>(?:/connect )?([A-Z2-9]{6})</code>', text)[1], text


def send(bot, text, uid=OWNER, **fields):
    CALLS.clear()
    bot.process_new_messages([message(text, uid, GID, **fields)])


def replies(chat):
    return [a[1] for name, a, kw in CALLS if name == 'send_message' and a[0] == chat]


def connected(bid):
    row = fs.fs_get_group(bid)
    assert row and row[1] == GID


def success(form):
    bid, bot = fresh()
    code, _ = issue(bot)
    send(bot, form.format(code))
    connected(bid)
    assert any('подключена' in text for text in replies(GID))
    assert any('подключена' in text for text in replies(OWNER))


def restart(form):
    bid, bot = fresh()
    code, _ = issue(bot)
    db.db_close_all()
    importlib.reload(fs)
    bot = runtime(bid)  # новые обработчики и пустая память, прежняя временная БД
    send(bot, form.format(code))
    connected(bid)


def expires(plain=False):
    bid, bot = fresh()
    with patch.object(fs.time, 'time', return_value=1000000):
        code, _ = issue(bot)
    with patch.object(fs.time, 'time', return_value=1000900):
        send(bot, code if plain else '/connect ' + code)
    assert not fs.fs_get_group(bid)
    assert replies(GID) == ([] if plain else [INVALID])


def unexpired():
    bid, bot = fresh()
    with patch.object(fs.time, 'time', return_value=1000000):
        code, _ = issue(bot)
    with patch.object(fs.time, 'time', return_value=1000899):
        send(bot, code)
    connected(bid)


def other_user(uid=STRANGER, form='/connect {}'):
    bid, bot = fresh()
    db.db_add_bot_admin(bid, STRANGER)
    code, _ = issue(bot)
    send(bot, form.format(code), uid)
    if uid == PRIMARY:
        connected(bid)
    else:
        assert not fs.fs_get_group(bid)
        assert replies(GID) == ([INVALID] if form.startswith('/') else [])
        send(bot, code)
        connected(bid)  # чужой ввод не уничтожил код


def denied(kind):
    global STATUS, MANAGE, FORUM
    bid, bot = fresh()
    code, _ = issue(bot)
    if kind == 'member':
        STATUS = 'member'
    elif kind == 'topics_right':
        MANAGE = False
    else:
        FORUM = False
    send(bot, '/connect ' + code)
    assert not fs.fs_get_group(bid)
    assert replies(GID) == [TOPICS if kind == 'forum' else RIGHTS]
    STATUS, MANAGE, FORUM = 'administrator', True, True
    send(bot, '/connect ' + code)
    connected(bid)


def invalid(text):
    bid, bot = fresh()
    issue(bot)
    send(bot, text)
    assert not fs.fs_get_group(bid)
    assert replies(GID) == ([INVALID] if text.startswith('/connect') else [])


def replacement(other_issuer=False):
    bid, bot = fresh()
    old, _ = issue(bot)
    new, _ = issue(bot, PRIMARY if other_issuer else OWNER)
    assert old != new
    send(bot, '/connect ' + old)
    assert not fs.fs_get_group(bid) and replies(GID) == [INVALID]
    send(bot, '/connect ' + new, PRIMARY if other_issuer else OWNER)
    connected(bid)


def consumed():
    bid, bot = fresh()
    code, _ = issue(bot)
    send(bot, code)
    connected(bid)
    send(bot, '/connect ' + code)
    assert replies(GID) == [INVALID]


def cancelled():
    bid, bot = fresh()
    code, _ = issue(bot)
    bot.process_new_callback_query([callback('fs_connect_cancel')])
    send(bot, '/connect ' + code)
    assert not fs.fs_get_group(bid) and replies(GID) == [INVALID]


def private_message(text):
    bid, bot = fresh(full=True)
    db.db_add_bot_admin(bid, OWNER)
    code, _ = issue(bot)
    bot.process_new_messages([message(text)])
    if text in ('/start', '/admin'):
        assert db.DBState(bid).get(OWNER) is None
    send(bot, code)
    connected(bid)


def forward(restart_bot=False, modern=False):
    bid, bot = fresh()
    issue(bot)
    if restart_bot:
        bot = runtime(bid)
    fields = dict(forward_origin=dict(type='chat', date=0,
        sender_chat=dict(id=GID, type='supergroup', title='Поддержка')))
    if modern:
        # Имитируем версию API без устаревшего свойства forward_from_chat.
        with patch.object(t.Message, 'forward_from_chat', property(lambda m: None)):
            bot.process_new_messages([message(**fields)])
    else:
        bot.process_new_messages([message(**fields)])
    connected(bid)


def restart_full():
    bid, bot = fresh(full=True)
    code, _ = issue(bot)
    db.db_close_all()
    bot = runtime(bid, full=True)
    send(bot, code)
    connected(bid)


def preserve_flow():
    bid, bot = fresh(full=True)
    code, _ = issue(bot)
    db.DBState(bid)[OWNER] = 'p_bcast_text'
    send(bot, code)
    connected(bid)
    assert db.DBState(bid).get(OWNER) == 'p_bcast_text'


def recipient_only():
    bid, bot = fresh()
    code, _ = issue(bot, PRIMARY)
    send(bot, '/connect ' + code, OWNER)
    assert not fs.fs_get_group(bid) and replies(GID) == [INVALID]
    send(bot, code, PRIMARY)
    connected(bid)


def replaced_during_forward():
    bid, bot = fresh()
    issue(bot)
    finish = fs._finish_connect
    issued = []
    def replace(*args, **kwargs):
        issued.append(issue(bot)[0])
        return finish(*args, **kwargs)
    with patch.object(fs, '_finish_connect', side_effect=replace):
        bot.process_new_messages([message(forward_origin=dict(type='chat', date=0,
            sender_chat=dict(id=GID, type='supergroup', title='Поддержка')))])
    assert not fs.fs_get_group(bid)
    send(bot, issued[0])
    connected(bid)


def instructions():
    _, bot = fresh()
    code, text = issue(bot)
    assert f'<code>/connect {code}</code>' in text
    assert '15 минут' in text and 'Privacy Mode' not in text
    assert 'Перешли' in text


def wrong_bot():
    bid, bot = fresh()
    code, _ = issue(bot)
    send(bot, '/connect@another_bot ' + code)
    assert not fs.fs_get_group(bid) and not replies(GID)


def isolated():
    bid, bot = fresh()
    code, _ = issue(bot)
    other_bid, other_bot = fresh()
    send(other_bot, '/connect ' + code)
    assert not fs.fs_get_group(other_bid) and replies(GID) == [INVALID]
    send(bot, code)
    connected(bid)


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


try:
    from contextlib import ExitStack
    with ExitStack() as stack:
        stack.enter_context(patch('telebot.apihelper._make_request', side_effect=AssertionError('Сеть запрещена')))
        for name in ('get_me', 'get_chat', 'get_chat_member', 'send_message', 'edit_message_text',
                     'edit_message_reply_markup', 'answer_callback_query', 'create_forum_topic',
                     'pin_chat_message', 'edit_forum_topic'):
            stack.enter_context(patch.object(telebot.TeleBot, name, api(name)))
        for form in ('{}', '/connect {}', '/connect@support_bot {}'):
            check('Подключение: ' + form, lambda form=form: success(form))
            check('Перезапуск: ' + form, lambda form=form: restart(form))
        check('Код действует до истечения 15 минут', unexpired)
        check('Истёкший код: команда с объяснением', expires)
        check('Истёкший код текстом: молчание', lambda: expires(True))
        check('Чужой админ бота не может применить код', other_user)
        check('Чужой код текстом не подключает группу', lambda: other_user(form='{}'))
        check('Главный админ может применить код владельца', lambda: other_user(PRIMARY))
        for kind in ('member', 'topics_right', 'forum'):
            check('Понятный отказ и повтор: ' + kind, lambda kind=kind: denied(kind))
        for text in ('WRONG1', '/connect WRONG1', '/connect', '/connect A B'):
            check('Неверный ввод: ' + text, lambda text=text: invalid(text))
        check('Новый код заменяет старый', replacement)
        check('Код главного админа заменяет код владельца', lambda: replacement(True))
        check('Использованный код больше не принимается', consumed)
        check('Отмена аннулирует код', cancelled)
        for text in ('обычный текст', '/start', '/admin'):
            check('Сообщение в личке сохраняет код: ' + text, lambda text=text: private_message(text))
        check('Пересылка из группы', forward)
        check('Пересылка после перезапуска', lambda: forward(True))
        check('Пересылка через современный forward_origin', lambda: forward(modern=True))
        check('Перезапуск полного дочернего бота с DBState', restart_full)
        check('Подключение не стирает другой диалог', preserve_flow)
        check('Владелец не применяет код другого получателя', recipient_only)
        check('Пересылка не расходует заменённый код', replaced_during_forward)
        check('Инструкция с копируемой командой и сроком', instructions)
        check('Команда другому боту игнорируется', wrong_bot)
        check('Код действует только у выдавшего бота', isolated)
finally:
    db.db_close_all()
    logging.shutdown()
    shutil.rmtree(DATA)
print(f'ИТОГО: {OK} OK, {FAIL} FAIL')
sys.exit(bool(FAIL))
