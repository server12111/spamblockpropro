# -*- coding: utf-8 -*-
"""Подключение: реальные обработчики, временная БД, Telegram без сети."""
import importlib
import json
import logging
import os
from pathlib import Path
import shutil
import sys
import tempfile
import types
from urllib.parse import urlparse, parse_qs
from unittest.mock import patch
from contextlib import ExitStack

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
CALLS, HISTORY, BOTS = [], [], []
STATUS, USER_STATUS, FORUM, CHAT_TYPE, BAD_CHAT = 'administrator', 'creator', True, 'supergroup', False
RIGHTS = dict(can_manage_topics=True, can_pin_messages=True, can_delete_messages=True)
SEQ = 100


def message(text=None, uid=OWNER, gid=None, **fields):
    data = dict(message_id=1, date=0,
                **{'from': dict(id=uid, is_bot=False, first_name='Владелец'),
                   'chat': dict(id=gid or uid, type='supergroup' if gid else 'private', title='Поддержка')})
    if text is not None: data['text'] = text
    data.update(fields)
    return t.Message.de_json(data)


def callback(data='fs_connect', uid=OWNER):
    return t.CallbackQuery.de_json(dict(id='cb', chat_instance='ci', data=data,
        **{'from': dict(id=uid, is_bot=False, first_name='Владелец'),
           'message': dict(message_id=1, date=0, text='Меню', chat=dict(id=uid, type='private'))}))


def api(name):
    def call(self, *args, **kwargs):
        global SEQ
        CALLS.append((name, args, kwargs)); HISTORY.append((name, args, kwargs)); SEQ += 1
        if name == 'get_forum_topic_icon_stickers':
            return [types.SimpleNamespace(emoji='⚙️', custom_emoji_id='test_gear')]
        if name == 'get_me': return t.User(SELF, True, 'Бот', username='support_bot')
        if name == 'get_chat':
            if BAD_CHAT: raise ValueError('chat not found')
            return types.SimpleNamespace(id=GID if isinstance(args[0], str) else args[0],
                title='Поддержка', type=CHAT_TYPE, is_forum=FORUM,
                first_name='Владелец', last_name='', username='')
        if name == 'get_chat_member':
            return types.SimpleNamespace(status=STATUS if args[1] == SELF else USER_STATUS, **RIGHTS)
        if name == 'create_forum_topic': return types.SimpleNamespace(message_thread_id=SEQ)
        return message('ответ')
    return call


def runtime(bid, full=False, state=None):
    if full:
        bot = pb.make_purchased_bot(bid, '123456:FAKE', PRIMARY)
        bot.threaded = False; bot.worker_pool.close()
    else:
        bot = telebot.TeleBot('123456:FAKE', threaded=False)
        fs.register_forum_support(bot, bid, {} if state is None else state,
                                  lambda uid: uid in (OWNER, PRIMARY))
    BOTS.append(bot)
    return bot


def fresh(full=False):
    global STATUS, USER_STATUS, FORUM, CHAT_TYPE, GID, BAD_CHAT
    STATUS, USER_STATUS, FORUM, CHAT_TYPE, BAD_CHAT = 'administrator', 'creator', True, 'supergroup', False
    RIGHTS.update(can_manage_topics=True, can_pin_messages=True, can_delete_messages=True)
    bid = db.db_add_bot(OWNER, f'{SEQ}:FAKE', PRIMARY); GID = -1009001 - bid
    return bid, runtime(bid, full)


def click(bot, data, uid=OWNER):
    CALLS.clear(); bot.process_new_callback_query([callback(data, uid)])


def markups():
    return [json.loads(kw['reply_markup'].to_json()) for _, _, kw in CALLS if kw.get('reply_markup')]


def buttons():
    return [b for kb in markups() for row in kb.get('inline_keyboard', kb.get('keyboard', [])) for b in row]


def issue(bot, uid=OWNER):
    click(bot, 'fs_connect', uid)
    shared = next(b['request_chat'] for b in buttons() if 'request_chat' in b)
    url = next(b['url'] for b in buttons() if b['text'] == 'Добавить в группу')
    return shared, parse_qs(urlparse(url).query)['startgroup'][0]


def send(bot, text=None, uid=OWNER, group=False, **fields):
    CALLS.clear(); bot.process_new_messages([message(text, uid, GID if group else None, **fields)])


def share(bot, request, uid=OWNER):
    send(bot, uid=uid, chat_shared=dict(request_id=request['request_id'], chat_id=GID, title='Поддержка'))


def member(bot, uid=OWNER, old='left', chat_type='supergroup'):
    CALLS.clear()
    def cm(status):
        return dict(status=status, user=dict(id=SELF, is_bot=True, first_name='Бот'),
                    is_anonymous=False, can_be_edited=True, can_manage_chat=True,
                    can_manage_video_chats=False, can_restrict_members=False,
                    can_post_stories=False, can_edit_stories=False, can_delete_stories=False,
                    can_promote_members=False, can_change_info=False, can_invite_users=False, **RIGHTS)
    bot.process_new_updates([t.Update.de_json(dict(update_id=SEQ, my_chat_member=dict(date=0,
        chat=dict(id=GID, type=chat_type, title='Поддержка'),
        **{'from': dict(id=uid, is_bot=False, first_name='Владелец')},
        old_chat_member=cm(old), new_chat_member=cm('administrator'))))])


def replies(chat=OWNER):
    return [a[1] for name, a, kw in CALLS if name == 'send_message' and a[0] == chat]


def connected(bid):
    row = fs.fs_get_group(bid); assert row and row[1] == GID


def retry_button():
    return next(b['callback_data'] for b in buttons() if b['text'] == 'Проверить снова')


def shared_success(full=False, restart=False):
    bid, bot = fresh(full); request, _ = issue(bot)
    if restart:
        db.db_close_all(); bot = runtime(bid, full)
    share(bot, request); connected(bid)
    assert any('подключена' in text for text in replies())
    assert any('подключена' in text for text in replies(GID))
    assert any(kb.get('remove_keyboard') for kb in markups())
    topics = [a[1] for n, a, kw in CALLS if n == 'create_forum_topic']
    assert '📊 Статистика' in topics and 'Управление' in topics


def denied(kind, mode='share'):
    global STATUS, USER_STATUS, FORUM, CHAT_TYPE
    bid, bot = fresh(True); request, token = issue(bot)
    errors = {'forum': 'В группе не включены темы', 'group': 'В группе не включены темы',
              'member': 'Бот не администратор в группе', 'user': 'Ты не администратор этой группы',
              'can_manage_topics': 'управление темами', 'can_pin_messages': 'закрепление сообщений',
              'can_delete_messages': 'удаление сообщений'}
    if kind == 'forum': FORUM = False
    elif kind == 'group': CHAT_TYPE = 'group'
    elif kind == 'member': STATUS = 'member'
    elif kind == 'user': USER_STATUS = 'member'
    else: RIGHTS[kind] = False
    if mode == 'share': share(bot, request)
    elif mode == 'start': send(bot, '/start ' + token, group=True)
    elif mode == 'member': member(bot)
    else:
        click(bot, 'fs_connect_id'); send(bot, str(GID))
    assert not fs.fs_get_group(bid) and any(errors[kind] in text for text in replies())
    if mode == 'share': assert any(kb.get('remove_keyboard') for kb in markups())
    retry = retry_button(); assert not replies(GID)
    STATUS, USER_STATUS, FORUM, CHAT_TYPE = 'administrator', 'administrator', True, 'supergroup'
    RIGHTS.update(can_manage_topics=True, can_pin_messages=True, can_delete_messages=True)
    db.db_close_all(); bot = runtime(bid, True); click(bot, retry); connected(bid)


def start_success(addressed=False, restart=False):
    bid, bot = fresh(True); _, token = issue(bot)
    if restart:
        db.db_close_all(); importlib.reload(fs); bot = runtime(bid, True)
    send(bot, ('/start@support_bot ' if addressed else '/start ') + token, group=True); connected(bid)
    send(bot, '/start ' + token, group=True)
    assert not any(n == 'create_forum_topic' for n, _, _ in CALLS) and not replies(GID)


def rejected_start(kind):
    bid, bot = fresh()
    with patch.object(fs.time, 'time', return_value=1000000): _, token = issue(bot)
    uid = STRANGER if kind == 'stranger' else PRIMARY if kind == 'primary' else OWNER
    text = ('/start@another_bot ' if kind == 'address' else '/start ') + token
    if kind == 'wrong': text += 'bad'
    with patch.object(fs.time, 'time', return_value=1000900 if kind == 'expired' else 1000899):
        send(bot, text, uid, group=True)
    assert not fs.fs_get_group(bid)


def token_lifecycle(kind):
    bid, bot = fresh(); request, old = issue(bot)
    if kind == 'replace':
        _, token = issue(bot); assert token != old
    elif kind == 'cancel': click(bot, 'fs_connect_cancel')
    else:
        share(bot, request); fs.fs_unbind_group(bid)
    send(bot, '/start ' + old, group=True); assert not fs.fs_get_group(bid)


def member_case(kind):
    bid, bot = fresh(True)
    if kind != 'inactive': issue(bot)
    member(bot, STRANGER if kind == 'stranger' else OWNER,
           old='member' if kind == 'promote' else 'left',
           chat_type='private' if kind == 'private' else 'supergroup')
    if kind in ('owner', 'promote'): connected(bid)
    else: assert not fs.fs_get_group(bid)


def id_case(form):
    bid, bot = fresh(True); issue(bot); click(bot, 'fs_connect_id')
    assert any('Добавь бота в группу администратором с правами:' in text for text in replies())
    assert any(b['text'] == 'Отмена' for b in buttons())
    send(bot, form.format(GID, abs(GID))); connected(bid)


def bad_id(lookup=False):
    global BAD_CHAT
    bid, bot = fresh(True); issue(bot); click(bot, 'fs_connect_id'); BAD_CHAT = lookup
    send(bot, '@missing_group' if lookup else 'это не ID')
    assert not fs.fs_get_group(bid) and replies()
    if lookup:
        retry = retry_button(); BAD_CHAT = False; click(bot, retry)
    else:
        assert any('Неверный ID группы' in text for text in replies()); send(bot, str(GID))
    connected(bid)


def cancel(reply=False):
    bid, bot = fresh(True); _, token = issue(bot); click(bot, 'fs_connect_id')
    if reply: send(bot, 'Отмена')
    else: click(bot, 'fs_connect_cancel')
    assert db.DBState(bid).get(OWNER) is None
    assert any(kb.get('remove_keyboard') for kb in markups())
    send(bot, str(GID)); member(bot); send(bot, '/start ' + token, group=True)
    assert not fs.fs_get_group(bid)


def conflict():
    bid, bot = fresh(); fs.fs_bind_group(bid + 100000, GID, 'Другой бот')
    request, _ = issue(bot); share(bot, request)
    assert not fs.fs_get_group(bid)
    assert any('Группа уже подключена к другому боту' in text for text in replies())
    retry = retry_button(); fs.fs_unbind_group(bid + 100000); click(bot, retry); connected(bid)


def request_guard(kind):
    bid, bot = fresh(); request, _ = issue(bot)
    if kind == 'replaced': issue(bot)
    if kind == 'wrong': request = dict(request, request_id=request['request_id'] + 1)
    if kind == 'consumed': share(bot, request); fs.fs_unbind_group(bid)
    share(bot, request, STRANGER if kind == 'stranger' else OWNER)
    assert not fs.fs_get_group(bid)


def unrelated_state(cancelled=False):
    bid, bot = fresh(True); _, token = issue(bot); db.DBState(bid)[OWNER] = 'p_bcast_text'
    if cancelled: click(bot, 'fs_connect_cancel')
    else: send(bot, '/start ' + token, group=True)
    assert db.DBState(bid).get(OWNER) == 'p_bcast_text'


def isolated():
    _, bot = fresh(); _, token = issue(bot); bid, other = fresh(); issue(other)
    send(other, '/start ' + token, group=True); assert not fs.fs_get_group(bid)


def primary():
    bid, bot = fresh(); request, _ = issue(bot, PRIMARY); share(bot, request, PRIMARY); connected(bid)


def expiry_boundary(expired=False):
    bid, bot = fresh()
    with patch.object(fs.time, 'time', return_value=1000000): request, _ = issue(bot)
    with patch.object(fs.time, 'time', return_value=1000900 if expired else 1000899): share(bot, request)
    if expired: assert not fs.fs_get_group(bid)
    else: connected(bid)


def instructions():
    _, bot = fresh(); request, _ = issue(bot)
    assert request['chat_is_channel'] is False and request['chat_is_forum'] is True and request['request_title'] is True
    for key in RIGHTS:
        assert request['bot_administrator_rights'][key] is True
        assert request['user_administrator_rights'][key] is True
    url = next(b['url'] for b in buttons() if b['text'] == 'Добавить в группу')
    assert url.startswith('https://t.me/support_bot?startgroup=')
    assert url.endswith('&admin=manage_topics+pin_messages+delete_messages')
    assert any(b['text'] == 'Выбрать группу' for b in buttons())
    assert any(b['text'] == 'Ввести ID группы' for b in buttons())


def no_legacy_texts():
    texts = [a[1] if n == 'send_message' else a[0] for n, a, _ in HISTORY if n in ('send_message', 'edit_message_text')]
    assert texts and all('/connect' not in text and 'Privacy Mode' not in text and 'отправь код' not in text
                         and 'Перешли в этот чат' not in text for text in texts)


def no_legacy_input():
    bid, bot = fresh(); issue(bot)
    for text in ('/connect ABC123', 'ABC123'):
        send(bot, text, group=True); assert not fs.fs_get_group(bid) and not replies(GID)
    send(bot, 'пересылка', forward_origin=dict(type='chat', date=0,
        sender_chat=dict(id=GID, type='supergroup', title='Поддержка')))
    assert not fs.fs_get_group(bid)


def private_command(text):
    bid, bot = fresh(True); request, _ = issue(bot)
    db.db_add_bot_admin(bid, OWNER)
    send(bot, text)
    assert db.DBState(bid).get(OWNER) is None
    share(bot, request); connected(bid)


def replaced_during_check():
    bid, bot = fresh(); request, _ = issue(bot)
    verify = fs.fs_verify_group
    replacement = []
    def replace(*args, **kwargs):
        replacement.append(issue(bot)[0])
        return verify(*args, **kwargs)
    with patch.object(fs, 'fs_verify_group', side_effect=replace): share(bot, request)
    assert not fs.fs_get_group(bid)
    share(bot, replacement[0]); connected(bid)


def expires_during_topic():
    bid, bot = fresh()
    now = [1000000]
    create = bot.create_forum_topic
    def slow(*args, **kwargs):
        now[0] += 901
        return create(*args, **kwargs)
    with patch.object(fs.time, 'time', side_effect=lambda: now[0]):
        request, _ = issue(bot)
        with patch.object(bot, 'create_forum_topic', side_effect=slow): share(bot, request)
    assert not fs.fs_get_group(bid)


def old_schema():
    bid, bot = fresh()
    c = db._conn()
    try:
        c.execute('CREATE TABLE IF NOT EXISTS fs_connect_codes (bot_id INTEGER PRIMARY KEY, '
                  'user_id INTEGER NOT NULL, code TEXT NOT NULL, expires_at REAL NOT NULL)')
        c.execute('INSERT INTO fs_connect_codes VALUES (?,?,?,?)', (bid, OWNER, 'OLD123', 9999999999))
        c.commit()
    finally: c.close()
    fs.fs_bind_group(bid + 100000, GID - 1, 'Существующая группа')
    db.init_db(); db.init_db()
    assert fs.fs_get_group(bid + 100000)[1] == GID - 1
    send(bot, '/connect OLD123', group=True)
    assert not fs.fs_get_group(bid)
    state = db.DBState(bid)
    state[OWNER] = {'step': 'fs_connect', 'code': 'OLD123'}
    bot = runtime(bid, full=True)
    click(bot, 'fs_connect_cancel')
    assert state.get(OWNER) is None
    request, _ = issue(bot); share(bot, request); connected(bid)


def retry_guard(stranger=False):
    global FORUM
    bid, bot = fresh(); request, _ = issue(bot); FORUM = False; share(bot, request)
    retry = retry_button(); FORUM = True
    if not stranger: issue(bot)
    click(bot, retry, STRANGER if stranger else OWNER)
    assert not fs.fs_get_group(bid)


def api_failure(kind):
    bid, bot = fresh(); request, _ = issue(bot)
    method = 'create_forum_topic' if kind == 'topic' else 'get_chat_member'
    original = getattr(bot, method)
    def fail(*args, **kwargs):
        if kind == 'user' and args[1] == SELF: return original(*args, **kwargs)
        raise ValueError('Telegram unavailable')
    with patch.object(bot, method, side_effect=fail): share(bot, request)
    assert not fs.fs_get_group(bid) and replies()
    click(bot, retry_button()); connected(bid)


def check(name, fn):
    global OK, FAIL
    try: fn()
    except Exception as exc:
        FAIL += 1
        import traceback
        line = traceback.extract_tb(exc.__traceback__)[-1].lineno
        print(f'[FAIL] {name} ({type(exc).__name__}, строка {line})')
    else:
        OK += 1; print('[OK] ' + name)


try:
    with ExitStack() as stack:
        stack.enter_context(patch('telebot.apihelper._make_request', side_effect=AssertionError('Сеть запрещена')))
        for name in ('get_me', 'get_chat', 'get_chat_member', 'send_message', 'edit_message_text',
                     'get_forum_topic_icon_stickers',
                     'edit_message_reply_markup', 'answer_callback_query', 'create_forum_topic',
                     'pin_chat_message', 'edit_forum_topic'):
            stack.enter_context(patch.object(telebot.TeleBot, name, api(name)))
        check('Фильтры выбора, права, три кнопки и ссылка Telegram', instructions)
        check('chat_shared подключает группу и создаёт обе служебные темы', shared_success)
        check('chat_shared имеет приоритет в полном боте', lambda: shared_success(True))
        check('chat_shared работает после перезапуска', lambda: shared_success(True, True))
        for kind in ('forum', 'group', 'member', 'user', *RIGHTS):
            check('Понятный отказ, повтор после исправления: ' + kind, lambda k=kind: denied(k))
        for mode in ('start', 'member', 'id'):
            check('Общая проверка и повтор: ' + mode, lambda m=mode: denied('can_delete_messages', m))
        check('startgroup автоматически подключает группу', start_success)
        check('startgroup с адресом бота', lambda: start_success(True))
        check('startgroup после перезапуска и перезагрузки модуля', lambda: start_success(restart=True))
        for kind in ('stranger', 'primary', 'expired', 'wrong', 'address'):
            check('startgroup отклоняет: ' + kind, lambda k=kind: rejected_start(k))
        for kind in ('replace', 'cancel', 'consumed'):
            check('Одноразовая ссылка: ' + kind, lambda k=kind: token_lifecycle(k))
        for kind in ('owner', 'promote', 'stranger', 'inactive', 'private'):
            check('my_chat_member: ' + kind, lambda k=kind: member_case(k))
        for form in ('{0}', '{1}', '@support_group'):
            check('Ввод ID: ' + form, lambda f=form: id_case(f))
        check('Неверный ID с исправлением', bad_id)
        check('Недоступный ID с повторной проверкой', lambda: bad_id(True))
        check('Отмена кнопкой сбрасывает ввод и ссылку', cancel)
        check('Отмена reply-кнопкой сбрасывает ввод и ссылку', lambda: cancel(True))
        check('Группа другого бота: отказ и повтор', conflict)
        for kind in ('wrong', 'stranger', 'replaced', 'consumed'):
            check('Защита chat_shared: ' + kind, lambda k=kind: request_guard(k))
        check('Подключение сохраняет другой диалог', unrelated_state)
        check('Отмена сохраняет другой диалог', lambda: unrelated_state(True))
        check('Ссылка действует только у выдавшего бота', isolated)
        check('Главный админ подключает свою выбранную группу', primary)
        check('Заявка действует 14:59', expiry_boundary)
        check('Заявка истекает в 15:00', lambda: expiry_boundary(True))
        check('Старый ввод и пересылка не подключают группу', no_legacy_input)
        for command in ('/start', '/admin'):
            check('Личный ' + command + ' сохраняет заявку', lambda c=command: private_command(c))
        check('Проверка не расходует заменённую заявку', replaced_during_check)
        check('Истечение заявки во время Telegram API', expires_during_topic)
        check('Старая таблица не мешает миграции и не используется', old_schema)
        check('Старая кнопка повтора не применяет новую заявку', retry_guard)
        check('Чужая кнопка повтора не подключает группу', lambda: retry_guard(True))
        for kind in ('topic', 'bot', 'user'):
            check('Сбой Telegram API и повтор: ' + kind, lambda k=kind: api_failure(k))
        check('В текстах нет старых команд, кодов и Privacy Mode', no_legacy_texts)
finally:
    db.db_close_all(); logging.shutdown(); shutil.rmtree(DATA)
print(f'ИТОГО: {OK} OK, {FAIL} FAIL')
sys.exit(bool(FAIL))
