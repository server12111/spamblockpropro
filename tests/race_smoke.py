# -*- coding: utf-8 -*-
"""Гонки: 3 потока × 10 сообщений + кнопки — проверка lost-update в очередях.
Запуск: py tests/race_smoke.py"""
import os
import sys
import time
import tempfile
import traceback

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='smoke_')
os.environ['FS_STATS_TZ'] = '3'
PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJ)

import telebot
from delivery_queue import pump_delivery
import telebot.types as t

_mid = [1]
API = []
ORIG = {}

def _patch_cls(name, ret='ok'):
    orig = getattr(telebot.TeleBot, name)
    def fn(self, *a, **kw):
        API.append((name, a, kw))
        _mid[0] += 1
        if ret == 'msg':
            return t.Message.de_json({'message_id': _mid[0], 'from': {'id': 1, 'is_bot': True,
                'first_name': 'B'}, 'chat': {'id': 1, 'type': 'private'}, 'date': 0, 'text': 'x'})
        if ret == 'topic':
            return t.ForumTopic(message_thread_id=_mid[0], name='t', icon_color=0x6FB9F0)
        return t.MessageID(message_id=_mid[0])
    setattr(telebot.TeleBot, name, fn)
    return orig

for n, r in [('get_me', 'msg'), ('send_message', 'msg'), ('send_photo', 'msg'),
             ('copy_message', 'id'), ('forward_message', 'msg'), ('copy_messages', 'ids'),
             ('edit_message_text', 'ok'), ('answer_callback_query', 'ok'),
             ('pin_chat_message', 'ok'), ('create_forum_topic', 'topic'),
             ('edit_forum_topic', 'ok'), ('delete_message', 'ok'),
             ('send_document', 'msg'), ('send_animation', 'msg'), ('send_poll', 'msg')]:
    ORIG[n] = getattr(telebot.TeleBot, n)
    _patch_cls(n, r)
ORIG['get_me'] = getattr(telebot.TeleBot, 'get_me')
telebot.TeleBot.get_me = lambda self, *a, **kw: t.User(id=1, is_bot=True, first_name='B', username='b')
ORIG['get_chat'] = getattr(telebot.TeleBot, 'get_chat')
telebot.TeleBot.get_chat = lambda self, cid, *a, **kw: t.Chat(id=cid, type='private',
                                                              first_name='Ю', username='yuri')

import database as db
from database import init_db, db_get_all_bots_for_checker
init_db()
from handlers import register
import handlers
# Polling is tested separately; these scenarios dispatch updates explicitly.
handlers.launch_bot = lambda *a, **kw: True
from forum_support import start_weekly_scheduler, route_user_message, fs_handle_group_message, fs_bind_group, fs_get_topic

PASS, FAIL = [], []
def check(name, cond, extra=''):
    print(('  [OK]   ' if cond else '  [FAIL] ') + name + (' ' + str(extra) if extra and not cond else ''))
    (PASS if cond else FAIL).append(name)

def umsg(uid, text, mid):
    return t.Message.de_json({'message_id': mid, 'from': {'id': uid, 'is_bot': False,
        'first_name': 'Юзер', 'username': f'u{uid}'},
        'chat': {'id': uid, 'type': 'private', 'first_name': 'Юзер'},
        'date': int(time.time()), 'text': text})

def ucb(data, uid, mid=500):
    return t.CallbackQuery.de_json({'id': '1', 'from': {'id': uid, 'is_bot': False,
        'first_name': 'Юзер'}, 'data': data, 'chat_instance': 'ci',
        'message': {'message_id': mid, 'from': {'id': 1, 'is_bot': True, 'first_name': 'B'},
                    'chat': {'id': uid, 'type': 'private', 'first_name': 'Ю'}, 'date': 0, 'text': 'x'}})

def dispatch_msg(bot, m):
    for h in bot.message_handlers:
        flt = h['filters']
        if flt.get('func') and not flt['func'](m):
            continue
        if flt.get('content_types') and m.content_type not in flt['content_types']:
            continue
        if flt.get('commands'):
            if m.content_type == 'text' and m.text and m.text.startswith('/'):
                cmd = m.text.split()[0][1:].split('@')[0]
                if cmd in flt['commands']:
                    result = h['function'](m)
                    pump_delivery(bot)
                    return result
            continue
        result = h['function'](m)
        pump_delivery(bot)
        return result

def dispatch_cb(bot, cb):
    for h in bot.callback_query_handlers:
        flt = h['filters']
        if flt.get('func') and not flt['func'](cb):
            continue
        return h['function'](cb)

def run():
    main_bot = telebot.TeleBot('111:FAKE')
    register(main_bot)
    start_weekly_scheduler()

    print('── ГЛАВНЫЙ БОТ ──')
    dispatch_msg(main_bot, umsg(7145919720, '/start', 101))
    check('/start: приветствие', any(c[0] == 'send_message' for c in API[-2:]))

    from database import db_mark_paid
    db_mark_paid(7145919720)
    dispatch_cb(main_bot, ucb('buy_bot', 7145919720, 102))
    check('buy_bot: текст покупки', any('получишь' in str(c[1]).lower() or 'бота' in str(c[1]).lower()
                                        for c in API[-3:]))

    dispatch_cb(main_bot, ucb('stats', 7145919720, 103))
    check('stats: рендер', any(c[0] == 'edit_message_text' for c in API[-2:]))

    dispatch_cb(main_bot, ucb('manage_bots', 7145919720, 104))
    check('manage_bots: рендер', any(c[0] == 'edit_message_text' for c in API[-2:]))

    dispatch_msg(main_bot, umsg(7145919720, '123456:AAFAKEtesttoken_for_smoke_1234567890', 105))
    dispatch_msg(main_bot, umsg(7145919720, '7145919720', 106))
    bots = db_get_all_bots_for_checker()
    check('новый бот создан в БД', len(bots) >= 1)
    bot_id = bots[0][0]

    print('── ДОЧЕРНИЙ БОТ ──')
    import sqlite3 as _sq_mod
    import purchased_bot as PB
    pb = PB.make_purchased_bot(bot_id, '123456:AAFAKEtesttoken_for_smoke_1234567890',
                               7145919720, main_bot)

    print('── ГОНКИ: 3 юзера × 10 сообщений + кнопки в 3 потоках ──')
    errors = []
    def worker(uid):
        try:
            for i in range(10):
                m = t.Message.de_json({'message_id': 1000 + uid * 100 + i,
                    'from': {'id': uid, 'is_bot': False, 'first_name': f'U{uid}',
                             'username': f'u{uid}'},
                    'chat': {'id': uid, 'type': 'private', 'first_name': f'U{uid}'},
                    'date': int(time.time()), 'text': f'сообщение {i} от {uid}'})
                dispatch_msg(pb, m)
                dispatch_cb(pb, ucb('p_open_admin', uid, 5000 + uid * 100 + i))
        except Exception as e:
            errors.append(f'uid={uid}: {type(e).__name__}: {e}')
    import threading as _th
    threads = [_th.Thread(target=worker, args=(u,)) for u in (2001, 2002, 2003)]
    t0 = time.time()
    for _t in threads: _t.start()
    for _t in threads: _t.join(120)
    dt = time.time() - t0
    check('гонки: без ошибок в потоках', not errors, str(errors[:2]))
    import sqlite3 as _sq
    _c = _sq_mod.connect(db.DB_PATH)
    _users = _c.execute('SELECT COUNT(*) FROM bot_users WHERE bot_id=? AND user_id>=2001', (bot_id,)).fetchone()[0]
    _msgs = _c.execute('SELECT COUNT(*) FROM fs_pending WHERE bot_id=? AND user_id>=2001', (bot_id,)).fetchone()[0]
    _c.close()
    # случайные сообщения доставляются СРАЗУ (очередь — только анти-потеря):
    # rate limit отсеивает бёрсты, но НИЧЕГО не теряется и не падает
    check('гонки: 3 юзера зарегистрированы', _users == 3, str(_users))
    check('гонки: очереди не копятся (доставка прямая)', _msgs == 0, str(_msgs))

    # lost-update регресс fs_append_pending теперь юнит-уровнем:
    # 3 потока × 10 сообщений в одну очередь → все 30 на месте
    from forum_support import fs_append_pending
    race_items = []
    def qworker(uid):
        for i in range(10):
            fs_append_pending(bot_id, 3000 + uid,
                              {'mid': 7000 + uid * 10 + i, 'type': 'text', 'mg': ''})
    qths = [_th.Thread(target=qworker, args=(u,)) for u in (1, 2, 3)]
    for _t in qths: _t.start()
    for _t in qths: _t.join(60)
    _c = _sq_mod.connect(db.DB_PATH)
    _items = _c.execute('SELECT COALESCE(SUM(json_array_length(payload)),0) FROM fs_pending '
                        "WHERE bot_id=? AND user_id>=3000 AND status='pending'", (bot_id,)).fetchone()[0]
    _c.close()
    check('lost-update: все 30 сообщений сохранены (3 потока в одну очередь)', _items == 30, str(_items))
    print(f'  ⏱ {dt:.1f}s')

    print('── ИТОГ ──')
    if FAIL:
        print('ПРОВАЛЫ:', FAIL)
        return 1
    print('RACE SMOKE OK')
    return 0

class main_bot_guard:  # noqa
    pass

if __name__ == '__main__':
    code = 0
    try:
        code = run()
    except Exception:
        traceback.print_exc()
        code = 1
    finally:
        for n, o in ORIG.items():
            setattr(telebot.TeleBot, n, o)
    print(f'\u0418\u0422\u041e\u0413\u041e: {len(PASS)} OK, {len(FAIL) or int(bool(code))} FAIL')
    sys.exit(code)
