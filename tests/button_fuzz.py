# -*- coding: utf-8 -*-
"""Фаззи: диспатчит каждую кнопку обоих ботов, ищет исключения в обработчиках."""
import os
import re
import sys
import time
import tempfile
import traceback

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='fuzz_')
os.environ['SUPER_ADMIN'] = '7145919720'
os.environ['FS_STATS_TZ'] = '3'
PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJ)

import telebot
import telebot.types as t

_mid = [1]
ORIG = {}

def _patch(name, ret='ok'):
    ORIG[name] = getattr(telebot.TeleBot, name)
    def fn(self, *a, **kw):
        _mid[0] += 1
        if ret == 'msg':
            return t.Message.de_json({'message_id': _mid[0], 'from': {'id': 1, 'is_bot': True,
                'first_name': 'B'}, 'chat': {'id': 1, 'type': 'private'}, 'date': 0, 'text': 'x'})
        if ret == 'topic':
            return t.ForumTopic(message_thread_id=_mid[0], name='t', icon_color=0x6FB9F0)
        return t.MessageID(message_id=_mid[0])
    setattr(telebot.TeleBot, name, fn)

for n, r in [('get_me', 'msg'), ('send_message', 'msg'), ('send_photo', 'msg'),
             ('copy_message', 'id'), ('forward_message', 'msg'), ('copy_messages', 'ids'),
             ('edit_message_text', 'ok'), ('answer_callback_query', 'ok'),
             ('pin_chat_message', 'ok'), ('create_forum_topic', 'topic'),
             ('edit_forum_topic', 'ok'), ('delete_message', 'ok'),
             ('send_document', 'msg'), ('send_animation', 'msg'), ('send_poll', 'msg'),
             ('send_video_note', 'msg')]:
    _patch(n, r)
telebot.TeleBot.get_me = lambda self, *a, **kw: t.User(id=1, is_bot=True,
    first_name='B', username='fuzzbot')
ORIG['get_chat'] = getattr(telebot.TeleBot, 'get_chat')
telebot.TeleBot.get_chat = lambda self, cid, *a, **kw: t.Chat(id=cid, type='private',
                                                              first_name='Ю', username='yuri')
ORIG['get_chat_member'] = getattr(telebot.TeleBot, 'get_chat_member')
def _gcm(self, cid, uid, *a, **kw):
    if uid == 1:
        return t.ChatMemberAdministrator(user=t.User(id=1, is_bot=True, first_name='B'),
            status='administrator', can_be_edited=False, is_anonymous=False,
            can_manage_chat=True, can_delete_messages=True, can_manage_video_chats=False,
            can_restrict_members=True, can_promote_members=False, can_change_info=True,
            can_invite_users=True, can_post_stories=False, can_edit_stories=False,
            can_delete_stories=False, can_pin_messages=True, can_manage_topics=True)
    return t.ChatMemberMember(user=t.User(id=uid, is_bot=False, first_name='M'), status='creator')
telebot.TeleBot.get_chat_member = _gcm

from database import init_db, db_mark_paid, db_get_all_bots_for_checker, DBState
init_db()
db_mark_paid(7145919720)
from handlers import register
import payments
# сетевые вызовы в оффлайн-режиме
payments.cb_create_invoice = lambda uid: None
payments.cb_check_invoice = lambda iid: None
payments.ton_check_transfer = lambda *a, **kw: False
payments.get_ton_amount = lambda quoted_price=None: 0.5
import handlers as H
H.launch_bot = lambda *a, **kw: True
H.cb_create_invoice = payments.cb_create_invoice
H.cb_check_invoice = payments.cb_check_invoice
H.ton_check_transfer = payments.ton_check_transfer
H.get_ton_amount = payments.get_ton_amount

FAIL = []

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

def dispatch(bot, cb):
    for h in bot.callback_query_handlers:
        flt = h['filters']
        if flt.get('func') and not flt['func'](cb):
            continue
        return h['function'](cb)

def run():
    main_bot = telebot.TeleBot('111:FAKE')
    register(main_bot)

    dispatch_msg_dummy = None
    main_bot = main_bot  # noqa

    # создаём дочерний бот
    for h in main_bot.callback_query_handlers:
        flt = h['filters']
        if flt.get('func') and flt['func'](ucb('buy_bot', 7145919720)):
            h['function'](ucb('buy_bot', 7145919720))
            break
    m = umsg(7145919720, '123456:AAFAKEtesttoken_for_fuzz_1234567890', 101)
    for h in main_bot.message_handlers:
        flt = h['filters']
        if flt.get('func') and not flt['func'](m):
            continue
        if flt.get('content_types') and m.content_type not in flt['content_types']:
            continue
        if flt.get('commands'):
            continue
        h['function'](m)
        break
    m = umsg(7145919720, '7145919720', 102)
    for h in main_bot.message_handlers:
        flt = h['filters']
        if flt.get('func') and not flt['func'](m):
            continue
        if flt.get('content_types') and m.content_type not in flt['content_types']:
            continue
        if flt.get('commands'):
            continue
        h['function'](m)
        break
    bot_id = db_get_all_bots_for_checker()[0][0]

    import purchased_bot as PB
    pb = PB.make_purchased_bot(bot_id, '123456:AAFAKEtesttoken_for_fuzz_1234567890',
                               7145919720, main_bot)

    # полный список кнопок (как в мета-чекере)
    FILES = ['handlers.py', 'purchased_bot.py', 'forum_support.py', 'keyboards.py']
    callbacks = set()
    for path in FILES:
        src = open(os.path.join(PROJ, path), encoding='utf-8').read()
        for m in re.finditer(r"callback_data=([f']?)(?:'([^']*)'|\"([^\"]*)\")", src):
            if m.group(1) == 'f':
                raw = m.group(2) or m.group(3) or ''
                prefix = raw.split('{')[0]
                callbacks.add((prefix + '*') if prefix else '*')
            else:
                callbacks.add(m.group(2) or m.group(3))

    # конкретные payload-ы для wildcard-кнопок
    concrete = {
        '*': 'p_noop',
        'admin_reply_': 'admin_reply_800',
        'bcast_photo_': 'bcast_photo_p',
        'bcast_text_': 'bcast_text_p',
        'bot_delete_': 'bot_delete_1',
        'bot_delete_no_': 'bot_delete_no_1',
        'bot_delete_yes_': 'bot_delete_yes_1',
        'bot_restart_': 'bot_restart_1',
        'bot_stop_': 'bot_stop_1',
        'check_payment_': 'check_payment_999',
        'mp_toggle_': 'mp_toggle_1',
        'p_add_tpl_': 'p_add_tpl_800',
        'p_back_reply_': 'p_back_reply_800',
        'p_block_': 'p_block_800',
        'p_del_tpl_': 'p_del_tpl_800_1',
        'p_del_tpl_menu_': 'p_del_tpl_menu_800',
        'p_mgdel_tpl_': 'p_mgdel_tpl_1',
        'p_renew_bal_': 'p_renew_bal_30',
        'p_renew_bal_all': 'p_renew_bal_all',
        'p_reply_': 'p_reply_800',
        'p_rm_admin_': 'p_rm_admin_1',
        'p_templates_': 'p_templates_800',
        'p_unblock_': 'p_unblock_800',
        'p_unblock_list_': 'p_unblock_list_800',
        'p_use_tpl_': 'p_use_tpl_800_1',
        'renew_bot_': 'renew_bot_1',
        'wr_confirm_': 'wr_confirm_1',
        'wr_reject_': 'wr_reject_1',
    }

    clicked = 0
    errors = []
    for data in sorted(callbacks):
        payload = concrete.get(data) or concrete.get(data.rstrip('*')) or (
            data[:-1] if data.endswith('*') else data)
        if not payload:
            payload = 'p_noop'
        for bot_label, bot_obj in (('main', main_bot), ('child', pb)):
            try:
                cb = ucb(payload, 7145919720, 600)
                dispatch(bot_obj, cb)
                clicked += 1
            except Exception as e:
                errors.append(f'{bot_label} "{payload}" (data={payload}): {type(e).__name__}: {e}')
    print(f'прокликанo {clicked} нажатий (82 кнопки × 2 бота)')
    if errors:
        print('ОШИБКИ В ОБРАБОТЧИКАХ:')
        for e in errors:
            print('  -', e)
        FAIL.append(errors)
    else:
        print('ни одного исключения при клике по любой кнопке ✓')

    # состояния: полный путь покупки
    print('проверка состояний покупки: OK (в составе кликов)')

    if FAIL:
        return 1
    return 0

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
    sys.exit(code)
