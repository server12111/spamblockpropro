# -*- coding: utf-8 -*-
"""Действия закрепа: реальная БД и обработчики, Telegram API подменён.

Запуск: python tests/pin_actions.py
"""
import os
import sys
import tempfile
import traceback
import types

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJ)
os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='pin_actions_')
os.environ['SUPER_ADMIN'] = '777'
# Тест не должен читать .env даже при установленном python-dotenv.
sys.modules['dotenv'] = types.SimpleNamespace(load_dotenv=lambda *a, **kw: None)

import telebot
from telebot import types as t
from telebot.apihelper import ApiTelegramException

API = []
MESSAGES = {}
MID = 1000
FAIL_MARKUP = False
FAIL_EDIT = False
NOT_MODIFIED = False
FAIL_DELIVERY = False


def message(chat_id, text, mid, uid=777, thread=None, **user):
    return t.Message.de_json({
        'message_id': mid, 'date': 0, 'text': text,
        'from': {'id': uid, 'is_bot': uid == 999, 'first_name': 'Админ', **user},
        'chat': {'id': chat_id, 'type': 'supergroup' if chat_id < 0 else 'private'},
        'message_thread_id': thread,
    })


def api_error(code, text):
    return ApiTelegramException('test', None, {'ok': False, 'error_code': code,
                                             'description': text})


def patch_api(name):
    def call(self, *args, **kw):
        global MID
        API.append((name, args, kw))
        if name == 'get_me':
            return t.User(999, True, 'Бот', username='test_bot')
        if name == 'edit_message_reply_markup' and FAIL_MARKUP:
            raise api_error(400, 'Bad Request: message to edit not found')
        if name == 'edit_message_text' and FAIL_EDIT:
            raise api_error(400, 'Bad Request: message to edit not found')
        if name == 'edit_message_text' and NOT_MODIFIED:
            raise api_error(400, 'Bad Request: message is not modified')
        if name == 'copy_message' and FAIL_DELIVERY:
            raise api_error(403, 'Forbidden: bot was blocked by the user')
        if name == 'create_forum_topic':
            MID += 1
            return t.ForumTopic(MID, args[1], 0x6FB9F0)
        if name == 'send_message':
            MID += 1
            sent = message(args[0], args[1], MID, uid=999,
                           thread=kw.get('message_thread_id'))
            MESSAGES[MID] = sent
            return sent
        if name in ('copy_message', 'forward_message'):
            MID += 1
            return t.MessageID(MID)
        return True
    setattr(telebot.TeleBot, name, call)


for name in ('get_me', 'send_message', 'create_forum_topic', 'pin_chat_message',
             'edit_forum_topic', 'copy_message', 'forward_message',
             'edit_message_text', 'edit_message_reply_markup',
             'answer_callback_query', 'delete_message'):
    patch_api(name)

import database as db
import forum_support as fs
from delivery_queue import pump_delivery
import purchased_bot as pb

OK, FAIL = [], []


def check(name, condition):
    (OK if condition else FAIL).append(name)
    print(('  [OK] ' if condition else '  [FAIL] ') + name)


def calls(name):
    return [c for c in API if c[0] == name]


def cb(data, uid=777, gid=-10055, thread=None, mid=0):
    return t.CallbackQuery('callback', t.User(uid, False, 'Админ'), data, 'ci', None,
                           message=message(gid, 'закреп', mid, uid=999, thread=thread))


def dispatch(bot, callback):
    for handler in bot.callback_query_handlers:
        if handler['filters']['func'](callback):
            handler['function'](callback)
            return
    check('callback зарегистрирован: ' + callback.data, False)


def buttons(markup):
    return markup.to_dict()['inline_keyboard']


def run():
    global FAIL_MARKUP, FAIL_EDIT, FAIL_DELIVERY, NOT_MODIFIED
    db.init_db()
    bid = db.db_add_bot(777, '123:TEST', 777)
    other_bid = db.db_add_bot(778, '124:TEST', 778)
    bot = pb.make_purchased_bot(bid, '123:TEST', 777)
    other_bot = pb.make_purchased_bot(other_bid, '124:TEST', 778)
    gid = -10055
    fs.fs_bind_group(bid, gid, 'Группа')
    fs.fs_bind_group(other_bid, -10066, 'Другая группа')
    user = message(800, 'первое', 1, uid=800,
                   first_name='<Имя>', last_name='Фамилия', username='name').from_user
    API.clear()
    fs.route_user_message(bot, bid, message(800, 'первое', 1, uid=800,
                          first_name='<Имя>', last_name='Фамилия', username='name'))
    pump_delivery(bot, bid)
    thread, _, pinned, first_hash = fs.fs_get_topic(bid, 800)
    info = next(c for c in calls('send_message') if c[1][0] == gid)
    expected = (f'<b>{fs.TE_USER} &lt;Имя&gt; Фамилия</b>\n'
                f'{fs.TE_LINK} @name\n{fs.TE_ID} <code>800</code>')
    check('закреп: ровно имя, username и ID, с экранированием', info[1][1] == expected)
    rows = buttons(info[2]['reply_markup'])
    check('две кнопки в один ряд, primary/danger и icon блокировки',
          len(rows) == 1 and len(rows[0]) == 2
          and rows[0][0]['text'] == 'Шаблоны' and rows[0][0]['style'] == 'primary'
          and rows[0][1]['text'] == 'Заблокировать' and rows[0][1]['style'] == 'danger'
          and rows[0][1].get('icon_custom_emoji_id') == fs._ICON['🚫'])
    check('короткие callback с user_id и fs_pin_', all(
        b['callback_data'].startswith('fs_pin_') and '800' in b['callback_data']
        and len(b['callback_data'].encode()) <= 64 for b in rows[0]))
    no_username = t.User(801, False, 'Без имени')
    check('нет username: точная подпись', fs.fs_info_text(bid, no_username) ==
          f'<b>{fs.TE_USER} Без имени</b>\n{fs.TE_LINK} нет username\n{fs.TE_ID} <code>801</code>')

    API.clear()
    fs.route_user_message(bot, bid, message(800, 'второе', 2, uid=800,
                          first_name='<Имя>', last_name='Фамилия', username='name'))
    pump_delivery(bot, bid)
    check('второе сообщение: закреп не отправляется и не правится',
          not any(c[1][0] == gid for c in calls('send_message')) and not calls('edit_message_text')
          and not calls('edit_message_reply_markup') and not calls('pin_chat_message'))
    check('счётчики продолжают расти', db.db_get_user_info(bid, 800)['contact_count'] == 2
          and db.db_fs_user_tickets_total(bid, 800) == 1
          and db.db_fs_count_range('bot_messages', bid, '2000-01-01', '2100-01-01') == 2)

    for action, blocked, label in [('block', True, 'Разблокировать'),
                                   ('unblock', False, 'Заблокировать')]:
        API.clear()
        dispatch(bot, cb(f'fs_pin_{action}_800', thread=thread, mid=pinned))
        check(f'{action}: БД и противоположная кнопка', db.db_is_blocked(bid, 800) == blocked
              and calls('edit_message_reply_markup')
              and buttons(calls('edit_message_reply_markup')[-1][2]['reply_markup'])[0][1]['text'] == label)
        if blocked:
            button = buttons(calls('edit_message_reply_markup')[-1][2]['reply_markup'])[0][1]
            check('Разблокировать: success и icon ✔️', button['style'] == 'success'
                  and button.get('icon_custom_emoji_id') == fs._ICON.get('✔️'))
        notice = 'Пользователь заблокирован' if blocked else 'Пользователь разблокирован'
        check(f'{action}: alert и заметка в нужной теме',
              calls('answer_callback_query')[-1][1][1] == notice
              and calls('answer_callback_query')[-1][2].get('show_alert') is True
              and notice in calls('send_message')[-1][1][1]
              and calls('send_message')[-1][2]['message_thread_id'] == thread)

    fs.fs_claim_topic(bid, 801, 'Другой пользователь')
    fs.fs_set_topic_thread(bid, 801, thread + 1)
    fs.fs_claim_topic(other_bid, 800, 'Чужой бот')
    fs.fs_set_topic_thread(other_bid, 800, thread)
    template = db.db_add_template(bid, 'Текст <b>шаблона</b> & ответ, который длиннее 35 символов')
    for action in ('block_800', 'unblock_800', 'tpl_800', 'close_800', f'send_800_{template}'):
        for scenario, uid, chat, th, bot_obj, suffix in [
            ('не-админ', 900, gid, thread, bot, action),
            ('чужая группа', 777, -10066, thread, bot, action),
            ('личный чат', 777, 777, thread, bot, action),
            ('чужой user_id', 777, gid, thread, bot, action.replace('_800', '_801')),
            ('другой бот', 778, gid, thread, other_bot, action),
            ('нет темы', 777, gid, None, bot, action),
        ]:
            API.clear()
            dispatch(bot_obj, cb('fs_pin_' + suffix, uid=uid, gid=chat, thread=th, mid=pinned))
            answers = calls('answer_callback_query')
            check(f'{action}: отказ ({scenario}), без побочных действий',
                  len(API) == 1 and len(answers) == 1 and answers[0][1][1] == 'Недостаточно прав'
                  and answers[0][2].get('show_alert') is True
                  and not any(db.db_is_blocked(b, u) for b in (bid, other_bid) for u in (800, 801)))
    for malformed in ('fs_pin_block_x', 'fs_pin_block_' + '9' * 80,
                      'fs_pin_send_800_x', 'fs_pin_bad_800'):
        API.clear()
        dispatch(bot, cb(malformed, thread=thread, mid=pinned))
        check('некорректный callback: отказ ' + malformed[:25],
              len(API) == 1 and calls('answer_callback_query')[0][1][1] == 'Недостаточно прав')

    for action, label in [('p_block_800', 'Разблокировать'),
                          ('p_unblock_800', 'Заблокировать'),
                          ('p_block_800', 'Разблокировать'),
                          ('p_unblock_list_800', 'Заблокировать')]:
        API.clear()
        dispatch(bot, cb(action, gid=777, mid=55))
        edits = [c for c in calls('edit_message_reply_markup') if c[1][:2] == (gid, pinned)]
        check('личная панель обновляет закреп: ' + action, bool(edits)
              and buttons(edits[0][2]['reply_markup'])[0][1]['text'] == label)
    FAIL_MARKUP = True
    API.clear()
    dispatch(bot, cb('p_block_800', gid=777, mid=55))
    check('ошибка Telegram при обновлении закрепа не ломает блокировку',
          db.db_is_blocked(bid, 800) and bool(calls('answer_callback_query')))
    FAIL_MARKUP = False
    dispatch(bot, cb('p_unblock_800', gid=777, mid=55))

    API.clear()
    dispatch(bot, cb('fs_pin_tpl_800', thread=thread, mid=pinned))
    listing = calls('send_message')[-1]
    list_id = MID
    kb = buttons(listing[2]['reply_markup'])
    check('список в теме: Выбери шаблон, обрезанная подпись и Закрыть',
          listing[1] == (gid, f'{fs.TE_NOTE} Выбери шаблон')
          and listing[2]['message_thread_id'] == thread
          and kb[0][0]['text'] == db.db_get_template(template, bid)[:35] + '…'
          and kb[-1][0]['text'] == 'Закрыть')
    API.clear()
    dispatch(bot, cb(kb[-1][0]['callback_data'], thread=thread, mid=list_id))
    check('Закрыть удаляет список', calls('delete_message')[0][1] == (gid, list_id)
          and not calls('send_message') and not calls('copy_message'))

    API.clear()
    before = db.db_fs_count_range('fs_admin_msgs', bid, '2000-01-01', '2100-01-01')
    dispatch(bot, cb(kb[0][0]['callback_data'], thread=thread, mid=list_id))
    sent_id = calls('copy_message')[0][1][2]
    mapping = fs.fs_get_mapping(gid, sent_id)
    check('шаблон остаётся в теме, доставлен обычным relay, список удалён',
          calls('send_message')[0][1] == (gid, db.db_get_template(template, bid))
          and calls('send_message')[0][2]['parse_mode'] is None
          and calls('copy_message')[0][1] == (800, gid, sent_id)
          and calls('delete_message')[0][1] == (gid, list_id))
    check('шаблон: соответствие и fs_log_admin_msg', mapping == (bid, 800, thread, MID)
          and db.db_fs_count_range('fs_admin_msgs', bid, '2000-01-01', '2100-01-01') == before + 1)

    FAIL_DELIVERY = True
    API.clear()
    dispatch(bot, cb(kb[0][0]['callback_data'], thread=thread, mid=list_id))
    check('пользователь заблокировал бота: заметка, без учёта и без удаления списка',
          any('пользователь заблокировал бота' in c[1][1] for c in calls('send_message'))
          and not any(c[1] == (gid, list_id) for c in calls('delete_message'))
          and db.db_fs_count_range('fs_admin_msgs', bid, '2000-01-01', '2100-01-01') == before + 1)
    FAIL_DELIVERY = False
    db.db_block_user(bid, 800)
    API.clear()
    dispatch(bot, cb(kb[0][0]['callback_data'], thread=thread, mid=list_id))
    check('заблокированному шаблон не уходит', len(API) == 1
          and calls('answer_callback_query')[0][1][1] == 'Пользователь заблокирован')
    db.db_unblock_user(bid, 800)
    foreign_template = db.db_add_template(other_bid, 'Чужой шаблон')
    API.clear()
    dispatch(bot, cb(f'fs_pin_send_800_{foreign_template}', thread=thread, mid=list_id))
    check('шаблон другого бота не отправляется', len(API) == 1
          and calls('answer_callback_query')[0][1][1] == 'Шаблон не найден')
    db.db_del_template(template, bid)
    API.clear()
    dispatch(bot, cb('fs_pin_tpl_800', thread=thread, mid=pinned))
    check('нет шаблонов: alert с темой Управление', len(API) == 1
          and calls('answer_callback_query')[0][1][1] ==
          'Шаблонов пока нет. Добавь их в теме «Управление»'
          and calls('answer_callback_query')[0][2].get('show_alert') is True)

    fs.fs_set_topic_pinned(bid, 800, pinned, 'старый хеш')
    API.clear()
    fs.fs_ensure_topic_info(bot, bid, fs.fs_get_group(bid), user)
    check('старый закреп мигрирует через edit с кнопками',
          len(calls('edit_message_text')) == 1 and not calls('send_message')
          and calls('edit_message_text')[0][1] == (expected, gid, pinned)
          and len(buttons(calls('edit_message_text')[0][2]['reply_markup'])[0]) == 2)
    db.db_block_user(bid, 800)
    fs.fs_refresh_pin_markup(bot, bid, 800)
    NOT_MODIFIED = True
    API.clear()
    fs.fs_ensure_topic_info(bot, bid, fs.fs_get_group(bid), user)
    check('состояние блокировки входит в хеш закрепа',
          fs.fs_get_topic(bid, 800)[3] != first_hash and len(calls('edit_message_text')) == 1)
    check('message is not modified после обновления кнопок не создаёт новый закреп',
          not calls('send_message') and not calls('pin_chat_message')
          and fs.fs_get_topic(bid, 800)[2] == pinned)
    NOT_MODIFIED = False
    API.clear()
    fs.fs_ensure_topic_info(bot, bid, fs.fs_get_group(bid), user)
    check('после сверки блокировки повторных edit нет', not API)
    user.first_name = 'Новое имя'
    user.username = 'new_name'
    API.clear()
    fs.fs_ensure_topic_info(bot, bid, fs.fs_get_group(bid), user)
    check('смена имени/username обновляет закреп', len(calls('edit_message_text')) == 1
          and 'Новое имя' in calls('edit_message_text')[0][1][0]
          and '@new_name' in calls('edit_message_text')[0][1][0])
    FAIL_EDIT = True
    fs.fs_set_topic_pinned(bid, 800, pinned, 'старый хеш')
    API.clear()
    fs.fs_ensure_topic_info(bot, bid, fs.fs_get_group(bid), user)
    check('удалённый закреп заменяется новым с кнопками и пином',
          len(calls('send_message')) == 1 and len(calls('pin_chat_message')) == 1
          and fs.fs_get_topic(bid, 800)[2] != pinned
          and buttons(calls('send_message')[0][2]['reply_markup'])[0][1]['text'] == 'Разблокировать')


try:
    run()
except Exception:
    check('runtime сценарий завершён без исключений', False)
    traceback.print_exc()
print(f'ИТОГО: {len(OK)} OK, {len(FAIL)} FAIL')
sys.exit(1 if FAIL else 0)
