# -*- coding: utf-8 -*-
"""Реальные обработчики, временная БД, подменённый Telegram API."""
import os
import sys
import tempfile
import traceback
import types
import time
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='mgmt_topic_')
os.environ['SUPER_ADMIN'] = '777'
sys.modules['dotenv'] = types.SimpleNamespace(load_dotenv=lambda *a, **kw: None)

import telebot
from telebot import types as t
from telebot.apihelper import ApiTelegramException

API, MENUS, DELETED, DELETED_MESSAGES = [], {}, set(), set()
MID = 1000
FAIL_EDIT = FAIL_GROUP_SEND = FAIL_PIN = NOT_MODIFIED = False


def message(gid, text='текст', uid=777, thread=None, mid=1):
    return t.Message.de_json({
        'message_id': mid, 'date': 0, 'text': text,
        'from': {'id': uid, 'is_bot': uid == 999, 'first_name': 'Админ'},
        'chat': {'id': gid, 'type': 'supergroup' if gid < 0 else 'private', 'title': 'Группа'},
        'message_thread_id': thread,
    })


def error(text):
    return ApiTelegramException('test', None, {'ok': False, 'error_code': 400,
                                             'description': text})


def patch(name):
    def call(self, *args, **kw):
        global MID
        API.append((name, args, kw))
        if name == 'get_me':
            return t.User(999, True, 'Бот', username='test_bot')
        if name == 'get_chat':
            return types.SimpleNamespace(id=args[0], type='supergroup', is_forum=True)
        if name == 'get_chat_member':
            return types.SimpleNamespace(status='administrator', can_manage_topics=True,
                                         can_pin_messages=True, can_delete_messages=True)
        if name == 'create_forum_topic':
            time.sleep(0.03)  # второй поток успевает увидеть незавершённую заявку
            MID += 1
            return t.ForumTopic(MID, args[1], 0x6FB9F0)
        if name == 'pin_chat_message' and FAIL_PIN:
            raise error('not enough rights')
        if name == 'edit_message_text':
            key = (args[1], args[2])
            old_thread = MENUS.get(key, ('', None, None))[2]
            if old_thread in DELETED:
                raise error('message thread not found')
            if NOT_MODIFIED:
                raise error('message is not modified')
            if (FAIL_EDIT and args[1] < 0) or args[2] in DELETED_MESSAGES:
                raise error('message to edit not found')
            MENUS[key] = (args[0], kw.get('reply_markup'), old_thread, kw.get('parse_mode'))
        if name == 'send_message':
            if kw.get('message_thread_id') in DELETED:
                raise error('message thread not found')
            if FAIL_GROUP_SEND and args[0] < 0:
                raise error('not enough rights')
            MID += 1
            MENUS[(args[0], MID)] = (args[1], kw.get('reply_markup'),
                                   kw.get('message_thread_id'), kw.get('parse_mode'))
            return message(args[0], args[1], 999, kw.get('message_thread_id'), MID)
        if name in ('copy_message', 'forward_message'):
            MID += 1
            return t.MessageID(MID)
        return True
    setattr(telebot.TeleBot, name, call)


for method in ('get_me', 'get_chat', 'get_chat_member', 'create_forum_topic',
               'send_message', 'pin_chat_message', 'edit_message_text',
               'answer_callback_query', 'copy_message', 'copy_messages',
               'forward_message', 'delete_message', 'edit_message_reply_markup'):
    patch(method)

import database as db
import forum_support as fs
import purchased_bot as pb

OK, FAIL = [], []


def check(name, condition):
    (OK if condition else FAIL).append(name)
    print(('[OK] ' if condition else '[FAIL] ') + name)


def calls(name):
    return [c for c in API if c[0] == name]


def state(bid):
    c = db._conn()
    try:
        return c.execute('SELECT chat_id, thread_mgmt, mgmt_msg_id, mgmt_wait_admin '
                         'FROM fs_groups WHERE bot_id=?', (bid,)).fetchone()
    finally:
        c.close()


def menu(bid):
    row = state(bid)
    return MENUS[(row[0], row[2])]


def buttons(bid):
    return [b for row in menu(bid)[1].to_dict()['inline_keyboard'] for b in row]


def callback(bot, bid, data, uid=777, gid=None, thread=None, mid=None):
    row = state(bid)
    cb = t.CallbackQuery('cb', t.User(uid, False, 'Админ'), data, 'ci', None,
                         message=message(gid if gid is not None else row[0], uid=999,
                                         thread=thread if thread is not None else row[1],
                                         mid=mid if mid is not None else row[2]))
    for handler in bot.callback_query_handlers:
        if handler['filters']['func'](cb):
            handler['function'](cb)
            return
    raise AssertionError('Нет обработчика ' + data)


def click(bot, bid, label, **kw):
    data = next(b['callback_data'] for b in buttons(bid) if b['text'] == label)
    callback(bot, bid, data, **kw)


def dispatch(bot, m):
    for handler in bot.message_handlers:
        if bot._test_message_handler(handler, m):
            handler['function'](m)
            return


def run():
    global FAIL_EDIT, FAIL_GROUP_SEND, FAIL_PIN, NOT_MODIFIED
    # Синтетическая старая схема: миграция не требует рабочей БД.
    c = sqlite3.connect(db.DB_PATH)
    c.execute('CREATE TABLE fs_groups (bot_id INTEGER PRIMARY KEY, '
              'chat_id INTEGER NOT NULL UNIQUE, title TEXT DEFAULT "", '
              'thread_stats INTEGER, connected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)')
    c.execute('INSERT INTO fs_groups (bot_id, chat_id, title) VALUES (999, -10099, "old")')
    c.commit()
    c.close()
    db.init_db()
    db.init_db()
    check('миграция старой схемы сохраняет привязку и повторяется без ошибок',
          state(999) == (-10099, None, None, None))
    fs.fs_unbind_group(999)
    bid = db.db_add_bot(777, '123:TEST', 777)
    other = db.db_add_bot(888, '124:TEST', 888)
    db.db_add_bot_admin(bid, 778)
    bot = pb.make_purchased_bot(bid, '123:TEST', 777)
    fs._finish_connect(bot, bid, 777, -10055, 'Группа', {})
    row = state(bid)
    check('подключение создаёт тему, сохраняет id и закрепляет меню',
          row[1] and row[2] and any(c[1][1] == 'Управление' for c in calls('create_forum_topic'))
          and any(c[1][:2] == (row[0], row[2]) for c in calls('pin_chat_message'))
          and menu(bid)[0] == f'<b>{fs.TE_NOTE} Шаблонов пока нет</b>')
    API.clear()
    fs.fs_refresh_management(bot, bid)
    check('повторное обращение правит то же меню без дублей', state(bid) == row
          and not calls('create_forum_topic') and not calls('send_message'))
    NOT_MODIFIED = True
    fs.fs_refresh_management(bot, bid)
    NOT_MODIFIED = False
    check('message is not modified не создаёт новое меню', state(bid) == row
          and not calls('send_message'))

    fs.fs_bind_group(other, -10066, 'Старая привязка')
    barrier = Barrier(2)
    API.clear()
    def ensure():
        barrier.wait()
        fs.fs_refresh_management(bot, other)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: ensure(), range(2)))
    check('лениво: два потока создают одну тему и одно меню',
          len(calls('create_forum_topic')) == len(calls('send_message')) ==
          len(calls('pin_chat_message')) == 1 and state(other)[1])
    fs.fs_unbind_group(other)
    fs.fs_bind_group(other, -10066, 'Старая привязка')
    API.clear()
    other_bot = pb.make_purchased_bot(other, '124:TEST', 888)
    check('запуск бота поднимает тему для старой привязки', bool(state(other)[1]))
    # Проверяем саму заявку БД без локальной блокировки меню.
    fs.fs_unbind_group(other)
    fs.fs_bind_group(other, -10066, 'Старая привязка')
    API.clear()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: fs._mgmt_ensure_thread(bot, other), range(2)))
    check('заявка БД предотвращает параллельное создание без локального lock',
          len(calls('create_forum_topic')) == 1 and results[0][1] == results[1][1])
    fs.fs_unbind_group(other)
    fs.fs_bind_group(other, -10066, 'Старая привязка')
    c = db._conn()
    c.execute('UPDATE fs_groups SET mgmt_claim=?, mgmt_claim_at=? WHERE bot_id=?',
              ('stale', time.time() - 600, other))
    c.commit()
    c.close()
    fs.fs_refresh_management(bot, other)
    check('залипшая заявка восстанавливается', bool(state(other)[1] and state(other)[2]))
    fs.fs_unbind_group(other)
    fs.fs_bind_group(other, -10066, 'Старая привязка')
    dispatch(other_bot, message(-10066, uid=888))
    check('первое сообщение в старой привязке лениво создаёт тему', bool(state(other)[1]))

    click(bot, bid, 'Добавить шаблон')
    check('приглашение и Отмена',
          menu(bid)[0] == f'<b>{fs.TE_NOTE} Отправь текст шаблона сюда</b>'
          and buttons(bid)[0]['text'] == 'Отмена')
    callback(bot, bid, 'p_mgadd_tpl', gid=777)
    body = 'Ответ <b>буквально</b> & спасибо'
    dispatch(bot, message(-10055, body, thread=row[1]))
    templates = db.db_get_templates(bid)
    check('добавление через dispatcher: БД, меню, подтверждение',
          len(templates) == 1 and templates[0][1] == body and '(1)' in menu(bid)[0]
          and state(bid)[3] is None
          and any(c[1][1] == 'Шаблон сохранён' for c in calls('send_message')))
    dispatch(bot, message(777, 'Из личной панели'))
    check('личное состояние независимо, добавление из лички обновляет меню',
          len(db.db_get_templates(bid)) == 2 and '(2)' in menu(bid)[0])
    tid = templates[0][0]
    click(bot, bid, body)
    check('полный текст без HTML, Удалить danger, Назад',
          menu(bid)[0] == body and menu(bid)[3] is None
          and buttons(bid)[0]['text'] == 'Удалить' and buttons(bid)[0].get('style') == 'danger'
          and buttons(bid)[1]['text'] == 'Назад')
    click(bot, bid, 'Назад')
    click(bot, bid, 'Добавить шаблон')
    click(bot, bid, 'Отмена')
    dispatch(bot, message(-10055, 'Не сохранять', thread=row[1]))
    check('Отмена: следующий текст игнорируется', len(db.db_get_templates(bid)) == 2
          and state(bid)[3] is None)

    before = db.db_get_templates(bid)
    foreign = db.db_add_template(other, 'Чужой шаблон')
    for uid in (778, 900):
        for data in ('fs_mgmt_add', 'fs_mgmt_cancel', 'fs_mgmt_back',
                     f'fs_mgmt_view_{tid}', f'fs_mgmt_delete_{tid}'):
            API.clear()
            callback(bot, bid, data, uid=uid)
            answers = calls('answer_callback_query')
            check(f'отказ {uid}: {data}', len(API) == 1 and bool(answers)
                  and answers[-1][1][1] == 'Доступно только главному админу'
                  and answers[-1][2].get('show_alert') and db.db_get_templates(bid) == before)
    for data in (f'fs_mgmt_view_{foreign}', f'fs_mgmt_delete_{foreign}',
                 'fs_mgmt_view_x', 'fs_mgmt_delete_' + '9' * 80):
        API.clear()
        callback(bot, bid, data)
        check('чужой/поддельный id отклоняется: ' + data[:35],
              len(API) == 1 and calls('answer_callback_query')[-1][2].get('show_alert')
              and db.db_get_templates(bid) == before and db.db_get_template(foreign, other))
    for kw in ({'gid': -10066}, {'gid': 777}, {'thread': row[1] + 100}, {'mid': row[2] + 100}):
        API.clear()
        callback(bot, bid, 'fs_mgmt_add', **kw)
        check('неверная группа/тема/сообщение: ' + str(kw), len(API) == 1
              and calls('answer_callback_query')[-1][2].get('show_alert')
              and state(bid)[3] is None)
    API.clear()
    callback(other_bot, bid, 'fs_mgmt_add', uid=888)
    check('меню другого бота недоступно даже его главному админу',
          len(API) == 1 and calls('answer_callback_query')[-1][2].get('show_alert')
          and state(bid)[3] is None)
    click(bot, bid, 'Добавить шаблон')
    for uid in (778, 900):
        dispatch(bot, message(-10055, 'Нельзя сохранять', uid=uid, thread=row[1]))
    dispatch(bot, message(-10055, 'Другая тема', thread=12345))
    dispatch(bot, message(-10066, 'Другая группа', thread=row[1]))
    check('ввод только от главного админа в нужной теме и группе',
          db.db_get_templates(bid) == before and state(bid)[3] == 777)
    fs.fs_claim_topic(bid, 801, 'Пользователь')
    fs.fs_set_topic_thread(bid, 801, 5678)
    API.clear()
    fs.fs_handle_group_message(bot, bid, message(-10055, 'Обычный ответ', thread=5678), 999, {})
    check('ожидание шаблона не мешает ответу в теме пользователя',
          any(c[1][0] == 801 for c in calls('copy_message'))
          and db.db_get_templates(bid) == before and state(bid)[3] == 777)
    dispatch(bot, message(-10055, '😀' * 2049, thread=row[1]))
    check('длинный текст отклоняется, ожидание остаётся', db.db_get_templates(bid) == before
          and state(bid)[3] == 777)
    dispatch(bot, message(-10055, '😀' * 2048, thread=row[1]))
    check('граничный текст 4096 UTF-16 сохраняется', len(db.db_get_templates(bid)) == 3)

    click(bot, bid, body)
    click(bot, bid, 'Удалить')
    check('удаление и возврат к списку', db.db_get_template(tid, bid) is None
          and '(2)' in menu(bid)[0])
    personal_tid = db.db_get_templates(bid)[0][0]
    callback(bot, bid, f'p_mgdel_tpl_{personal_tid}', gid=777)
    check('удаление из личной панели обновляет меню', '(1)' in menu(bid)[0])
    callback(bot, bid, 'p_add_tpl_800', gid=777)
    dispatch(bot, message(777, 'Шаблон из режима ответа'))
    reply_tid = db.db_get_templates(bid)[-1][0]
    check('добавление из режима ответа обновляет меню', '(2)' in menu(bid)[0])
    callback(bot, bid, f'p_del_tpl_800_{reply_tid}', gid=777)
    check('удаление из режима ответа обновляет меню', '(1)' in menu(bid)[0])
    callback(bot, bid, 'p_cancel', gid=777)
    long_id = db.db_add_template(bid, '😀' * 2200 + '<>&')
    fs.fs_refresh_management(bot, bid)
    callback(bot, bid, f'fs_mgmt_view_{long_id}')
    check('старый длинный текст обрезается с … до лимита UTF-16',
          menu(bid)[0].endswith('…') and len(menu(bid)[0].encode('utf-16-le')) // 2 <= 4096)
    click(bot, bid, 'Назад')

    # Даже ошибочное соответствие пользовательской темы не допускает доставки.
    fs.fs_claim_topic(bid, 800, 'Пользователь')
    fs.fs_set_topic_thread(bid, 800, row[1])
    count_before = db.db_fs_count_range('fs_admin_msgs', bid, '2000-01-01', '2100-01-01')
    API.clear()
    for uid in (777, 778, 900):
        dispatch(bot, message(-10055, '/templates', uid=uid, thread=row[1]))
        fs.fs_handle_group_message(bot, bid, message(-10055, uid=uid, thread=row[1]), 999, {})
    check('Управление не доставляется и не считается ответом',
          not calls('copy_message') and not calls('copy_messages')
          and not [c for c in calls('send_message') if c[1][0] > 0]
          and db.db_fs_count_range('fs_admin_msgs', bid, '2000-01-01', '2100-01-01') == count_before)
    old_mid = state(bid)[2]
    DELETED_MESSAGES.add(old_mid)
    fs.fs_refresh_management(bot, bid)
    check('удалённое меню заменяется', state(bid)[2] != old_mid)
    old_thread = state(bid)[1]
    DELETED.add(old_thread)
    API.clear()
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: fs.fs_refresh_management(bot, bid), range(2)))
    check('удалённая тема пересоздаётся без дублей, закреп и id обновляются',
          state(bid)[1] != old_thread and len(calls('create_forum_topic')) == 1
          and len(calls('pin_chat_message')) == 1)
    FAIL_PIN = True
    DELETED_MESSAGES.add(state(bid)[2])
    fs.fs_refresh_management(bot, bid)
    FAIL_PIN = False
    check('ошибка закрепления не теряет id меню', state(bid)[2] not in DELETED_MESSAGES)

    click(bot, bid, 'Добавить шаблон')
    FAIL_EDIT = FAIL_GROUP_SEND = True
    click(bot, bid, 'Отмена')
    FAIL_EDIT = FAIL_GROUP_SEND = False
    dispatch(bot, message(-10055, 'Отменено при ошибке API', thread=state(bid)[1]))
    check('Отмена гарантирована при ошибке Telegram', state(bid)[3] is None
          and not any(v == 'Отменено при ошибке API' for _, v in db.db_get_templates(bid)))
    callback(bot, bid, 'p_mgadd_tpl', gid=777)
    FAIL_EDIT = FAIL_GROUP_SEND = True
    dispatch(bot, message(777, 'Сохранено при сбое меню'))
    FAIL_EDIT = FAIL_GROUP_SEND = False
    check('ошибка меню не прерывает сохранение и ответ личной панели',
          any(v == 'Сохранено при сбое меню' for _, v in db.db_get_templates(bid))
          and any(c[1][0] == 777 for c in calls('send_message')))

    for template_id, _ in db.db_get_templates(bid):
        db.db_del_template(template_id, bid)
    fs.fs_refresh_management(bot, bid)
    fs.fs_set_topic_thread(bid, 800, 4321)
    API.clear()
    callback(bot, bid, 'fs_pin_tpl_800', thread=4321)
    check('пустые шаблоны направляют в Управление',
          calls('answer_callback_query')[-1][1][1] ==
          'Шаблонов пока нет. Добавь их в теме «Управление»'
          and calls('answer_callback_query')[-1][2].get('show_alert'))
    for i in range(50):
        db.db_add_template(bid, 'Шаблон ' + str(i))
    fs.fs_refresh_management(bot, bid)
    click(bot, bid, 'Добавить шаблон')
    dispatch(bot, message(-10055, '51-й', thread=state(bid)[1]))
    check('51-й шаблон не добавляется', len(db.db_get_templates(bid)) == 50)


if __name__ == '__main__':
    try:
        run()
    except Exception:
        traceback.print_exc()
        check('выполнение сценария', False)
    print(f'ИТОГО: {len(OK)} OK, {len(FAIL)} FAIL')
    sys.exit(bool(FAIL))
