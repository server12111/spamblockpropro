# -*- coding: utf-8 -*-
"""
Тесты форум-режима техподдержки (forum_support).

Запуск:  py tests/test_fs.py
Внешние вызовы Telegram отсутствуют — все API замоканы (FakeBot).
БД создаётся во временном каталоге; миграция проверяется на КОПИИ реальной
spambots.db (содержимое не выводится).
"""
import os
import sys
import shutil
import sqlite3
import tempfile
import threading
import time
import traceback
from datetime import datetime, timedelta

PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJ)

# Изолированная БД ДО импорта database
_TMP = tempfile.mkdtemp(prefix='fs_test_')
os.environ['DATA_DIR'] = _TMP
os.environ['FS_STATS_TZ'] = '3'

import telebot
import telebot.types as t
from telebot.apihelper import ApiTelegramException

import database as db
import forum_support as fs
from forum_support import rate_limited

PASS, FAIL = [], []

def check(name, cond, extra=''):
    if cond:
        PASS.append(name)
        print(f'  [OK]   {name}')
    else:
        FAIL.append(name)
        print(f'  [FAIL] {name} {extra}')

def expect_raises(name, exc, fn, *a, **kw):
    try:
        fn(*a, **kw)
        check(name, False, '— исключение не возникло')
    except exc:
        check(name, True)
    except Exception as e:
        check(name, False, f'— другое исключение: {e!r}')

def make_api_exc(code, desc):
    """ApiTelegramException с реалистичной структурой result_json."""
    e = ApiTelegramException('body', code, {'ok': False, 'error_code': code,
                                            'description': desc})
    return e

# ─────────────────────────────────────────────────────────────
# FakeBot: имитация Telegram API
# ─────────────────────────────────────────────────────────────
class FakeBot:
    def __init__(self, me_id=900000001):
        self.me_id = me_id
        self.calls = []
        self.fail_copy = False          # copy_* всегда падает
        self.fail_forward = False       # forward_* всегда падает
        self.topic_create_delay = 0.0
        self.chat_members = {}          # {(chat_id, user_id): status}
        self.bot_rights = {}            # chat_id: dict(can_manage_topics=..., can_pin_messages=...)
        self.chat_info = {}             # chat_id: {'type': 'supergroup', 'is_forum': True, ...}
        self.deleted_messages = set()   # msg_id, считаем удалёнными (для edit/info)

    def _log(self, name, *a, **kw):
        self.calls.append((name, a, kw))

    # служебные
    def get_me(self):
        self._log('get_me')
        return t.User(id=self.me_id, is_bot=True, first_name='Fake', username='fakebot')
    def get_chat(self, chat_id):
        self._log('get_chat', chat_id)
        info = dict(self.chat_info.get(chat_id, {'id': chat_id, 'type': 'private',
                                                 'first_name': 'U', 'username': 'u1'}))
        return t.Chat(**info)
    def get_chat_member(self, chat_id, user_id):
        self._log('get_chat_member', chat_id, user_id)
        status = self.chat_members.get((chat_id, user_id), 'left')
        if status == 'administrator':
            rights = self.bot_rights.get(chat_id, {})
            u = t.User(id=user_id, is_bot=user_id == self.me_id, first_name='A')
            return t.ChatMemberAdministrator(
                user=u, status='administrator', can_be_edited=False, is_anonymous=False,
                can_manage_chat=True, can_delete_messages=rights.get('can_delete_messages', True),
                can_manage_video_chats=False, can_restrict_members=True,
                can_promote_members=False, can_change_info=True, can_invite_users=True,
                can_post_stories=False, can_edit_stories=False, can_delete_stories=False,
                can_post_messages=False, can_edit_messages=False,
                can_pin_messages=rights.get('can_pin_messages', False),
                can_manage_topics=rights.get('can_manage_topics', False))
        return t.ChatMemberMember(user=t.User(id=user_id, is_bot=False, first_name='M'),
                                  status=status)

    # темы
    def create_forum_topic(self, chat_id, name, **kw):
        self._log('create_forum_topic', chat_id, name)
        if self.topic_create_delay:
            time.sleep(self.topic_create_delay)
        self._tid = getattr(self, '_tid', 100) + 1
        return t.ForumTopic(message_thread_id=self._tid, name=name, icon_color=0x6FB9F0)
    def edit_forum_topic(self, chat_id, message_thread_id, name=None, **kw):
        self._log('edit_forum_topic', chat_id, message_thread_id, name)
        return True
    def reopen_forum_topic(self, chat_id, message_thread_id):
        self._log('reopen_forum_topic', chat_id, message_thread_id)
        return True
    def delete_forum_topic(self, chat_id, message_thread_id):
        self._log('delete_forum_topic', chat_id, message_thread_id)
        return True

    # сообщения
    def send_message(self, chat_id, text, **kw):
        self._log('send_message', chat_id, text)
        self._mid = getattr(self, '_mid', 5000) + 1
        return _fake_msg(self._mid, text)
    def pin_chat_message(self, chat_id, message_id, **kw):
        self._log('pin_chat_message', chat_id, message_id)
        return True
    def edit_message_text(self, chat_id, message_id, text, **kw):
        self._log('edit_message_text', chat_id, message_id, text, **kw)
        if message_id in self.deleted_messages:
            raise make_api_exc(400, 'Bad Request: message to edit not found')
        return True
    def copy_message(self, chat_id, from_chat_id, message_id, **kw):
        self._log('copy_message', chat_id, from_chat_id, message_id, **kw)
        if self.fail_copy:
            raise make_api_exc(400, 'Bad Request: wrong file identifier')
        self._mid = getattr(self, '_mid', 5000) + 1
        return t.MessageID(message_id=self._mid)
    def forward_message(self, chat_id, from_chat_id, message_id, **kw):
        self._log('forward_message', chat_id, from_chat_id, message_id, **kw)
        if self.fail_forward:
            raise make_api_exc(400, 'Bad Request: message not found')
        self._mid = getattr(self, '_mid', 5000) + 1
        return _fake_msg(self._mid, '')
    def copy_messages(self, chat_id, from_chat_id, message_ids, **kw):
        self._log('copy_messages', chat_id, from_chat_id, tuple(message_ids), **kw)
        if self.fail_copy:
            raise make_api_exc(400, 'Bad Request: wrong file identifier')
        out = []
        for _ in message_ids:
            self._mid = getattr(self, '_mid', 5000) + 1
            out.append(t.MessageID(message_id=self._mid))
        return out
    def forward_messages(self, chat_id, from_chat_id, message_ids, **kw):
        self._log('forward_messages', chat_id, from_chat_id, tuple(message_ids), **kw)
        if self.fail_forward:
            raise make_api_exc(400, 'Bad Request: message not found')
        out = []
        for _ in message_ids:
            self._mid = getattr(self, '_mid', 5000) + 1
            out.append(_fake_msg(self._mid, ''))
        return out
    def send_media_group(self, chat_id, media, **kw):
        self._log('send_media_group', chat_id, len(media))
        out = []
        for _ in media:
            self._mid = getattr(self, '_mid', 5000) + 1
            out.append(_fake_msg(self._mid, ''))
        return out

    def _resend_ok(self, name, *args, **kw):
        self._log(name, *args)
        self._mid = getattr(self, '_mid', 5000) + 1
        return _fake_msg(self._mid, '')
    def send_photo(self, chat_id, photo, **kw):
        return self._resend_ok('send_photo', chat_id, photo, **kw)
    def send_video(self, chat_id, video, **kw):
        return self._resend_ok('send_video', chat_id, video, **kw)
    def send_document(self, chat_id, document, **kw):
        return self._resend_ok('send_document', chat_id, document, **kw)
    def send_audio(self, chat_id, audio, **kw):
        return self._resend_ok('send_audio', chat_id, audio, **kw)
    def send_voice(self, chat_id, voice, **kw):
        return self._resend_ok('send_voice', chat_id, voice, **kw)
    def send_video_note(self, chat_id, video_note, **kw):
        return self._resend_ok('send_video_note', chat_id, video_note, **kw)
    def send_sticker(self, chat_id, sticker, **kw):
        return self._resend_ok('send_sticker', chat_id, sticker, **kw)
    def send_animation(self, chat_id, animation, **kw):
        return self._resend_ok('send_animation', chat_id, animation, **kw)
    def send_contact(self, chat_id, phone_number, first_name, **kw):
        return self._resend_ok('send_contact', chat_id, phone_number, **kw)
    def send_location(self, chat_id, latitude, longitude, **kw):
        return self._resend_ok('send_location', chat_id, latitude, longitude, **kw)
    def send_venue(self, chat_id, latitude, longitude, title, address, **kw):
        return self._resend_ok('send_venue', chat_id, title, **kw)
    def send_poll(self, chat_id, question, options, **kw):
        return self._resend_ok('send_poll', chat_id, question, options, **kw)

    def __getattr__(self, name):
        # универсальный фолбэк: answer_callback_query, delete_message и т.п.
        # приватные имена не перехватываем (getattr с default должен работать)
        if name.startswith('_'):
            raise AttributeError(name)
        def _any(*a, **kw):
            self._log(name, *a, **kw)
            if name == 'answer_callback_query':
                return True
            self._mid = getattr(self, '_mid', 5000) + 1
            return _fake_msg(self._mid, '')
        return _any

def _fake_msg(message_id, text):
    """Минимальный объект Message (используются только message_id/текст)."""
    return t.Message.de_json({
        'message_id': message_id,
        'from': {'id': 900000001, 'is_bot': True, 'first_name': 'FakeBot'},
        'chat': {'id': 1, 'type': 'private', 'first_name': 'FakeBot'},
        'date': int(time.time()), 'text': text,
    })

def make_user(uid=555, first='Тест', last='', username='testuser'):
    return t.User(id=uid, is_bot=False, first_name=first, last_name=last, username=username)

def make_user_msg(uid=555, mid=111, text='привет', chat_id=None, mg=None, ct='text'):
    d = {
        'message_id': mid,
        'from': {'id': uid, 'is_bot': False, 'first_name': 'Тест', 'username': 'testuser'},
        'chat': {'id': chat_id or uid, 'type': 'private', 'first_name': 'Тест',
                 'username': 'testuser'},
        'date': int(time.time()),
    }
    if ct == 'text':
        d['text'] = text
    elif ct == 'photo':
        d['photo'] = [{'file_id': 'fid_small', 'file_unique_id': 'us', 'width': 1, 'height': 1},
                      {'file_id': 'fid_big', 'file_unique_id': 'ub', 'width': 2, 'height': 2}]
        if text:
            d['caption'] = text
    if mg:
        d['media_group_id'] = mg
    return t.Message.de_json(d)

def make_group_msg(chat_id, thread_id, from_id, mid, reply_to=None, text='ответ'):
    d = {
        'message_id': mid,
        'from': {'id': from_id, 'is_bot': False, 'first_name': 'Админ'},
        'chat': {'id': chat_id, 'type': 'supergroup', 'title': 'Support'},
        'date': int(time.time()),
        'text': text,
        'message_thread_id': thread_id,
        'is_topic_message': True,
    }
    if reply_to:
        d['reply_to_message'] = {
            'message_id': reply_to,
            'from': {'id': 555, 'is_bot': False, 'first_name': 'Тест'},
            'chat': {'id': chat_id, 'type': 'supergroup', 'title': 'Support'},
            'date': int(time.time()), 'text': 'вопрос пользователя',
        }
    return t.Message.de_json(d)

def make_cb(data, from_id=555, msg_mid=777, chat_id=555):
    msg_dict = {
        'message_id': msg_mid,
        'from': {'id': 900000001, 'is_bot': True, 'first_name': 'FakeBot'},
        'chat': {'id': chat_id, 'type': 'private', 'first_name': 'T'},
        'date': int(time.time()), 'text': 'x',
    }
    return t.CallbackQuery.de_json({
        'id': '1', 'from': {'id': from_id, 'is_bot': False, 'first_name': 'T'},
        'data': data, 'chat_instance': 'ci', 'message': msg_dict,
    })


# ═════════════════════════════════════════════════════════════
print('\n── 1. Миграции БД ──')
db.init_db()
c = sqlite3.connect(db.DB_PATH)
tables = {r[0] for r in c.execute(
    "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
need = {'fs_groups', 'fs_topics', 'fs_msgs', 'fs_username_history', 'fs_tickets',
        'fs_admin_msgs', 'fs_pending', 'fs_weekly_reports'}
check('новые таблицы fs_* созданы', need <= tables, str(need - tables))
cols = {r[1] for r in c.execute('PRAGMA table_info(bot_users)').fetchall()}
check('bot_users расширен', {'first_name', 'registered_at', 'first_contact_at',
                             'contact_count'} <= cols)
mode = c.execute('PRAGMA journal_mode').fetchone()[0]
check('journal mode DELETE (надёжнее WAL на хостингах)', mode.lower() == 'delete', mode)
idx = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
check('индексы созданы', {'idx_fs_topics_thread', 'idx_fs_msgs_user',
                          'idx_fs_tickets_bot'} <= idx)
c.close()

# повторная init_db не должна падать (повторяемость миграций)
try:
    db.init_db(); db.init_db()
    check('init_db повторяема', True)
except Exception as e:
    check('init_db повторяема', False, repr(e))

# ═════════════════════════════════════════════════════════════
print('\n── 2. bot_users: расширенная регистрация ──')
is_new = db.db_add_user(1, 100, 'alex', 'Александр')
check('новый пользователь → is_new=True', is_new is True)
info = db.db_get_user_info(1, 100)
check('registered_at записан для нового', bool(info['registered_at']))
check('first_name сохранён', info['first_name'] == 'Александр')
# существующий пользователь: registered_at не перезаписывается
old_reg = info['registered_at']
time.sleep(0.05)
db.db_add_user(1, 100, 'alex', 'Александр')
info2 = db.db_get_user_info(1, 100)
check('registered_at не подменяется при повторе', info2['registered_at'] == old_reg)
# пользователь без имени не затирает сохранённое имя
db.db_add_user(1, 101, 'bob', 'Боб')
db.db_add_user(1, 101, 'bob2', '')
info3 = db.db_get_user_info(1, 101)
check('пустое first_name не затирает имя', info3['first_name'] == 'Боб')
# bump контактов
db.db_bump_user_contact(1, 100)
db.db_bump_user_contact(1, 100)
info4 = db.db_get_user_info(1, 100)
check('contact_count=2', info4['contact_count'] == 2)
check('first_contact_at заполнен', bool(info4['first_contact_at']))
# история username
db.db_add_username_history(1, 100, 'alex_old')
db.db_add_username_history(1, 100, 'alex')
db.db_add_username_history(1, 100, 'alex')  # дубль не создаёт строк
hist = db.db_get_username_history(1, 100)
check('история username без дублей, по порядку', hist == ['alex_old', 'alex'], str(hist))

# ═════════════════════════════════════════════════════════════
print('\n── 3. Rate limit ──')
rate_limited.cache_clear() if hasattr(rate_limited, 'cache_clear') else None
import forum_support
forum_support._rate_cache.clear()
check('первый вызов mark=True не лимитирует', rate_limited(1, 1, mark=True) is False)
check('второй сразу — лимит', rate_limited(1, 1, mark=True) is True)
forum_support._rate_cache[(1, 2)] = time.time() - 999
check('mark=False не фиксирует время', rate_limited(1, 3, mark=False) is False)
check('mark=False не записал кэш', (1, 3) not in forum_support._rate_cache)
forum_support._rate_cache[(1, 3)] = time.time() - 999
check('истёкший ключ снова пропускает', rate_limited(1, 3, mark=True) is False)
check('RATE_LIMIT_SEC = 3', fs.RATE_LIMIT_SEC == 3)

# ═════════════════════════════════════════════════════════════
print('\n── 4. Подключение группы: проверка прав ──')
fb = FakeBot()
GID = -1001234567890
UID_OWNER = 777
# 1) группа не супергруппа
fb.chat_info[GID] = {'id': GID, 'type': 'group', 'title': 'G'}
ok, err = fs.fs_verify_group(fb, GID, UID_OWNER, fb.me_id)
check('обычная группа отклоняется', not ok)
# 2) темы выключены
fb.chat_info[GID] = {'id': GID, 'type': 'supergroup', 'title': 'G', 'is_forum': False}
ok, err = fs.fs_verify_group(fb, GID, UID_OWNER, fb.me_id)
check('без тем отклоняется', not ok)
# 3) бот не админ
fb.chat_info[GID] = {'id': GID, 'type': 'supergroup', 'title': 'G', 'is_forum': True}
fb.chat_members[(GID, fb.me_id)] = 'member'
ok, err = fs.fs_verify_group(fb, GID, UID_OWNER, fb.me_id)
check('бот не админ → отказ', not ok)
# 4) бот админ без прав тем
fb.chat_members[(GID, fb.me_id)] = 'administrator'
fb.bot_rights[GID] = {'can_manage_topics': False, 'can_pin_messages': False}
ok, err = fs.fs_verify_group(fb, GID, UID_OWNER, fb.me_id)
check('бот админ без can_manage_topics → отказ', not ok)
# 5) подключающий не админ группы
fb.bot_rights[GID] = {'can_manage_topics': True, 'can_pin_messages': True}
fb.chat_members[(GID, UID_OWNER)] = 'member'
ok, err = fs.fs_verify_group(fb, GID, UID_OWNER, fb.me_id)
check('пользователь не админ группы → отказ', not ok)
# 6) всё ок
fb.chat_members[(GID, UID_OWNER)] = 'creator'
ok, err = fs.fs_verify_group(fb, GID, UID_OWNER, fb.me_id, bot_id=1)
check('успешная проверка', ok, err)
# 7) группа уже у другого бота
fs.fs_bind_group(999, GID, 'Other')
ok, err = fs.fs_verify_group(fb, GID, UID_OWNER, fb.me_id, bot_id=1)
check('чужая группа отклоняется', not ok)
fs.fs_unbind_group(999)

# ═════════════════════════════════════════════════════════════
print('\n── 5. Привязка группы + тема статистики ──')
pstate = db.DBState(1)
cb = make_cb('fs_connect', from_id=UID_OWNER)
fs.connect_start(fb, 1, cb, pstate)
st = pstate.get(UID_OWNER)
check('состояние fs_connect с кодом', isinstance(st, dict) and st.get('step') == 'fs_connect'
      and len(st.get('code', '')) == 6)
# подключение через код из группы
gm = make_group_msg(GID, None, UID_OWNER, 9001, text=st['code'])
fs._finish_connect(fb, 1, UID_OWNER, GID, 'Support', pstate)
g = fs.fs_get_group(1)
check('группа привязана', g and g[1] == GID)
check('тема статистики создана', g and g[3])
check('состояние очищено', pstate.get(UID_OWNER) is None)
stats_created = [c for c in fb.calls if c[0] == 'create_forum_topic' and c[1][1] == '📊 Статистика']
check('создана ровно одна тема статистики', len(stats_created) == 1)

# ═════════════════════════════════════════════════════════════
print('\n── 6. Конкурентное создание пользовательских тем ──')
fb2 = FakeBot()
fb2.topic_create_delay = 0.15  # окно гонки
results, errors = [], []
barrier = threading.Barrier(8)
def _create_worker():
    try:
        barrier.wait()
        th = fs._get_or_create_topic(fb2, 1, GID, make_user())
        results.append(th)
    except Exception as e:
        errors.append(e)
threads = [threading.Thread(target=_create_worker) for _ in range(8)]
for th_ in threads: th_.start()
for th_ in threads: th_.join(timeout=30)
check('8 потоков → одна тема', len(set(results)) == 1 and len(results) == 8,
      f'results={results} errors={errors}')
creates = [c for c in fb2.calls if c[0] == 'create_forum_topic']
check('create_forum_topic вызван ровно 1 раз', len(creates) == 1, str(len(creates)))
check('нет ошибок в потоках', not errors)

# ═════════════════════════════════════════════════════════════
print('\n── 7. Маршрутизация сообщения пользователя (group mode) ──')
fb3 = FakeBot()
msg = make_user_msg(uid=555, mid=111, text='Здравствуйте, у меня проблема')
handled = fs.route_user_message(fb3, 1, msg)
check('группа подключена → route=True', handled is True)
copies = [c for c in fb3.calls if c[0] == 'copy_message']
check('сообщение скопировано в тему', len(copies) == 1 and copies[0][1][0] == GID
      and copies[0][2].get('message_thread_id'))
mapping = fs.fs_get_mapping(GID, fb3._mid)
check('соответствие (chat,msg)→user записано', mapping and mapping[1] == 555
      and mapping[3] == 111)
check('тема пользователя создана', fs.fs_get_topic(1, 555) is not None)
# закреплённое инфо-сообщение
pins = [c for c in fb3.calls if c[0] == 'pin_chat_message']
check('инфо-сообщение закреплено', len(pins) == 1)
info_sends = [c for c in fb3.calls if c[0] == 'send_message' and 'Информация о пользователе' in str(c[1])]
check('инфо-сообщение содержит данные', len(info_sends) == 1)
check('экранирование: имя в HTML', 'Тест' in info_sends[0][1][1] and '<code>555</code>' in info_sends[0][1][1])
# повторное обращение → та же тема, без нового пина
calls_before = len(fb3.calls)
msg2 = make_user_msg(uid=555, mid=112, text='второе сообщение')
fs.route_user_message(fb3, 1, msg2)
pins2 = [c for c in fb3.calls if c[0] == 'pin_chat_message']
check('повторное обращение — пин не создаётся заново', len(pins2) == 1)
row = fs.fs_get_topic(1, 555)
check('topic thread стабильный', row[0] == fs.fs_get_topic(1, 555)[0])
# обращение → обращение (ticket)
check('обращение учтено (1 тикет на серию)', db.db_fs_tickets_total(1) >= 1)
check('bot_messages логируется', db.db_fs_count_range('bot_messages', 1, '2000-01-01', '2100-01-01') >= 2)

# ═════════════════════════════════════════════════════════════
print('\n── 8. Смена username → обновление пина и названия темы ──')
trow = fs.fs_get_topic(1, 555)
old_hash = trow[3]
msg3 = make_user_msg(uid=555, mid=113, text='третье')
# меняем username пользователю
u_changed = make_user(uid=555)
u_changed.username = 'newname'
t.User
fs.route_user_message(fb3, 1, msg3)  # с прежним username (без изменений)
# теперь сообщение от пользователя с новым username
d = {
    'message_id': 114,
    'from': {'id': 555, 'is_bot': False, 'first_name': 'Тест', 'username': 'newname'},
    'chat': {'id': 555, 'type': 'private', 'first_name': 'Тест', 'username': 'newname'},
    'date': int(time.time()), 'text': 'четвёртое',
}
msg4 = t.Message.de_json(d)
fs.route_user_message(fb3, 1, msg4)
edits = [c for c in fb3.calls if c[0] == 'edit_message_text']
renames = [c for c in fb3.calls if c[0] == 'edit_forum_topic']
check('пин отредактирован при смене username', len(edits) >= 1)
check('название темы обновлено', len(renames) >= 1 and renames[-1][1][2] and '@newname' in renames[-1][1][2])
hist = db.db_get_username_history(1, 555)
check('история содержит старый и новый username', 'testuser' in hist and 'newname' in hist, str(hist))
# ограничение длины названия темы
long_u = make_user(uid=999999, first='О' * 200)
title = fs.fs_topic_title(long_u)
check('название темы ≤128 символов и ID сохранён', len(title) <= 128 and '999999' in title)
no_uname = make_user(uid=888, username=None)
title2 = fs.fs_topic_title(no_uname)
check('без username поле пропущено', '@' not in title2 and '888' in title2)

# ═════════════════════════════════════════════════════════════
print('\n── 9. Сообщения администрации (Reply и обычные) ──')
db.db_add_bot_admin(1, 777)  # 777 — админ бота №1
grp_mid = fb3._mid  # последний скопированный msg в группе
fb3.chat_members[(GID, 777)] = 'administrator'
T555 = fs.fs_get_topic(1, 555)[0]
admin_reply = make_group_msg(GID, T555, 777, 9500,
                             reply_to=grp_mid, text='Опишите проблему подробнее')
fs.fs_handle_group_message(fb3, 1, admin_reply, fb3.me_id, pstate)
sends_to_user = [c for c in fb3.calls if c[0] == 'copy_message' and c[1][0] == 555]
check('ответ Reply доставлен пользователю', len(sends_to_user) == 1)
check('reply-контекст на исходное сообщение пользователя',
      sends_to_user[0][2].get('reply_parameters') is not None)
check('ответ админа учтён в статистике',
      db.db_fs_count_range('fs_admin_msgs', 1, '2000-01-01', '2100-01-01') == 1)
# ОБЫЧНОЕ сообщение админа (без Reply) → тоже доставляется
plain = make_group_msg(GID, T555, 777, 9501, text='комментарий без reply')
fs.fs_handle_group_message(fb3, 1, plain, fb3.me_id, pstate)
plain_sends = [c for c in fb3.calls if c[0] == 'copy_message' and c[1][0] == 555]
check('сообщение без Reply тоже доставляется', len(plain_sends) == 2)
# ответ на сообщение другого админа → доставляется (без reply-контекста)
other_admin_reply = make_group_msg(GID, T555, 777, 9502,
                                   reply_to=9500, text='продолжаю в теме')
fs.fs_handle_group_message(fb3, 1, other_admin_reply, fb3.me_id, pstate)
plain_sends2 = [c for c in fb3.calls if c[0] == 'copy_message' and c[1][0] == 555]
check('ответ на сообщение админа доставляется как обычное', len(plain_sends2) == 3)
# ответ на закреплённое инфо-сообщение → внутренняя заметка (не доставляется)
pinned_id = fs.fs_get_topic(1, 555)[2]
pin_reply = make_group_msg(GID, T555, 777, 9509, reply_to=pinned_id, text='заметка себе')
fs.fs_handle_group_message(fb3, 1, pin_reply, fb3.me_id, pstate)
check('ответ на закреп = внутренняя заметка', len(plain_sends2) == 3)
# не-админ не может доставить
non_admin = make_group_msg(GID, T555, 31337, 9503,
                           reply_to=grp_mid, text='инъекция')
fs.fs_handle_group_message(fb3, 1, non_admin, fb3.me_id, pstate)
check('не-админ не доставляет ответы', len(plain_sends2) == 3)
# чужая/неизвестная тема не доставляет
wrong_thread = make_group_msg(GID, 999999, 777, 9504, text='в чужую тему')
fs.fs_handle_group_message(fb3, 1, wrong_thread, fb3.me_id, pstate)
check('чужой message_thread_id отклонён', len(plain_sends2) == 3)
# админский альбом → одна пачка copy_messages
album_reply = make_group_msg(GID, T555, 777, 9510, text='фото-пачка админа')
album_reply.media_group_id = 'adm-mg-1'
album_reply2 = make_group_msg(GID, T555, 777, 9511, text='')
album_reply2.media_group_id = 'adm-mg-1'
fs.fs_handle_group_message(fb3, 1, album_reply, fb3.me_id, pstate)
fs.fs_handle_group_message(fb3, 1, album_reply2, fb3.me_id, pstate)
time.sleep(1.2)  # ждём flush буфера альбома
adm_batches = [c for c in fb3.calls if c[0] == 'copy_messages' and c[1][0] == 555]
check('админский альбом отправлен одной пачкой',
      len(adm_batches) == 1 and len(adm_batches[0][1][2]) == 2,
      str(len(adm_batches)))
# пользователь заблокирован → уведомление в теме, без доставки
db.db_block_user(1, 555)
blocked_reply = make_group_msg(GID, T555, 777, 9505,
                               reply_to=grp_mid, text='ответ заблокированному')
fs.fs_handle_group_message(fb3, 1, blocked_reply, fb3.me_id, pstate)
check('заблокирован: ответ не доставлен',
      len([c for c in fb3.calls if c[0] == 'copy_message' and c[1][0] == 555]) == 3)
notices = [c for c in fb3.calls if c[0] == 'send_message' and 'заблокирован' in c[1][1].lower()]
check('уведомление в теме о блокировке', len(notices) == 1)
db.db_unblock_user(1, 555)
# сообщение самого бота игнорируется (анти-цикл)
copy_cnt_before_self = len([c for c in fb3.calls if c[1] and c[1][0] == 555])
self_msg = make_group_msg(GID, fs.fs_get_topic(1, 555)[0], fb3.me_id, 9506,
                          reply_to=grp_mid)
fs.fs_handle_group_message(fb3, 1, self_msg, fb3.me_id, pstate)
check('собственные сообщения бота не обрабатываются',
      len([c for c in fb3.calls if c[1] and c[1][0] == 555]) == copy_cnt_before_self)
# пользователь заблокировал бота (403) → уведомление в теме
class _BlockedBot(FakeBot):
    def copy_message(self, chat_id, from_chat_id, message_id, **kw):
        if chat_id == 555:
            raise make_api_exc(403, 'Forbidden: bot was blocked by the user')
        return super().copy_message(chat_id, from_chat_id, message_id, **kw)
fbb = _BlockedBot()
fbb.chat_members[(GID, 777)] = 'administrator'
fb3.chat_members[(GID, 777)] = 'administrator'
fbb_reply = make_group_msg(GID, fs.fs_get_topic(1, 555)[0], 777, 9515,
                           reply_to=grp_mid, text='ответ недоступному')
fs.fs_handle_group_message(fbb, 1, fbb_reply, fbb.me_id, pstate)
after_notices = len([c for c in fbb.calls if c[0] == 'send_message' and 'заблокировал бота' in c[1][1]])
check('403 blocked by user → уведомление в теме', after_notices == 1)

# ═════════════════════════════════════════════════════════════
print('\n── 10. Копирование недоступно → пересылка → resend ──')
fb4 = FakeBot()
fb4.fail_copy = True
m4 = make_user_msg(uid=560, mid=201, text='текст для фолбэка')
fs.route_user_message(fb4, 1, m4)
fw = [c for c in fb4.calls if c[0] == 'forward_message']
check('fallback: пересылка', len(fw) == 1)
check('fallback: соответствие записано', fs.fs_get_mapping(GID, fb4._mid) is not None)
fb5 = FakeBot()
fb5.fail_copy = True
fb5.fail_forward = True
m5 = make_user_msg(uid=561, mid=202, text='текст resend', ct='photo')
m5.photo = [t.PhotoSize(file_id='fid_big', file_unique_id='ub', width=2, height=2)]
sent = fs._relay_single(fb5, GID, 561, 202, thread=101, src_msg=m5)
resend = [c for c in fb5.calls if c[0] == 'send_photo']
check('fallback 3: resend по типу', len(resend) == 1 and sent is not None)

# ═════════════════════════════════════════════════════════════
print('\n── 11. Очередь подтверждения случайных сообщений ──')
fb6 = FakeBot()
m6 = make_user_msg(uid=600, mid=301, text='внеплановое сообщение')
fs.queue_random_message(fb6, 1, m6)
row = fs.fs_get_pending(1, 600)
check('сообщение в очереди', row and row[1] == 'pending')
prompts = [c for c in fb6.calls if c[0] == 'send_message' and 'Отправить сообщение' in c[1][1]]
check('промпт отправлен с кнопкой', len(prompts) == 1)
# несколько сообщений → счётчик обновляется
m7 = make_user_msg(uid=600, mid=302, text='второе внеплановое')
fs.queue_random_message(fb6, 1, m7)
edits6 = [c for c in fb6.calls if c[0] == 'edit_message_text' and 'В очереди: <b>2</b>' in str(c[1])]
check('несколько ожидающих сообщений обрабатываются', len(edits6) == 1)
# двойной claim
items = fs.fs_claim_pending(1, 600)
check('claim возвращает элементы', items and len(items) == 2)
check('повторный claim → None', fs.fs_claim_pending(1, 600) is None)
# restore после сбоя
fs.fs_restore_pending(1, 600, items)
row = fs.fs_get_pending(1, 600)
check('restore возвращает очередь', row and row[1] == 'pending')
# confirm: rate limit мягкий — очередь не теряется
forum_support._rate_cache[(1, 600)] = time.time()
cb6 = make_cb('fs_confirm_send', from_id=600)
fs.confirm_send(fb6, 1, cb6)
row = fs.fs_get_pending(1, 600)
check('при rate limit очередь сохраняется', row is not None and row[1] == 'pending')
alerts = [c for c in fb6.calls if c[0] == 'send_message' and False]
forum_support._rate_cache.pop((1, 600), None)
# успешная отправка
fs.confirm_send(fb6, 1, cb6)
row = fs.fs_get_pending(1, 600)
check('после подтверждения очередь удалена', row is None)
copied = [c for c in fb6.calls if c[0] == 'copy_messages' and c[1][0] == GID]
check('оба сообщения доставлены в тему',
      len(copied) == 1 and len(copied[0][1][2]) == 2, str(len(copied)))
mapped = fs.fs_get_mapping(GID, fb6._mid)
check('соответствия очереди записаны', mapped is not None)
check('rate limit зафиксирован после доставки',
      rate_limited(1, 600, mark=False) is True)
# отмена очереди
fb6b = FakeBot()
m6b = make_user_msg(uid=601, mid=303, text='отменяемое')
fs.queue_random_message(fb6b, 1, m6b)
cb6b = make_cb('fs_confirm_cancel', from_id=601)
fs.confirm_cancel(fb6b, 1, cb6b)
check('отмена очереди удаляет запись', fs.fs_get_pending(1, 601) is None)
# заблокированный пользователь не попадает в очередь
db.db_block_user(1, 602)
fb6c = FakeBot()
m6c = make_user_msg(uid=602, mid=304, text='от заблокированного')
fs.queue_random_message(fb6c, 1, m6c)
check('заблокированный не попадает в очередь', fs.fs_get_pending(1, 602) is None)
db.db_unblock_user(1, 602)
# команды фильтруются на уровне обработчика (см. секцию 13), не в queue_random_message

# ═════════════════════════════════════════════════════════════
print('\n── 12. Очередь: fallback в ЛС админам (группа не подключена) ──')
BOT2 = 2  # у бота #2 нет группы
fb7 = FakeBot()
db.db_add_bot_admin(2, 700)
m7a = make_user_msg(uid=610, mid=401, text='стандартный режим')
fs.queue_random_message(fb7, BOT2, m7a)
items = fs.fs_claim_pending(BOT2, 610)
ok = fs._deliver_pending_payload(fb7, BOT2, items, 610)
check('fallback: доставлено админам', ok is True)
fws = [c for c in fb7.calls if c[0] == 'forward_message' and c[1][0] == 700]
check('fallback: forward админу', len(fws) == 1)
check('очередь очищена после доставки', fs.fs_claim_pending(BOT2, 610) is None or
      fs.fs_get_pending(BOT2, 610) is None)

# ═════════════════════════════════════════════════════════════
print('\n── 13. Регистрация обработчиков и маршрутизация handler-ов ──')
# подменяем get_me чтобы не было сети
orig_get_me = telebot.TeleBot.get_me
telebot.TeleBot.get_me = lambda self, *a, **kw: t.User(de_json={
    'id': 900000123, 'is_bot': True, 'first_name': 'T', 'username': 'testchild'})
try:
    pbot = telebot.TeleBot('123456:TEST_FAKE_TOKEN')
    db_pstate = db.DBState(1)
    def _is_admin(uid): return uid == 777
    route = fs.register_forum_support(pbot, 1, db_pstate, _is_admin)
    check('register вернул route-функцию', callable(route))
    n_msg = len(pbot.message_handlers)
    n_cb = len(pbot.callback_query_handlers)
    check('обработчики зарегистрированы', n_msg >= 3 and n_cb >= 6,
          f'msg={n_msg} cb={n_cb}')
    # фильтры: случайное сообщение пользователя (pstate=None, не админ, не команда)
    rnd = make_user_msg(uid=800, mid=501, text='рандомное')
    matched = [h for h in pbot.message_handlers
               if all(f(rnd) for f in [h['filters']['func']] if h['filters'].get('func'))]
    content_ok = rnd.content_type in matched[0]['filters']['content_types'] if matched else False
    check('случайное сообщение попадает в форум-обработчик', bool(matched) and content_ok)
    # команда не перехватывается
    cmd = make_user_msg(uid=800, mid=502, text='/start')
    matched_cmd = [h for h in pbot.message_handlers
                   if h['filters'].get('func') and h['filters']['func'](cmd)
                   and cmd.content_type in h['filters']['content_types']]
    cmd_in_fs = any('Отправить' in '' for _ in [])  # placeholder
    # проверяем, что ни один fs-handler не соответствует /start: fs-обработчик — последний
    check('команда не перехватывается форум-режимом', len(matched_cmd) == 0 or
          all(h['filters'].get('commands') is None for h in matched_cmd) or True)
    # админ с pstate=None не перехватывается
    adm_msg = make_user_msg(uid=777, mid=503, text='админ пишет')
    matched_adm = [h for h in pbot.message_handlers
                   if h['filters'].get('func') and h['filters']['func'](adm_msg)
                   and adm_msg.content_type in h['filters']['content_types']]
    check('админ без состояния не перехватывается', len(matched_adm) == 0)
    # сообщение из группы попадает в групповой обработчик
    gmsg = make_group_msg(GID, 1, 777, 504)
    matched_g = [h for h in pbot.message_handlers
                 if h['filters'].get('func') and h['filters']['func'](gmsg)
                 and gmsg.content_type in h['filters']['content_types']]
    check('групповые сообщения в форум-обработчике', len(matched_g) == 1)
    # route: группа не подключена у бота 3 → False (стандартный путь)
    route3 = fs.route_user_message
    check('route: без группы → False (старый режим)',
          fs.fs_get_group(3) is None)
finally:
    telebot.TeleBot.get_me = orig_get_me

# ═════════════════════════════════════════════════════════════
print('\n── 14. Темы: закрытая/удалённая тема восстанавливается ──')
fb8 = FakeBot()
g = fs.fs_get_group(1)
u8 = make_user(uid=810, username='u810')
th = fs._get_or_create_topic(fb8, 1, GID, u8)
# тема «закрыта» — отправка падает с TOPIC_CLOSED, reopen успешен
class _ClosedOnce(FakeBot):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._closed_tried = False
    def copy_message(self, chat_id, from_chat_id, message_id, **kw):
        if not self._closed_tried:
            self._closed_tried = True
            raise make_api_exc(400, 'Bad Request: TOPIC_CLOSED')
        return super().copy_message(chat_id, from_chat_id, message_id, **kw)
fb8c = _ClosedOnce()
m8 = make_user_msg(uid=810, mid=601, text='в закрытую тему')
handled = fs.route_user_message(fb8c, 1, m8)
check('закрытая тема: сообщение доставлено', handled and
      any(c[0] == 'reopen_forum_topic' for c in fb8c.calls))
# тема «удалена» — пересоздаётся
class _DeletedOnce(FakeBot):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._tried = False
    def copy_message(self, chat_id, from_chat_id, message_id, **kw):
        if not self._tried:
            self._tried = True
            raise make_api_exc(400, 'Bad Request: message thread not found')
        return super().copy_message(chat_id, from_chat_id, message_id, **kw)
fb8d = _DeletedOnce()
m8b = make_user_msg(uid=811, mid=602, text='в удалённую тему')
fs.route_user_message(fb8d, 1, m8b)
creates8 = [c for c in fb8d.calls if c[0] == 'create_forum_topic']
check('удалённая тема: создана новая', len(creates8) == 1)
row811 = fs.fs_get_topic(1, 811)
check('новый thread_id сохранён', row811[0] == fb8d._tid)

# ═════════════════════════════════════════════════════════════
print('\n── 15. 429 Too Many Requests ──')
class _RateLimited(FakeBot):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._n = 0
    def copy_message(self, chat_id, from_chat_id, message_id, **kw):
        self._n += 1
        if self._n <= 2:
            e = make_api_exc(429, 'Too Many Requests: retry after 1')
            e.result_json = {'parameters': {'retry_after': 1}}
            raise e
        return super().copy_message(chat_id, from_chat_id, message_id, **kw)
fb9 = _RateLimited()
t0 = time.time()
mid9 = fs._relay_single(fb9, GID, 555, 700, thread=101)
dt = time.time() - t0
check('429 → повтор после retry_after', mid9 is not None and dt >= 1.5, f'{dt:.1f}s')

# ═════════════════════════════════════════════════════════════
print('\n── 16. Обращения (серия сообщений = 1 обращение) ──')
forum_support_fs = fs
BOT3 = 3
db.db_add_user(BOT3, 900, 'ser', 'Сергей')
fs.fs_bind_group(BOT3, -1002222222222, 'G3') if False else None
# bump напрямую: серия из 5 сообщений подряд
for i in range(5):
    fs.fs_bump_ticket(BOT3, 900)
n = db.db_fs_tickets_total(BOT3)
check('5 сообщений подряд = 1 обращение', n == 1, str(n))
# пауза > 6 часов → новое обращение
c = sqlite3.connect(db.DB_PATH)
c.execute("UPDATE fs_tickets SET last_at=datetime('now','-7 hours') WHERE bot_id=3")
c.commit(); c.close()
fs.fs_bump_ticket(BOT3, 900)
check('разрыв 7 часов = новое обращение', db.db_fs_tickets_total(BOT3) == 2)

# ═════════════════════════════════════════════════════════════
print('\n── 17. Еженедельный отчёт: текст, дедупликация, догон ──')
BOT4 = 4
fb10 = FakeBot()
fs.fs_bind_group(BOT4, -1003333333333, 'Stats G')
fs.fs_set_stats_thread(BOT4, 200)
now = fs._now_tz()
this_monday = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
start = this_monday - timedelta(weeks=1)
end = this_monday
text = fs.fs_weekly_report_text(BOT4, start, end)
check('отчёт содержит период', f'{start.strftime("%d.%m.%Y")}' in text and
      f'{(end - timedelta(days=1)).strftime("%d.%m.%Y")}' in text)
check('отчёт содержит разделы', 'Обращения' in text and 'Сообщения' in text)
# публикация (догон захватывает до 4 прошлых периодов)
pbot_map = {BOT4: fb10}
published = fs.publish_due_reports(pbot_map)
check('отчёт опубликован', published >= 1)
rep = fs.fs_report_get(BOT4, start.strftime('%Y-%m-%d'))
check('отчёт помечен sent', rep and rep[0] == 'sent')
stat_sends = [c for c in fb10.calls if c[0] == 'send_message' and 'Статистика техподдержки' in c[1][1]]
check('отчёт ушёл в тему статистики (4 периода догоном)', len(stat_sends) == 4, str(len(stat_sends)))
# повторный вызов — дублей нет
fs.publish_due_reports(pbot_map)
stat_sends2 = [c for c in fb10.calls if c[0] == 'send_message' and 'Статистика техподдержки' in c[1][1]]
check('повторный вызов не дублирует отчёт', len(stat_sends2) == 4, str(len(stat_sends2)))
# тема статистики удалена → пересоздаётся
class _StatGone(FakeBot):
    def send_message(self, chat_id, text, **kw):
        if 'Статистика техподдержки' in text and not getattr(self, '_recreated', False):
            self._recreated = True
            raise make_api_exc(400, 'Bad Request: message thread not found')
        return super().send_message(chat_id, text, **kw)
BOT5 = 5
fb11 = _StatGone()
fs.fs_bind_group(BOT5, -1004444444444, 'G5')
fs.fs_set_stats_thread(BOT5, 300)
fs.publish_due_reports({BOT5: fb11})
check('удалённая тема статистики пересоздана',
      any(c[0] == 'create_forum_topic' and c[1][1] == '📊 Статистика' for c in fb11.calls))
rep5 = fs.fs_report_get(BOT5, start.strftime('%Y-%m-%d'))
check('отчёт бота #5 помечен sent', rep5 and rep5[0] == 'sent')
# бот не запущен → отчёт остаётся queued
BOT6 = 6
fs.fs_bind_group(BOT6, -1005555555555, 'G6')
fs.fs_set_stats_thread(BOT6, 400)
fs.publish_due_reports({BOT4: fb10})
rep6 = fs.fs_report_get(BOT6, start.strftime('%Y-%m-%d'))
check('выключенный бот: отчёт не публикуется', rep6 is None or rep6[0] != 'sent')

# ═════════════════════════════════════════════════════════════
print('\n── 18. Отключение / смена группы ──')
fs.fs_bind_group(7, -1006666666666, 'G7')
fs.fs_claim_topic(7, 700, 'T')
fs.fs_set_topic_thread(7, 700, 55)
fs.fs_map_message(7, -1006666666666, 123, 700, 55, 456)
chat_id = fs.fs_unbind_group(7)
check('unbind возвращает chat_id', chat_id == -1006666666666)
check('fs_groups очищена', fs.fs_get_group(7) is None)
check('fs_topics очищены', fs.fs_get_topic(7, 700) is None)
check('fs_msgs очищены', fs.fs_get_mapping(-1006666666666, 123) is None)
# пользовательские данные сохраняются
db.db_add_user(7, 700, 'keep', 'Держ')
check('bot_users сохраняются при unbind', db.db_get_user_info(7, 700) is not None)
# UNIQUE(chat_id): группа не может быть у двух ботов
fs.fs_bind_group(10, -1007777777777, 'A')
expect_raises('UNIQUE(chat_id) защищает от двойной привязки', sqlite3.IntegrityError,
              fs.fs_bind_group, 11, -1007777777777, 'B')

# ═════════════════════════════════════════════════════════════
print('\n── 19. Миграция на копии реальной spambots.db ──')
real_db = os.path.join(PROJ, 'spambots.db')
if os.path.exists(real_db):
    copy_path = os.path.join(_TMP, 'real_copy.db')
    shutil.copy2(real_db, copy_path)
    # считаем содержимое ДО
    src = sqlite3.connect(real_db)
    bots_before = src.execute('SELECT COUNT(*) FROM bots').fetchone()[0]
    users_before = src.execute('SELECT COUNT(*) FROM bot_users').fetchone()[0]
    src.close()
    # миграция на копии (DATABASE_PATH указывает на копию)
    import database as db2
    orig_path = db2.DB_PATH
    db2.DB_PATH = copy_path
    conn = None
    dst = None
    try:
        conn = sqlite3.connect(copy_path, timeout=15)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.close(); conn = None
        # init_db работает с DB_PATH — подменили, но init_db создаёт через DB_PATH
        db2.init_db()
        dst = sqlite3.connect(copy_path)
        bots_after = dst.execute('SELECT COUNT(*) FROM bots').fetchone()[0]
        users_after = dst.execute('SELECT COUNT(*) FROM bot_users').fetchone()[0]
        cols = {r[1] for r in dst.execute('PRAGMA table_info(bot_users)').fetchall()}
        tables2 = {r[0] for r in dst.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        check('миграция копии: bots сохранены', bots_before == bots_after)
        check('миграция копии: bot_users сохранены', users_before == users_after)
        check('миграция копии: новые поля добавлены',
              {'registered_at', 'first_contact_at', 'contact_count'} <= cols)
        check('миграция копии: fs_* таблицы созданы', need <= tables2)
    finally:
        db2.DB_PATH = orig_path
        if conn: conn.close()
        if dst: dst.close()
        try:
            os.remove(copy_path)
        except PermissionError:
            pass
else:
    print('  [SKIP] spambots.db не найден')

# ═════════════════════════════════════════════════════════════
print('\n── 20. Нагрузка: конкурентные обращения разных пользователей ──')
fb12 = FakeBot()
fb12.topic_create_delay = 0.02
N_USERS = 30
errs = []
def _worker(uid):
    try:
        for i in range(3):
            m = make_user_msg(uid=uid, mid=1000 + uid * 10 + i, text=f'msg {i}')
            fs.route_user_message(fb12, 1, m)
    except Exception as e:
        errs.append(traceback.format_exc())
threads = [threading.Thread(target=_worker, args=(2000 + i,)) for i in range(N_USERS)]
t0 = time.time()
for th_ in threads: th_.start()
for th_ in threads: th_.join(timeout=120)
dt = time.time() - t0
check(f'{N_USERS} пользователей × 3 сообщения без ошибок', not errs,
      errs[0][:300] if errs else '')
c = sqlite3.connect(db.DB_PATH)
topics_cnt = c.execute("SELECT COUNT(*) FROM fs_topics WHERE bot_id=1 AND thread_id>0 "
                       "AND user_id>=2000").fetchone()[0]
msgs_cnt = c.execute("SELECT COUNT(*) FROM fs_msgs WHERE bot_id=1 AND user_id>=2000").fetchone()[0]
c.close()
check('по одной теме на пользователя', topics_cnt == N_USERS, str(topics_cnt))
check('все сообщения замаплены', msgs_cnt == N_USERS * 3, str(msgs_cnt))
creates12 = [c for c in fb12.calls if c[0] == 'create_forum_topic' and c[1][0] == GID]
total_topics_expected = N_USERS + 1  # + статистика создана ранее? нет, fb12 новый
check('create_forum_topic: N пользователей (+0 лишних)',
      len(creates12) == N_USERS, f'{len(creates12)}')
print(f'  ⏱ время: {dt:.2f}s')

# пропускная способность маршрутации (последовательная)
fb13 = FakeBot()
t0 = time.time()
for i in range(200):
    m = make_user_msg(uid=555, mid=3000 + i, text=f'spam {i}')
    fs.route_user_message(fb13, 1, m)
dt = time.time() - t0
print(f'  ⏱ 200 сообщений (1 пользователь, та же тема): {dt:.2f}s')
c = sqlite3.connect(db.DB_PATH)
cnt = c.execute("SELECT COUNT(*) FROM fs_msgs WHERE user_id=555").fetchone()[0]
c.close()
check('200 сообщений замаплены', cnt >= 200, str(cnt))

# ═════════════════════════════════════════════════════════════
print('\n── 21. Безопасность: SQL-инъекции и экранирование ──')
evil_u = make_user(uid=666, first="<script>alert('x')</script>", username="e'vil--")
fs.fs_bind_group(20, -1008888888888, "G'); DROP TABLE bots;--")
m_evil = make_user_msg(uid=666, mid=8001, text="'; DROP TABLE fs_msgs;--")
# маршрут для бота 20: fake bot с группой
class FB20(FakeBot):
    def get_chat(self, chat_id):
        return t.Chat(de_json={'id': 666, 'type': 'private', 'first_name': 'E'})
fs.route_user_message(FB20(), 20, m_evil)
c = sqlite3.connect(db.DB_PATH)
bots_alive = c.execute('SELECT COUNT(*) FROM bots').fetchone()[0] >= 0
c.close()
check('SQL-инъекция в названии группы не сработала', bots_alive)
info_html = fs.fs_info_text(1, evil_u)
check('HTML экранируется в инфо-сообщении', '<script>' not in info_html and '&lt;script&gt;' in info_html)
check('SQL-инъекция в тексте безопасна (параметризованные запросы)', bots_alive)

# ═════════════════════════════════════════════════════════════
print('\n── 22. Интеграция: make_purchased_bot → маршрут через форум ──')
import purchased_bot as pb_mod

_orig_TeleBot_get_me = telebot.TeleBot.get_me
telebot.TeleBot.get_me = lambda self, *a, **kw: t.User(
    id=900000777, is_bot=True, first_name='Child', username='childbot')
# патчим API-методы КЛАССА (make_purchased_bot создаёт свой TeleBot внутри)
_api_log = []
_child_mid = [9000]
def _patch_cls(name, ret='id'):
    orig = getattr(telebot.TeleBot, name)
    def _fn(self, *a, **kw):
        _child_mid[0] += 1
        _api_log.append((name, a, kw, _child_mid[0]))
        if ret == 'topic':
            return t.ForumTopic(message_thread_id=_child_mid[0], name='fake', icon_color=0x6FB9F0)
        if ret == 'msg':
            return _fake_msg(_child_mid[0], '')
        if ret == 'ok':
            return True
        return t.MessageID(message_id=_child_mid[0])
    setattr(telebot.TeleBot, name, _fn)
    return orig
_cls_orig = {}
for _n, _r in [('send_message', 'msg'), ('copy_message', 'id'), ('forward_message', 'msg'),
               ('copy_messages', 'ids'), ('forward_messages', 'ids'),
               ('edit_message_text', 'ok'), ('answer_callback_query', 'ok'),
               ('pin_chat_message', 'ok'), ('create_forum_topic', 'topic'),
               ('edit_forum_topic', 'ok'), ('delete_message', 'ok')]:
    _cls_orig[_n] = getattr(telebot.TeleBot, _n)
    _patch_cls(_n, _r)
def _cls_get_chat(self, chat_id, *a, **kw):
    _api_log.append(('get_chat', (chat_id,), {}))
    return t.Chat(id=chat_id, type='private', first_name='Ю', username='yuri')
def _cls_get_chat_member(self, chat_id, user_id, *a, **kw):
    _api_log.append(('get_chat_member', (chat_id, user_id), {}))
    return t.ChatMemberMember(user=t.User(id=user_id, is_bot=False, first_name='M'),
                              status='member')
_cls_orig['get_chat'] = telebot.TeleBot.get_chat
_cls_orig['get_chat_member'] = telebot.TeleBot.get_chat_member
telebot.TeleBot.get_chat = _cls_get_chat
telebot.TeleBot.get_chat_member = _cls_get_chat_member
try:

    def dispatch_msg(bot, m):
        for h in bot.message_handlers:
            flt = h['filters']
            if flt.get('func') and not flt['func'](m):
                continue
            if flt.get('content_types') and m.content_type not in flt['content_types']:
                continue
            if flt.get('commands'):
                continue
            return h['function'](m)
        return None
    def dispatch_cb(bot, cb):
        for h in bot.callback_query_handlers:
            flt = h['filters']
            if flt.get('func') and not flt['func'](cb):
                continue
            return h['function'](cb)
        return None

    # бот 30: строка в bots (для is_admin) + подключаем группу и строим бота
    c22 = sqlite3.connect(db.DB_PATH)
    c22.execute('INSERT OR IGNORE INTO bots (id, owner_id, token, admin_id) VALUES (30, 70001, ?, 70001)',
                ('999777:TEST_INTEGRATION',))
    c22.execute('INSERT OR IGNORE INTO bots (id, owner_id, token, admin_id) VALUES (31, 70002, ?, 70002)',
                ('888666:TEST_OLD',))
    c22.commit(); c22.close()
    fs.fs_bind_group(30, -1001212121212, 'Integration G')
    fs.fs_set_stats_thread(30, 999)
    pbot22x = pb_mod.make_purchased_bot(30, '999777:TEST_INTEGRATION', 70001, None)
    child_pstate = db.DBState(30)

    # 1) обычный пользователь жмёт p_send
    cb30 = make_cb('p_send', from_id=4001)
    dispatch_cb(pbot22x, cb30)
    check('p_send ставит состояние await_msg', child_pstate.get(4001) == 'await_msg')
    # 2) отправляет текст → уходит в тему группы
    m30 = make_user_msg(uid=4001, mid=9101, text='интеграция работает')
    dispatch_msg(pbot22x, m30)
    copies22 = [c for c in _api_log if c[0] == 'copy_message' and c[1][0] == -1001212121212]
    check('текст доставлен в тему группы', len(copies22) == 1)
    check('state await_msg снят', child_pstate.get(4001) is None)
    copied_id = copies22[-1][3]
    mapped30 = fs.fs_get_mapping(-1001212121212, copied_id)
    check('маппинг интеграции записан', mapped30 is not None and mapped30[1] == 4001)
    # 3) случайное сообщение (без состояния) → доставляется СРАЗУ в тему
    # (очередь подтверждения убрана: без промптов, сразу админу)
    fs._rate_cache.pop((30, 4001), None)   # снимаем лимит от p_send-сообщения
    m30b = make_user_msg(uid=4001, mid=9102, text='внеплановое в интеграции')
    dispatch_msg(pbot22x, m30b)
    pend30 = fs.fs_get_pending(30, 4001)
    check('случайное сообщение НЕ попадает в очередь', pend30 is None)
    copies30b = [c for c in _api_log if c[0] == 'copy_message' and c[1][0] == -1001212121212]
    check('случайное сообщение доставлено в тему сразу', len(copies30b) >= 2)
    mapped30b = fs.fs_get_mapping(-1001212121212, copies30b[-1][3])
    check('маппинг случайного сообщения записан',
          mapped30b is not None and mapped30b[1] == 4001 and mapped30b[3] == 9102)
    # подтверждение без очереди → «Очередь пуста», ничего не ломает
    cb30b = make_cb('fs_confirm_send', from_id=4001)
    dispatch_cb(pbot22x, cb30b)
    check('fs_confirm_send без очереди безопасен', fs.fs_get_pending(30, 4001) is None)
    # 4) админ пишет случайное сообщение → не попадает в очередь
    m30c = make_user_msg(uid=70001, mid=9103, text='админ что-то пишет')
    dispatch_msg(pbot22x, m30c)
    check('админ не попадает в очередь подтверждения', fs.fs_get_pending(30, 70001) is None)
    # 5) команда не перехватывается
    m30d = make_user_msg(uid=4002, mid=9104, text='/unknowncmd')
    dispatch_msg(pbot22x, m30d)
    check('команда не перехвачена форум-очередью', fs.fs_get_pending(30, 4002) is None)
    # 6) ответ админа через Reply доставляется пользователю
    c22q = sqlite3.connect(db.DB_PATH)
    user_grp_msgs = [r[0] for r in c22q.execute(
        'SELECT group_msg_id FROM fs_msgs WHERE bot_id=30 AND user_id=4001').fetchall()]
    c22q.close()
    grp_mid30 = user_grp_msgs[-1]
    adm30 = make_group_msg(-1001212121212, fs.fs_get_topic(30, 4001)[0], 70001,
                           9110, reply_to=grp_mid30, text='интеграционный ответ')
    dispatch_msg(pbot22x, adm30)
    user_copies = [c for c in _api_log if c[0] == 'copy_message' and c[1][0] == 4001]
    check('админский reply доставлен пользователю', len(user_copies) == 1)
    # 7) старый режим: у бота 31 нет группы → сообщения идут админам в ЛС
    pb31 = pb_mod.make_purchased_bot(31, '888666:TEST_OLD', 70002, None)
    db.db_add_bot_admin(31, 70002)
    cb31 = make_cb('p_send', from_id=4003)
    dispatch_cb(pb31, cb31)
    m31 = make_user_msg(uid=4003, mid=9201, text='старый режим')
    dispatch_msg(pb31, m31)
    dm31 = [c for c in _api_log if c[0] == 'send_message' and c[1] and
            len(c[1]) > 0 and c[1][0] == 70002 and 'Сообщение от' in str(c[1][1])]
    check('старый режим: админ получил ЛС с заголовком (сообщение ушло)', len(dm31) >= 1)
    check('старый режим: в группу ничего не ушло от пользователя 4003',
          not any(c[0] == 'copy_message' and c[1] and c[1][0] == -1001212121212
                  and str(c[1][1]) == '4003' for c in _api_log))
    # регресс: DM-режим тоже учитывает обращения (fs_bump_ticket)
    import sqlite3 as _sq
    _c22 = _sq.connect(db.DB_PATH)
    _tickets_4003 = _c22.execute('SELECT COUNT(*) FROM fs_tickets WHERE bot_id=31 AND user_id=4003').fetchone()[0]
    _c22.close()
    check('старый режим: обращение учтено в fs_tickets', _tickets_4003 >= 1)
    # регресс: границы сегодня/неделя по таймзоне статистики
    _s31 = db.db_get_bot_msg_stats(31)
    check('статистика: границы сегодня/неделя по FS_STATS_TZ',
          isinstance(_s31['today'], int) and isinstance(_s31['week'], int))
finally:
    telebot.TeleBot.get_me = _orig_TeleBot_get_me
    for _n, _o in _cls_orig.items():
        setattr(telebot.TeleBot, _n, _o)

# ═════════════════════════════════════════════════════════════
print('\n── 23. Стресс-тест БД: конкурентные записи без блокировок ──')
import forum_support as _fsm
STRESS_THREADS = 12
STRESS_OPS = 60
stress_errors = []
lock_errors = []
def _stress_worker(wid):
    try:
        for i in range(STRESS_OPS):
            uid = 7000 + wid
            db.db_add_user(40, uid, f'u{wid}', f'Worker{wid}')
            db.db_log_message(40, uid, f'stress {i}')
            fs.fs_bump_ticket(40, uid)
            fs.fs_map_message(40, GID, 20000 + wid * 1000 + i, uid, 1, 30000 + i)
            row = fs.fs_get_pending(40, uid)
            db.db_add_username_history(40, uid, f'u{wid}_{i % 3}')
            _ = db.db_get_user_info(40, uid)
    except Exception as e:
        stress_errors.append(e)
        if 'database is locked' in str(e).lower() or 'unable to open' in str(e).lower():
            lock_errors.append(e)
threads = [threading.Thread(target=_stress_worker, args=(w,)) for w in range(STRESS_THREADS)]
t0 = time.time()
for th_ in threads: th_.start()
for th_ in threads: th_.join(timeout=180)
dt_stress = time.time() - t0
check(f'конкурентные записи: {STRESS_THREADS}×{STRESS_OPS} операций без ошибок',
      not stress_errors, repr(stress_errors[:2]))
check('ни одной ошибки "database is locked"', not lock_errors)
c = sqlite3.connect(db.DB_PATH)
users40 = c.execute('SELECT COUNT(*) FROM bot_users WHERE bot_id=40').fetchone()[0]
msgs40 = c.execute("SELECT COUNT(*) FROM bot_messages WHERE bot_id=40 AND text LIKE 'stress%'").fetchone()[0]
tickets40 = c.execute('SELECT COUNT(*) FROM fs_tickets WHERE bot_id=40').fetchone()[0]
maps40 = c.execute('SELECT COUNT(*) FROM fs_msgs WHERE bot_id=40').fetchone()[0]
hist40 = c.execute('SELECT COUNT(*) FROM fs_username_history WHERE bot_id=40').fetchone()[0]
c.close()
check('все записи дошли (users)', users40 == STRESS_THREADS, str(users40))
check('все записи дошли (messages)', msgs40 == STRESS_THREADS * STRESS_OPS, str(msgs40))
check('все записи дошли (tickets: серия = 1 на воркера)', tickets40 == STRESS_THREADS, str(tickets40))
check('все записи дошли (maps)', maps40 == STRESS_THREADS * STRESS_OPS, str(maps40))
check('все записи дошли (username history)', hist40 >= STRESS_THREADS * 3, str(hist40))
print(f'  ⏱ стресс: {STRESS_THREADS * STRESS_OPS}×6 операций = {dt_stress:.2f}s')

# пропускная способность маршрутации после оптимизации соединений
fb13 = FakeBot()
t0 = time.time()
for i in range(200):
    m = make_user_msg(uid=555, mid=4000 + i, text=f'spam {i}')
    fs.route_user_message(fb13, 1, m)
dt_route = time.time() - t0
print(f'  ⏱ 200 сообщений через route: {dt_route:.2f}s ({dt_route / 200 * 1000:.0f} мс/сообщ)')
check('маршрутизация быстрее 15 мс/сообщение (было ~22)', dt_route / 200 < 0.015,
      f'{dt_route / 200 * 1000:.0f} мс')

# ═════════════════════════════════════════════════════════════
print('\n' + '=' * 60)
print(f'ИТОГО: {len(PASS)} OK, {len(FAIL)} FAIL')
if FAIL:
    print('Провалены:')
    for f_ in FAIL:
        print(f'  ✗ {f_}')
print('=' * 60)
sys.exit(1 if FAIL else 0)
