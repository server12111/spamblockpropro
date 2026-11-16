# -*- coding: utf-8 -*-
"""Оформление тем: настоящий код/временная SQLite, Telegram полностью подменён."""
import importlib
import logging
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace as NS
from unittest.mock import patch
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.stdout.reconfigure(encoding='utf-8')
os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='topics_style_', dir=ROOT / 'tests')
os.environ['SUPER_ADMIN'] = '777'
sys.modules['dotenv'] = NS(load_dotenv=lambda *a, **kw: None)

import telebot
from telebot import types as t
from telebot.apihelper import ApiTelegramException
import database as db
import forum_support as fs

CALLS, WARNINGS = [], []
GONE = set()
STICKERS = []
FAIL_LIST = FAIL_EDIT = FAIL_CREATE_ICON = False
MID = 100
BOT_SEQ = 1000
OK = FAIL = 0


def error(text):
    return ApiTelegramException('test', None, dict(ok=False, error_code=400, description=text))


def api(name):
    def call(self, *args, **kw):
        global MID
        CALLS.append((name, args, kw))
        if name == 'get_forum_topic_icon_stickers':
            if FAIL_LIST:
                raise error('not enough rights')
            return STICKERS
        if name == 'get_me':
            return t.User(999, True, 'Бот', username='test_bot')
        if name == 'get_chat':
            return NS(id=args[0], type='supergroup', is_forum=True, title='Поддержка')
        if name == 'get_chat_member':
            return NS(status='administrator', can_manage_topics=True,
                      can_pin_messages=True, can_delete_messages=True)
        if name == 'create_forum_topic':
            if FAIL_CREATE_ICON and kw.get('icon_custom_emoji_id'):
                raise error('CUSTOM_EMOJI_INVALID')
            MID += 1
            return t.ForumTopic(MID, args[1], kw.get('icon_color', 0x6FB9F0))
        if name == 'edit_forum_topic' and FAIL_EDIT:
            raise error('not enough rights')
        if name == 'send_message':
            if kw.get('message_thread_id') in GONE:
                raise error('message thread not found')
            MID += 1
            return NS(message_id=MID)
        return True
    return call


class Capture(logging.Handler):
    def emit(self, record):
        WARNINGS.append(record.getMessage())


def calls(name):
    return [c for c in CALLS if c[0] == name]


def fresh(legacy=False):
    global BOT_SEQ
    BOT_SEQ += 1
    bid = db.db_add_bot(777, f'{BOT_SEQ}:TEST', 777)
    bot = telebot.TeleBot('123:TEST', threaded=False)
    gid = -10000 - bid
    if legacy:
        fs.fs_bind_group(bid, gid, 'Поддержка')
        c = db._conn()
        c.execute('UPDATE fs_groups SET thread_stats=41, thread_mgmt=42, mgmt_msg_id=43 '
                  'WHERE bot_id=?', (bid,))
        c.commit(); c.close()
    return bid, bot, gid


def connect():
    bid, bot, gid = fresh()
    fs._finish_connect(bot, bid, 777, gid, 'Поддержка', {})
    assert fs.fs_get_group(bid), 'Группа не подключена'
    return bid, bot, gid


def creation():
    bid, bot, gid = connect()
    created = calls('create_forum_topic')
    assert [(c[1][1], c[2]) for c in created] == [
        ('Статистика', dict(icon_color=0xFF93B2, icon_custom_emoji_id='stats')),
        ('Управление', dict(icon_color=0x6FB9F0, icon_custom_emoji_id='gear'))]
    assert len(calls('get_forum_topic_icon_stickers')) == 1
    fs.fs_refresh_management(bot, bid)
    connect()  # другой бот использует тот же кэш процесса
    assert len(calls('get_forum_topic_icon_stickers')) == 1
    assert not calls('edit_forum_topic'), 'Новые темы уже оформлены при создании'
    CALLS.clear()
    user = t.User(987, False, 'Пользователь')
    fs._get_or_create_topic(bot, bid, gid, user)
    assert len(calls('create_forum_topic')) == 1
    assert not calls('create_forum_topic')[0][2], 'Обычная тема без параметров иконки'
    assert not calls('edit_forum_topic')


def migration():
    global fs
    bid, bot, gid = fresh(legacy=True)
    fs.register_forum_support(bot, bid, {}, lambda uid: uid == 777)
    edits = calls('edit_forum_topic')
    assert {(c[1], tuple(sorted(c[2].items()))) for c in edits} == {
        ((gid, 41), (('icon_custom_emoji_id', 'stats'), ('name', 'Статистика'))),
        ((gid, 42), (('icon_custom_emoji_id', 'gear'), ('name', 'Управление')))}
    assert not calls('create_forum_topic')
    # Перезагрузка модуля сбрасывает память, но отметки должны остаться в SQLite.
    fs = importlib.reload(fs)
    fs.register_forum_support(bot, bid, {}, lambda uid: uid == 777)
    fs.fs_refresh_management(bot, bid)
    assert len(calls('edit_forum_topic')) == 2, 'Миграция повторилась после перезапуска'
    fs.fs_unbind_group(bid)
    fs.fs_bind_group(bid, gid - 1000, 'Новая группа')
    c = db._conn()
    c.execute('UPDATE fs_groups SET thread_stats=41, thread_mgmt=42 WHERE bot_id=?', (bid,))
    c.commit(); c.close()
    fs.fs_refresh_management(bot, bid)
    assert len(calls('edit_forum_topic')) == 4, 'Новая привязка требует своего оформления'


def denied():
    global FAIL_EDIT
    bid, bot, gid = fresh(legacy=True)
    FAIL_EDIT = True
    fs.fs_refresh_management(bot, bid)
    fs.fs_refresh_management(bot, bid)
    assert len(calls('edit_forum_topic')) == 2, 'По одной попытке на каждую старую тему'
    assert len(WARNINGS) >= 2
    assert fs.fs_get_group(bid)[3] == 41 and fs._mgmt_row(bid)[1] == 42
    assert not calls('create_forum_topic'), 'Отказ прав не должен пересоздавать темы'
    FAIL_EDIT = False


def fallback():
    global FAIL_CREATE_ICON
    FAIL_CREATE_ICON = True
    connect()
    successful = [c for c in calls('create_forum_topic') if not c[2].get('icon_custom_emoji_id')]
    assert [(c[1][1], c[2]) for c in successful] == [
        ('📊 Статистика', dict(icon_color=0xFF93B2)),
        ('⚙️ Управление', dict(icon_color=0x6FB9F0))]
    assert len(WARNINGS) >= 2
    FAIL_CREATE_ICON = False


def missing():
    connect()
    assert [c[1][1] for c in calls('create_forum_topic')] == ['📊 Статистика', '⚙️ Управление']
    assert all('icon_custom_emoji_id' not in c[2] for c in calls('create_forum_topic'))
    assert WARNINGS


def unavailable():
    global FAIL_LIST
    FAIL_LIST = True
    missing()
    connect()
    assert len(calls('get_forum_topic_icon_stickers')) == 1
    FAIL_LIST = False


def alternatives():
    connect()
    assert [c[2].get('icon_custom_emoji_id') for c in calls('create_forum_topic')] == ['rise', 'tools']


def lazy():
    bid, bot, gid = fresh(legacy=True)
    fs._mgmt_ensure_thread(bot, bid)
    assert any(c[1] == (gid, 42) for c in calls('edit_forum_topic'))


def recreated():
    bid, bot, gid = connect()
    stats, mgmt = fs.fs_get_group(bid)[3], fs._mgmt_row(bid)[1]
    GONE.update((stats, mgmt))
    c = db._conn()
    c.execute('UPDATE fs_groups SET mgmt_msg_id=NULL WHERE bot_id=?', (bid,))
    c.commit(); c.close()
    CALLS.clear()
    fs.fs_refresh_management(bot, bid)
    start = datetime(2026, 11, 2, tzinfo=timezone.utc)
    fs._publish_period(bot, bid, gid, stats, start, start + timedelta(days=7))
    created = calls('create_forum_topic')
    assert [(c[1][1], c[2]) for c in created] == [
        ('Управление', dict(icon_color=0x6FB9F0, icon_custom_emoji_id='gear')),
        ('Статистика', dict(icon_color=0xFF93B2, icon_custom_emoji_id='stats'))]
    assert fs._mgmt_row(bid)[1] != mgmt and fs.fs_get_group(bid)[3] != stats
    assert fs.fs_report_get(bid, '2026-11-02')[0] == 'sent'
    fs.fs_refresh_management(bot, bid)
    assert not calls('edit_forum_topic'), 'Пересозданные темы уже оформлены'


def check(name, fn, stickers):
    global OK, FAIL, STICKERS, FAIL_LIST, FAIL_EDIT, FAIL_CREATE_ICON
    CALLS.clear(); WARNINGS.clear()
    GONE.clear()
    STICKERS = [NS(emoji=em, custom_emoji_id=ident) for em, ident in stickers]
    FAIL_LIST = FAIL_EDIT = FAIL_CREATE_ICON = False
    try:
        with patch.object(fs, '_service_icon_cache', None, create=True):
            fn()
    except Exception as exc:
        FAIL += 1
        print(f'[FAIL] {name}: {type(exc).__name__}: {exc}')
    else:
        OK += 1
        print('[OK] ' + name)


handler = Capture()
fs.log.addHandler(handler)
try:
    # Реальная миграция прежней схемы, init_db должен быть повторяемым.
    c = sqlite3.connect(db.DB_PATH)
    c.execute('CREATE TABLE fs_groups (bot_id INTEGER PRIMARY KEY, chat_id INTEGER UNIQUE, '
              'title TEXT, thread_stats INTEGER, connected_at TIMESTAMP)')
    c.commit(); c.close()
    db.init_db(); db.init_db()
    with ExitStack() as stack:
        stack.enter_context(patch('telebot.apihelper._make_request', side_effect=AssertionError('Сеть запрещена')))
        for name in ('get_me', 'get_chat', 'get_chat_member', 'get_forum_topic_icon_stickers',
                     'create_forum_topic', 'edit_forum_topic', 'send_message',
                     'edit_message_text', 'pin_chat_message'):
            stack.enter_context(patch.object(telebot.TeleBot, name, api(name)))
        preferred = [('📈', 'rise'), ('📝', 'note'), ('🛠️', 'tools'), ('⚙', 'gear'), ('📊', 'stats')]
        check('создание: иконки, цвета, кэш процесса; темы пользователей без иконок', creation, preferred)
        check('старые темы оформляются при запуске один раз, отметки переживают перезапуск', migration, preferred)
        check('первое обращение к старой теме запускает оформление', lazy, preferred)
        check('пересоздание удалённых служебных тем сохраняет иконки, цвета и отчёт', recreated, preferred)
        check('нет прав: предупреждения, без исключений и повторных попыток', denied, preferred)
        check('недоступная иконка при создании: эмодзи в названии и цвет', fallback, preferred)
        check('нет подходящих стикеров: эмодзи в названии', missing, [])
        check('ошибка списка стикеров: безопасный результат кэшируется', unavailable, preferred)
        check('запасные иконки выбираются по приоритету, учитывая вариант без VS16', alternatives,
              [('📝', 'note'), ('🛠', 'tools'), ('📈', 'rise')])
finally:
    fs.log.removeHandler(handler)
print(f'ИТОГО: {OK} OK, {FAIL} FAIL')
sys.exit(1 if FAIL else 0)
