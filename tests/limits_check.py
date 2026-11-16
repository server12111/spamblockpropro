# -*- coding: utf-8 -*-
"""Проверка жёстких лимитов Telegram API на РЕАЛЬНЫХ кнопках/текстах обоих ботов.
Инструментирует InlineKeyboardButton и send_message во время фаззи-прогона:
callback_data ≤64 байт, текст кнопки ≤64 симв., сообщение ≤4096.
Запуск: py tests/limits_check.py"""
import os, sys, tempfile
os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='limits_')
os.environ['SUPER_ADMIN'] = '7145919720'
os.environ['FS_STATS_TZ'] = '3'
PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJ)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import telebot
from telebot.types import InlineKeyboardButton

BTN_LOG, MSG_LOG = [], []
_orig_btn = InlineKeyboardButton.__init__
def _rec_btn(self, text, **kw):
    BTN_LOG.append((text or '', kw.get('callback_data'), kw.get('url')))
    return _orig_btn(self, text, **kw)
InlineKeyboardButton.__init__ = _rec_btn

import button_fuzz  # его патчи send_message применяются первыми

# враппер ПОВЕРХ патча button_fuzz — иначе recorder затирается
_sm_after_fuzz = telebot.TeleBot.send_message
def _rec_sm(self, chat_id, text=None, **kw):
    MSG_LOG.append((chat_id, text or ''))
    return _sm_after_fuzz(self, chat_id, text, **kw)
telebot.TeleBot.send_message = _rec_sm

code = button_fuzz.run()

FAIL = []
btn_cb  = [(t, cb) for t, cb, _ in BTN_LOG if cb]
btn_url = [(t, u) for t, _, u in BTN_LOG if u]
for t, cb in btn_cb:
    if len(cb.encode('utf-8')) > 64:
        FAIL.append(f'callback_data >64 байт: "{cb[:70]}" ({len(cb.encode())}B)')
for t, _ in btn_cb:
    if not (1 <= len(t) <= 64):
        FAIL.append(f'текст кнопки {len(t)} симв.: "{t[:70]}"')
for t, u in btn_url:
    if len(u.encode('utf-8')) > 256:
        FAIL.append(f'url >256 байт: {u[:80]}')
for cid, t in MSG_LOG:
    if len(t.encode('utf-8')) > 4096:
        FAIL.append(f'сообщение {len(t.encode())} байт: "{t[:60]}..."')

from forum_support import fs_topic_title, TOPIC_NAME_LIMIT
class U:
    id = 123
    username = 'a' * 32
    first_name = 'И' * 200
    last_name = None
tt = fs_topic_title(U())
if len(tt) > TOPIC_NAME_LIMIT:
    FAIL.append(f'fs_topic_title {len(tt)} > {TOPIC_NAME_LIMIT}')

print(f'собрано кнопок: {len(BTN_LOG)} (cb={len(btn_cb)}, url={len(btn_url)}), сообщений: {len(MSG_LOG)}')
print(f'fs_topic_title: {len(tt)} симв. (лимит {TOPIC_NAME_LIMIT}) — "{tt[:50]}..."')
if FAIL or code != 0:
    for f in FAIL:
        print('  [FAIL]', f)
    sys.exit(1)
print('ЛИМИТЫ OK: все кнопки/сообщения/темы в пределах Telegram API')
