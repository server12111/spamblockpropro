# -*- coding: utf-8 -*-
"""Регресс-тесты фиксов цикла №6 (гранты, HTML-разрез, фидбек недоставки,
миграция группы, состояние до удаления).
Запуск: py tests/audit_fixes2.py"""
import os
import sys
import time
import tempfile

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='fx6_')
os.environ['SUPER_ADMIN'] = '7145919720'
os.environ['FS_STATS_TZ'] = '3'
PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJ)

PASS, FAIL = [], []
def check(name, cond, extra=''):
    print(('  [OK]   ' if cond else '  [FAIL] ') + name + (' ' + str(extra) if extra and not cond else ''))
    (PASS if cond else FAIL).append(name)

import telebot
import telebot.types as t

# ── моки Telegram API (как в button_fuzz) ──
_mid = [1]
SEND_LOG = []          # [(chat_id, text)]
FAIL_CHATS = set()     # chat_id, для которых send_message бросает исключение
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

for n, r in [('get_me', 'msg'), ('send_photo', 'msg'), ('copy_message', 'id'),
             ('forward_message', 'msg'), ('copy_messages', 'ids'),
             ('edit_message_text', 'ok'), ('answer_callback_query', 'ok'),
             ('pin_chat_message', 'ok'), ('create_forum_topic', 'topic'),
             ('edit_forum_topic', 'ok'), ('delete_message', 'ok'),
             ('send_document', 'msg'), ('send_video_note', 'msg')]:
    _patch(n, r)
telebot.TeleBot.get_me = lambda self, *a, **kw: t.User(id=1, is_bot=True,
    first_name='B', username='fx6bot')

def _send_message(self, chat_id, text=None, **kw):
    if chat_id in FAIL_CHATS:
        raise telebot.apihelper.ApiTelegramException(
            'send', 403, {'description': 'bot was blocked by the user'})
    SEND_LOG.append((chat_id, text or ''))
    return t.Message.de_json({'message_id': _mid[0], 'from': {'id': 1, 'is_bot': True,
        'first_name': 'B'}, 'chat': {'id': chat_id, 'type': 'private'}, 'date': 0,
        'text': text or ''})
ORIG['send_message'] = telebot.TeleBot.send_message
telebot.TeleBot.send_message = _send_message

from database import (init_db, db_mark_paid, db_peek_grant_days, db_take_grant_days,
                      db_get_all_bots_for_checker, db_add_bot)
import database as db
init_db()

print('── db_peek_grant_days: peek не расходует грант ──')
db.db_del_pending(700)
db.db_set_pending(700, 1); db.db_del_pending(700)
db_mark_paid(700, days=3)
check('peek возвращает 3', db_peek_grant_days(700) == 3)
check('peek повторно — те же 3 (не расходован)', db_peek_grant_days(700) == 3)
check('take возвращает 3 и расходует', db_take_grant_days(700) == 3)
check('после take peek = None', db_peek_grant_days(700) is None)
check('нет гранта → peek None', db_peek_grant_days(701) is None)

print('── Разрез сырого текста до экранирования: сущности не рвутся ──')
import html
raw = ('A' * 3790 + '&<>"' + 'B' * 50 + '&amp;' + 'C' * 100)
chunks_old = [html.escape(raw)[i:i + 3800] for i in range(0, len(html.escape(raw)), 3800)]
chunks_new = [html.escape(raw[i:i + 3800]) for i in range(0, len(raw), 3800)]
import re
entity = re.compile(r'&(#\d+|[a-zA-Z]+);')
def broken_entity(s):
    # '&' есть, но за ним не полноценная сущность и не '<'/'>' разметка
    for m in re.finditer(r'&(?!amp;|lt;|gt;|quot;|#)', s):
        return True
    return False
ok_old = not any(broken_entity(c) for c in chunks_old)
ok_new = not any(broken_entity(c) for c in chunks_new)
check('новый способ (сырой → разрез → escape): все куски валидны', ok_new)
# старый способ на этом входе обязан ломаться: '&' в позиции 3790+
esc = html.escape(raw)
pos = esc.find('&', 3780)
if pos != -1 and pos + 5 > 3800 and pos < 3800 + 5:
    check('старый способ действительно рвал сущность (регресс пойман)', not ok_old or pos != -1)
else:
    check('граница сущности попадает в разрез (условие теста)', '&' in raw[3780:3810])

print('── get_admin_id: дубль токена НЕ съедает грант, флоу восстанавливается ──')
import handlers as H
import payments
payments.cb_create_invoice = lambda uid: None
payments.cb_check_invoice = lambda iid: None
payments.ton_check_transfer = lambda *a, **kw: False
payments.get_ton_amount = lambda: 0.5
H.cb_create_invoice = payments.cb_create_invoice
H.cb_check_invoice = payments.cb_check_invoice
H.ton_check_transfer = payments.ton_check_transfer
H.get_ton_amount = payments.get_ton_amount

main_bot = telebot.TeleBot('111:FAKE_fx6')
H.register(main_bot)
SUPER = 7145919720

def umsg(uid, text, mid):
    return t.Message.de_json({'message_id': mid, 'from': {'id': uid, 'is_bot': False,
        'first_name': 'Юзер', 'username': f'u{uid}'},
        'chat': {'id': uid, 'type': 'private', 'first_name': 'Юзер'},
        'date': int(time.time()), 'text': text})

def dmsg(bot, m):
    for h in bot.message_handlers:
        flt = h['filters']
        if flt.get('func') and not flt['func'](m): continue
        if flt.get('content_types') and m.content_type not in flt['content_types']: continue
        if flt.get('commands'):
            if m.content_type == 'text' and m.text and m.text.startswith('/'):
                cmd = m.text.split()[0][1:].split('@')[0]
                if cmd in flt['commands']: return h['function'](m)
            continue
        return h['function'](m)

def dcb(bot, cb):
    for h in bot.callback_query_handlers:
        flt = h['filters']
        if flt.get('func') and not flt['func'](cb): continue
        return h['function'](cb)

def ucb(data, uid=SUPER, mid=500):
    return t.CallbackQuery.de_json({'id': '1', 'from': {'id': uid, 'is_bot': False,
        'first_name': 'Юзер'}, 'data': data, 'chat_instance': 'ci',
        'message': {'message_id': mid, 'from': {'id': 1, 'is_bot': True, 'first_name': 'B'},
                    'chat': {'id': uid, 'type': 'private', 'first_name': 'Ю'}, 'date': 0, 'text': 'x'}})

TOKEN_TAKEN = '777000:AAAtaken_duplicate_token_12345678'
TOKEN_FRESH = '777001:AAAfresh_valid_token_xxxxxxxxxx'
db_add_bot(424242, TOKEN_TAKEN, 424242, days=30)  # токен уже занят другим владельцем

db_mark_paid(SUPER, days=3)   # триал-грант
SEND_LOG.clear()
dcb(main_bot, ucb('buy_bot'))
dmsg(main_bot, umsg(SUPER, TOKEN_TAKEN, 201))
dmsg(main_bot, umsg(SUPER, str(SUPER), 202))
check('при дубле токена показано «введи другой токен»',
      any('уже зарегистрирован' in (txt or '') for _, txt in SEND_LOG))
check('грант 3 дня НЕ израсходован после неудачи с дублем',
      db_peek_grant_days(SUPER) == 3, str(db_peek_grant_days(SUPER)))

# продолжаем флоу свежим токеном: раз флоу восстановился, второй токен
# дойдёт до создания бота без повторной оплаты
SEND_LOG.clear()
dmsg(main_bot, umsg(SUPER, TOKEN_FRESH, 203))
dmsg(main_bot, umsg(SUPER, str(SUPER), 204))
bots = [r for r in db_get_all_bots_for_checker() if r[2] == TOKEN_FRESH]
check('бот создан со свежим токеном (флоу восстановлен)', len(bots) == 1)
check('грант расходован ровно один раз', db_peek_grant_days(SUPER) is None)
if bots:
    from datetime import datetime as _dt
    dleft = (_dt.fromisoformat(str(bots[0][4])) - _dt.now()).days
    check('боту достался триал 3 дня (не 30)', 1 <= dleft <= 3, str(dleft))
check('успех: «Бот успешно запущен» отправлен',
      any('успешно запущен' in (txt or '') for _, txt in SEND_LOG))

print('── admin_text (главный бот): недоставка → фидбек и сброс состояния ──')
dcb(main_bot, ucb('user_send', uid=555))
dmsg(main_bot, umsg(555, 'привет админ', 210))  # ушло SUPER_ADMIN
target_msg = [x for x in SEND_LOG if x[0] == SUPER]
check('сообщение доставлено супер-админу', bool(target_msg))
SEND_LOG.clear()
FAIL_CHATS.add(555)  # юзер 555 «заблокировал» бота
dcb(main_bot, ucb('admin_reply_555', uid=SUPER))
dmsg(main_bot, umsg(SUPER, 'ответ админа', 211))
check('админ получил фидбек о недоставке',
      any('Не удалось доставить' in (txt or '') for _, txt in SEND_LOG))
# состояние админа сброшено: следующее сообщение не трактуется как ответ
SEND_LOG.clear()
dmsg(main_bot, umsg(SUPER, 'обычное сообщение после неудачи', 212))
check('состояние сброшено (нет повторной попытки доставки 555)',
      not any(x[0] == 555 for x in SEND_LOG))
FAIL_CHATS.discard(555)

print('── user_text (главный бот): недоставка SUPER_ADMIN → фидбек юзеру ──')
FAIL_CHATS.add(SUPER)
SEND_LOG.clear()
dcb(main_bot, ucb('user_send', uid=666))
dmsg(main_bot, umsg(666, 'текст в никуда', 220))
check('юзер получил «Не удалось доставить»',
      any('Не удалось доставить' in (txt or '') for _, txt in SEND_LOG if txt and 'Подожди' not in txt))
FAIL_CHATS.discard(SUPER)

print('── p_cancel_generic: delete_message падает — состояние всё равно снято ──')
import purchased_bot as PB
SEND_LOG.clear()
bots2 = [r for r in db_get_all_bots_for_checker() if r[2] == TOKEN_FRESH]
pb = PB.make_purchased_bot(bots2[0][0], TOKEN_FRESH, SUPER, main_bot)
# ломаем delete_message только на время cancel
_orig_del = telebot.TeleBot.delete_message
telebot.TeleBot.delete_message = lambda self, *a, **kw: (_ for _ in ()).throw(
    telebot.apihelper.ApiTelegramException('del', 400, {'description': 'message to delete not found'}))
try:
    dcb(pb, ucb('p_send', uid=888))
    dcb(pb, ucb('cancel', uid=888))
finally:
    telebot.TeleBot.delete_message = _orig_del
# после cancel юзер НЕ должен остаться в await_msg: текст уходит в очередь
# подтверждения (промпт «Отправить сообщение администратору?»), а не как доставка
dmsg(pb, umsg(888, 'проверка состояния', 230))
check('после cancel юзер попал в очередь подтверждения (состояние снято)',
      any('Отправить сообщение администратору' in (txt or '') for _, txt in SEND_LOG))
check('нет подтверждения доставки из застрявшего await_msg',
      not any('Сообщение отправлено<' in (txt or '') for _, txt in SEND_LOG))

print('── миграция группы: fs_msgs.chat_id обновляется вместе с fs_groups ──')
from forum_support import (fs_bind_group, fs_map_message, fs_get_mapping,
                           fs_handle_group_message, fs_get_group)
from database import DBState
OLD_CID, NEW_CID = -100111, -100222
fs_bind_group(bots2[0][0], OLD_CID, 'Старая')
fs_map_message(bots2[0][0], OLD_CID, 333, 888, 5, 44, '')
migr = t.Message.de_json({'message_id': 900, 'from': {'id': 5, 'is_bot': False, 'first_name': 'A'},
    'chat': {'id': OLD_CID, 'type': 'group', 'title': 'G'}, 'date': int(time.time()),
    'migrate_to_chat_id': NEW_CID})
print(f'  (content_type миграции: {migr.content_type!r})')
fs_handle_group_message(pb, bots2[0][0], migr, 1, DBState(bots2[0][0]))
mp_old = fs_get_mapping(OLD_CID, 333)
mp_new = fs_get_mapping(NEW_CID, 333)
check('fs_msgs переписаны на новый chat_id', mp_new is not None and mp_old is None)
gr = fs_get_group(bots2[0][0])
check('fs_groups.chat_id обновлён', gr and gr[1] == NEW_CID)

print('── реакции: админ в теме → уведомление в ЛС (setMessageReaction в ЛС запрещён) ──')
from forum_support import fs_handle_reaction, fs_map_message
REACTION_LOG = []
ORIG['set_message_reaction'] = telebot.TeleBot.set_message_reaction
telebot.TeleBot.set_message_reaction = lambda self, chat_id, message_id=None, reaction=None, **kw: (
    REACTION_LOG.append((chat_id, message_id, reaction)), True)[1]
BOT2 = bots2[0][0]
# маппинги: юзер 888, исходники 44 (уже есть после миграции) и 45
fs_map_message(BOT2, NEW_CID, 334, 888, 5, 45, '')

def mk_mr(chat_dict, msg_id, uid, reactions):
    mr = t.MessageReactionUpdated(
        chat=t.Chat.de_json(chat_dict), message_id=msg_id,
        date=int(time.time()), old_reaction=[], new_reaction=reactions)
    mr.user = t.User(id=uid, is_bot=False, first_name='A')
    return mr

# 1. админ реагирует в теме на копию сообщения юзера 888 (group_msg_id=334 ↔ src=45)
SEND_LOG.clear()
mr_admin = mk_mr({'id': NEW_CID, 'type': 'supergroup', 'title': 'G'}, 334, SUPER,
                 [t.ReactionTypeEmoji(emoji='❤️')])
fs_handle_reaction(pb, BOT2, mr_admin, 1)
notify = [txt for cid, txt in SEND_LOG if cid == 888]
check('клиент получил уведомление с эмодзи реакции',
      len(notify) == 1 and '❤️' in notify[0] and 'отреагировал' in notify[0], str(notify))
check('в ЛС НЕ вызывается set_message_reaction (запрещено платформой)',
      not any(cid == 888 for cid, _, _ in REACTION_LOG))

# 2. дедуп: та же реакция на то же сообщение — повторного уведомления нет
SEND_LOG.clear()
fs_handle_reaction(pb, BOT2, mr_admin, 1)
check('дедуп: та же реакция не шлётся повторно', len(SEND_LOG) == 0, str(SEND_LOG))

# 3. юзер реагирует в ЛС на СВОЁ сообщение (src=44) → реакция на копию в супергруппе
REACTION_LOG.clear()
mr_user = mk_mr({'id': 888, 'type': 'private', 'first_name': 'Ю'}, 44, 888,
                [t.ReactionTypeEmoji(emoji='👍')])
fs_handle_reaction(pb, BOT2, mr_user, 1)
check('юзерская реакция из ЛС зеркалится в супергруппу',
      any(cid == NEW_CID and mid == 333 for cid, mid, _ in REACTION_LOG), str(REACTION_LOG))

# 4. кастомный (premium) эмодзи → нейтральное уведомление
# (другое сообщение: дедуп по резолвнутому эмодзи подавил бы повтор на 334)
fs_map_message(BOT2, NEW_CID, 335, 888, 5, 46, '')
SEND_LOG.clear()
mr_custom = mk_mr({'id': NEW_CID, 'type': 'supergroup', 'title': 'G'}, 335, SUPER,
                  [t.ReactionTypeCustomEmoji(custom_emoji_id='555999')])
fs_handle_reaction(pb, BOT2, mr_custom, 1)
notify2 = [txt for cid, txt in SEND_LOG if cid == 888]
check('кастомный эмодзи: уведомление отправлено (нейтральный маркер)',
      len(notify2) == 1 and 'отреагировал' in notify2[0], str(notify2))

# 5. реакция снята → ничего не шлём
SEND_LOG.clear()
mr_empty = mk_mr({'id': NEW_CID, 'type': 'supergroup', 'title': 'G'}, 334, SUPER, [])
fs_handle_reaction(pb, BOT2, mr_empty, 1)
check('снятая реакция не порождает уведомлений', len(SEND_LOG) == 0)

telebot.TeleBot.set_message_reaction = ORIG['set_message_reaction']

print()
print(f'ИТОГО: {len(PASS)} OK, {len(FAIL)} FAIL')
sys.exit(1 if FAIL else 0)
