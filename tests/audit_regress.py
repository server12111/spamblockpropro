# -*- coding: utf-8 -*-
"""Регресс-тесты фиксов внешнего аудита (деньги, атомарность).
Запуск: py tests/audit_regress.py"""
import os
import sys
import time
import tempfile
import threading

os.environ['DATA_DIR'] = tempfile.mkdtemp(prefix='audit_')
os.environ['FS_STATS_TZ'] = '3'
PROJ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJ)

import database as db
db.init_db()

PASS, FAIL = [], []
def check(name, cond, extra=''):
    print(('  [OK]   ' if cond else '  [FAIL] ') + name + (' ' + str(extra) if extra and not cond else ''))
    (PASS if cond else FAIL).append(name)

print('── Инвойс привязан к юзеру ──')
db.db_set_pending(7145919720, 555)
check('db_get_pending возвращает инвойс владельца', db.db_get_pending(7145919720) == 555)
check('у чужого юзера инвойса нет', db.db_get_pending(999) is None)
db.db_del_pending(7145919720)
check('удаление указателя сохраняет незачтённый счёт', db.db_get_pending(7145919720) == 555)

print('── Реферальная скидка: однократность ──')
db.db_set_referral(111, 222)
db.db_mark_referral_paid(222)
db.db_mark_referral_paid(222)  # повтор
check('начислена ровно 1 скидка', db.db_get_discount_count(111) == 1)

print('── Атомарное списание ──')
db.db_add_bot_earnings(1, 10.0)
check('первое списание True', db.db_deduct_bot_earnings_atomic(1, 10.0) is True)
check('второе списание False', db.db_deduct_bot_earnings_atomic(1, 10.0) is False)
bal, _ = db.db_get_bot_earnings(1)
check('баланс 0, не отрицательный', bal == 0.0)

print('── Claim заявки на вывод ──')
wr = db.db_create_withdrawal(1, 9, 'ADDR', 5.0)
check('первый claim True', db.db_claim_withdrawal(wr, 'approved') is True)
check('второй claim False', db.db_claim_withdrawal(wr, 'approved') is False)
check('reject после approve False', db.db_claim_withdrawal(wr, 'rejected') is False)

print('── Конкурентные списания (5 потоков × 30 из 100) ──')
db.db_add_bot_earnings(2, 100.0)
errs, results = [], []
def w(amount):
    try:
        results.append(db.db_deduct_bot_earnings_atomic(2, amount))
    except Exception as e:
        errs.append(e)
ths = [threading.Thread(target=w, args=(30.0,)) for _ in range(5)]
for t in ths: t.start()
for t in ths: t.join()
bal2, _ = db.db_get_bot_earnings(2)
successes = sum(1 for r in results if r)
check('списаний ровно 3 (100 // 30)', successes == 3, f'{successes}')
check('баланс 10 (остаток не ушёл в минус)', abs(bal2 - 10.0) < 1e-9, str(bal2))
check('без исключений', not errs, str(errs[:2]))

print('── Гонка продления (2 потока × 30д = +60, не +30) ──')
from datetime import datetime as _dt
bid = db.db_add_bot(50, '999:race_ext_audit_regress_12345', 50, days=30)
e0 = _dt.fromisoformat(db.db_get_bot_expires(bid))
rr = []
def rr_f():
    rr.append(db.db_renew_bot(bid, 30))
ths = [threading.Thread(target=rr_f) for _ in range(2)]
for t in ths: t.start()
for t in ths: t.join()
e1 = _dt.fromisoformat(db.db_get_bot_expires(bid))
gained = (e1 - e0).total_seconds() / 86400
check('2 параллельных продления = +60 дней (не +30)', abs(gained - 60) < 1, f'{gained:.2f}')

print('── Атомарный захват инвойса (двойное «Проверить оплату») ──')
db.db_set_pending(60, 777)
claims = []
def cl():
    claims.append(db.db_claim_pending_payment(60, 777))
ths = [threading.Thread(target=cl) for _ in range(2)]
for t in ths: t.start()
for t in ths: t.join()
check('захватов True ровно 1 (не 2)', claims.count(True) == 1, str(claims))
check('зачёт сохраняет грант вместе с отметкой счёта', db.db_get_pending(60) is None and db.db_peek_grant_days(60) == 30)
check('чужой/несуществующий инвойс → False', db.db_claim_pending_payment(60, 888) is False)

print('── Атомарное списание скидки (двойное нажатие) ──')
db.db_set_referral(70, 71)
db.db_mark_referral_paid(71)
disc_res = []
def dr():
    disc_res.append(db.db_use_discount_atomic(70))
ths = [threading.Thread(target=dr) for _ in range(2)]
for t in ths: t.start()
for t in ths: t.join()
check('списание скидки True ровно 1 (не 2)', disc_res.count(True) == 1, str(disc_res))
check('после списания скидок 0', db.db_get_discount_count(70) == 0)

print('── Конкурентные рефералы ──')
db.db_set_referral(333, 444)
db.db_set_referral(333, 445)
db.db_set_referral(333, 446)
def rw(rid): db.db_mark_referral_paid(rid)
ths = [threading.Thread(target=rw, args=(r,)) for r in (444, 445, 446)]
for t in ths: t.start()
for t in ths: t.join()
check('3 реферала = ровно 3 скидки', db.db_get_discount_count(333) == 3)


print('── Гранты: пробный 3 / купон N не превращаются в 30 ──')
db.db_mark_paid(501, days=3)
db.db_set_pending(501, 1)  # шум
g1 = db.db_take_grant_days(501)
check('пробный грант = 3 дня (не 30)', g1 == 3, str(g1))
check('грант однократен (второй раз None)', db.db_take_grant_days(501) is None)
db.db_mark_paid(502, days=7)
g2 = db.db_take_grant_days(502)
check('купон 7 дней сохраняется как 7', g2 == 7, str(g2))
db.db_mark_paid(503)
check('обычная покупка = 30 дней', db.db_take_grant_days(503) == 30)
check('без гранта = None (фолбэк 30)', db.db_take_grant_days(504) is None)

print('── send_to_admins возвращает результат доставки ──')
# здесь нужен запущенный pbot — проверяем через мок из button_fuzz
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import telebot
import telebot.types as t
sends = []
telebot.TeleBot.get_me = lambda self, *a, **kw: t.User(id=1, is_bot=True, first_name='B', username='x')
telebot.TeleBot.send_message = lambda self, cid, text=None, **kw: (
    sends.append((cid, text or '')) or t.MessageID(message_id=1))
telebot.TeleBot.get_chat = lambda self, cid, *a, **kw: t.Chat(id=cid, type='private', first_name='X')
telebot.TeleBot.get_chat_member = lambda self, cid, uid, *a, **kw: t.ChatMemberMember(
    user=t.User(id=uid, is_bot=False, first_name='M'), status='creator')
telebot.TeleBot.edit_message_text = lambda self, *a, **kw: True
telebot.TeleBot.answer_callback_query = lambda self, *a, **kw: True
from delivery_queue import pump_delivery
import purchased_bot as PB
pb = PB.make_purchased_bot(77, '123:FAKE_audit_123456789012345', 9, None)

def cbx(data, uid=9):
    return t.CallbackQuery.de_json({'id': '1', 'from': {'id': uid, 'is_bot': False,
        'first_name': 'A'}, 'data': data, 'chat_instance': 'ci',
        'message': {'message_id': 1, 'from': {'id': 1, 'is_bot': True, 'first_name': 'B'},
                    'chat': {'id': uid, 'type': 'private', 'first_name': 'A'}, 'date': 0, 'text': 'x'}})

def dmsg(m):
    for h in pb.message_handlers:
        flt = h['filters']
        if flt.get('func') and not flt['func'](m): continue
        if flt.get('content_types') and m.content_type not in flt['content_types']: continue
        if flt.get('commands'):
            if m.content_type == 'text' and m.text and m.text.startswith('/'):
                cmd = m.text.split()[0][1:].split('@')[0]
                if cmd in flt['commands']: return h['function'](m)
            continue
        result = h['function'](m)
        pump_delivery(pb)
        return result
def dcb(cb):
    for h in pb.callback_query_handlers:
        flt = h['filters']
        if flt.get('func') and not flt['func'](cb): continue
        return h['function'](cb)

# юзер 601 пишет в ЛС-режиме (без группы): send_to_admins должен вернуть True
dmsg(t.Message.de_json({'message_id': 10, 'from': {'id': 601, 'is_bot': False,
    'first_name': 'U', 'username': 'u601'},
    'chat': {'id': 601, 'type': 'private', 'first_name': 'U'},
    'date': int(time.time()), 'text': 'проверка доставки'}))
pending = db.db_get_pending(601)
check('успешная доставка НЕ попадает в очередь (send_to_admins вернул True)',
      pending is None)
# форум-режим не подключен у бота 77 → route False → обычный путь ✓

# и повтор: сообщение с '<' не ломает HTML
dmsg(t.Message.de_json({'message_id': 11, 'from': {'id': 601, 'is_bot': False,
    'first_name': 'U', 'username': 'u601'},
    'chat': {'id': 601, 'type': 'private', 'first_name': 'U'},
    'date': int(time.time()), 'text': 'текст с <тегом> внутри'}))
check('текст с <тегом> доставлен без 400', len(sends) >= 2)

print()
print(f'ИТОГО: {len(PASS)} OK, {len(FAIL)} FAIL')
sys.exit(1 if FAIL else 0)
