# -*- coding: utf-8 -*-
"""Регресс-тесты фиксов внешнего аудита (деньги, атомарность).
Запуск: py tests/audit_regress.py"""
import os
import sys
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
check('привязка удаляется после оплаты', db.db_get_pending(7145919720) is None)

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

print('── Конкурентные рефералы ──')
db.db_set_referral(333, 444)
db.db_set_referral(333, 445)
db.db_set_referral(333, 446)
def rw(rid): db.db_mark_referral_paid(rid)
ths = [threading.Thread(target=rw, args=(r,)) for r in (444, 445, 446)]
for t in ths: t.start()
for t in ths: t.join()
check('3 реферала = ровно 3 скидки', db.db_get_discount_count(333) == 3)

print()
print(f'ИТОГО: {len(PASS)} OK, {len(FAIL)} FAIL')
sys.exit(1 if FAIL else 0)
