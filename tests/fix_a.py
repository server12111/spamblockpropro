# -*- coding: utf-8 -*-
"""Runtime-проверки главного бота, оплаты, восстановления и запуска"""
import contextlib
import atexit
import html
import io
import logging
import os
from pathlib import Path
import re
import runpy
import shutil
import sqlite3
import json
import subprocess
import sys
import tempfile
import threading
import hashlib
from datetime import datetime, timedelta
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.update(DATA_DIR=tempfile.mkdtemp(prefix='fix_a_', dir=ROOT / 'tests'),
                  PYTHON_DOTENV_DISABLED='1', SUPER_ADMIN='900',
                  BOT_TOKEN='123456:' + 'A' * 35, CRYPTOBOT_TKN='fake-crypto-key',
                  TONCENTER_KEY='fake-ton-key', WEBHOOK_URL='')
# Старые версии python-dotenv не поддерживают PYTHON_DOTENV_DISABLED
import types
sys.modules['dotenv'] = types.SimpleNamespace(load_dotenv=lambda *a, **k: False)
import telebot
import telebot.types as t
import database as db
import handlers as H
import purchased_bot as PB
import payments

db.init_db()
API = []

def cleanup():
    db.db_close_all()
    logging.shutdown()
    shutil.rmtree(os.environ['DATA_DIR'])

atexit.register(cleanup)

def msg(uid, text):
    return t.Message.de_json({'message_id': 1, 'from': {'id': uid, 'is_bot': False,
        'first_name': 'Юзер'}, 'chat': {'id': uid, 'type': 'private'}, 'date': 0, 'text': text})

def cb(uid, data):
    return t.CallbackQuery.de_json({'id': 'c', 'from': {'id': uid, 'is_bot': False,
        'first_name': 'Юзер'}, 'data': data, 'chat_instance': 'x',
        'message': {'message_id': 1, 'from': {'id': 1, 'is_bot': True, 'first_name': 'Бот'},
                    'chat': {'id': uid, 'type': 'private'}, 'date': 0, 'text': 'x'}})

def api(name):
    def call(self, *a, **kw):
        if name in ('send_message', 'edit_message_text'):
            text = a[1] if name == 'send_message' else a[0]
            plain = html.unescape(re.sub(r'<[^>]*>', '', text)) if kw.get('parse_mode') == 'HTML' else text
            assert len(plain.encode('utf-16-le')) // 2 <= 4096, 'Telegram: message is too long'
        API.append((name, a, kw))
        return msg(1, 'ok')
    return call

for name in ('send_message', 'edit_message_text', 'answer_callback_query', 'delete_message',
             'delete_webhook', 'set_webhook', 'infinity_polling'):
    setattr(telebot.TeleBot, name, api(name))
telebot.TeleBot.get_me = lambda *a, **k: t.User(1, True, 'Бот', username='testbot')
H.launch_bot = lambda *a, **k: True
H.cb_check_invoice = lambda iid: {'invoice_id': iid, 'status': 'paid', 'amount': '10', 'asset': 'USDT'}
H.get_ton_amount = lambda: 1
PB.get_ton_amount = lambda: 1
bot = telebot.TeleBot('111:FAKE', threaded=False)
H.register(bot)

def callback(uid, data, target=bot):
    c = cb(uid, data)
    for h in target.callback_query_handlers:
        if h['filters'].get('func', lambda _: True)(c):
            return h['function'](c)

def message(uid, text, target=bot):
    m = msg(uid, text)
    for h in target.message_handlers:
        f = h['filters']
        if f.get('commands') and text.split()[0].lstrip('/') not in f['commands']:
            continue
        if m.content_type not in f.get('content_types', ['text']):
            continue
        if f.get('func', lambda _: True)(m):
            return h['function'](m)

def invoice(uid, iid, amount='10'):
    H.cb_create_invoice = lambda _: {'invoice_id': iid, 'pay_url': 'https://example.test/pay',
                                    'amount': amount, 'expiration_date': (datetime.utcnow() + timedelta(hours=1)).isoformat()}
    callback(uid, 'pay_cryptobot')

def setup(uid, suffix=''):
    message(uid, '777777:' + 'B' * 32 + suffix)
    message(uid, str(uid))

PASS = FAIL = 0
def test(name, fn):
    global PASS, FAIL
    API.clear()
    try:
        fn()
    except Exception as exc:
        FAIL += 1
        print('[FAIL] ' + name + ' (' + type(exc).__name__ + ')')
    else:
        PASS += 1
        print('[OK] ' + name)

def admin_forgery():
    callback(101, 'admin_reply_102')
    message(101, 'поддельный ответ')
    assert not any(n == 'send_message' and a[0] == 102 for n, a, k in API)
    db.DBState(0)[101] = 'await_admin_msg_102'
    message(101, 'старое состояние')
    assert not any(n == 'send_message' and a[0] == 102 for n, a, k in API)

def commission_eligibility():
    bid = db.db_add_bot(110, '110:test', 110)
    message(110, '/start via_' + str(bid))
    invoice(110, 110)
    callback(110, 'check_payment_110')
    assert db.db_get_bot_earnings(bid)[0] == 0
    db.db_set_marketplace(bid, True)
    message(110, '/start via_' + str(bid))
    invoice(110, 111)
    callback(110, 'check_payment_111')
    assert db.db_get_bot_earnings(bid)[0] == 0
    db.db_set_marketplace(bid, False)
    message(112, '/start via_' + str(bid))
    invoice(112, 112)
    callback(112, 'check_payment_112')
    assert db.db_get_bot_earnings(bid)[0] == 0

def commission_amount():
    bid = db.db_add_bot(120, '120:test', 120)
    db.db_set_marketplace(bid, True)
    message(121, '/start via_' + str(bid))
    invoice(121, 121)
    db.set_price(100)
    callback(121, 'check_payment_121')
    assert db.db_get_bot_earnings(bid)[0] == 5
    callback(121, 'check_payment_121')
    assert db.db_get_bot_earnings(bid)[0] == 5

def payment_back():
    invoice(130, 130)
    callback(130, 'back_to_start')
    callback(130, 'check_payment_130')
    assert db.db_is_paid(130)

def launch_failure():
    callback(140, 'try_free')
    with patch.object(H, 'launch_bot', return_value=False):
        setup(140, 'first')
    first = db.db_get_owner_bots(140)
    assert len(first) == 1
    callback(140, 'buy_bot')
    setup(140, 'second')
    assert len(db.db_get_owner_bots(140)) == 1
    assert not db.db_is_paid(140)

def paid_then_trial():
    invoice(150, 150)
    callback(150, 'check_payment_150')
    callback(150, 'try_free')
    assert db.db_peek_grant_days(150) == 30
    assert not db.db_has_used_trial(150)

def discount_new():
    db.db_set_referral(160, 161)
    db.db_mark_referral_paid(161)
    callback(160, 'use_discount')
    setup(160, 'discount')
    b = db.db_get_owner_bots(160)
    assert len(b) == 1
    duration = datetime.fromisoformat(b[0][4]) - datetime.now()
    assert 9.99 < duration.total_seconds() / 86400 <= 10

def subscription_time():
    token = '170:test'
    bid = db.db_add_bot(170, token, 170)
    c = db._conn()
    c.execute('UPDATE bots SET expires_at=? WHERE id=?', ((datetime.now() + timedelta(hours=20)).isoformat(), bid))
    c.commit(); c.close()
    H.running_bots[token] = object()
    stopped = []
    class EndLoop(BaseException): pass
    calls = [0]
    def sleep(_):
        calls[0] += 1
        if calls[0] > 1: raise EndLoop()
    def start(th):
        with contextlib.suppress(EndLoop): th.run()
    with patch.object(H.time, 'sleep', sleep), patch.object(H.threading.Thread, 'start', start), \
            patch.object(H, 'stop_bot', side_effect=stopped.append):
        H.start_subscription_checker(bot)
    H.running_bots.pop(token, None)
    assert token not in stopped

def stale_renewal():
    bid = db.db_add_bot(180, '180:test', 180)
    exp = db.db_get_bot_expires(bid)
    callback(180, 'renew_bot_' + str(bid))
    message(180, '/start')
    callback(180, 'buy_bot')
    callback(180, 'buy_new_bot')
    invoice(180, 180)
    callback(180, 'check_payment_180')
    assert db.db_get_bot_expires(bid) == exp and db.db_is_paid(180)

def free_restart():
    bid = db.db_add_bot(190, '190:test', 190, days=-1)
    with patch.object(H, 'launch_bot', return_value=True) as launch:
        callback(900, 'free_renew')
        message(900, str(bid) + ' 30')
        assert launch.call_count == 1

def long_reply():
    callback(900, 'admin_reply_201')
    message(900, 'я' * 4096)
    delivered = [a[1] for n, a, k in API if n == 'send_message' and a[0] == 201]
    assert ''.join(delivered).count('я') == 4096

def withdrawal_list():
    db.db_create_withdrawal(1, 210, 'a' * 4096, 1)
    callback(900, 'withdrawal_list')
    assert any('a' * 100 in str(a) for n, a, k in API if n in ('send_message', 'edit_message_text'))
    assert any(k.get('reply_markup') for n, a, k in API if n in ('send_message', 'edit_message_text'))

def two_invoices():
    invoice(220, 220)
    invoice(220, 221)
    callback(220, 'check_payment_220')
    assert db.db_is_paid(220)
    callback(220, 'check_payment_221')
    setup(220, 'one')
    callback(220, 'buy_bot')
    setup(220, 'two')
    assert len(db.db_get_owner_bots(220)) == 2 and not db.db_is_paid(220)

def crash_payment():
    db.db_set_pending(230, 230)
    bootstrap = ("import os,sys,types;sys.modules['dotenv']=types.SimpleNamespace(load_dotenv=lambda:False);"
                 "import database as db;db.init_db();"
                 "assert db.db_claim_pending_payment(230,230);os._exit(17)")
    result = subprocess.run([sys.executable, '-c', bootstrap], cwd=ROOT,
                            capture_output=True, timeout=20)
    assert result.returncode == 17
    db.db_close_all(); db.init_db()
    assert db.db_is_paid(230)
    assert not db.db_claim_pending_payment(230, 230)

def rollback_payment():
    import purchase_store
    db.db_set_pending(231, 231)
    apply = purchase_store._apply_purchase
    def fail(*args, **kwargs):
        apply(*args, **kwargs)
        raise RuntimeError('имитация сбоя до commit')
    with patch.object(purchase_store, '_apply_purchase', fail):
        with contextlib.suppress(RuntimeError):
            db.db_claim_pending_payment(231, 231)
    db.db_close_all(); db.init_db()
    assert db.db_get_pending(231) == 231 and not db.db_is_paid(231)
    assert db.db_claim_pending_payment(231, 231)
    assert db.db_peek_grant_days(231) == 30

def parallel_payment():
    invoice(232, 232)
    errors = []
    def pay():
        try: callback(232, 'check_payment_232')
        except Exception: errors.append(True)
    workers = [threading.Thread(target=pay) for _ in range(4)]
    for th in workers: th.start()
    for th in workers: th.join(5)
    assert not errors and not any(th.is_alive() for th in workers)
    assert db.db_take_grant_days(232) == 30 and db.db_take_grant_days(232) is None

def legacy_migration():
    original = db.DB_PATH
    db.DB_PATH = str(Path(os.environ['DATA_DIR']) / 'legacy.sqlite')
    try:
        c = sqlite3.connect(db.DB_PATH)
        c.executescript('''
            CREATE TABLE pending_payments (user_id INTEGER PRIMARY KEY, invoice_id INTEGER NOT NULL);
            CREATE TABLE paid_setups (user_id INTEGER PRIMARY KEY);
            CREATE TABLE bot_grants (user_id INTEGER PRIMARY KEY, days INTEGER NOT NULL);
            CREATE TABLE states (bot_id INTEGER, user_id INTEGER, state TEXT, PRIMARY KEY(bot_id,user_id));
            INSERT INTO pending_payments VALUES (280,280);
            INSERT INTO paid_setups VALUES (281);
            INSERT INTO bot_grants VALUES (281,3);
        ''')
        c.execute('INSERT INTO states VALUES (0,280,?)',
                  (json.dumps({'step': 'ton_pending', 'renewing': True, 'bot_id': 5, 'via_bot_id': 8}),))
        c.commit(); c.close()
        db.init_db(); db.init_db()
        order = db.db_get_payment(280, 280)
        assert order['bot_id'] == 5 and order['via_bot_id'] == 8
        assert db.db_get_pending(280) == 280
        assert db.db_take_grant_days(281) == 3
        db.init_db()
        assert db.db_take_grant_days(281) is None and not db.db_is_paid(281)
    finally:
        db.db_close_all()
        db.DB_PATH = original

def ton_back():
    bid = db.db_add_bot(240, '240:test', 240)
    exp = db.db_get_bot_expires(bid)
    callback(240, 'renew_bot_' + str(bid))
    callback(240, 'pay_ton')
    callback(240, 'back_to_payment')
    invoice(240, 240)
    callback(240, 'check_payment_240')
    assert db.db_get_bot_expires(bid) != exp and not db.db_is_paid(240)

def ton_persisted():
    callback(241, 'pay_ton')
    buttons = [button.callback_data for n, a, k in API for row in getattr(k.get('reply_markup'), 'keyboard', [])
               for button in row if (button.callback_data or '').startswith('ton_check_')]
    assert buttons
    action = buttons[-1]
    callback(241, 'cancel')
    db.db_close_all(); db.init_db()
    with patch.object(H, 'ton_check_transfer', return_value='synthetic-hash'):
        callback(241, action)
        callback(241, action)
    assert db.db_peek_grant_days(241) == 30 and db.db_is_tx_used('synthetic-hash')
    assert db.db_take_grant_days(241) == 30 and db.db_take_grant_days(241) is None

def ton_commission():
    bid = db.db_add_bot(242, '242:test', 242)
    db.db_set_marketplace(bid, True)
    message(243, '/start via_' + str(bid))
    db.set_price(10)
    callback(243, 'pay_ton')
    code = db.DBState(0).get(243)['code']
    db.set_price(100)
    tx = {'utime': __import__('time').time(),
          'in_msg': {'value': '950000000', 'message': code},
          'transaction_id': {'hash': 'ton-actual-amount'}}
    response = types.SimpleNamespace(json=lambda: {'ok': True, 'result': [tx]})
    with patch.object(payments.requests, 'get', return_value=response):
        callback(243, 'ton_check_' + code)
        callback(243, 'ton_check_' + code)
    assert db.db_get_bot_earnings(bid)[0] == 4.75
    assert db.db_is_tx_used('ton-actual-amount')

def restore_threads():
    ready, done = threading.Event(), threading.Event()
    held = []
    def worker():
        held.append(db._conn())
        held[0].execute('SELECT 1').fetchone()
        held[0].close()
        ready.set(); done.wait(10)
    th = threading.Thread(target=worker); th.start(); ready.wait(10)
    try:
        with db.db_restore_section():
            db.db_close_all()
            closed = False
            try: held[0]._c.execute('SELECT 1')
            except Exception as exc: closed = 'closed' in str(exc).lower()
            assert closed
            old = db.DB_PATH + '.old'
            os.replace(db.DB_PATH, old)
            os.replace(old, db.DB_PATH)
            db.init_db()
    finally:
        done.set(); th.join(10)

def owner_money():
    bid = db.db_add_bot(250, '250:test', 251)
    db.db_add_bot_admin(bid, 252)
    pbot = PB.make_purchased_bot(bid, '250:test', 251, bot)
    db.db_add_bot_earnings(bid, 100)
    for uid in (251, 252):
        for action in ('p_balance', 'p_withdraw', 'p_renew_balance', 'p_renew_bal_30'):
            API.clear(); callback(uid, action, pbot)
            assert any('Доступно только владельцу бота' in str(a) for n, a, k in API)
        db.DBState(bid)[uid] = {'step': 'p_await_withdraw_amount', 'balance': 100, 'address': 'a' * 48}
        message(uid, '1', pbot)
    assert db.db_get_bot_earnings(bid)[0] == 100
    db.DBState(bid)[250] = {'step': 'p_await_ton_address', 'balance': 100}
    message(250, 'a' * 4096, pbot)
    assert db.DBState(bid).get(250)['step'] == 'p_await_ton_address'

def startup_updates():
    import config
    with patch.object(config, 'WEBHOOK_URL', ''), patch.object(threading.Thread, 'start'), \
         patch.object(H, 'start_subscription_checker'), patch.object(PB, 'launch_bot'), \
         patch('signal.signal'), patch('time.sleep'):
        runpy.run_path(str(ROOT / 'main.py'), run_name='__main__')
    assert all(k.get('drop_pending_updates') is False for n, a, k in API if n == 'delete_webhook')
    API.clear()
    with patch.object(threading.Thread, 'start'):
        assert PB.launch_bot(999, '999:test', 999, bot)
    PB.running_bots.pop('999:test', None); PB.running_bot_ids.pop(999, None)
    assert any(n == 'delete_webhook' and k.get('drop_pending_updates') is False for n, a, k in API)

def webhook_routes():
    import config
    from flask import Flask
    checked = []
    def serve(app, **kwargs):
        client = app.test_client()
        suffix = config.TOKEN.replace(':', '_')
        headers = {'X-Telegram-Bot-Api-Secret-Token': hashlib.sha256(('whk:' + config.TOKEN).encode()).hexdigest()}
        for route in ('/' + suffix, '/webhook/' + suffix):
            assert client.post(route, json={'update_id': 1}, headers=headers).status_code == 200
            assert client.post(route, json={'update_id': 2}).status_code == 403
        assert client.post('/unknown', json={}).status_code == 404
        checked.append(True)
    with patch.object(config, 'WEBHOOK_URL', 'https://example.test/webhook'), \
         patch.object(threading.Thread, 'start'), patch.object(H, 'start_subscription_checker'), \
         patch.object(PB, 'launch_bot'), patch('time.sleep'), patch.object(Flask, 'run', serve):
        runpy.run_path(str(ROOT / 'main.py'), run_name='__main__')
    assert checked
    assert any(n == 'set_webhook' and k.get('drop_pending_updates') is False for n, a, k in API)

def secret_traceback():
    import safe_logging
    stream = io.StringIO()
    output = logging.StreamHandler(stream)
    logger = logging.getLogger('fix_a.secrets'); logger.addHandler(output); logger.setLevel(logging.INFO)
    token = '555555:' + 'T' * 35
    safe_logging.register_secret(token)
    try:
        try: raise RuntimeError('url=' + token.replace(':', '_'))
        except RuntimeError: logger.exception('Сбой %s', token)
        assert token not in stream.getvalue() and token.replace(':', '_') not in stream.getvalue()
        assert 'RuntimeError' in stream.getvalue()
    finally:
        logger.removeHandler(output)

def logs_secret(kind):
    import main  # рабочая настройка журналов
    import config
    stream = io.StringIO()
    capture = logging.StreamHandler(stream)
    root = logging.getLogger(); root.addHandler(capture); root.setLevel(logging.INFO)
    try:
        if kind == 'token':
            token = '333333:' + 'Z' * 35
            callback(260, 'cancel'); message(260, token)
            assert token not in stream.getvalue()
        elif kind == 'werkzeug':
            logging.getLogger('werkzeug').info('POST /webhook/%s HTTP/1.1', config.TOKEN.replace(':', '_'))
            assert config.TOKEN.replace(':', '_') not in stream.getvalue()
            assert '/webhook/' not in stream.getvalue()
        else:
            with patch.object(payments.requests, 'get', side_effect=payments.requests.ConnectionError(
                    'https://toncenter.com?api_key=' + config.TONCENTER_KEY)):
                payments.ton_check_transfer(270, 1, 'test')
            assert config.TONCENTER_KEY not in stream.getvalue()
    finally:
        root.removeHandler(capture)

if __name__ == '__main__':
    for name, fn in [
        ('Права на ответ администрации', admin_forgery),
        ('Комиссия: маркетплейс и владелец', commission_eligibility),
        ('Комиссия от суммы счёта и один раз', commission_amount),
        ('Оплата после возврата в меню', payment_back),
        ('Сбой запуска не даёт второго бота', launch_failure),
        ('Триал не заменяет оплату', paid_then_trial),
        ('Скидка новому боту даёт 10 дней', discount_new),
        ('Подписка действует последние 20 часов', subscription_time),
        ('Новая покупка сбрасывает цель продления', stale_renewal),
        ('Бесплатное продление запускает бота', free_restart),
        ('Ответ администрации в 4096 символов', long_reply),
        ('Длинный адрес не скрывает выводы', withdrawal_list),
        ('Второй счёт сохраняет первый', two_invoices),
        ('Перезапуск в момент зачёта', crash_payment),
        ('Откат незавершённого зачёта', rollback_payment),
        ('Одновременные проверки оплаты', parallel_payment),
        ('Миграция старых счетов и трёхдневного гранта', legacy_migration),
        ('Возврат из TON сохраняет продление', ton_back),
        ('Счёт TON переживает отмену и повторный запуск', ton_persisted),
        ('Комиссия TON от реально полученной суммы', ton_commission),
        ('Восстановление с соединением рабочего потока', restore_threads),
        ('Деньги доступны только владельцу', owner_money),
        ('Запуск сохраняет обновления Telegram', startup_updates),
        ('Прежние пути webhook и проверка подписи', webhook_routes),
        ('Фильтр очищает аргументы и traceback', secret_traceback),
        ('Токен вне настройки не попадает в журнал', lambda: logs_secret('token')),
        ('Werkzeug не пишет путь webhook', lambda: logs_secret('werkzeug')),
        ('Ошибка TON не раскрывает API-ключ', lambda: logs_secret('api')),
    ]:
        test(name, fn)
    print(f'ИТОГО: {PASS} OK, {FAIL} FAIL')
    sys.exit(bool(FAIL))
