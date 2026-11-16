"""Круг 5: настоящие обработчики, workers и временная SQLite без сети"""
import importlib.util
import json
import os
import subprocess
import sys
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('r2', ROOT / 'tests/fix_r2.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
q, db, pb, fs, h, dq, ps = r.q, r.db, r.pb, r.fs, r.h, r.dq, r.ps
import service_broadcast as svc


def fallback_race():
    bid, bot, sender = q.fresh()
    uid = 951
    sender.enqueue(q.message(uid, 1))
    with patch.object(bot, 'create_forum_topic', side_effect=q.error(403, 'bot was kicked')):
        sender.pump()
        assert fs.fs_get_pending(bid, uid)
        restore = fs.fs_restore_pending
        def racing(*args):
            worker = threading.Thread(target=sender.pump)
            worker.start(); worker.join(3)
            assert not worker.is_alive()
            return restore(*args)
        with patch.object(fs, 'fs_restore_pending', side_effect=racing):
            fs.confirm_send(bot, bid, q.callback(uid, 'fs_confirm_send'))
    pending = fs.fs_get_pending(bid, uid)
    assert pending and json.loads(pending[0]), 'повторный fallback потерял указатель'
    sender.enqueue(q.message(uid, 2))
    fs.confirm_send(bot, bid, q.callback(uid, 'fs_confirm_send'))
    sender.pump()
    assert all(v['status'] == 'delivered' for v in db.db_delivery_rows(bid, active=False))


def stale_report():
    bid, bot, _ = q.fresh()
    old = fs.fs_get_group(bid)[1]
    waiting, release = threading.Event(), threading.Event()
    original = fs._mgmt_lock
    @contextmanager
    def gate(value):
        if threading.current_thread().name == 'report-r5':
            waiting.set(); assert release.wait(3)
        with original(value): yield
    now = fs._now_tz()
    start = len(q.API)
    with patch.object(fs, '_mgmt_lock', gate):
        thread = threading.Thread(name='report-r5', target=fs._publish_period,
            args=(bot, bid, old, 77, now-timedelta(days=7), now))
        thread.start()
        try:
            assert waiting.wait(2)
            fs.fs_unbind_group(bid); fs.fs_bind_group(bid, old-100000, 'Новая')
        finally:
            release.set(); thread.join(3)
    assert not any(n == 'send_message' and a[0] == old for n, a, _, _ in q.API[start:]), 'отчёт ушёл в старую группу'


def report_topic():
    bid, bot, _ = q.fresh()
    fs.fs_set_stats_thread(bid, 77)
    send = bot.send_message
    def gone(*a, **kw):
        if kw.get('message_thread_id') == 77: raise q.error(400, 'message thread not found')
        return send(*a, **kw)
    with patch.object(bot, 'send_message', side_effect=gone), \
         patch.object(bot, 'create_forum_topic', wraps=bot.create_forum_topic) as created:
        fs.publish_due_reports({bid: bot})
    assert created.call_count == 1, f'создано тем: {created.call_count}'


def backups():
    folder = Path(os.environ['DATA_DIR']) / 'r5_backups'
    class Clock(datetime):
        moment = datetime(2026, 10, 1, 12)
        @classmethod
        def utcnow(cls): return cls.moment
    with patch.object(db, 'datetime', Clock):
        for day in range(9):
            for halfhour in range(5):
                Clock.moment = datetime(2026, 10, 1, 12) + timedelta(days=day, minutes=30*halfhour)
                db.db_backup(str(folder))
    daily = sorted(folder.glob('spambots_daily_*.db'))
    current = [p for p in folder.glob('spambots_*.db') if p not in daily]
    assert len(daily) == 7 and len(current) == 4, (len(daily), len(current))
    assert [p.stem[-8:] for p in daily] == [f'202610{d:02}' for d in range(3, 10)]
    # Читаем только копии, созданные этой проверкой
    for path in daily + current:
        with r.sqlite3.connect(path) as c: assert c.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'


def lifecycle_child():
    bid, bot, _ = q.fresh(False)
    db.set_price(1); db.db_add_bot_earnings(bid, 10)
    token = db.db_get_bot_info(bid)[0]
    pb.running_bots[token] = bot; pb.running_bot_ids[bid] = token
    ready, closing, renewed = threading.Event(), threading.Event(), threading.Event()
    close = pb._close_bot
    def at_close(value):
        if value is bot: closing.set()
        return close(value)
    def worker():
        ready.set(); assert closing.wait(3)
        r.handler(bot, 'p_renew_bal_cb')(q.callback(777, 'p_renew_bal_30'))
    spend = db.db_spend_balance
    def paid(*a, **kw):
        result = spend(*a, **kw); renewed.set(); return result
    with patch.object(pb, '_close_bot', side_effect=at_close), \
         patch.object(pb, 'launch_bot', return_value=True), \
         patch.object(db, 'db_spend_balance', side_effect=paid):
        bot.worker_pool.put(worker)
        assert ready.wait(2)
        result = []
        thread = threading.Thread(target=lambda: result.append(pb.stop_bot(token)), daemon=True)
        thread.start(); thread.join(3)
        assert result == [True] and renewed.wait(2), 'stop и worker заблокировали друг друга'
        for action in list(threading.enumerate()):
            if action.name == 'bot-lifecycle':
                action.join(3)
                assert not action.is_alive(), 'отложенный обработчик не завершился'


def lifecycle():
    result = subprocess.run([sys.executable, str(Path(__file__)), '--lifecycle'],
        cwd=ROOT, capture_output=True, timeout=12)
    assert result.returncode == 0, 'stop и настоящий worker не завершились'


def deleted():
    bid, bot, _ = q.fresh(False)
    main = r.mainbot()
    context = dict(step='renewing', bot_id=bid)
    ps.db_set_pending(777, 95001, context=context, amount=1)
    old = db.db_get_bot_expires(bid)
    db.db_add_bot_earnings(bid, 10)
    c = db._conn(); c.execute('INSERT OR REPLACE INTO referral_discounts VALUES (?,?)', (777, 1)); c.commit(); c.close()
    q.click(main, 900, f'bot_delete_yes_{bid}')
    start = len(q.API)
    q.click(main, 777, f'renew_bot_{bid}')
    assert any(n == 'answer_callback_query' and 'удалён' in str(a) for n, a, _, _ in q.API[start:]), 'нет понятного отказа'
    db.DBState(0)[777] = context
    q.click(main, 777, 'use_discount')
    q.click(bot, 777, 'p_renew_bal_30')
    assert db.db_get_bot_expires(bid) == old and not db.db_bot_can_run(bid)
    assert db.db_get_bot_earnings(bid)[0] == 10 and db.db_get_discount_count(777) == 1
    # Деньги по уже созданному счёту дают подключение нового бота
    before = svc.query('SELECT COUNT(*) FROM setup_grants WHERE user_id=777')[0][0]
    assert ps.db_claim_pending_payment(777, 95001, paid_amount=1)
    assert not ps.db_claim_pending_payment(777, 95001, paid_amount=1)
    assert not db.db_bot_can_run(bid) and ps.db_is_paid(777)
    assert svc.query('SELECT COUNT(*) FROM setup_grants WHERE user_id=777')[0][0] == before + 1


def transferred_invoice():
    bid, bot, _ = q.fresh(False, owner=952)
    db.db_set_primary_admin(bid, 953)
    main = r.mainbot()
    q.click(main, 953, f'renew_bot_{bid}')
    invoice = dict(invoice_id=95002, amount=2, pay_url='https://example.com/pay')
    with patch.object(h, 'cb_create_invoice', return_value=invoice): q.click(main, 953, 'pay_cryptobot')
    old = datetime.fromisoformat(str(db.db_get_bot_expires(bid)))
    q.click(bot, 953, 'p_change_primary'); q.dispatch(bot, q.message(953, 3, '954'))
    db.db_close_all()
    with patch.object(h, 'cb_check_invoice', return_value=dict(status='paid', amount=2)), \
         patch.object(h, 'launch_bot', return_value=True):
        q.click(main, 953, 'check_payment_95002'); q.click(main, 953, 'check_payment_95002')
    assert ps.db_get_payment(953, 95002)['settled']
    assert datetime.fromisoformat(str(db.db_get_bot_expires(bid))) == old + timedelta(days=30)
    for index, mode in enumerate(('crypto_background', 'ton_callback', 'ton_background')):
        uid = 960 + index
        bid, bot, _ = q.fresh(False, owner=970+index)
        db.db_set_primary_admin(bid, uid)
        q.click(main, uid, f'renew_bot_{bid}')
        provider = 'ton' if mode.startswith('ton') else 'crypto'
        invoice_id = f'r5_transfer_{index}' if provider == 'ton' else 95010 + index
        ps.db_set_pending(uid, invoice_id, provider=provider, amount=2, expected_ton=1,
                          expires_at=q.time.time()+7200, context=db.DBState(0).get(uid))
        old = datetime.fromisoformat(str(db.db_get_bot_expires(bid)))
        q.click(bot, uid, 'p_change_primary'); q.dispatch(bot, q.message(uid, 10, str(980+index)))
        found = dict(tx_hash=f'r5_tx_{index}', ton_amount=1, utime=q.time.time())
        db.db_close_all()
        if mode == 'ton_callback':
            with patch.object(h, 'ton_check_transfer', return_value=found), patch.object(h, 'launch_bot', return_value=True):
                q.click(main, uid, f'ton_check_{invoice_id}')
        else:
            with patch.object(r.payments, 'cb_check_invoice', return_value=dict(status='paid', amount=2)), \
                 patch.object(r.payments, 'ton_check_transfer', return_value=found), \
                 patch.object(pb, 'launch_bot', return_value=True):
                r.payments.reconcile_payments(main)
        assert ps.db_get_payment(uid, invoice_id, provider)['settled'], mode
        assert not ps.db_claim_pending_payment(uid, invoice_id, provider=provider, paid_amount=2)
        assert datetime.fromisoformat(str(db.db_get_bot_expires(bid))) == old + timedelta(days=30), mode


def broadcast_fallback():
    bid, bot, _ = q.fresh(False)
    main = r.mainbot(); uid = 955
    token = db.db_get_bot_info(bid)[0]
    pb.running_bots[token] = bot; pb.running_bot_ids[bid] = token
    db.db_add_user(0, uid); db.db_add_user(bid, uid)
    try:
        jid = svc.append(main, 900, 900, 'all', q.message(900, 5, 'Объявление'))
        svc.preview(main, jid); assert svc.confirm(main, jid, 900, start=False)
        send = main.send_message
        def blocked(chat, *a, **kw):
            if chat == uid: raise q.error(403, 'bot was blocked by the user')
            return send(chat, *a, **kw)
        with patch.object(main, 'send_message', side_effect=blocked): svc.pump(main, 0)
        # Маршрут должен пережить перезапуск процесса
        db.db_close_all()
        with patch.object(bot, 'send_message', wraps=bot.send_message) as sent:
            svc.pump(main, bid)
        assert any(c.args[:2] == (uid, 'Объявление') for c in sent.call_args_list), 'нет запасного маршрута'
        assert svc.query('SELECT status FROM service_recipients WHERE job_id=? AND user_id=?', (jid, uid)) == [('sent',)]
    finally:
        pb.running_bots.pop(token, None); pb.running_bot_ids.pop(bid, None)


def precision():
    bid, bot, _ = q.fresh(False)
    db.db_add_bot_earnings(bid, 1)
    for i in range(3):
        try: db.db_spend_balance(bid, 777, 0.00000000001, f'tiny:{i}', address='U'*48)
        except ValueError: pass
    assert not [v for v in db.db_get_pending_withdrawals() if v[1] == bid], 'созданы заявки без списания'
    assert db.db_get_bot_earnings(bid)[0] == 1
    db.db_spend_balance(bid, 777, 0.0000000001, 'minimum', address='U'*48)
    assert db.db_get_bot_earnings(bid)[0] == .9999999999


def cancel_address():
    bid, bot, _ = q.fresh(False)
    db.db_add_bot_earnings(bid, 10)
    q.click(bot, 777, 'p_withdraw')
    waiting, release = threading.Event(), threading.Event()
    uuid4 = pb.uuid.uuid4
    def gate():
        if threading.current_thread().name == 'address-r5':
            waiting.set(); assert release.wait(3)
        return uuid4()
    with patch.object(pb.uuid, 'uuid4', side_effect=gate):
        thread = threading.Thread(name='address-r5', target=q.dispatch, args=(bot, q.message(777, 6, 'U'*48)))
        thread.start()
        try:
            assert waiting.wait(2); q.click(bot, 777, 'p_back_admin')
        finally:
            release.set(); thread.join(3)
    q.dispatch(bot, q.message(777, 7, '1'))
    assert db.db_get_bot_earnings(bid)[0] == 10 and db.DBState(bid).get(777) is None, 'отмена восстановила диалог'


def renewal_identity():
    bid, bot, _ = q.fresh(False)
    db.set_price(1); db.db_add_bot_earnings(bid, 10)
    old = db.db_get_bot_info(bid)[0]
    pb.running_bots[old] = bot; pb.running_bot_ids[bid] = old
    try:
        q.click(bot, 777, 'p_renew_bal_30'); q.click(bot, 777, 'p_renew_bal_30')
        first = db.db_get_bot_expires(bid)
        assert db.db_get_bot_earnings(bid)[0] == 9
        # Квитанция предыдущей версии переживает миграцию и повтор
        c = db._conn()
        c.execute('UPDATE balance_operations SET operation_key=? WHERE bot_id=?', ('renew:777:999', bid))
        c.commit(); c.close(); db.init_db()
        q.click(bot, 777, 'p_renew_bal_30')
        assert db.db_get_bot_earnings(bid)[0] == 9
        def launch(b, tok, admin, main):
            new = pb.make_purchased_bot(b, tok, admin); q.BOTS.append(new)
            pb.running_bots[tok] = new; pb.running_bot_ids[b] = tok
            return True
        with patch.object(h, 'launch_bot', side_effect=launch):
            rotated = old.split(':', 1)[0] + ':ROTATED'
            h._replace_owned_token(777, bid, old, rotated,
                                   r.NS(id=int(old.split(':', 1)[0]), username='same'), None)
            same = pb.running_bots[pb.running_bot_ids[bid]]
            q.click(same, 777, 'p_renew_bal_30')
            assert db.db_get_bot_earnings(bid)[0] == 9, 'ротация токена повторила списание'
            old = rotated
            h._replace_owned_token(777, bid, old, '8800001:TEST', r.NS(id=8800001, username='new'), None)
        q.click(same, 777, 'p_renew_bal_30')
        assert db.db_get_bot_earnings(bid)[0] == 9, 'запоздавший callback списал деньги другого бота'
        new = pb.running_bots[pb.running_bot_ids[bid]]
        q.click(new, 777, 'p_renew_bal_30')
        assert db.db_get_bot_earnings(bid)[0] == 8, 'чужая квитанция продления'
        assert datetime.fromisoformat(str(db.db_get_bot_expires(bid))) == datetime.fromisoformat(str(first)) + timedelta(days=30)
    finally:
        tok = pb.running_bot_ids.pop(bid, None)
        if tok: pb.running_bots.pop(tok, None)


def cancel_ton():
    bid, _, _ = q.fresh(False, owner=991)
    main = r.mainbot()
    q.click(main, 991, f'renew_bot_{bid}')
    waiting, release = threading.Event(), threading.Event()
    def rate(*a):
        waiting.set(); assert release.wait(3); return 1
    with patch.object(h, 'get_ton_amount', side_effect=rate):
        thread = threading.Thread(target=q.click, args=(main, 991, 'pay_ton'))
        thread.start()
        try:
            assert waiting.wait(2); q.click(main, 991, 'back_to_start')
        finally:
            release.set(); thread.join(3)
    assert db.DBState(0).get(991) is None, 'TON восстановил отменённый диалог'
    assert svc.query("SELECT COUNT(*) FROM payment_orders WHERE user_id=991 AND provider='ton'")[0][0] == 1
    # Начало покупки без записи состояния тоже должно работать
    q.click(main, 992, 'buy_new_bot')
    with patch.object(h, 'get_ton_amount', return_value=1): q.click(main, 992, 'pay_ton')
    assert db.DBState(0).get(992)['step'] == 'ton_pending'


def cancel_preview():
    main = r.mainbot()
    db.DBState(0)[900] = 'bcast_any_all'
    waiting, release = threading.Event(), threading.Event()
    def preview(*a):
        waiting.set(); assert release.wait(3); raise ValueError('Неподходящий файл')
    with patch.object(svc, 'preview', side_effect=preview):
        thread = threading.Thread(target=q.dispatch, args=(main, q.message(900, 95, 'Отменённая рассылка')))
        thread.start()
        try:
            assert waiting.wait(2); q.click(main, 900, 'back_to_start')
        finally:
            release.set(); thread.join(3)
    assert db.DBState(0).get(900) is None, 'сбой предпросмотра восстановил отменённый диалог'


def faq_restart():
    for mode in ('text', 'skip', 'edit', 'add_title', 'rename'):
        bid, bot, _ = q.fresh(False)
        if mode in ('edit', 'rename'):
            nid = db.db_faq_add(bid, None, 'Вопрос', 'старый')
            q.click(bot, 777, f'fq_{"text" if mode == "edit" else "name"}_{nid}')
        else:
            q.click(bot, 777, 'fq_add_0')
            if mode != 'add_title': q.dispatch(bot, q.message(777, 8, 'Вопрос'))
        db.db_close_all()
        new = pb.make_purchased_bot(bid, db.db_get_bot_info(bid)[0], 777); q.BOTS.append(new)
        start = len(q.API)
        if mode == 'add_title': q.dispatch(new, q.message(777, 8, 'Вопрос'))
        if mode == 'skip': q.click(new, 777, 'fq_skip_0')
        else: q.dispatch(new, q.message(777, 9, 'Ответ'))
        nodes = db.db_faq_nodes(bid)
        expected = '' if mode == 'skip' else 'старый' if mode == 'rename' else 'Ответ'
        assert len(nodes) == 1 and nodes[0]['text'] == expected, mode
        assert nodes[0]['title'] == ('Ответ' if mode == 'rename' else 'Вопрос'), mode
        assert len(q.API) > start, 'нет ответа администратору'


if __name__ == '__main__':
    try:
        with patch.multiple(q.telebot.TeleBot, **{n:q.fake_api(n) for n in r.METHODS}), \
             patch.object(fs, 'RATE_LIMIT_SEC', 0):
            cases = [('Остановка и продление на настоящем пуле', lifecycle_child)] if '--lifecycle' in sys.argv else [
                ('Повторный fallback сохраняет подтверждение', fallback_race),
                ('Отчёт повторно проверяет текущую группу', stale_report),
                ('Одна восстановленная тема для четырёх периодов', report_topic),
                ('Семь ежедневных копий и четыре текущие', backups),
                ('Остановка и продление на настоящем пуле', lifecycle),
                ('Удалённый бот нельзя продлить, оплата сохранена', deleted),
                ('Счёт засчитывается после передачи главного админа', transferred_invoice),
                ('Рассылка использует купленного бота после 403', broadcast_fallback),
                ('Вывод меньше точности не создаёт бесплатную заявку', precision),
                ('Отмена побеждает задержанный адрес', cancel_address),
                ('TON сохраняет счёт и не восстанавливает отменённый диалог', cancel_ton),
                ('Сбой предпросмотра не восстанавливает отменённую рассылку', cancel_preview),
                ('Квитанция продления учитывает Telegram-бота', renewal_identity),
                ('FAQ продолжает ввод и пропуск после перезапуска', faq_restart),
            ]
            for name, fn in cases: r.check(name, fn)
    finally:
        # При воспроизведении deadlock закрытие того же пула тоже зависнет
        if '--lifecycle' not in sys.argv or not r.FAIL:
            for bot in q.BOTS: pb._close_bot(bot)
        db.db_close_all()
    print(f'ИТОГО: {r.OK} OK, {r.FAIL} FAIL')
    sys.exit(bool(r.FAIL))
