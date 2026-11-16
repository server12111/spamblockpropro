"""Круг 4б: обработчики, настоящий пул telebot и временная SQLite без сети"""
import importlib.util
import os
import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('r2', ROOT / 'tests/fix_r2.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
q, db, pb, fs, h, dq = r.q, r.db, r.pb, r.fs, r.h, r.dq
import reply_queue as rq
import service_broadcast as svc


def known_fixed():
    # Именно обычный пользователь и настоящий пул telebot
    main = q.telebot.TeleBot(os.environ['BOT_TOKEN'])
    h.register(main)
    q.BOTS.append(main)
    cb = q.callback(812, 'renew_bot_' + '9' * 25)
    main.process_new_callback_query([cb])
    done = threading.Event()
    main.worker_pool.put(done.set)
    assert done.wait(2)
    result = []
    reader = threading.Thread(target=lambda: result.append(db.get_price()), daemon=True)
    reader.start(); reader.join(1)
    assert result, 'гейт остался захвачен после callback'
    # И прямой вызов БД с удержанием traceback
    caught = None
    try:
        db.db_get_bot_info(10**25)
    except (ValueError, OverflowError) as exc:
        caught = exc
    assert caught
    reader = threading.Thread(target=lambda: result.append(db.get_price()), daemon=True)
    reader.start(); reader.join(1)
    assert len(result) == 2


def numeric_inputs():
    bid, bot, _ = q.fresh(False)
    for value in ('0', 'nan', 'inf', str(10**25)):
        q.click(bot, 777, 'p_change_primary')
        q.dispatch(bot, q.message(777, 5, value))
        assert db.db_get_bot_admins(bid) == [777]
    main = r.mainbot()
    old = db.get_price()
    for value in ('nan', 'inf', '0', str(10**25)):
        db.DBState(0)[900] = 'await_new_price'
        q.dispatch(main, q.message(900, 6, value))
        assert db.get_price() == old


def replace_delivery():
    bid, bot, sender = q.fresh(False)
    sender.enqueue(q.message(813, 2)); sender.pump()
    assert db.db_delivery_rows(bid, active=False)[0]['status'] == 'delivered'
    old = db.db_get_bot_info(bid)[0]
    new = '800001:TEST'
    snapshot = db.db_replace_bot_token(bid, old, new, 'new', reset_group=True)
    other = pb.make_purchased_bot(bid, new, 777); q.BOTS.append(other)
    start = len(q.SENT)
    dq.get_sender(other, bid).enqueue(q.message(813, 2, 'новое обращение'))
    dq.get_sender(other, bid).pump()
    assert any(v[1][0] == 777 and 'новое обращение' in str(v) for v in q.SENT[start:]), 'новое обращение потеряно'
    # Откат возвращает исходную очередь вместе с квитанциями
    db.db_restore_bot_token(bid, new, snapshot)
    rows = db.db_delivery_rows(bid, active=False)
    assert len(rows) == 1 and 'новое обращение' not in rows[0]['payload']


def replace_reply(same=False):
    bid, bot, _ = q.fresh(False)
    old = db.db_get_bot_info(bid)[0]
    payload = dict(mode='album', target=814, chat=777, ids=[11, 12])
    with patch.object(bot, 'copy_messages', side_effect=q.error(429, 'retry', 60)):
        rq.enqueue(bot, bid, 'old-album', payload)
    assert rq.pending(bid)
    new = old.split(':')[0] + ':ROTATED' if same else '800002:TEST'
    snapshot = db.db_replace_bot_token(bid, old, new, 'new', reset_group=not same)
    other = pb.make_purchased_bot(bid, new, 777); q.BOTS.append(other)
    with patch.object(other, 'copy_messages', wraps=other.copy_messages) as copied, \
         patch.object(rq.time, 'time', return_value=time.time()+61):
        rq.pump(other, bid)
    assert copied.call_count == (1 if same else 0), 'чужие ID исполнены новым ботом'
    if not same:
        db.db_restore_bot_token(bid, new, snapshot)
        assert rq.pending(bid), 'откат не восстановил ответ'


def drain_workers():
    bid, bot, _ = q.fresh(False)
    token = db.db_get_bot_info(bid)[0]
    pb.running_bots[token] = bot; pb.running_bot_ids[bid] = token
    release = threading.Event()
    started = [threading.Event(), threading.Event()]
    def busy(index):
        started[index].set(); release.wait(5)
    for i in range(2): bot.worker_pool.put(busy, i)
    assert all(e.wait(2) for e in started)
    bot.process_new_messages([q.message(815, 3, 'в очереди worker')])
    stopped = []
    thread = threading.Thread(target=lambda: stopped.append(pb.stop_bot(token)))
    thread.start()
    try:
        # close старого пула уже останавливает workers, но они ещё заняты
        time.sleep(.15)
        release.set(); thread.join(4)
        assert stopped == [True]
        assert any(v['message_id'] == 3 for v in db.db_delivery_rows(bid, active=False)), 'подтверждённое обращение отброшено'
    finally:
        release.set(); thread.join(3)
        pb.running_bots.pop(token, None); pb.running_bot_ids.pop(bid, None)


def cancelled_save(faq=False, rule=False):
    bid, bot, _ = q.fresh(False)
    if faq:
        nid = db.db_faq_add(bid, None, 'FAQ', 'исходный')
        q.click(bot, 777, f'fq_text_{nid}')
        reset = lambda: q.click(bot, 777, f'fq_e_{nid}_0')
        read = lambda: db.db_faq_get(bid, nid)['text']
        expected = 'исходный'
    elif rule:
        rid = db.db_auto_save(bid, ['проверка'], 'исходный')
        q.click(bot, 777, f'ps_answer_{rid}')
        reset = lambda: q.click(bot, 777, 'ps_rules_0')
        read = lambda: db.db_auto_rules(bid)[0]['text']
        expected = 'исходный'
    else:
        q.click(bot, 777, 'ps_edit_welcome')
        def reset():
            q.click(bot, 777, 'ps_text_welcome')
            q.click(bot, 777, 'ps_reset_welcome')
        read = lambda: db.db_get_bot_setting(bid, 'welcome')
        expected = pb.TEXTS['welcome'][1]
    original = bot.send_message
    waiting, release = threading.Event(), threading.Event()
    def slow(chat, text, *a, **kw):
        if text == 'отменённый ввод':
            waiting.set(); assert release.wait(5)
        return original(chat, text, *a, **kw)
    with patch.object(bot, 'send_message', side_effect=slow):
        thread = threading.Thread(target=lambda: q.dispatch(bot, q.message(777, 30, 'отменённый ввод')))
        thread.start()
        try:
            assert waiting.wait(2)
            reset()
        finally:
            release.set(); thread.join(3)
    assert read() == expected, 'отменённый ввод записан после отмены'


def blocked_notification():
    bid, bot, sender = q.fresh(False)
    db.db_auto_save(bid, ['привет'], 'автоматический ответ')
    sender.enqueue(q.message(816, 8, 'привет'))
    original = bot.send_message
    def limited(chat, text, *a, **kw):
        if chat == 816 and text == 'автоматический ответ':
            raise q.error(429, 'retry', 60)
        return original(chat, text, *a, **kw)
    with patch.object(bot, 'send_message', side_effect=limited): sender.pump()
    assert db.db_support_notifications(bid, time.time()+61)
    db.db_block_user(bid, 816)
    with patch.object(bot, 'send_message', wraps=bot.send_message) as sent, \
         patch.object(pb.time, 'time', return_value=time.time()+61):
        bot._pump_notifications()
    assert not any(c.args[0] == 816 for c in sent.call_args_list), 'автоответ пришёл после блокировки'
    assert not db.db_support_notifications(bid, time.time()+1000)


def backup_private():
    main = r.mainbot()
    with patch.object(main, 'send_document', return_value=True) as sent:
        m = q.message(-100123, 50, '/backup', **{'from': dict(id=900, is_bot=False, first_name='Админ')})
        cb = q.callback(900, 'backup_db'); cb.message = m
        start = len(q.API)
        r.handler(main, 'cmd_backup')(m)
        r.handler(main, 'backup_db_cb')(cb)
        assert not sent.called and len(q.API) == start, 'копия или ответ в группе'
        r.handler(main, 'cmd_backup')(q.message(900, 51, '/backup'))
        r.handler(main, 'backup_db_cb')(q.callback(900, 'backup_db'))
        assert sent.call_count == 2 and all(c.args[0] == 900 for c in sent.call_args_list)


def restore_lifecycle(fail=False):
    # Файл создан только этой проверкой, никаких рабочих БД
    snapshot = db.db_backup(os.environ['DATA_DIR'])
    with open(snapshot, 'rb') as f: uploaded = f.read()
    bid, bot, _ = q.fresh(False)
    token = db.db_get_bot_info(bid)[0]
    pb.running_bots[token] = bot; pb.running_bot_ids[bid] = token
    main = r.mainbot()
    m = q.message(900, 61, None, document=dict(file_id='snapshot', file_unique_id='s', file_name='spambots.db'))
    db.DBState(0)[900] = 'await_db_file'
    launched = []
    real_replace = os.replace
    def replace(src, dst):
        assert not pb.running_bots and not pb.running_bot_ids, 'замена при работающем боте'
        if fail: raise OSError('test replacement error')
        return real_replace(src, dst)
    try:
        with patch.object(main, 'get_file', return_value=r.NS(file_path='snapshot')), \
             patch.object(main, 'download_file', return_value=uploaded), \
             patch.object(h.os, 'replace', side_effect=replace), \
             patch.object(h, 'launch_bot', side_effect=lambda *a: launched.append(a) or True):
            r.handler(main, 'handle_db_restore')(m)
        assert any(a[0] == bid for a in launched) == fail, 'запуск не соответствует восстановленной БД'
        if not fail:
            assert not db.db_get_bot_info(bid), 'восстановление не выполнено'
            assert not pb.running_bots, 'старый экземпляр остался активен'
    finally:
        pb.running_bots.pop(token, None); pb.running_bot_ids.pop(bid, None)


def stopped_broadcast():
    with r.broadcast_environment() as e:
        jid = r.new_job(e, [q.message(900, 70, 'объявление')])
        svc.confirm(e.main, jid, 900, start=False)
        token = pb.running_bot_ids[e.bid1]
        bot = pb.running_bots.pop(token)
        try:
            svc.pump(e.main, e.bid1)
            assert all(v[0] == 'pending' for v in svc.query('SELECT status FROM service_recipients WHERE job_id=? AND bot_id=?', (jid,e.bid1))), 'остановка помечена окончательным отказом'
        finally: pb.running_bots[token] = bot
        svc.pump(e.main, e.bid1)
        assert all(v[0] == 'sent' for v in svc.query('SELECT status FROM service_recipients WHERE job_id=? AND bot_id=?', (jid,e.bid1)))


def broadcast_cooldown(migrate=False):
    with r.broadcast_environment() as e:
        jid = r.new_job(e, [q.message(900, 71, 'первое')])
        svc.confirm(e.main, jid, 900, start=False)
        e.fault[0] = lambda *a: (_ for _ in ()).throw(q.error(429, 'retry', 60))
        svc.pump(e.main, e.bid1)
        e.fault[0] = None
        if migrate:
            svc.query('DROP TABLE IF EXISTS service_limits', write=True)
            db.init_db()
        jid2 = r.new_job(e, [q.message(900, 72, 'второе')])
        svc.confirm(e.main, jid2, 900, start=False)
        db.db_close_all()  # срок живёт в БД, а не в памяти worker
        start = len(e.calls)
        svc.pump(e.main, e.bid1)
        assert len(e.calls) == start, 'новая рассылка обошла retry_after'
        with patch.object(svc.time, 'time', return_value=time.time()+61): svc.pump(e.main, e.bid1)
        assert any(v[0] == e.bid1 and v[3] == 'второе' for v in e.calls[start:])


def delayed_album():
    with r.broadcast_environment() as e, patch.object(svc, 'resume'):
        q.click(e.main, 900, 'broadcast_all')
        q.dispatch(e.main, q.photo(900, 80, 'slow-album'))
        jid = db.DBState(0)[900]['job_id']
        timer = svc._timers[jid]; timer.join(4)
        assert svc.job(jid)['status'] == 'preview'
        # Старое подтверждение во время сборки новой части не обрывает альбом
        with patch.object(threading.Timer, 'start'):
            q.dispatch(e.main, q.photo(900, 81, 'slow-album'))
            q.click(e.main, 900, f'svc_send_{jid}')
            assert db.DBState(0)[900]['job_id'] == jid
        svc._timers.pop(jid).cancel()
        svc.preview(e.main, jid)
        assert db.DBState(0)[900]['job_id'] == jid, 'альбом разделился на задания'
        assert len(svc.job(jid)['payload']) == 2
        q.click(e.main, 900, f'svc_send_{jid}')
        assert svc.job(jid)['status'] == 'queued'
        svc.pump(e.main, e.bid1)
        assert any(v[0] == e.bid1 and v[1] == 'album' and len(v[3]) == 2 for v in e.calls)


def stopped_report():
    from datetime import timedelta
    bid, bot, _ = q.fresh(True)
    now = fs._now_tz()
    pb._close_bot(bot)
    # Планировщик мог сохранить ссылку на экземпляр перед остановкой
    start = len(q.API)
    fs._publish_period(bot, bid, fs.fs_get_group(bid)[1], 77, now-timedelta(days=7), now)
    assert len(q.API) == start, 'остановленный экземпляр снова обращается к API'


def restore_rollback():
    snapshot = db.db_backup(os.environ['DATA_DIR'])
    with open(snapshot, 'rb') as f: uploaded = f.read()
    bid, bot, _ = q.fresh(False)
    token = db.db_get_bot_info(bid)[0]
    pb.running_bots[token] = bot; pb.running_bot_ids[bid] = token
    main = r.mainbot()
    m = q.message(900, 62, None, document=dict(file_id='snapshot', file_unique_id='s', file_name='spambots.db'))
    launched = []
    try:
        with patch.object(main, 'get_file', return_value=r.NS(file_path='snapshot')), \
             patch.object(main, 'download_file', return_value=uploaded), \
             patch.object(h, 'init_db', side_effect=RuntimeError('migration error')), \
             patch.object(h, 'launch_bot', side_effect=lambda *a: launched.append(a) or True):
            r.handler(main, 'handle_db_restore')(m)
        assert db.db_get_bot_info(bid), 'ошибка миграции потеряла прежнюю БД'
        assert any(a[0] == bid for a in launched), 'прежний бот не запущен после отката'
    finally:
        pb.running_bots.pop(token, None); pb.running_bot_ids.pop(bid, None)


if __name__ == '__main__':
    try:
        with patch.multiple(q.telebot.TeleBot, **{n:q.fake_api(n) for n in r.METHODS}), \
             patch.object(fs, 'RATE_LIMIT_SEC', 0):
            for name, fn in [
                ('Большой callback и traceback освобождают гейт', known_fixed),
                ('Недопустимые ID и цены отклоняются', numeric_inputs),
                ('Другой бот принимает совпавший message_id, откат возвращает очередь', replace_delivery),
                ('Другой бот не выполняет старые ответы', replace_reply),
                ('Ротация токена того же бота сохраняет ответы', lambda: replace_reply(True)),
                ('Остановка обрабатывает очередь настоящего пула telebot', drain_workers),
                ('Отмена и сброс приветствия побеждают задержанный ввод', cancelled_save),
                ('Отмена автоответа побеждает задержанный ввод', lambda: cancelled_save(rule=True)),
                ('Отмена FAQ побеждает задержанный ввод', lambda: cancelled_save(faq=True)),
                ('Блокировка отменяет отложенный автоответ', blocked_notification),
                ('Обе копии доступны только в личке суперадмина', backup_private),
                ('Восстановление останавливает старые экземпляры', restore_lifecycle),
                ('Сбой восстановления возвращает запуск прежних ботов', lambda: restore_lifecycle(True)),
                ('Ошибка миграции возвращает прежнюю БД и запуск', restore_rollback),
                ('Планировщик не использует остановленный экземпляр', stopped_report),
                ('Остановленный бот продолжает рассылку после запуска', stopped_broadcast),
                ('Новая рассылка соблюдает сохранённый retry_after', broadcast_cooldown),
                ('Миграция сохраняет retry_after прежней очереди', lambda: broadcast_cooldown(True)),
                ('Поздняя часть альбома входит в то же задание', delayed_album),
            ]:
                r.check(name, fn)
    finally:
        for timer in list(svc._timers.values()): timer.cancel(); timer.join(3)
        for bot in q.BOTS: pb._close_bot(bot)
        db.db_close_all()
    print(f'ИТОГО: {r.OK} OK, {r.FAIL} FAIL')
    sys.exit(bool(r.FAIL))
