"""Четвёртый круг: настоящие обработчики и SQLite, Telegram без сети"""
import importlib.util
import os
import subprocess
import sys
import threading
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('r2', ROOT / 'tests/fix_r2.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
q, db, pb, fs, h, dq = r.q, r.db, r.pb, r.fs, r.h, r.dq


def restart_album(forum=True):
    bid, bot, _ = q.fresh(forum)
    uid = 901
    if forum:
        fs.fs_claim_topic(bid, uid, 'Пользователь')
        fs.fs_set_topic_thread(bid, uid, 77)
        chat = fs.fs_get_group(bid)[1]
    else:
        chat = 777
        db.DBState(bid)[777] = f'await_reply_{uid}'
    with patch.object(threading.Timer, 'start'):
        for mid in (12, 11):
            m = q.photo(chat, mid, 'restart')
            m.message_thread_id = 77
            q.dispatch(bot, m)
    # Потеря всей оперативной памяти буферов и новый экземпляр бота
    fs._admin_album_buffers.clear()
    token = db.db_get_bot_info(bid)[0]
    new = pb.make_purchased_bot(bid, token, 777)
    q.BOTS.append(new)
    db.db_close_all()
    start = len(q.SENT)
    with patch.object(dq.time, 'time', return_value=q.time.time() + 5):
        dq.get_sender(new, bid).pump()
        dq.get_sender(new, bid).pump()
    sent = [v for v in q.SENT[start:] if v[0] == 'copy_messages' and v[1][0] == uid]
    assert len(sent) == 1 and sent[0][1][2] == [11, 12], 'альбом потерян или отправлен повторно'


def prompt_retry(restart=False):
    bid, bot, sender = q.fresh()
    uid = 902
    sender.enqueue(q.message(uid, 1))
    send = bot.send_message
    def limited(chat, text, *args, **kw):
        if chat == uid and 'Отправить сообщение' in text:
            raise q.error(429, 'retry', 30)
        return send(chat, text, *args, **kw)
    with patch.object(bot, 'create_forum_topic', side_effect=q.error(403, 'bot was kicked')), \
         patch.object(bot, 'send_message', side_effect=limited):
        sender.pump()
    pending = fs.fs_get_pending(bid, uid)
    assert pending and not pending[2]
    sender.enqueue(q.message(uid, 2, 'следующее'))
    if restart:
        token = db.db_get_bot_info(bid)[0]
        bot = pb.make_purchased_bot(bid, token, 777)
        q.BOTS.append(bot)
        sender = dq.get_sender(bot, bid)
    with patch.object(bot, 'send_message', wraps=bot.send_message) as send:
        sender.pump()
        assert not any('Отправить сообщение' in str(c) for c in send.call_args_list), 'повтор раньше retry_after'
        with patch.object(dq.time, 'time', return_value=q.time.time() + 31):
            sender.pump()
    assert fs.fs_get_pending(bid, uid)[2], 'окно страховки не восстановлено'
    fs.confirm_send(bot, bid, q.callback(uid, 'fs_confirm_send'))
    sender.pump()
    rows = db.db_delivery_rows(bid, active=False)
    assert len(rows) == 2 and all(v['status'] == 'delivered' for v in rows)


def hidden_links(forum=False, caption=False):
    bid, bot, sender = q.fresh(forum)
    uid = 903
    url = 'https://example.com/document?a=1&b=2'
    entities = [dict(type='text_link', offset=3, length=8, url=url)]
    if caption:
        m = q.message(uid, 1, None, caption='😀 Документ', caption_entities=entities,
                      photo=[dict(file_id='photo', file_unique_id='p', width=1, height=1)])
    else:
        m = q.message(uid, 1, '😀 Документ', entities=entities)
    sender.enqueue(m)
    if forum:
        sender.enqueue(q.message(uid, 2, 'ещё текст'))
    start = len(q.SENT)
    sender.pump()
    target = fs.fs_get_group(bid)[1] if forum else 777
    bodies = '\n'.join(str(v[1:3]) for v in q.SENT[start:] if v[1][0] == target)
    assert 'https://example.com/document?a=1&amp;b=2' in bodies, 'скрытый URL потерян'
    assert all(v['status'] == 'delivered' for v in db.db_delivery_rows(bid, active=False))


def database_gate():
    bid, _, _ = q.fresh(False)
    caught = None
    try:
        db.db_add_bot_admin(bid, 2**80)
    except Exception as exc:
        caught = exc  # удерживаем traceback как журнал/обработчик ошибок
    assert caught is not None
    acquired = threading.Event()
    def probe():
        if db._restore_gate.acquire(timeout=.3):
            acquired.set()
            db._restore_gate.release()
    worker = threading.Thread(target=probe)
    worker.start(); worker.join(1)
    try:
        assert acquired.is_set(), 'исключение оставило общую блокировку БД'
    finally:
        # Только очистка неисправленной версии после воспроизведения
        if not acquired.is_set():
            tb = caught.__traceback__
            while tb:
                lease = tb.tb_frame.f_locals.get('c')
                if isinstance(lease, db._ConnLease):
                    lease.close()
                tb = tb.tb_next


def input_ids():
    bid, bot, _ = q.fresh(False)
    for callback, handler, state in [('p_add_admin', 'p_save_new_admin', 'p_await_new_admin'),
                                      ('p_change_primary', 'p_save_primary', 'p_await_primary')]:
        for text in (str(2**63), str(-2**80), '0', 'не число'):
            q.click(bot, 777, callback)
            start = len(q.SENT)
            r.handler(bot, handler)(q.message(777, 10, text))
            assert any('Неверный ID' in str(v[1]) for v in q.SENT[start:])
            assert db.DBState(bid).get(777) == state
    assert db.db_get_bot_admins(bid) == [777]


def other_inputs():
    main = r.mainbot()
    bid, _, _ = q.fresh(False)
    for fn, state, text in [('get_admin_id', dict(step='await_admin_id', token='123:TEST'), str(2**63)),
                            ('handle_coupon_input', 'await_coupon_input', f'{2**63} 30'),
                            ('handle_free_renew_input', 'await_free_renew_input', f'{2**63} 30')]:
        db.DBState(0)[900] = state
        start = len(q.SENT)
        r.handler(main, fn)(q.message(900, 10, text))
        assert any('Неверный ID' in str(v[1]) for v in q.SENT[start:])
        assert db.DBState(0).get(900) == state
    old = db.get_price()
    for value in ('nan', 'inf', '1e309', str(2**80)):
        db.DBState(0)[900] = 'await_new_price'
        r.handler(main, 'handle_new_price')(q.message(900, 11, value))
        assert db.get_price() == old
    assert fs._parse_group_id('-100' + '9'*30) is None
    assert fs._parse_group_id('1001234567890') == -1001234567890


def callback_ids():
    bid, bot, _ = q.fresh(False)
    main = r.mainbot()
    for current, uid, action in [(bot, 777, 'p_block_'), (main, 900, 'wr_confirm_')]:
        start = len(q.API)
        q.click(current, uid, action + str(2**80))
        assert any(n == 'answer_callback_query' and a[1] == 'Неверный ID'
                   for n, a, _, _ in q.API[start:])
    assert not db.db_get_blocked_list(bid)


def admin_links():
    bid, bot, _ = q.fresh(False)
    db.DBState(bid)[777] = 'await_reply_904'
    start = len(q.SENT)
    q.dispatch(bot, q.message(777, 20, 'Документ', entities=[dict(type='text_link', offset=0,
        length=8, url='https://example.com/admin')]))
    assert any(v[0] == 'send_message' and v[1][0] == 904 and
               'https://example.com/admin' in v[1][1] for v in q.SENT[start:])


def album_permissions():
    bid, bot, sender = q.fresh()
    gid = fs.fs_get_group(bid)[1]
    db.db_add_bot_admin(bid, 778)
    for mid, admin in [(31, 777), (32, 778)]:
        m = q.photo(gid, mid, 'rights')
        m.from_user.id = admin
        with patch.object(threading.Timer, 'start'):
            fs._buffer_admin_album(bot, bid, gid, 905, 77, m)
    with patch.object(dq.time, 'time', return_value=q.time.time()+2), \
         patch.object(bot, 'copy_messages', side_effect=q.error(429, 'retry', 30)):
        sender.pump()
    db.db_remove_bot_admin(bid, 778)
    with patch.object(dq.time, 'time', return_value=q.time.time()+33), \
         patch.object(bot, 'copy_messages', return_value=[q.t.MessageID(100)]) as copy:
        sender.pump()
    assert copy.call_args.args[2] == [31]


def real_restart(forum=True):
    bid, _, _ = q.fresh(forum)
    uid = 906
    if forum:
        fs.fs_claim_topic(bid, uid, 'Пользователь')
        fs.fs_set_topic_thread(bid, uid, 77)
    else:
        db.DBState(bid)[777] = f'await_reply_{uid}'
    # Дочерний процесс принимает часть альбома и аварийно завершается
    env = dict(os.environ, R4_DB_PATH=db.DB_PATH, R4_BID=str(bid), R4_FORUM=str(int(forum)), PYTHONUTF8='1')
    child = subprocess.run([sys.executable, '-X', 'utf8', __file__, '--album-child'],
                           env=env, capture_output=True, timeout=20)
    assert child.returncode == 0, 'дочерний процесс не завершил приём'
    token = db.db_get_bot_info(bid)[0]
    bot = pb.make_purchased_bot(bid, token, 777)
    q.BOTS.append(bot)
    chat = fs.fs_get_group(bid)[1] if forum else 777
    m = q.photo(chat, 42, 'real-restart'); m.message_thread_id = 77
    with patch.object(threading.Timer, 'start'):
        q.dispatch(bot, m)
    start = len(q.SENT)
    with patch.object(dq.time, 'time', return_value=q.time.time()+3):
        dq.get_sender(bot, bid).pump()
        dq.get_sender(bot, bid).pump()
    sent = [v for v in q.SENT[start:] if v[0] == 'copy_messages' and v[1][0] == uid]
    assert len(sent) == 1 and sent[0][1][2] == [41, 42]


def album_child():
    db.db_close_all()
    db.DB_PATH = os.environ['R4_DB_PATH']
    bid = int(os.environ['R4_BID'])
    with patch.multiple(q.telebot.TeleBot, **{n:q.fake_api(n) for n in r.METHODS}), \
         patch.object(threading.Timer, 'start'):
        bot = pb.make_purchased_bot(bid, db.db_get_bot_info(bid)[0], 777)
        chat = fs.fs_get_group(bid)[1] if os.environ['R4_FORUM'] == '1' else 777
        m = q.photo(chat, 41, 'real-restart'); m.message_thread_id = 77
        q.dispatch(bot, m)
    os._exit(0)


def blocked_prompt():
    bid, bot, sender = q.fresh(False)
    fs.fs_append_pending(bid, 907, dict(mid=1, type='text'))
    db.db_block_user(bid, 907)
    with patch.object(bot, 'send_message', wraps=bot.send_message) as send:
        sender.pump()
        assert not any(c.args[0] == 907 for c in send.call_args_list)
    db.db_unblock_user(bid, 907)
    sender.pump()
    assert fs.fs_get_pending(bid, 907)[2]


if __name__ == '__main__':
    if '--album-child' in sys.argv:
        album_child()
    try:
        with patch.multiple(q.telebot.TeleBot, **{n:q.fake_api(n) for n in r.METHODS}):
            for name, fn in [
                ('Форумный альбом переживает перезапуск', restart_album),
                ('Личный альбом переживает перезапуск', lambda: restart_album(False)),
                ('Окно страховки повторяется после 429', prompt_retry),
                ('Срок повтора окна переживает перезапуск', lambda: prompt_retry(True)),
                ('Скрытая ссылка в личной доставке', hidden_links),
                ('Скрытая ссылка в склейке форума', lambda: hidden_links(True)),
                ('Скрытая ссылка в подписи', lambda: hidden_links(caption=True)),
                ('Ошибка БД освобождает общую блокировку', database_gate),
                ('Ввод ID админов проверяет диапазон', input_ids),
                ('Все поля ID и сумм проверяют диапазон', other_inputs),
                ('ID из кнопок проверяются до БД', callback_ids),
                ('Ссылка в тексте ответа админа', admin_links),
                ('Отзыв прав до повтора альбома', album_permissions),
                ('Форумный альбом после гибели процесса и новой части', real_restart),
                ('Личный альбом после гибели процесса и новой части', lambda: real_restart(False)),
                ('Заблокированный не получает окно до разблокировки', blocked_prompt),
            ]:
                r.check(name, fn)
    finally:
        for bot in q.BOTS: pb._close_bot(bot)
        db.db_close_all()
    print(f'ИТОГО: {r.OK} OK, {r.FAIL} FAIL')
    sys.exit(bool(r.FAIL))
