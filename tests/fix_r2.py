"""Второй круг: реальные обработчики и временная SQLite, Telegram без сети"""
import os
import sys
import logging
import threading
import time
import json
import tempfile
import traceback
import io
import sqlite3
import runpy
import subprocess
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch
from contextlib import contextmanager

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.update(BOT_TOKEN='123456:' + 'A' * 35, SUPER_ADMIN='900', WEBHOOK_URL='')
import importlib.util
spec = importlib.util.spec_from_file_location('r2_fixture', ROOT / 'tests/delivery_queue.py')
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)
import database as db
import purchased_bot as pb
import forum_support as fs
import handlers as h
import payments
import purchase_store as ps
import delivery_queue as dq
db.init_db()
logging.disable(logging.CRITICAL)
OK = FAIL = 0


def check(name, fn):
    global OK, FAIL
    try:
        fn()
        OK += 1
        print('[OK] ' + name)
    except Exception as exc:
        FAIL += 1
        print('[FAIL] ' + name + ': ' + type(exc).__name__)
        traceback.print_exc()


def handler(bot, name):
    return next(x['function'] for x in bot.message_handlers + bot.callback_query_handlers
                if x['function'].__name__ == name)


def mainbot():
    bot = q.telebot.TeleBot(os.environ['BOT_TOKEN'], threaded=False)
    h.register(bot)
    q.BOTS.append(bot)
    return bot


def revoked():
    bid, bot, _ = q.fresh()
    db.db_add_bot_admin(bid, 710)
    assert fs.fs_is_admin(bid, 710)
    db.db_remove_bot_admin(bid, 710)
    fs.fs_claim_topic(bid,701,'Юзер');fs.fs_set_topic_thread(bid,701,77)
    gid=fs.fs_get_group(bid)[1]
    m=q.message(gid,50,'Ответ',message_thread_id=77,
                **{'from':dict(id=710,is_bot=False,first_name='Бывший админ')})
    before=len(q.API)
    fs.fs_handle_group_message(bot,bid,m,999,{})
    cb=q.callback(710,'fs_pin_block_701');cb.message=m
    fs.fs_pin_callback(bot,bid,cb)
    assert not fs.fs_is_admin(bid,710) and not db.db_is_blocked(bid,701)
    assert not any(a and a[0]==701 and n in ('copy_message','forward_message','send_message') for n,a,k,_ in q.API[before:])


def old_group():
    bid, bot, _ = q.fresh()
    old = fs.fs_get_group(bid)
    fs.fs_unbind_group(bid)
    fs.fs_bind_group(bid, -800000 - bid, 'Новая')
    start = len(q.API)
    try:
        fs.fs_ensure_topic_info(bot, bid, old, q.t.User(711, False, 'Юзер'))
    except Exception:
        pass
    assert not any(a and a[0] == old[1] and n in ('create_forum_topic', 'copy_message', 'send_message')
                   for n, a, k, _ in q.API[start:])


def reconnect():
    bid, bot, _ = q.fresh()
    fs.fs_claim_topic(bid, 712, 'Юзер'); fs.fs_set_topic_thread(bid, 712, 77)
    fs.fs_map_message(bid, fs.fs_get_group(bid)[1], 11, 712, 77, 1)
    old = fs.fs_get_group(bid)
    with patch.object(fs, 'fs_verify_group', return_value=(True, None)), \
         patch.object(bot, 'create_forum_topic', side_effect=TimeoutError), \
         patch.object(fs, 'fs_refresh_management'):
        fs._finish_connect(bot, bid, 777, old[1], 'Группа', {})
    assert fs.fs_get_group(bid) and fs.fs_get_topic(bid, 712)
    assert fs.fs_get_mapping(old[1], 11)


def admin_album():
    bid, bot, _ = q.fresh()
    gid = fs.fs_get_group(bid)[1]
    key = (bid, 713, 'album')
    fs._admin_album_buffers[key] = dict(ids=[11, 10], msgs=[], gid=gid, thread=77)
    with patch.object(bot, 'copy_messages', side_effect=lambda c, g, ids: (
            [q.t.MessageID(i) for i in ids] if ids == sorted(ids) else (_ for _ in ()).throw(q.error(400, 'MESSAGE_IDS_INVALID')))) as api:
        fs._flush_admin_album(bot, bid, key)
    assert api.call_args.args[2] == [10, 11]


def markup_retry():
    bid, bot, _ = q.fresh(False)
    fs.fs_append_pending(bid, 714, dict(mid=1, type='text'))
    with patch.object(bot, 'edit_message_reply_markup', side_effect=[TimeoutError(), q.error(400, 'message is not modified')]):
        fs.confirm_send(bot, bid, q.callback(714, 'fs_send'))
        fs.confirm_send(bot, bid, q.callback(714, 'fs_send'))
    assert not fs.fs_get_pending(bid, 714)


def no_confirm_limit():
    bid, bot, _ = q.fresh(False)
    fs.fs_append_pending(bid, 715, dict(mid=1, type='text'))
    fs.rate_limited(bid, 715)
    fs.confirm_send(bot, bid, q.callback(715, 'fs_send'))
    assert not fs.fs_get_pending(bid, 715)


def coupon():
    bot = mainbot(); db.db_set_free_coupon(716, 7)
    barrier = threading.Barrier(2)
    original = h.db_get_free_coupon
    def read(uid):
        result = original(uid); barrier.wait(3); return result
    with patch.object(h, 'db_get_free_coupon', side_effect=read):
        workers = [threading.Thread(target=handler(bot, 'buy_bot_cb'), args=(q.callback(716, 'buy_bot'),)) for _ in range(2)]
        for w in workers: w.start()
        for w in workers: w.join(5)
    c = db._conn()
    count = c.execute('SELECT COUNT(*) FROM setup_grants WHERE user_id=716').fetchone()[0]; c.close()
    assert count == 1


def crypto_context():
    bot = mainbot(); bid = db.db_add_bot(717, '123717:FAKE', 717)
    db.DBState(0)[717] = dict(step='renewing', bot_id=bid)
    def invoice(uid):
        db.DBState(0)[717] = dict(step='new')
        return dict(invoice_id=9871, amount='10', pay_url='https://example.com')
    with patch.object(h, 'cb_create_invoice', side_effect=invoice):
        handler(bot, 'pay_cryptobot_cb')(q.callback(717, 'pay_cryptobot'))
    assert ps.db_get_payment(717, 9871)['bot_id'] == bid


def token_threads():
    bot = mainbot(); ps.db_mark_paid(718)
    db.DBState(0)[718] = 'await_bot_token_trial'
    before = set(threading.enumerate())
    with patch.object(q.telebot.TeleBot, 'get_me', side_effect=ValueError):
        for i in range(4):
            handler(bot, 'get_bot_token')(q.message(718, 1, '123456:' + 'Z'*35))
    assert not [t for t in threading.enumerate() if t not in before and t.is_alive()]


def main_reply_state():
    bot = mainbot(); db.DBState(0)[900] = 'await_admin_msg_719'
    original = bot.send_message
    def send(uid, *a, **kw):
        if uid == 719: db.DBState(0)[900] = 'await_admin_msg_720'
        return original(uid, *a, **kw)
    with patch.object(bot, 'send_message', side_effect=send):
        handler(bot, 'admin_text')(q.message(900, 1, 'Ответ'))
    assert db.DBState(0).get(900) == 'await_admin_msg_720'


def revoked_state():
    bid, bot, _ = q.fresh(False)
    db.db_add_bot_admin(bid, 721); db.DBState(bid)[721] = 'await_reply_722'
    db.db_remove_bot_admin(bid, 721)
    q.dispatch(bot, q.message(721, 2, 'Обращение'))
    assert db.DBState(bid).get(721) is None
    assert db.db_delivery_rows(bid)


def user_album():
    bid, bot, _ = q.fresh(False); db.DBState(bid)[723] = 'await_msg'
    fn = handler(bot, 'p_user_media')
    fn(q.photo(723, 10, 'ua')); fn(q.photo(723, 11, 'ua'))
    assert len(db.db_delivery_rows(bid)) == 2


def private_admin_album():
    bid, bot, _ = q.fresh(False); db.DBState(bid)[777] = 'await_reply_724'
    start = len(q.API)
    q.dispatch(bot, q.photo(777, 11, 'aa')); q.dispatch(bot, q.photo(777, 10, 'aa'))
    flush = getattr(bot, '_flush_reply_albums', None)
    if flush: flush()
    assert any(n == 'copy_messages' and a[0] == 724 and a[2] == [10, 11] for n,a,k,_ in q.API[start:])


def voice_caption():
    bid, bot, sender = q.fresh(False)
    m = q.message(725, 10, None, voice=dict(file_id='voice', file_unique_id='v', duration=1), caption='подпись')
    q.dispatch(bot, m)
    start = len(q.API); q.NOW[0] += 3
    q.drain(bot, bid)
    assert any(n == 'send_voice' and k.get('caption') and 'подпись' in k['caption'] for n,a,k,_ in q.API[start:])


def construction_cleanup():
    pool = []
    original = q.telebot.TeleBot
    def create(*a, **kw):
        bot = original(*a, **kw); pool.append(bot); return bot
    bid = db.db_add_bot(777, '190000:FAKE', 777)
    with patch.object(pb.telebot, 'TeleBot', side_effect=create), patch.object(pb, 'register_forum_support', side_effect=RuntimeError):
        assert not pb.launch_bot(bid, '190000:FAKE', 777)
    assert pool and all(not w.is_alive() for b in pool for w in b.worker_pool.workers)


def ton_pages():
    tx = lambda i, comment='other': dict(utime=int(time.time()), transaction_id=dict(lt=str(i), hash=f'h{i}'),
                                         in_msg=dict(value='1000000000', message=comment))
    pages = [dict(ok=True, result=[tx(i) for i in range(100, 50, -1)]), dict(ok=True, result=[tx(50, 'code')])]
    with patch.object(payments.requests, 'get', side_effect=lambda *a, **k: NS(json=lambda: pages.pop(0))) as req:
        assert payments.ton_check_transfer(726, 1, 'code', return_tx=True)
    assert req.call_count == 2


def ton_expired():
    bot = mainbot()
    ps.db_set_pending(727, 'old', provider='ton', expected_ton=1, amount=10, expires_at=time.time()-86400)
    with patch.object(h, 'ton_check_transfer', return_value=dict(tx_hash='fresh', ton_amount=1, utime=time.time())):
        handler(bot, 'ton_check_auto_cb')(q.callback(727, 'ton_check_old'))
    assert not ps.db_get_payment(727, 'old', 'ton')['settled']


class ProcessDeath(BaseException): pass


def money_crash(kind):
    bid, bot, _ = q.fresh(False)
    db.db_add_bot_earnings(bid, 20)
    old_exp = db.db_get_bot_expires(bid)
    if kind == 'withdraw':
        db.DBState(bid)[777] = dict(step='p_await_withdraw_amount', balance=20, address='U'*48)
        call = lambda: handler(bot, 'p_save_withdraw_amount')(q.message(777, 101, '5'))
        needle = 'INSERT INTO withdrawal_requests'
    else:
        db.set_price(5)
        call = lambda: handler(bot, 'p_renew_bal_cb')(q.callback(777, 'p_renew_bal_30'))
        needle = 'UPDATE bots SET expires_at'
    original = db._CachedConn.execute
    def execute(conn, sql, *args, **kw):
        if needle in sql: raise ProcessDeath()
        return original(conn, sql, *args, **kw)
    with patch.object(db._CachedConn, 'execute', execute):
        try: call()
        except ProcessDeath: pass
    db.db_close_all()
    assert db.db_get_bot_earnings(bid)[0] == 20
    assert db.db_get_bot_expires(bid) == old_exp
    assert not [r for r in db.db_get_pending_withdrawals() if r[1] == bid]
    call(); call()
    assert db.db_get_bot_earnings(bid)[0] == 15


def rejection():
    bid, bot, _ = q.fresh(False); main = mainbot()
    wr = db.db_create_withdrawal(bid, 777, 'U'*48, 5)
    original = db._CachedConn.execute
    def execute(conn, sql, *args, **kw):
        if 'INSERT INTO bot_earnings' in sql or 'UPDATE bot_earnings SET balance_usdt=balance_usdt+' in sql:
            raise sqlite3.OperationalError('injected')
        return original(conn, sql, *args, **kw)
    with patch.object(db._CachedConn, 'execute', execute):
        try: handler(main, 'wr_reject_cb')(q.callback(900, f'wr_reject_{wr}'))
        except sqlite3.OperationalError: pass
    assert db.db_get_withdrawal(wr)[-1] == 'pending'
    handler(main, 'wr_reject_cb')(q.callback(900, f'wr_reject_{wr}'))
    handler(main, 'wr_reject_cb')(q.callback(900, f'wr_reject_{wr}'))
    assert db.db_get_bot_earnings(bid)[0] == 5


def real_process_death(kind):
    bid, _, _ = q.fresh(False); db.db_add_bot_earnings(bid, 20); db.set_price(5)
    db.DBState(bid)[777] = dict(step='p_await_withdraw_amount', balance=20, address='U'*48)
    # Дочерний процесс завершается между SQL-списанием и записью результата
    code = '''
import os,sys,types
from unittest.mock import patch
sys.modules['dotenv']=types.SimpleNamespace(load_dotenv=lambda *a,**k:None)
import database as db, purchased_bot as pb, telebot
db.DB_PATH=sys.argv[1];bid=int(sys.argv[2]);kind=sys.argv[3]
bot=telebot.TeleBot(os.environ['BOT_TOKEN'],threaded=False)
bot.get_me=lambda:types.SimpleNamespace(id=999)
with patch.object(telebot,'TeleBot',return_value=bot),patch.object(pb,'register_forum_support'):
    bot=pb.make_purchased_bot(bid,os.environ['BOT_TOKEN'],777)
needle='INSERT INTO withdrawal_requests' if kind=='withdraw' else 'UPDATE bots SET expires_at'
original=db._CachedConn.execute
def execute(conn,sql,*a,**kw):
    if needle in sql:os._exit(73)
    return original(conn,sql,*a,**kw)
db._CachedConn.execute=execute
user=types.SimpleNamespace(id=777)
m=types.SimpleNamespace(from_user=user,chat=types.SimpleNamespace(id=777),message_id=141,text='5')
name='p_save_withdraw_amount' if kind=='withdraw' else 'p_renew_bal_cb'
event=m if kind=='withdraw' else types.SimpleNamespace(from_user=user,message=m,data='p_renew_bal_30',id='cb')
next(v['function'] for v in bot.message_handlers+bot.callback_query_handlers if v['function'].__name__==name)(event)
'''
    result = subprocess.run([sys.executable, '-c', code, db.DB_PATH, str(bid), kind], cwd=ROOT,
                            capture_output=True, timeout=20)
    assert result.returncode == 73
    db.db_close_all()
    assert db.db_get_bot_earnings(bid)[0] == 20


def checker_renewal():
    bid, bot, _ = q.fresh(False)
    old = (h.datetime.now() - __import__('datetime').timedelta(days=31)).isoformat()
    c = db._conn(); c.execute('UPDATE bots SET expires_at=? WHERE id=?', (old, bid)); c.commit(); c.close()
    token = db.db_get_bot_info(bid)[0]; pb.running_bots[token] = bot
    def snapshot():
        db.db_renew_bot(bid)
        return [(bid, 777, token, 777, old)]
    ticks = [0]
    def sleep(_):
        ticks[0] += 1
        if ticks[0] > 1: raise ProcessDeath()
    def thread(*a, **kw): return NS(start=kw['target'])
    try:
        with patch.object(h, 'db_get_all_bots_for_checker', side_effect=snapshot), \
             patch.object(h.threading, 'Thread', side_effect=thread), \
             patch.object(h.time, 'sleep', side_effect=sleep), patch.object(h, 'stop_bot') as stop:
            try: h.start_subscription_checker(mainbot())
            except ProcessDeath: pass
        assert not stop.called
        assert any(row[0] == bid for row in db.db_get_all_bots_for_checker())
    finally: pb.running_bots.pop(token, None)


def webhook_fallback(busy=False):
    import config
    import flask
    original_start = threading.Thread.start
    def start(thread):
        if type(thread).__name__ == 'WorkerThread': return original_start(thread)
    try:
        with patch.object(config, 'WEBHOOK_URL', 'https://example.com/webhook'), \
             patch.object(db, 'db_get_all_bots_for_checker', return_value=[]), \
             patch.object(h, 'start_subscription_checker'), patch.object(fs, 'start_weekly_scheduler'), \
             patch('service_broadcast.start'), patch.object(threading.Thread, 'start', start), \
             patch.object(time, 'sleep'), patch.object(q.telebot.TeleBot, 'set_webhook',
                  side_effect=None if busy else RuntimeError('network')) as set_hook, \
             patch.object(q.telebot.TeleBot, 'delete_webhook', return_value=True) as delete, \
             patch.object(q.telebot.TeleBot, 'infinity_polling', side_effect=lambda **k: calls.append(('poll', k))), \
             patch.object(flask.Flask, 'run', side_effect=SystemExit(1)):
            calls = []
            original = delete.side_effect
            delete.side_effect = lambda **k: calls.append(('delete', k)) or True
            scope = runpy.run_path(str(ROOT / 'main.py'), run_name='__main__')
            pb._close_bot(scope['bot'])
        assert any(n == 'poll' for n,k in calls)
        assert calls[0][0] == 'delete' and all(not k.get('drop_pending_updates') for n,k in calls if n == 'delete')
        assert set_hook.call_count == (1 if busy else 3)
    except SystemExit as exc:
        raise AssertionError('Выход вместо polling') from exc


def backup_snapshot():
    bot = mainbot(); originals = []
    def document(uid, file, **kw):
        originals.append(Path(file.name).resolve() == Path(db.DB_PATH).resolve())
        # Проверяем полученный снимок, пока исходная БД имеет незавершённую запись
        with sqlite3.connect(file.name) as c:
            assert c.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    with patch.object(bot, 'send_document', side_effect=document):
        handler(bot, 'cmd_backup')(q.message(900, 1, '/backup'))
        handler(bot, 'backup_db_cb')(q.callback(900, 'backup_db'))
    assert originals == [False, False]


def premium_fallback():
    from keyboards import TE_CHECK, _btn
    calls = []
    def reject(self, chat, text, **kw):
        calls.append((text, kw))
        if 'tg-emoji' in text:
            raise q.error(400, 'CUSTOM_EMOJI_NOT_ALLOWED')
        if any(getattr(b, 'icon_custom_emoji_id', None) for row in kw['reply_markup'].keyboard for b in row):
            raise q.error(400, 'BUTTON_ICON_INVALID')
        return q.message(chat, 10, text)
    with patch.object(q.telebot.TeleBot, 'send_message', reject):
        bid, bot, _ = q.fresh(False)
        kb = q.t.InlineKeyboardMarkup(); kb.add(_btn('Да', icon='✔️', callback_data='ok'))
        bot.send_message(728, TE_CHECK + ' Готово', parse_mode='HTML', reply_markup=kb)
        assert len(calls) == 2 and 'tg-emoji' not in calls[-1][0]
        assert not getattr(calls[-1][1]['reply_markup'].keyboard[0][0], 'icon_custom_emoji_id', None)
        assert float(db.db_get_bot_setting(bid, 'premium_unavailable_until')) > time.time()
        token = db.db_get_bot_info(bid)[0]
        other = pb.make_purchased_bot(bid, token, 777);q.BOTS.append(other)
        other.send_message(728, TE_CHECK, reply_markup=kb)
        assert len(calls) == 3
        db.db_set_bot_setting(bid, 'premium_unavailable_until', '0')
        bot.send_message(728, TE_CHECK, reply_markup=kb)
        assert len(calls) == 5
        db.db_set_bot_setting(bid, 'premium_unavailable_until', '0')
        bot.send_message(728, 'Текст', reply_markup=kb)
        assert len(calls) == 7


def welcome_retry():
    bid, bot, _ = q.fresh(False)
    q.click(bot, 777, 'p_edit_welcome')
    m = q.message(777, 3, 'Плохой')
    with patch.object(type(m), 'html_text', property(lambda self: '<b>Плохой')):
        q.dispatch(bot, m)
    q.dispatch(bot, q.message(777, 4, 'Исправлено'))
    assert db.db_get_bot_setting(bid, 'welcome') == 'Исправлено'


@contextmanager
def broadcast_environment():
    import service_broadcast as svc
    from premium_emoji import protect_premium_emoji
    for table in ('service_broadcasts','service_recipients','service_files','service_file_ids'):
        svc.query(f'DELETE FROM {table}', write=True)
    main = mainbot()
    bid1, bot1, _ = q.fresh(False)
    bid2, bot2, _ = q.fresh(False)
    stopped, _, _ = q.fresh(False)
    expired, expired_bot, _ = q.fresh(False)
    tokens = []
    for bid, bot in ((bid1,bot1),(bid2,bot2),(expired,expired_bot)):
        token = db.db_get_bot_info(bid)[0]; tokens.append(token)
        pb.running_bots[token] = bot; pb.running_bot_ids[bid] = token
    c = db._conn(); c.execute('UPDATE bots SET expires_at=? WHERE id=?', ('2000-01-01', expired)); c.commit(); c.close()
    for bid, ids in ((0,[8000]),(bid1,[8001,8002,8004]),(bid2,[8001,8003]),(stopped,[8005]),(expired,[8006])):
        for uid in ids: db.db_add_user(bid, uid)
    db.db_block_user(bid1,8004)
    calls, downloads, fault = [], [], [None]
    seq = [50000]
    def sender(bid, kind):
        def send(uid, value, **kw):
            calls.append((bid,kind,uid,value,kw))
            if fault[0]: fault[0](bid,kind,uid,value,kw)
            seq[0] += 1
            def result(media, index=0):
                raw = dict(file_id=f'{bid}_{kind}_{index}', file_unique_id=f'u{index}')
                if media == 'photo': raw.update(width=1,height=1)
                elif media in ('video','animation','video_note'): raw.update(width=1,height=1,duration=1,length=1)
                elif media in ('voice','audio'): raw.update(duration=1)
                elif media == 'sticker': raw.update(type='regular',width=1,height=1,is_animated=False,is_video=False)
                return q.message(uid,seq[0]+index,None,**{media:[raw] if media=='photo' else raw})
            if kind=='text': return q.message(uid,seq[0],value)
            if kind=='album': return [result(m.type,i) for i,m in enumerate(value)]
            return result(kind)
        return send
    for bid, bot in ((0,main),(bid1,bot1),(bid2,bot2)):
        for kind in ('text','photo','video','animation','document','audio','voice','video_note','sticker','album'):
            setattr(bot,'send_message' if kind=='text' else 'send_media_group' if kind=='album' else 'send_'+kind,sender(bid,kind))
        if bid:
            bot._premium_guard=False
            protect_premium_emoji(bot,bid)
    main.get_file=lambda fid:NS(file_path=f'files/{fid}.bin')
    main.download_file=lambda path:downloads.append(path) or b'file data'
    try: yield NS(svc=svc,main=main,bid1=bid1,bid2=bid2,bot1=bot1,bot2=bot2,calls=calls,downloads=downloads,fault=fault)
    finally:
        for token in tokens: pb.running_bots.pop(token,None)
        for bid in (bid1,bid2,expired): pb.running_bot_ids.pop(bid,None)


def new_job(e, messages):
    jid=None
    for m in messages: jid=e.svc.append(e.main,900,900,'all',m,jid)
    e.svc.preview(e.main,jid)
    return jid


def service_targets():
    with broadcast_environment() as e:
        q.click(e.main,900,'broadcast_all')
        q.dispatch(e.main,q.message(900,210,'Объявление',entities=[dict(type='bold',offset=0,length=10)]))
        value=db.DBState(0).get(900)
        assert isinstance(value,dict) and value.get('job_id')
        jid=value['job_id']
        assert not [v for v in e.calls if v[2] in (8000,8001,8002,8003)]
        assert any('Получат 4 человек через 3 ботов' in v[3] for v in e.calls if v[1]=='text')
        buttons=[b for v in e.calls if v[1]=='text' for row in getattr(v[4].get('reply_markup'),'keyboard',[]) for b in row]
        assert {b.text for b in buttons}>={'Отправить','Отмена'}
        assert e.svc.confirm(e.main,jid,900,start=False)
        assert not e.svc.confirm(e.main,jid,900,start=False)
        for bid in (0,e.bid1,e.bid2): e.svc.pump(e.main,bid)
        delivered=[v for v in e.calls if v[2] in range(8000,8007)]
        assert sorted(v[2] for v in delivered)==[8000,8001,8002,8003]
        assert next(v for v in delivered if v[2]==8001)[0]==e.bid1
        assert all(v[4]['entities'][0].type=='bold' for v in delivered)
        assert any('Отправлено: 4\nНе доставлено: 0\nВсего: 4' in v[3] for v in e.calls if v[1]=='text')


def service_retry_restart():
    with broadcast_environment() as e:
        jid=new_job(e,[q.message(900,220,'Объявление')]);e.svc.confirm(e.main,jid,900,start=False)
        attempt=[0]
        def fault(bid,kind,uid,*args):
            if uid==8001 and attempt[0]==0:
                attempt[0]+=1;raise q.error(429,'retry',30)
            if uid==8003:raise q.error(403,'blocked')
        e.fault[0]=fault
        e.svc.pump(e.main,e.bid1)
        assert not [v for v in e.calls if v[2]==8002]
        before=len(e.calls);e.svc.pump(e.main,e.bid1)
        assert len(e.calls)==before
        future=time.time()+31
        with patch.object(e.svc.time,'time',return_value=future):e.svc.pump(e.main,e.bid1,limit=1)
        db.db_close_all()
        # Новый объект/импорт отправителя получает прогресс только из БД
        import importlib
        svc=importlib.reload(e.svc)
        with patch.object(svc.time,'time',return_value=future):
            for bid in (0,e.bid1,e.bid2):svc.pump(e.main,bid)
        assert len([v for v in e.calls if v[2]==8001])==2 # 429 + единственная успешная отправка
        assert len([v for v in e.calls if v[2]==8002])==1
        assert svc.job(jid)['status']=='done'
        assert any('Отправлено: 3\nНе доставлено: 1\nВсего: 4' in v[3] for v in e.calls if v[1]=='text')


def service_media():
    with broadcast_environment() as e:
        entities=[dict(type='bold',offset=0,length=2),dict(type='custom_emoji',offset=0,length=2,custom_emoji_id='123'),
                  dict(type='text_link',offset=3,length=4,url='https://example.com'),dict(type='spoiler',offset=3,length=4),
                  dict(type='blockquote',offset=0,length=7)]
        for kind in ('photo','video','animation','document','audio','voice','video_note','sticker','album'):
            messages=[]
            for i in range(2 if kind=='album' else 1):
                actual='photo' if kind=='album' else kind
                data=dict(file_id=f'{kind}_{i}',file_unique_id=f'u{i}',width=1,height=1,duration=1,length=1,
                          type='regular',is_animated=False,is_video=False)
                messages.append(q.message(900,300+i,None,caption='🙂 текст',caption_entities=entities,
                                          **{actual:[data] if actual=='photo' else data}))
            before=len(e.downloads);jid=new_job(e,messages)
            assert len(e.downloads)-before==len(messages)
            e.svc.confirm(e.main,jid,900,start=False)
            for bid in (0,e.bid1,e.bid2):e.svc.pump(e.main,bid)
            sent=[v for v in e.calls if v[1]==kind and v[2] in (8001,8002,8003)]
            assert len(sent)==3
            for bid in (e.bid1,e.bid2):
                first=next(v for v in sent if v[0]==bid)
                assert isinstance(first[3][0].media if kind=='album' else first[3],io.BytesIO)
            second=next(v for v in sent if v[2]==8002)
            assert isinstance(second[3][0].media if kind=='album' else second[3],str)
            if kind not in ('sticker','video_note','album'):
                assert [{k:v for k,v in item.to_dict().items() if v is not None} for item in second[4]['caption_entities']]==entities
            if kind=='album':
                assert all([{k:v for k,v in item.to_dict().items() if v is not None} for item in m.caption_entities]==entities for m in second[3])
            assert len(e.downloads)-before==len(messages)


def service_custom_emoji():
    with broadcast_environment() as e:
        entities=[dict(type='custom_emoji',offset=0,length=2,custom_emoji_id='123'),dict(type='bold',offset=3,length=4)]
        jid=new_job(e,[q.message(900,410,'🙂 текст',entities=entities)]);e.svc.confirm(e.main,jid,900,start=False)
        def fault(bid,kind,uid,value,kw):
            if bid==e.bid1 and any(v.type=='custom_emoji' for v in kw.get('entities',[])):
                raise q.error(400,'CUSTOM_EMOJI_NOT_ALLOWED')
        e.fault[0]=fault
        for bid in (0,e.bid1,e.bid2):e.svc.pump(e.main,bid)
        attempts=[v for v in e.calls if v[2]==8001]
        assert len(attempts)==2 and attempts[0][3]==attempts[1][3]=='🙂 текст'
        assert [{k:v for k,v in item.to_dict().items() if v is not None} for item in attempts[1][4]['entities']]==[entities[1]]
        assert e.svc.job(jid)['status']=='done'
        assert any('Не доставлено: 0' in v[3] for v in e.calls if v[1]=='text')


def service_parallel():
    with broadcast_environment() as e:
        jid=new_job(e,[q.message(900,430,'Объявление')]);e.svc.confirm(e.main,jid,900,start=False)
        barrier=threading.Barrier(2)
        parallel=[]
        def fault(bid,kind,uid,*args):
            if uid in (8001,8003):
                parallel.append(bid);barrier.wait(5)
        e.fault[0]=fault
        e.svc.resume(e.main)
        for worker in list(e.svc._workers.values()):worker.join(8)
        assert sorted(parallel)==sorted([e.bid1,e.bid2])
        assert e.svc.job(jid)['status']=='done'


def service_process_restart():
    with broadcast_environment() as e:
        jid=new_job(e,[q.message(900,440,'Объявление')]);e.svc.confirm(e.main,jid,900,start=False)
        receipt=Path(db.DATA_DIR)/'test_delivered.jsonl'
        code='''
import os,sys,types,json
sys.modules['dotenv']=types.SimpleNamespace(load_dotenv=lambda *a,**k:None)
import database as db, purchased_bot as pb, service_broadcast as svc
db.DB_PATH=sys.argv[1]; ids=json.loads(sys.argv[2]); path=sys.argv[3]; first=sys.argv[4]=='first'
class Bot:
    def send_message(self,uid,text,**kw):
        if 8000<=uid<=8003:
            with open(path,'a',encoding='utf-8') as f:f.write(str(uid)+'\\n')
main=Bot()
for bid in ids:
    token=db.db_get_bot_info(bid)[0];pb.running_bot_ids[bid]=token;pb.running_bots[token]=Bot()
if first:
    svc.pump(main,ids[0],limit=1);os._exit(74)
for bid in [0]+ids:svc.pump(main,bid)
'''
        args=[sys.executable,'-c',code,db.DB_PATH,json.dumps([e.bid1,e.bid2]),str(receipt)]
        first=subprocess.run(args+['first'],cwd=ROOT,capture_output=True,timeout=20)
        second=subprocess.run(args+['resume'],cwd=ROOT,capture_output=True,timeout=20)
        assert first.returncode==74 and second.returncode==0
        assert sorted(map(int,receipt.read_text().splitlines()))==[8000,8001,8002,8003]
        assert e.svc.job(jid)['status']=='done'


def failed_preview_once():
    with broadcast_environment() as e:
        e.main.get_file=lambda fid:(_ for _ in ()).throw(q.error(400,'file is too big'))
        q.click(e.main,900,'broadcast_all')
        q.dispatch(e.main,q.photo(900,445))
        assert e.svc.query('SELECT status FROM service_broadcasts')==[('failed',)]
        e.svc.resume(e.main)
        assert len([v for v in e.calls if v[1]=='text' and 'Не удалось подготовить рассылку' in v[3]])==1


def launch_once():
    bid, _, _=q.fresh(False);token=db.db_get_bot_info(bid)[0]
    calls=[]
    def make(*args):
        bot=q.telebot.TeleBot(token,threaded=False);q.BOTS.append(bot);calls.append(bot)
        bot.get_me=lambda:(time.sleep(.05) or q.t.User(999,True,'Бот',username='test'))
        bot.infinity_polling=lambda **kw:None
        return bot
    try:
        with patch.object(pb,'make_purchased_bot',side_effect=make),patch.object(dq.DeliverySender,'start'):
            workers=[threading.Thread(target=pb.launch_bot,args=(bid,token,777)) for _ in range(2)]
            for worker in workers:worker.start()
            for worker in workers:worker.join(5)
        assert len(calls)==1 and pb.running_bots.get(token) is calls[0]
        assert pb.stop_bot(token)
        assert token not in pb.running_bots and bid not in pb.running_bot_ids
    finally:
        pb.running_bots.pop(token,None);pb.running_bot_ids.pop(bid,None)


def faq_full():
    bid, bot, _=q.fresh(False)
    limit=getattr(db,'FAQ_TEXT_LIMIT',3847)
    body='<b>'+'я'*(limit-7)+'</b>'
    nid=db.db_faq_add(bid,None,'"'*40,body)
    start=len(q.API);q.click(bot,729,f'fq_u_{nid}_0')
    shown=[a[0] for n,a,k,_ in q.API[start:] if n=='edit_message_text']
    assert len(shown)==1 and body in shown[0] and len(shown[0].encode('utf-16-le'))//2<=4096
    try:db.db_faq_edit(bid,nid,'text','я'*(limit+1))
    except ValueError as exc:assert str(exc)==f'Текст раздела — не больше {limit} символов'
    else:raise AssertionError('Превышение лимита принято')


def settings_new_flow():
    bid,bot,_=q.fresh(False)
    q.click(bot,777,'p_edit_welcome')
    original=bot.send_message
    def send(uid,text,*args,**kw):
        if text=='Новый':q.click(bot,777,'p_edit_welcome')
        return original(uid,text,*args,**kw)
    with patch.object(bot,'send_message',side_effect=send):
        q.dispatch(bot,q.message(777,450,'Новый'))
    value=db.DBState(bid).get(777)
    assert isinstance(value,dict) and value.get('step')=='support_input'


def legacy_service_route():
    main=mainbot();bid,bot,_=q.fresh(False);token=db.db_get_bot_info(bid)[0]
    db.db_add_user(bid,8555);calls=[]
    pb.running_bots[token]=bot;pb.running_bot_ids[bid]=token
    original=main.send_message
    def send_main(uid,text,*args,**kw):
        if uid==8555:
            calls.append('main');raise q.error(403,'user never started main bot')
        return original(uid,text,*args,**kw)
    def send_bot(uid,text,*args,**kw):
        if uid==8555:calls.append('purchased')
        return q.message(uid,5555,text)
    try:
        with patch.object(main,'send_message',side_effect=send_main),patch.object(bot,'send_message',side_effect=send_bot):
            q.click(main,900,'broadcast_all');q.click(main,900,'bcast_text_all')
            q.dispatch(main,q.message(900,455,'Объявление'))
            value=db.DBState(0).get(900)
            if isinstance(value,dict) and value.get('job_id'):
                import service_broadcast as svc
                svc.confirm(main,value['job_id'],900,start=False)
                for bot_id in (0,bid):svc.pump(main,bot_id)
        assert calls==['purchased']
    finally:
        pb.running_bots.pop(token,None);pb.running_bot_ids.pop(bid,None)


def legacy_main_429():
    main=mainbot();db.db_add_user(0,8556);attempts=[]
    original=main.send_message
    def send(uid,text,*args,**kw):
        if uid==8556:
            attempts.append(uid)
            if len(attempts)==1:raise q.error(429,'retry',2)
        return original(uid,text,*args,**kw)
    with patch.object(main,'send_message',side_effect=send):
        q.click(main,900,'broadcast');q.click(main,900,'bcast_text_mine')
        q.dispatch(main,q.message(900,456,'Объявление'))
        value=db.DBState(0).get(900)
        if isinstance(value,dict) and value.get('job_id'):
            import service_broadcast as svc
            svc.confirm(main,value['job_id'],900,start=False);svc.pump(main,0)
            future=time.time()+3
            with patch.object(svc.time,'time',return_value=future):svc.pump(main,0)
    assert len(attempts)==2


def reversed_user_album():
    bid,bot,_=q.fresh(False)
    q.dispatch(bot,q.photo(8560,11,'reversed'));q.dispatch(bot,q.photo(8560,10,'reversed'))
    copied=[]
    def copy(chat,source,ids,**kw):
        assert ids==[10,11]
        copied.extend(ids)
        return [q.t.MessageID(910),q.t.MessageID(911)]
    future=time.time()+2
    with patch.object(bot,'copy_messages',side_effect=copy),patch.object(dq.time,'time',return_value=future):q.drain(bot,bid)
    assert copied==[10,11] and not db.db_delivery_rows(bid)


def paid_orders_survive():
    main=mainbot();uid=8570;bid=db.db_add_bot(uid,'123857:FAKE',uid)
    old=db.db_get_bot_expires(bid)
    db.DBState(0)[uid]=dict(step='renewing',bot_id=bid)
    invoices=iter([dict(invoice_id=85701,amount='7',pay_url='https://example.com/1'),
                   dict(invoice_id=85702,amount='9',pay_url='https://example.com/2')])
    with patch.object(h,'cb_create_invoice',side_effect=lambda uid:next(invoices)):
        q.click(main,uid,'pay_cryptobot');q.click(main,uid,'back_to_start')
        q.click(main,uid,'pay_cryptobot');q.click(main,uid,'back_to_start')
    db.db_close_all();db.init_db()
    with patch.object(payments,'cb_check_invoice',side_effect=lambda iid:dict(status='paid',amount='7' if str(iid)=='85701' else '9')), \
         patch.object(payments,'ton_check_transfer',return_value=False), \
         patch.object(pb,'launch_bot',return_value=True):
        assert payments.reconcile_payments(main)>=2
        assert payments.reconcile_payments(main)==0
    assert db.db_get_bot_expires(bid)!=old
    assert ps.db_take_grant_days(uid)==30 and ps.db_take_grant_days(uid) is None


def secret_filter():
    import safe_logging
    secret='987654321:'+('T'*35);key='test-r2-api-secret'
    safe_logging.register_secret(secret);safe_logging.register_secret(key)
    output=io.StringIO();sink=logging.StreamHandler(output)
    logger=logging.getLogger('r2-secret');logger.addHandler(sink)
    previous_propagate=logger.propagate;logger.propagate=False
    old_disable=logging.root.manager.disable;logging.disable(logging.NOTSET)
    try:
        try:raise RuntimeError(secret+' '+key)
        except RuntimeError:logger.error('request /%s %s',secret.replace(':','_'),key,exc_info=True)
        werkzeug=logging.getLogger('werkzeug');werkzeug.addHandler(sink)
        try:werkzeug.error('/'+secret.replace(':','_'))
        finally:werkzeug.removeHandler(sink)
    finally:
        logger.removeHandler(sink);logger.propagate=previous_propagate;logging.disable(old_disable)
    text=output.getvalue()
    assert secret not in text and secret.replace(':','_') not in text and key not in text
    assert '[СКРЫТО]' in text and logging.getLogger('werkzeug').disabled


METHODS = ('get_me','get_chat','send_message','copy_message','copy_messages','forward_message','forward_messages',
           'send_media_group','send_photo','send_voice','edit_message_text','edit_message_reply_markup',
           'edit_forum_topic','create_forum_topic','pin_chat_message','delete_message','answer_callback_query','delete_webhook')

if __name__ == '__main__':
    try:
        with patch.multiple(q.telebot.TeleBot, **{n:q.fake_api(n) for n in METHODS}):
            for name, fn in [
                ('Отзыв прав без кэша', revoked), ('Старая группа не получает обращения', old_group),
                ('Повторное подключение сохраняет связи', reconnect), ('Порядок админского альбома', admin_album),
                ('Потерянный ответ editMarkup', markup_retry), ('Подтверждение без rate limit', no_confirm_limit),
                ('Купон выдаёт один грант', coupon), ('Контекст CryptoBot до запроса', crypto_context),
                ('Проверка токена без worker-потоков', token_threads), ('Ответ не стирает новый адресат', main_reply_state),
                ('Отозванный админ снова пишет как пользователь', revoked_state), ('Альбом пользователя целиком', user_album),
                ('Альбом ответа в личке целиком', private_admin_album), ('Подпись голосового', voice_caption),
                ('Сбой конструктора закрывает workers', construction_cleanup), ('TON дальше 50 транзакций', ton_pages),
                ('Просроченная котировка не принимает свежую оплату', ton_expired),
                ('Вывод: сбой и повтор не теряют и не удваивают деньги', lambda: money_crash('withdraw')),
                ('Продление: сбой и повтор не теряют и не удваивают деньги', lambda: money_crash('renew')),
                ('Отклонение и возврат одной транзакцией', rejection),
                ('Настоящий выход процесса во время вывода', lambda: real_process_death('withdraw')),
                ('Настоящий выход процесса во время продления', lambda: real_process_death('renew')),
                ('Продление после снимка проверяющего потока', checker_renewal),
                ('Старый webhook снят перед polling', webhook_fallback),
                ('Занятый порт Werkzeug не завершает процесс', lambda: webhook_fallback(True)),
                ('Оба скачивания используют согласованный снимок', backup_snapshot),
                ('Premium: повтор, кнопки и суточный кэш', premium_fallback),
                ('Исправленный HTML приветствия сохраняется со второго ввода', welcome_retry),
                ('Сервисная рассылка: предпросмотр, подтверждение и уникальные адресаты', service_targets),
                ('Рассылка: 429, перезапуск, без повторов и верный итог', service_retry_restart),
                ('Рассылка: все виды файлов, альбом и собственные file_id', service_media),
                ('Рассылка: entities и повтор без custom emoji', service_custom_emoji),
                ('Боты рассылают параллельно', service_parallel),
                ('Рассылка продолжена новым процессом', service_process_restart),
                ('Отказ файла не запускает бесконечные предпросмотры', failed_preview_once),
                ('Параллельный запуск уже защищён общей блокировкой', launch_once),
                ('FAQ целиком с наихудшим названием и точный лимит', faq_full),
                ('Завершение настройки сохраняет новый ввод того же поля', settings_new_flow),
                ('Объявление приходит клиенту через купленного бота', legacy_service_route),
                ('Главная рассылка повторяет 429 вместо пропуска', legacy_main_429),
                ('Перестановка пользовательского альбома worker-потоками', reversed_user_album),
                ('Два оплаченных счёта зачтены после Назад и перезапуска', paid_orders_survive),
                ('Общее маскирование уже защищает исключения и werkzeug', secret_filter),
            ]: check(name, fn)
    finally:
        for bot in q.BOTS: pb._close_bot(bot)
        db.db_close_all()
    print(f'ИТОГО: {OK} OK, {FAIL} FAIL')
    sys.exit(bool(FAIL))
