"""Третий круг: обработчики, временная SQLite и Telegram без сети"""
import importlib.util
import sys
import os
import threading
import sqlite3
import html
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('r2', ROOT / 'tests/fix_r2.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
q, db, pb, fs, h, ps, dq = r.q, r.db, r.pb, r.fs, r.h, r.ps, r.dq


def partial_album():
    for group in (False, True):
        bid, bot, sender = q.fresh(group)
        sender.enqueue(q.photo(801, 1, 'part')); sender.enqueue(q.photo(801, 2, 'part'))
        sender.enqueue(q.message(801, 3, 'следующее'))
        with patch.object(bot, 'copy_messages', return_value=[q.t.MessageID(120)]), \
             patch.object(dq.time, 'time', return_value=q.time.time()+10):
            sender.pump()
        rows = db.db_delivery_rows(bid, active=False)
        assert all(v['status'] in ('delivered', 'deleted') for v in rows)
        assert not fs.fs_get_mapping(fs.fs_get_group(bid)[1] if group else 777,120)


def repeated_chunks():
    bid, bot, sender = q.fresh(False)
    sender.enqueue(q.message(802, 1, '&'*4096))
    start = len(q.SENT); sender.pump()
    text = ''.join(html.unescape(v[1][1]) for v in q.SENT[start:]
                   if v[0] == 'send_message' and v[1][0] == 777)
    assert text.count('&') == 4096


def forum_flood():
    bid, bot, _ = q.fresh()
    gid = fs.fs_get_group(bid)[1]
    m = q.message(gid, 4, 'ответ')
    start = len(q.API)
    with patch.object(bot, 'copy_message', side_effect=q.error(429, 'retry', 3600)) as copy, \
         patch.object(bot, 'forward_message', side_effect=q.error(429, 'retry', 3600)) as forward, \
         patch.object(bot, 'send_message', side_effect=q.error(429, 'retry', 3600)), \
         patch.object(fs.time, 'sleep', side_effect=AssertionError('handler slept')) as sleep:
        fs._deliver_admin_reply(bot, bid, m, 803, 77)
    assert copy.call_count == 1 and forward.call_count == 0 and not sleep.called
    import reply_queue as rq
    assert rq.pending(bid)
    with patch.object(bot, 'copy_message') as copy:
        rq.pump(bot, bid)
        copy.assert_not_called()
    with patch.object(rq.time, 'time', return_value=q.time.time()+3601):
        rq.pump(bot, bid)
    assert not rq.pending(bid)
    assert not any('не поддерживается' in str(v[1]) for v in q.API[start:])


def expired_token():
    bid, bot, _ = q.fresh(False); main=r.mainbot()
    c=db._conn(); c.execute("UPDATE bots SET active=0,expires_at='2000-01-01' WHERE id=?", (bid,));c.commit();c.close()
    old=db.db_get_bot_info(bid)[0]
    new=old.split(':')[0]+':'+'A'*35
    start=len(q.SENT)
    with patch.object(h, 'launch_bot', return_value=True) as launch, \
         patch.object(q.telebot.TeleBot,'get_me',return_value=q.t.User(int(old.split(':')[0]),True,'Бот',username='new')):
        q.click(main,777,f'change_token_{bid}');q.dispatch(main,q.message(777,32,new))
    assert not launch.called and db.db_get_bot_info(bid)[0] == new
    assert any('Токен обновлён' in str(v[1]) for v in q.SENT[start:])


def broadcast_nonfiles():
    import service_broadcast as sb
    main=r.mainbot(); bid,bot,_=q.fresh(False)
    values=[('contact',dict(phone_number='123',first_name='Контакт')),
            ('location',dict(latitude=12.5,longitude=34.5)),
            ('venue',dict(location=dict(latitude=12.5,longitude=34.5),title='Место',address='Адрес')),
            ('poll',dict(id='poll',question='Вопрос',options=[dict(text='Да',voter_count=0,persistent_id='1'),dict(text='Нет',voter_count=0,persistent_id='2')],total_voter_count=0,is_closed=False,is_anonymous=True,type='regular',allows_multiple_answers=False))]
    for i,(kind,value) in enumerate(values):
        m=q.message(900,20+i,None,**{kind:value}); jid=sb.append(main,900,900,'all',m)
        with patch.object(bot,'copy_message',side_effect=AssertionError('foreign history')), \
             patch.object(bot,'send_'+kind,return_value=NS(message_id=10)) as send:
            sb.send(bot,jid,bid,804)
        assert send.called
        expected={'contact':('123','Контакт'),'location':(12.5,34.5),
                  'venue':(12.5,34.5,'Место','Адрес'),'poll':('Вопрос',['Да','Нет'])}
        assert send.call_args.args[1:]==expected[kind]


def bad_restore():
    bot=r.mainbot(); bid,_,_=q.fresh(False)
    path=Path(os.environ['DATA_DIR'])/'incompatible.sqlite'
    c=sqlite3.connect(path);c.execute('CREATE TABLE bots(id INTEGER PRIMARY KEY)');c.commit();c.close()
    m=q.message(900,30,None,document=dict(file_id='file',file_unique_id='u',file_name='snapshot.db'))
    backup=db.db_backup(os.environ['DATA_DIR'])
    try:
        with patch.object(bot,'get_file',return_value=NS(file_path='test')), \
             patch.object(bot,'download_file',return_value=path.read_bytes()):
            r.handler(bot,'handle_db_restore')(m)
        assert db.db_get_bot_info(bid) and db.db_get_all_bots_for_checker()
    finally:
        db.db_close_all();shutil.copy2(backup,db.DB_PATH)


def duplicate_bot():
    uid=805; ps.db_mark_paid(uid); ps.db_mark_paid(uid)
    first=ps.db_register_paid_bot(uid,'188805:OLD',uid)
    try: ps.db_register_paid_bot(uid,'188805:NEW',uid)
    except sqlite3.IntegrityError: pass
    else: raise AssertionError('duplicate accepted')
    assert ps.db_is_paid(uid) and db.db_get_bot_info(first)


def renewal_math():
    bid,bot,_=q.fresh(False);db.set_price(.1)
    for _ in range(6): db.db_add_bot_earnings(bid,.05)
    old=datetime.fromisoformat(str(db.db_get_bot_expires(bid)))
    with patch.object(pb,'launch_bot',return_value=True):
        r.handler(bot,'p_renew_bal_cb')(q.callback(777,'p_renew_bal_all'))
    assert (datetime.fromisoformat(str(db.db_get_bot_expires(bid)))-old).days == 90
    bid,bot,_=q.fresh(False);db.set_price(1);db.db_add_bot_earnings(bid,.05)
    with patch.object(pb,'launch_bot',return_value=True):
        r.handler(bot,'p_renew_bal_cb')(q.callback(777,'p_renew_bal_all'))
    assert abs(db.db_get_bot_earnings(bid)[0] - (0.05-1/30))<1e-10


def renewal_lifecycle():
    bid,bot,_=q.fresh(False);db.set_price(1);db.db_add_bot_earnings(bid,20)
    token=db.db_get_bot_info(bid)[0]
    c=db._conn();c.execute('UPDATE bots SET expires_at=? WHERE id=?',((datetime.now()-timedelta(days=1)).isoformat(),bid));c.commit();c.close()
    pb.running_bots[token]=bot;pb.running_bot_ids[bid]=token
    stopped=threading.Event();release=threading.Event();started=threading.Event();done=threading.Event()
    targets=[];ticks=[0]
    class EndLoop(BaseException): pass
    def factory(*a,**kw): targets.append(kw['target']);return NS(start=lambda:None)
    with patch.object(h.threading,'Thread',side_effect=factory): h.start_subscription_checker(bot)
    def sleep(_):
        ticks[0]+=1
        if ticks[0]>1: raise EndLoop()
    def stop(value):
        stopped.set();assert release.wait(3)
        pb.running_bots.pop(value,None);pb.running_bot_ids.pop(bid,None)
    def check():
        try: targets[0]()
        except EndLoop: pass
    def launch(*args):
        pb.running_bots[token]=bot;pb.running_bot_ids[bid]=token;return True
    def renew():
        started.set()
        r.handler(bot,'p_renew_bal_cb')(q.callback(777,'p_renew_bal_30'));done.set()
    try:
        with patch.object(h,'db_get_all_bots_for_checker',return_value=[(bid,777,token,777,'expired')]), \
             patch.object(h.time,'sleep',side_effect=sleep),patch.object(h,'stop_bot',side_effect=stop), \
             patch.object(pb,'launch_bot',side_effect=launch):
            checker=threading.Thread(target=check);worker=threading.Thread(target=renew)
            checker.start()
            try:
                assert stopped.wait(2)
                worker.start();assert started.wait(2)
                assert not done.wait(.1), 'renew bypasses checker lock'
            finally:
                release.set();checker.join(3)
                if worker.ident is not None: worker.join(3)
            assert done.is_set() and not checker.is_alive() and not worker.is_alive()
        assert token in pb.running_bots and db.db_get_bot_earnings(bid)[0]==19
        assert datetime.fromisoformat(str(db.db_get_bot_expires(bid)))>datetime.now()+timedelta(days=29)
    finally:
        release.set();pb.running_bots.pop(token,None);pb.running_bot_ids.pop(bid,None)


def stale_phrases():
    bid,bot,_=q.fresh(False);state=db.DBState(bid)
    state[777]=dict(step='support_input',action='new_phrases',back='ps_rules_0')
    original=pb.parse_phrases
    def parse(text):
        q.click(bot,777,'p_reply_806');return original(text)
    with patch.object(pb,'parse_phrases',side_effect=parse):
        r.handler(bot,'support_settings_input')(q.message(777,40,'фраза'))
    assert state.get(777)=='await_reply_806'
    q.dispatch(bot,q.message(777,41,'Личный ответ'))
    assert not db.db_auto_rules(bid)


def mixed_album():
    bid,bot,_=q.fresh(False);db.DBState(bid)[777]='await_reply_807'
    q.dispatch(bot,q.photo(777,50,'mixed'))
    q.dispatch(bot,q.message(777,51,None,media_group_id='mixed',video=dict(file_id='video',file_unique_id='v',width=1,height=1,duration=1)))
    with patch.object(bot,'copy_messages',return_value=[q.t.MessageID(1),q.t.MessageID(2)]) as send:
        bot._flush_reply_albums()
    assert send.call_args.args[2]==[50,51]


def revoked_broadcast():
    for state in ('p_bcast_text','p_bcast_photo','mgadd_tpl','await_add_tpl_900'):
        bid,bot,_=q.fresh(False);db.db_add_bot_admin(bid,808);db.DBState(bid)[808]=state
        db.db_remove_bot_admin(bid,808)
        q.dispatch(bot,q.photo(808,61) if state=='p_bcast_photo' else q.message(808,61,'Обращение'))
        assert db.DBState(bid).get(808) is None and db.db_delivery_rows(bid)


def private_flood():
    bid,bot,_=q.fresh(False);db.DBState(bid)[777]='await_reply_809'
    original=bot.send_message;start=len(q.API)
    def send(uid,*args,**kw):
        if uid==809: raise q.error(429,'retry',3600)
        return original(uid,*args,**kw)
    with patch.object(bot,'send_message',side_effect=send):
        q.dispatch(bot,q.message(777,70,'&'*4096))
    assert not any('пользователь недоступен' in str(v[1]) for v in q.API[start:])
    import reply_queue as rq
    assert rq.pending(bid)
    # Новый sender после перезапуска продолжает сохранённые части
    if hasattr(bot,'_reply_sender'): del bot._reply_sender
    with patch.object(rq.time,'time',return_value=q.time.time()+3601): rq.pump(bot,bid)
    assert not rq.pending(bid)


def reply_process_restart():
    """Квитанция первой части переживает запуск другого интерпретатора"""
    import reply_queue as rq
    bid,bot,_=q.fresh(False);db.DBState(bid)[777]='await_reply_813'
    original=bot.send_message;count=[0];start=len(q.SENT)
    def send(uid,*a,**kw):
        if uid==813:
            count[0]+=1
            if count[0]==2: raise q.error(429,'retry',3600)
        return original(uid,*a,**kw)
    with patch.object(bot,'send_message',side_effect=send): q.dispatch(bot,q.message(777,71,'&'*4096))
    first=''.join(html.unescape(v[1][1]) for v in q.SENT[start:] if v[0]=='send_message' and v[1][0]==813)
    assert 0 < first.count('&') < 4096 and rq.pending(bid)
    code='''
import sys,types,json,html
sys.modules['dotenv']=types.SimpleNamespace(load_dotenv=lambda *a,**k:None)
import database as db,reply_queue as rq,delivery_queue as dq,telebot
bid=int(sys.argv[1]);info=db.db_get_bot_info(bid)
bot=telebot.TeleBot(info[0],threaded=False)
parts=[]
def send(uid,text,**kw):
    if uid==813: parts.append(html.unescape(text))
    return types.SimpleNamespace(message_id=100+len(parts))
bot.send_message=send
rq.time=types.SimpleNamespace(time=lambda:10**12)
dq.time=rq.time
rq.pump(bot,bid)
assert not rq.pending(bid)
print(json.dumps(parts))
'''
    env=dict(os.environ,DATA_DIR=str(Path(db.DB_PATH).parent),PYTHONIOENCODING='utf-8')
    result=subprocess.run([sys.executable,'-c',code,str(bid)],cwd=ROOT,env=env,capture_output=True,text=True,timeout=15)
    assert result.returncode==0
    import json
    rest=''.join(json.loads(result.stdout))
    assert (first+rest).count('&')==4096 and not rq.pending(bid)


def ton_quote():
    bot=r.mainbot();db.set_price(10)
    def rate(): db.set_price(.1);return 2
    with patch.object(r.payments,'get_ton_price_usd',side_effect=rate):
        r.handler(bot,'pay_ton_cb')(q.callback(810,'pay_ton'))
    c=db._conn();row=c.execute("SELECT amount,expected_ton FROM payment_orders WHERE user_id=810 AND provider='ton'").fetchone();c.close()
    assert row==(10,5)


def cancelled_setup():
    bot=r.mainbot();ps.db_mark_paid(811);db.DBState(0)[811]='await_bot_token'
    def me(*a,**k):
        r.handler(bot,'cancel_cb')(q.callback(811,'cancel'))
        return NS(id=188811,username='test')
    with patch.object(q.telebot.TeleBot,'get_me',side_effect=me):
        r.handler(bot,'get_bot_token')(q.message(811,80,'188811:'+'A'*35))
    assert db.DBState(0).get(811) is None


def group_setup():
    bot=r.mainbot();ps.db_mark_paid(812)
    session=dict(step='await_admin_id',token='188812:TEST',username='test')
    db.DBState(0)[812]=session
    m=q.message(-100123,90,'999',**{'from':dict(id=812,is_bot=False,first_name='Имя')})
    with patch.object(h,'launch_bot',return_value=True) as launch: q.dispatch(bot,m)
    assert db.DBState(0).get(812)==session and ps.db_is_paid(812) and not launch.called


if __name__=='__main__':
    try:
        with patch.multiple(q.telebot.TeleBot,**{n:q.fake_api(n) for n in r.METHODS}), patch.object(fs,'RATE_LIMIT_SEC',0):
            for name,fn in [
                ('Частичный альбом не блокирует очередь',partial_album),('Одинаковые части текста доставлены целиком',repeated_chunks),
                ('Форумный ответ сохраняется при длительном 429',forum_flood),('Смена токена не запускает просроченного бота',expired_token),
                ('Рассылка контакта, геопозиции, места и опроса',broadcast_nonfiles),('Несовместимая схема не заменяет БД',bad_restore),
                ('Перевыпущенный токен не создаёт дубль',duplicate_bot),('Целые дни и остаток баланса',renewal_math),
                ('Продление синхронизировано с остановкой',renewal_lifecycle),('Старые фразы не заменяют новый ответ',stale_phrases),
                ('Смешанный альбом ответа целиком',mixed_album),('Отзыв прав очищает каждый админский ввод',revoked_broadcast),
                ('Текстовый ответ переживает 429 и перезапуск',private_flood),('TON использует одну цену',ton_quote),
                ('Другой процесс продолжает ответ без повторения частей',reply_process_restart),
                ('Отмена во время проверки токена',cancelled_setup),('ID настройки принимается только в личке',group_setup),
                ('Счета переживают Назад, второй счёт и перезапуск',r.paid_orders_survive),
                ('Общий фильтр секретов и отключение werkzeug',r.secret_filter),
            ]: r.check(name,fn)
    finally:
        for bot in q.BOTS: pb._close_bot(bot)
        db.db_close_all()
    print(f'ИТОГО: {r.OK} OK, {r.FAIL} FAIL')
    sys.exit(bool(r.FAIL))
