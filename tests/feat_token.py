"""Смена токена: реальные handlers, БД, launch/stop; API и polling без сети."""
import importlib.util
import io
import logging
import sys
import threading
import traceback
from pathlib import Path
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('queue_harness',Path(__file__).with_name('delivery_queue.py'))
q=importlib.util.module_from_spec(spec);spec.loader.exec_module(q)
import handlers as h
db,pb,fs=q.db,q.pb,q.fs
check=q.check
TOKENS=[]
INVALID=set()
FAIL_START=set()
LAUNCHES=[]
STOPPED=[]


def token(botnum,letter):
    value=str(botnum)+':'+letter*35
    TOKENS.append(value)
    return value


def get_me(self,*a,**kw):
    if self.token in INVALID or (self.token in FAIL_START and self.threaded):
        raise RuntimeError('API failed '+self.token)
    num=int(self.token.split(':')[0])
    return q.t.User(num,True,'Бот',username='bot_'+str(num))


def launch(*a,**kw):
    LAUNCHES.append(a[:3])
    return pb.launch_bot(*a,**kw)


def stop(value):
    STOPPED.append(value)
    return pb.stop_bot(value)


def run():
    db.init_db()
    main=q.telebot.TeleBot(token(999999,'M')); q.BOTS.append(main); h.register(main)
    old=token(100001,'A'); new=token(100001,'B')
    bid=db.db_add_bot(777,old,777); db.db_update_bot_username(bid,'bot_100001')
    faq=db.db_faq_add(bid,None,'Сохранить FAQ','Ответ')
    tpl=db.db_add_template(bid,'Шаблон'); db.db_set_bot_setting(bid,'welcome','Своё приветствие'); db.db_add_user(bid,123)
    fs.fs_bind_group(bid,-100500,'Поддержка')
    expires=db.db_get_bot_expires(bid)
    pb.launch_bot(bid,old,777,main)
    old_sender=pb.running_bots[old]._delivery_sender
    click=lambda data,uid=777:q.click(main,uid,data)
    send=lambda text,mid=10,uid=777:q.dispatch(main,q.message(uid,mid,text))
    click('buy_bot')
    edits=[a for a in q.API if a[0]=='edit_message_text']
    check('кнопка смены у бота владельца',any(b.callback_data==f'change_token_{bid}' for row in edits[-1][2]['reply_markup'].keyboard for b in row))
    click(f'change_token_{bid}'); send(new)
    check('перевыпуск того же бота без подтверждения',db.db_get_bot_info(bid)[0]==new and new in pb.running_bots and old not in pb.running_bots)
    check('старый отправитель завершён',not old_sender.thread.is_alive())
    check('успех с username',any(s[0]=='send_message' and 'Токен обновлён, бот @bot_100001 запущен' in s[1][1] for s in q.SENT))
    check('настройки, FAQ, шаблоны, пользователи и подписка сохранены',db.db_faq_get(bid,faq) and db.db_get_template(tpl,bid)=='Шаблон' and db.db_get_bot_setting(bid,'welcome')=='Своё приветствие' and 123 in db.db_get_bot_users(bid) and db.db_get_bot_expires(bid)==expires)
    check('группа при перевыпуске сохранена',fs.fs_get_group(bid)[1]==-100500)
    check('сообщение с токеном удалено',any(a[0]=='delete_message' and a[1]==(777,10) for a in q.API))
    count=len(LAUNCHES);before=len(q.API);click(f'change_token_{bid}',778)
    check('чужой bot_id — отказ',any(a[0]=='answer_callback_query' and a[2].get('show_alert') and a[1][1]=='Этот бот тебе недоступен' for a in q.API[before:]) and len(LAUNCHES)==count)
    for value,name in ((new,'тот же токен'),('ошибка','неверный формат')):
        click(f'change_token_{bid}');send(value)
        check(name+' — отказ без изменения БД',db.db_get_bot_info(bid)[0]==new and len(LAUNCHES)==count)
    invalid=token(200001,'I');INVALID.add(invalid);click(f'change_token_{bid}');send(invalid)
    check('get_me отверг токен без изменения БД',db.db_get_bot_info(bid)[0]==new and len(LAUNCHES)==count)
    busy=token(300001,'C');other=db.db_add_bot(778,busy,778)
    click(f'change_token_{bid}');send(busy)
    check('занятый токен отклонён',db.db_get_bot_info(bid)[0]==new and len(LAUNCHES)==count)
    busy_rotated=token(300001,'D');click(f'change_token_{bid}');send(busy_rotated)
    check('новый токен уже подключённого Telegram-бота отклонён',db.db_get_bot_info(bid)[0]==new and len(LAUNCHES)==count)
    different=token(200002,'E');click(f'change_token_{bid}');send(different)
    warning=q.SENT[-1]
    confirm=next(b.callback_data for row in warning[2]['reply_markup'].keyboard for b in row if b.text=='Сменить')
    check('другой бот требует предупреждения и подтверждения','Это другой бот (@bot_200002)' in warning[1][1] and 'Пользователи @bot_100001' in warning[1][1] and 'группу поддержки нужно будет подключить заново' in warning[1][1] and db.db_get_bot_info(bid)[0]==new)
    click('token_cancel');click(confirm)
    check('отмена инвалидирует подтверждение',db.db_get_bot_info(bid)[0]==new and db.DBState(0).get(777) is None)
    click(f'change_token_{bid}');send('/start')
    check('/start сбрасывает ожидание токена',db.DBState(0).get(777) is None)
    click(f'change_token_{bid}');send(different)
    stale_confirm=next(b.callback_data for row in q.SENT[-1][2]['reply_markup'].keyboard for b in row if b.text=='Сменить')
    restarted_main=q.telebot.TeleBot(token(999998,'N'));q.BOTS.append(restarted_main);h.register(restarted_main)
    q.click(restarted_main,777,stale_confirm)
    check('перезапуск сбрасывает подтверждение без сохранения нового токена в state',db.DBState(0).get(777) is None and db.db_get_bot_info(bid)[0]==new)
    click(f'change_token_{bid}');send(different)
    confirm=next(b.callback_data for row in q.SENT[-1][2]['reply_markup'].keyboard for b in row if b.text=='Сменить')
    click(confirm)
    check('другой бот запущен после подтверждения',db.db_get_bot_info(bid)[0]==different and different in pb.running_bots)
    check('привязка группы сброшена',fs.fs_get_group(bid) is None and fs.fs_get_topic(bid,123) is None)
    check('данные сохранены при смене Telegram-бота',db.db_faq_get(bid,faq) and db.db_get_bot_setting(bid,'welcome')=='Своё приветствие' and db.db_get_bot_expires(bid)==expires)
    fs.fs_bind_group(bid,-100501,'Новая поддержка')
    failed=token(200002,'F');FAIL_START.add(failed)
    click(f'change_token_{bid}');send(failed)
    check('сбой запуска возвращает старый токен и запускает старый бот',db.db_get_bot_info(bid)[0]==different and different in pb.running_bots and failed not in pb.running_bots)
    check('понятное сообщение при откате',any(s[0]=='send_message' and s[1][1]=='Не удалось запустить бота с новым токеном, оставлен прежний' for s in q.SENT))
    # Откат смены на ДРУГОГО бота восстанавливает также группу/темы/соответствия.
    failed_other=token(200003,'G');FAIL_START.add(failed_other)
    c=db._conn(); group_before=c.execute('SELECT * FROM fs_groups WHERE bot_id=?',(bid,)).fetchone(); c.close()
    click(f'change_token_{bid}');send(failed_other)
    confirm=next(b.callback_data for row in q.SENT[-1][2]['reply_markup'].keyboard for b in row if b.text=='Сменить');click(confirm)
    c=db._conn(); group_after=c.execute('SELECT * FROM fs_groups WHERE bot_id=?',(bid,)).fetchone(); c.close()
    check('сбой другого бота восстановил привязку целиком',group_before==group_after and db.db_get_bot_info(bid)[0]==different and different in pb.running_bots)
    before=len(q.API);click(f'change_token_{bid}');db.DBState(0)[777]=dict(db.DBState(0)[777],expires=q.NOW[0]-1);send(token(200002,'H'),31)
    check('10 минут истекли — состояние сброшено, токен не меняется',db.DBState(0).get(777) is None and db.db_get_bot_info(bid)[0]==different and any(a[0]=='delete_message' and a[1]==(777,31) for a in q.API[before:]))
    click(f'change_token_{bid}');click('token_cancel')
    check('отмена сбрасывает ожидание',db.DBState(0).get(777) is None)
    # Старое подтверждение другого бота просрочено.
    pending=token(200004,'J');click(f'change_token_{bid}');send(pending)
    confirm=next(b.callback_data for row in q.SENT[-1][2]['reply_markup'].keyboard for b in row if b.text=='Сменить')
    q.NOW[0]+=601;click(confirm)
    check('подтверждение также ограничено 10 минутами',db.DBState(0).get(777) is None and db.db_get_bot_info(bid)[0]==different)
    # Одновременные попытки выполняют только одну операцию restart.
    concurrent1,concurrent2=token(200002,'K'),token(200002,'L')
    entered,release=threading.Event(),threading.Event();results=[]
    def slow_launch(*a,**kw):
        if a[1]==concurrent1: entered.set();release.wait(5)
        return launch(*a,**kw)
    me=q.t.User(200002,True,'Бот',username='bot_200002')
    with patch.object(h,'launch_bot',slow_launch):
        th=threading.Thread(target=lambda:results.append(h._replace_owned_token(777,bid,different,concurrent1,me,main)))
        th.start();arrived=entered.wait(5)
        second=h._replace_owned_token(777,bid,different,concurrent2,me,main)
        release.set();th.join(5)
    check('две смены одновременно — одна',arrived and not th.is_alive() and second=='Смена токена уже выполняется' and db.db_get_bot_info(bid)[0]==concurrent1 and concurrent1 in pb.running_bots and concurrent2 not in pb.running_bots)
    check('поздняя смена со старой версией отклонена','Токен уже изменён' in h._replace_owned_token(777,bid,different,concurrent2,me,main))
    # Ошибка удаления сообщения best effort не отменяет успешную смену.
    with patch.object(main,'delete_message',side_effect=RuntimeError('удаление недоступно')):
        click(f'change_token_{bid}');send(concurrent2,50)
    check('ошибка удаления не мешает смене',db.db_get_bot_info(bid)[0]==concurrent2 and concurrent2 in pb.running_bots)
    texts='\n'.join(str(a[1]) for a in q.API if a[0] in ('send_message','edit_message_text','answer_callback_query'))
    check('токенов нет в ответах бота',not any(t in texts for t in TOKENS))
    callbacks=[b.callback_data for a in q.API if a[0] in ('send_message','edit_message_text') and a[2].get('reply_markup') for row in a[2]['reply_markup'].keyboard for b in row if b.callback_data]
    check('callback_data без токена и ≤64 байт',all(len(v.encode())<=64 and not any(t in v for t in TOKENS) for v in callbacks))


if __name__=='__main__':
    methods=('get_chat','send_message','copy_message','copy_messages','forward_message','forward_messages','send_media_group','send_photo','edit_message_text','edit_forum_topic','create_forum_topic','pin_chat_message','delete_message','answer_callback_query','delete_webhook')
    capture=io.StringIO();handler=logging.StreamHandler(capture);logging.getLogger().addHandler(handler);logging.getLogger().setLevel(logging.INFO)
    try:
        with patch.multiple(q.telebot.TeleBot,**{n:q.fake_api(n) for n in methods}),patch.object(q.telebot.TeleBot,'get_me',get_me),patch.object(q.telebot.TeleBot,'infinity_polling',lambda self,**kw:self._TeleBot__stop_polling.wait(20)),patch.object(h,'launch_bot',launch),patch.object(h,'stop_bot',stop),patch.object(h,'time',q.CLOCK):run()
    except Exception:traceback.print_exc();check('нет необработанных исключений',False)
    finally:
        for value in list(pb.running_bots):pb.stop_bot(value)
        for b in q.BOTS:pb._close_bot(b)
        logging.getLogger().removeHandler(handler)
    check('токенов нет в журнале, включая ошибки запуска',not any(t in capture.getvalue() for t in TOKENS))
    print(f'ИТОГО: {len(q.OK)} OK, {len(q.FAIL)} FAIL');sys.exit(bool(q.FAIL))
