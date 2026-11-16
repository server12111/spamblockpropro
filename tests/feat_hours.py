"""Тексты, расписание и автоответы: настоящие обработчики, временная БД."""
import importlib.util
import sys
import traceback
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('queue_harness', Path(__file__).with_name('delivery_queue.py'))
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)
db, pb, dq, fs = q.db, q.pb, q.dq, q.fs
check = q.check


def timestamp(s, offset=3):
    return datetime.fromisoformat(s).replace(tzinfo=timezone(timedelta(hours=offset))).timestamp()


def run():
    db.init_db()
    bid, bot, sender = q.fresh()
    uid = 8000
    gid = fs.fs_get_group(bid)[1]
    setval = lambda k, v: db.db_set_bot_setting(bid, 'hours_' + k, v)
    setval('enabled', '1')
    at = lambda s: pb.support_schedule(bid, timestamp(s))
    check('внутри и вне рабочего времени', at('2026-11-16T10:00')['working'] and not at('2026-11-16T19:00')['working'])
    check('выходные', not at('2026-11-21T12:00')['working'])
    check('пример пятница 23:10 → 65 часов', at('2026-11-20T23:10')['wait'] == '65 часов')
    check('ровно N часов и округление на минуте', at('2026-11-16T09:00')['wait'] == '7 часов' and at('2026-11-16T08:59')['wait'] == '8 часов')
    check('склонения 1/2/5/21/22', [pb.hour_words(n) for n in (1,2,5,21,22)] == ['1 час','2 часа','5 часов','21 час','22 часа'])
    setval('start', '22'); setval('end', '6')
    check('ночной интервал и перенос с пятницы на субботу', at('2026-11-21T05:00')['working'] and not at('2026-11-21T22:00')['working'])
    setval('end', '22')
    check('начало=конец означает 24 часа', at('2026-11-17T21:00')['working'])
    check('круглосуточный день заканчивается в полночь', not at('2026-11-21T00:00')['working'] and at('2026-11-20T01:00')['working'])
    setval('start', '10'); setval('end', '19'); setval('tz', '-3')
    check('часовой пояс', not at('2026-11-16T12:00')['working'] and at('2026-11-16T17:00')['working'])
    setval('enabled', '0')
    check('выключенное расписание', at('2026-11-21T03:00')['working'])
    setval('tz', '3'); setval('enabled', '1')
    db.db_set_bot_setting(bid, 'txt_offhours', '{start}–{end}: {wait}; {другое} {} {{x}}')
    check('подстановки и посторонние скобки', pb.offhours_text(bid, at('2026-11-20T23:10')) == '10:00–19:00: 65 часов; {другое} {} {{x}}')

    q.click(bot, 777, 'ps_edit_welcome')
    q.dispatch(bot, q.message(777, 1, 'Привет', entities=[dict(type='bold', offset=0, length=6)]))
    check('форматирование entities сохраняется', db.db_get_bot_setting(bid, 'welcome') == '<b>Привет</b>')
    q.dispatch(bot, q.message(uid, 1, '/start'))
    check('новое приветствие реально отправлено', q.SENT[-1][1][1] == '<b>Привет</b>')
    q.click(bot, 777, 'ps_reset_welcome')
    check('сброс приветствия', db.db_get_bot_setting(bid, 'welcome') == pb.DEFAULT_WELCOME)
    q.click(bot, 777, 'ps_edit_txt_sent'); q.dispatch(bot, q.message(777, 2, 'Принято'))
    q.dispatch(bot, q.message(uid, 2, 'письмо'))
    check('txt_sent отправлен до доставки', q.SENT[-1][1][1] == 'Принято' and not any(s[0]=='copy_message' for s in q.SENT))
    q.drain(bot, bid)
    q.click(bot, 777, 'ps_reset_txt_sent')
    check('сброс txt_sent', db.db_get_bot_setting(bid, 'txt_sent') == pb.DEFAULT_SENT)
    q.click(bot, 777, 'ps_edit_txt_offhours'); q.dispatch(bot, q.message(777, 3, 'Закрыто {wait}'))
    check('редактирование txt_offhours', db.db_get_bot_setting(bid, 'txt_offhours') == 'Закрыто {wait}')
    q.click(bot, 777, 'ps_reset_txt_offhours')
    check('сброс txt_offhours', db.db_get_bot_setting(bid, 'txt_offhours') == pb.DEFAULT_OFFHOURS)
    q.click(bot, 777, 'ps_edit_welcome')
    bad = q.message(777, 4, 'сломано')
    with patch.object(pb, 'message_html', return_value='<b>сломано'):
        q.dispatch(bot, bad)
    check('битый HTML не сохраняется', db.db_get_bot_setting(bid, 'welcome') == pb.DEFAULT_WELCOME)
    q.click(bot, 777, 'ps_texts')
    check('отмена сбросила ввод', db.DBState(bid).get(777) is None)
    q.click(bot, 777, 'ps_edit_welcome'); q.click(bot, 777, 'p_back_admin')
    check('Назад сбросил ввод', db.DBState(bid).get(777) is None)
    q.click(bot,777,'ps_edit_welcome');q.dispatch(bot,q.message(777,99,'/start'))
    check('/start при редактировании не сохраняет команду как текст',db.DBState(bid).get(777) is None and db.db_get_bot_setting(bid,'welcome')==pb.DEFAULT_WELCOME)
    before = len(q.API)
    db.db_add_bot_admin(bid, 778)
    q.click(bot, 778, 'ps_edit_welcome')
    check('дополнительный админ — alert отказ', any(a[0]=='answer_callback_query' and a[2].get('show_alert') and a[1][1]=='Доступно только главному админу' for a in q.API[before:]))
    q.click(bot, 778, 'p_open_admin')
    # Перехватываем именно клавиатуру, показанную дополнительному админу.
    marks = [a[2].get('reply_markup') for a in q.API[before:] if a[0]=='edit_message_text']
    check('кнопки настроек скрыты от доп. админа', marks and not any(b.callback_data.startswith('ps_') for row in marks[-1].keyboard for b in row))

    q.click(bot, 777, 'ps_add'); q.dispatch(bot, q.message(777, 5, 'цена, сколько стоит\nёлка'))
    q.dispatch(bot, q.message(777, 6, 'Наш ответ', entities=[dict(type='italic',offset=0,length=9)]))
    rule = db.db_auto_rules(bid)[0]
    check('добавление правила в два шага и форматирование', rule['phrases']==['цена','сколько стоит','ёлка'] and rule['text']=='<i>Наш ответ</i>')
    check('целые слова и последовательность', not pb.matching_rules(bid,'оценка') and pb.matching_rules(bid,'Сколько, СТОИТ?!') and not pb.matching_rules(bid,'сколько это стоит'))
    check('ё/е, регистр, пунктуация и пробелы', bool(pb.matching_rules(bid, '  ЕЛКА!!! ')))
    q.NOW[0] = timestamp('2026-11-20T23:10')
    start = len(q.SENT)
    q.dispatch(bot, q.message(uid+1, 10, 'ЦЕНА'))
    check('до прокачки только подтверждение', len(q.SENT)==start+1)
    q.drain(bot, bid)
    sent = q.SENT[start:]
    delivered_index = next(i for i,s in enumerate(sent) if s[0]=='copy_message')
    response_index = next(i for i,s in enumerate(sent) if s[0]=='send_message' and s[1][1]=='<i>Наш ответ</i>')
    check('автоответ и нерабочее время после доставки', response_index > delivered_index and any('65 часов' in s[1][1] for s in sent if s[0]=='send_message'))
    notice = next(s for s in sent if s[0]=='send_message' and 'Автоответ на «цена»' in s[1][1])
    check('заметка: фраза и полный форматированный ответ', notice[1][1] == f'{pb.TE_ROBOT} <b>Автоответ на «цена»</b>\n<blockquote><i>Наш ответ</i></blockquote>')
    c=db._conn(); mapped=c.execute('SELECT 1 FROM fs_msgs WHERE bot_id=? AND group_msg_id=?',(bid,notice[3])).fetchone(); c.close()
    check('заметка в теме не стала ответом и не вошла в соответствия', mapped is None and notice[2].get('message_thread_id'))
    start=len(q.SENT); q.NOW[0]+=100
    q.dispatch(bot,q.message(uid+1,11,'цена')); q.drain(bot,bid)
    check('6 и 12 часов: повторов нет', not any(s[0]=='send_message' and ('Наш ответ' in s[1][1] or 'Поддержка работает' in s[1][1]) for s in q.SENT[start:]))
    # Новый объект бота использует сохранённые в SQLite кулдауны.
    bot2=pb.make_purchased_bot(bid,'100000:TEST',777); q.BOTS.append(bot2)
    start=len(q.SENT); q.dispatch(bot2,q.message(uid+1,12,'цена')); q.drain(bot2,bid)
    check('кулдауны пережили перезапуск', not any(s[0]=='send_message' and 'Наш ответ' in s[1][1] for s in q.SENT[start:]))
    q.NOW[0]+=21600; start=len(q.SENT); q.dispatch(bot,q.message(uid+1,13,'цена')); q.drain(bot,bid)
    check('через 6 часов вне рабочего времени снова показан текст', sum(s[0]=='send_message' and 'Поддержка работает' in s[1][1] for s in q.SENT[start:])==1)
    q.NOW[0]+=21600; start=len(q.SENT); q.dispatch(bot,q.message(uid+1,14,'цена')); q.drain(bot,bid)
    check('через 12 часов правило снова отвечает', any(s[0]=='send_message' and s[1][1]=='<i>Наш ответ</i>' for s in q.SENT[start:]))
    start=len(q.SENT)
    q.dispatch(bot,q.photo(uid+2,20)); q.drain(bot,bid)
    check('неподходящая подпись не вызывает правило', not any(s[0]=='send_message' and s[1][1]=='<i>Наш ответ</i>' for s in q.SENT[start:]))
    start=len(q.SENT)
    photo=q.photo(uid+3,21); photo.caption='цена'; photo.json['caption']='цена'
    q.dispatch(bot,photo); q.drain(bot,bid)
    check('подпись к фото сопоставляется', any(s[0]=='send_message' and s[1][1]=='<i>Наш ответ</i>' for s in q.SENT[start:]))
    start=len(q.SENT)
    for mid in (22,23):
        photo=q.photo(uid+4,mid,'album'); photo.caption='цена'; photo.json['caption']='цена'; q.dispatch(bot,photo)
    q.NOW[0]+=2; q.drain(bot,bid)
    check('альбом получает один автоответ', sum(s[0]=='send_message' and s[1][1]=='<i>Наш ответ</i>' for s in q.SENT[start:])==1)
    second=db.db_auto_save(bid,['цена'],'Второй ответ')
    start=len(q.SENT); q.dispatch(bot,q.message(uid+5,30,'цена')); q.drain(bot,bid)
    check('первое правило и один ответ на сообщение', sum(s[0]=='send_message' and s[1][1] in ('<i>Наш ответ</i>','Второй ответ') for s in q.SENT[start:])==1)
    db.db_auto_delete(bid,second)
    q.click(bot,777,f"ps_flag_{rule['id']}")
    check('флаг только вне рабочего времени', not pb.matching_rules(bid,'цена',dict(enabled=True,working=True)) and pb.matching_rules(bid,'цена',dict(enabled=True,working=False)))
    start=len(q.SENT)
    q.click(bot,777,'ps_test'); q.dispatch(bot,q.message(777,40,'цена'))
    check('проверка фразы пишет только админу', all(s[1][0]==777 for s in q.SENT[start:]) and any('Сработало бы правило' in s[1][1] for s in q.SENT[start:]))
    start=len(q.SENT)
    q.dispatch(bot,q.message(777,50,'цена')); q.dispatch(bot,q.message(uid+6,51,'/цена'))
    db.db_block_user(bid,uid+7); q.dispatch(bot,q.message(uid+7,52,'цена'))
    db.DBState(bid)[uid+8]='другой шаг'; q.dispatch(bot,q.message(uid+8,53,'цена')); q.drain(bot,bid)
    check('админ, команда, блок и шаг не вызывают автоответ', not any(s[0]=='send_message' and s[1][1]=='<i>Наш ответ</i>' for s in q.SENT[start:]))
    # Сбой автоответа не повторяет доставленное обращение, ответ переживает рестарт.
    q.click(bot,777,f"ps_flag_{rule['id']}")
    def fault(name,a,k):
        if name=='send_message' and a[0]==uid+9 and a[1]=='<i>Наш ответ</i>': raise q.error(429,'retry',60)
    q.FAULT=fault; start=len(q.SENT)
    q.dispatch(bot,q.message(uid+9,60,'цена')); q.drain(bot,bid)
    check('429 автоответа не возвращает доставку в очередь', not db.db_delivery_rows(bid) and db.db_support_notifications(bid,q.NOW[0]+60))
    q.FAULT=None; q.NOW[0]+=60; q.drain(bot2,bid)
    check('отложенный автоответ отправлен после рестарта без дубля обращения', sum(s[0]=='copy_message' for s in q.SENT[start:])==1 and any(s[0]=='send_message' and s[1][0]==uid+9 and s[1][1]=='<i>Наш ответ</i>' for s in q.SENT[start:]))
    def notice_fault(name,a,k):
        if name=='send_message' and a[0]==gid and 'Автоответ на «цена»' in a[1]: raise q.error(429,'retry',30)
    q.FAULT=notice_fault;start=len(q.SENT)
    q.dispatch(bot,q.message(uid+10,70,'цена'));q.drain(bot,bid)
    q.FAULT=None;q.NOW[0]+=30;q.drain(bot2,bid)
    check('429 заметки повторяет только заметку после рестарта',sum(s[0]=='send_message' and s[1][0]==uid+10 and s[1][1]=='<i>Наш ответ</i>' for s in q.SENT[start:])==1 and any(s[0]=='send_message' and s[1][0]==gid and 'Автоответ на «цена»' in s[1][1] for s in q.SENT[start:]))
    db.db_auto_save(bid,['цена'],'Ответ второго правила')
    start=len(q.SENT)
    for mid in (80,81):
        photo=q.photo(uid+11,mid,'late_album');photo.caption='цена';photo.json['caption']='цена'
        q.dispatch(bot,photo);q.NOW[0]+=2;q.drain(bot,bid)
    check('поздняя часть альбома не вызывает второе правило',sum(s[0]=='send_message' and s[1][0]==uid+11 and s[1][1] in ('<i>Наш ответ</i>','Ответ второго правила') for s in q.SENT[start:])==1)
    private_id,private_bot,_=q.fresh(group=False)
    db.db_auto_save(private_id,['цена'],'Личный автоответ')
    start=len(q.SENT);q.dispatch(private_bot,q.message(uid+12,90,'цена'));q.drain(private_bot,private_id)
    private_sent=q.SENT[start:]
    check('личный режим: полная заметка после обращения админам',any(s[0]=='send_message' and s[1][0]==777 and s[1][1]==f'{pb.TE_ROBOT} <b>Автоответ на «цена»</b>\n<blockquote>Личный автоответ</blockquote>' for s in private_sent) and any(s[0]=='send_message' and s[1][0]==uid+12 and s[1][1]=='Личный автоответ' for s in private_sent))
    def rejected(value):
        try:pb.parse_phrases(value)
        except ValueError:return True
        return False
    check('лимиты фраз 20 и 2–50 символов',rejected('я') and rejected('я'*51) and rejected(','.join(['цена']*21)) and len(pb.parse_phrases(','.join(['цена']*20)))==20)
    for i in range(29):db.db_auto_save(private_id,[f'фраза {i}'],'Ответ')
    try:db.db_auto_save(private_id,['лишняя'],'Ответ');limit=False
    except ValueError:limit=True
    check('лимит 30 правил',limit and len(db.db_auto_rules(private_id))==30)
    q.click(private_bot,777,'ps_rules_1')
    edit=next(a for a in reversed(q.API) if a[0]=='edit_message_text')
    check('правила по 8 на страницу',sum(b.callback_data.startswith('ps_rule_') for row in edit[2]['reply_markup'].keyboard for b in row)==8)
    q.click(private_bot,777,'ps_grid_start')
    edit=next(a for a in reversed(q.API) if a[0]=='edit_message_text')
    check('сетка часов 00–23 по 6',all(len(row)==6 for row in edit[2]['reply_markup'].keyboard[:-1]) and len(edit[2]['reply_markup'].keyboard)==5)


if __name__ == '__main__':
    methods=('get_me','get_chat','send_message','copy_message','copy_messages','forward_message','forward_messages','send_media_group','send_photo','edit_message_text','edit_forum_topic','create_forum_topic','pin_chat_message','delete_message','answer_callback_query')
    try:
        with patch.multiple(q.telebot.TeleBot, **{n:q.fake_api(n) for n in methods}), patch.object(dq,'time',q.CLOCK), patch.object(db,'time',q.CLOCK), patch.object(pb,'time',q.CLOCK), patch.object(fs,'RATE_LIMIT_SEC',0):
            run()
    except Exception:
        traceback.print_exc(); check('нет необработанных исключений',False)
    finally:
        for bot in q.BOTS: pb._close_bot(bot)
    print(f'ИТОГО: {len(q.OK)} OK, {len(q.FAIL)} FAIL')
    sys.exit(bool(q.FAIL))
