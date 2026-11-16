"""FAQ: реальные обработчики и БД, Telegram подменён."""
import importlib.util
import sys
import traceback
from pathlib import Path
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('queue_harness',Path(__file__).with_name('delivery_queue.py'))
q=importlib.util.module_from_spec(spec); spec.loader.exec_module(q)
db,pb=q.db,q.pb
check=q.check


def run():
    db.init_db(); bid,bot,sender=q.fresh(); uid=9000
    click=lambda data,who=777:q.click(bot,who,data)
    def last_edit(): return next(a for a in reversed(q.API) if a[0]=='edit_message_text')
    def buttons(): return [b for row in last_edit()[2]['reply_markup'].keyboard for b in row]
    def menu():
        q.dispatch(bot,q.message(uid,1,'/start'))
        return [b.text for row in q.SENT[-1][2]['reply_markup'].keyboard for b in row]
    check('пустой FAQ скрыт в главном меню','FAQ' not in menu())
    click('fq_e_0_0'); click('fq_add_0'); q.dispatch(bot,q.message(777,2,'Доставка'))
    q.dispatch(bot,q.message(777,3,'Быстро',entities=[dict(type='bold',offset=0,length=6)]))
    node=db.db_faq_nodes(bid)[0]; nid=node['id']
    check('добавление и форматирование',node['title']=='Доставка' and node['text']=='<b>Быстро</b>')
    check('FAQ появляется под отправкой',menu()[:2]==['Отправить сообщение','FAQ'])
    click(f'fq_name_{nid}'); q.dispatch(bot,q.message(777,4,'Срок доставки'))
    check('переименование',db.db_faq_get(bid,nid)['title']=='Срок доставки')
    click(f'fq_text_{nid}'); q.dispatch(bot,q.message(777,5,'Завтра',entities=[dict(type='italic',offset=0,length=6)]))
    check('изменение форматированного текста',db.db_faq_get(bid,nid)['text']=='<i>Завтра</i>')
    click('fq_add_0'); q.dispatch(bot,q.message(777,6,'Папка')); click('fq_skip_0')
    folder=db.db_faq_nodes(bid)[-1]['id']
    check('Пропустить создаёт папку',db.db_faq_get(bid,folder)['text']=='')
    for i in range(18): db.db_faq_add(bid,None,f'Вопрос {i}')
    nodes=db.db_faq_nodes(bid)
    click('fq_u_0_0',uid)
    check('первая страница 20 узлов — 8 кнопок и 1/3',len([b for b in buttons() if b.callback_data.startswith('fq_u_') and b.text not in ('◀️','▶️')])==8 and '1/3' in [b.text for b in buttons()] and '◀️' not in [b.text for b in buttons()])
    click('fq_u_0_2',uid)
    check('третья страница — 4 кнопки и 3/3',len([b for b in buttons() if b.callback_data.startswith('fq_u_') and b.text not in ('◀️','▶️')])==4 and '3/3' in [b.text for b in buttons()] and '▶️' not in [b.text for b in buttons()])
    selected=nodes[17]['id']; child=db.db_faq_add(bid,selected,'Дочерний'); grand=db.db_faq_add(bid,child,'Внутри')
    start=len(q.SENT); click(f'fq_u_{selected}_0',uid); click(f'fq_u_{child}_0',uid); click(f'fq_u_{grand}_0',uid)
    check('переход вглубь правит то же сообщение',not q.SENT[start:] and 'Внутри' in last_edit()[1][0])
    back=next(b.callback_data for b in buttons() if b.text=='Назад'); click(back,uid)
    check('Назад ведёт к родителю','Дочерний' in last_edit()[1][0])
    click(f'fq_u_{selected}_0',uid)
    check('Назад из узла возвращает нужную страницу',next(b.callback_data for b in buttons() if b.text=='Назад')=='fq_u_0_2')
    click(f'fq_up_{folder}')
    check('⬆️ меняет порядок',db.db_faq_nodes(bid)[0]['id']==folder)
    click(f'fq_down_{folder}')
    check('⬇️ меняет порядок',db.db_faq_nodes(bid)[1]['id']==folder)
    click(f'fq_v_{nid}_0')
    check('предпросмотр показывает текст и возврат к редактору','<i>Завтра</i>' in last_edit()[1][0] and 'К редактору' in [b.text for b in buttons()])
    click(f'fq_text_{nid}'); bad=q.message(777,10,'сломано')
    with patch.object(type(bad),'html_text',new=property(lambda self:'<b>сломано')): q.dispatch(bot,bad)
    check('битый HTML не сохраняется',db.db_faq_get(bid,nid)['text']=='<i>Завтра</i>')
    click(f'fq_e_{nid}_0'); q.dispatch(bot,q.message(777,11,'Не менять'))
    check('Отмена сбрасывает отдельный ввод',db.db_faq_get(bid,nid)['text']=='<i>Завтра</i>' and db.DBState(bid).get(777) is None)
    click(f'fq_name_{nid}'); click('p_back_admin'); q.dispatch(bot,q.message(777,12,'Не менять'))
    check('Назад из панели тоже сбрасывает ввод',db.db_faq_get(bid,nid)['title']=='Срок доставки')
    click(f'fq_name_{nid}');q.dispatch(bot,q.message(777,99,'/start'));q.dispatch(bot,q.message(777,100,'Не менять'))
    check('/start сбрасывает редактор без переименования узла',db.db_faq_get(bid,nid)['title']=='Срок доставки')
    click(f'fq_add_{nid}'); click(f'p_reply_{uid}'); q.dispatch(bot,q.message(777,13,'Ответ пользователю'))
    check('редактор не мешает ответам пользователю',any(s[0]=='send_message' and s[1][0]==uid and 'Ответ пользователю' in s[1][1] for s in q.SENT))
    full_text='<b>'+'я'*(db.FAQ_TEXT_LIMIT-7)+'</b>'
    db.db_faq_edit(bid,nid,'text',full_text)
    start=len(q.SENT); click(f'fq_u_{nid}_0',uid)
    shown=last_edit()[1][0]
    check('текст максимальной длины показан целиком',len(shown.encode('utf-16-le'))//2<=4096 and full_text in shown and not shown.endswith('…') and not q.SENT[start:])
    click(f'fq_text_{nid}'); q.dispatch(bot,q.message(777,103,'я'*(db.FAQ_TEXT_LIMIT+1)))
    check('превышение лимита отклоняется с точной подсказкой',db.db_faq_get(bid,nid)['text']==full_text and any(
        s[0]=='send_message' and s[1][1]==f'Текст раздела — не больше {db.FAQ_TEXT_LIMIT} символов' for s in q.SENT[start:]))
    click(f'fq_del_{selected}')
    check('подтверждение удаления считает потомков','(2)' in last_edit()[1][0] and next(b for b in buttons() if b.text=='Да, удалить').style=='danger')
    click(f'fq_yes_{selected}')
    check('удалено всё поддерево',all(db.db_faq_get(bid,i) is None for i in (selected,child,grand)))
    before=len(q.API); click(f'fq_u_{selected}_0',uid)
    check('старая кнопка — alert и корень',any(a[0]=='answer_callback_query' and a[1][1]=='Раздел больше недоступен' for a in q.API[before:]) and 'Выбери вопрос' in last_edit()[1][0])
    other=db.db_add_bot(700,'200000:TEST',700); foreign=db.db_faq_add(other,None,'Чужое')
    before=len(q.API); click(f'fq_u_{foreign}_0',uid)
    check('чужой узел — alert и корень',any(a[0]=='answer_callback_query' and a[1][1]=='Раздел больше недоступен' for a in q.API[before:]) and 'Чужое' not in last_edit()[1][0])
    db.db_add_bot_admin(bid,778); before=len(q.API); click(f'fq_text_{nid}',778)
    check('доп. админ — отказ',any(a[0]=='answer_callback_query' and a[1][1]=='Доступно только главному админу' and a[2]['show_alert'] for a in q.API[before:]))
    click('p_open_admin',778)
    check('редактор скрыт от доп. админа',not any(b.callback_data.startswith('fq_') for b in buttons()))
    count_before=len(db.db_delivery_rows(bid,False)); start=len(q.SENT)
    for n in db.db_faq_nodes(bid): click(f"fq_u_{n['id']}_0",uid)
    check('просмотр не доставляет админам и не создаёт обращений',len(db.db_delivery_rows(bid,False))==count_before and not q.SENT[start:])
    callbacks=[b.callback_data for a in q.API if a[0]=='edit_message_text' and a[2].get('reply_markup') for row in a[2]['reply_markup'].keyboard for b in row if b.callback_data]
    check('все callback_data ≤64 байт',all(len(v.encode())<=64 for v in callbacks))
    def rejects(fn):
        try: fn()
        except ValueError: return True
        return False
    check('названия 1–40 символов',rejects(lambda:db.db_faq_add(bid,None,'')) and rejects(lambda:db.db_faq_add(bid,None,'я'*41)))
    depth=folder
    for i in range(3): depth=db.db_faq_add(bid,depth,f'Глубина {i}')
    check('максимум 4 уровня',rejects(lambda:db.db_faq_add(bid,depth,'Пятый')))
    check('чужой parent_id отклоняется',rejects(lambda:db.db_faq_add(bid,foreign,'Чужой')))
    for i in range(49): db.db_faq_add(bid,nid,f'Кнопка {i}')
    db.db_faq_add(bid,nid,'Пятидесятая')
    check('максимум 50 кнопок в разделе',rejects(lambda:db.db_faq_add(bid,nid,'Лишняя')))
    c=db._conn(); total=c.execute('SELECT COUNT(*) FROM faq_nodes WHERE bot_id=?',(bid,)).fetchone()[0]; c.close()
    roots=db.db_faq_nodes(bid)
    for r in roots[2:]:
        for i in range(min(50,300-total)):
            db.db_faq_add(bid,r['id'],f'Общий лимит {i}'); total+=1
        if total==300: break
    check('максимум 300 узлов',total==300 and rejects(lambda:db.db_faq_add(bid,folder,'Лишняя')))
    for r in db.db_faq_nodes(bid): db.db_faq_subtree(bid,r['id'],delete=True)
    check('после удаления последнего узла FAQ исчезает','FAQ' not in menu())


if __name__=='__main__':
    methods=('get_me','get_chat','send_message','copy_message','copy_messages','forward_message','forward_messages','send_media_group','send_photo','edit_message_text','edit_forum_topic','create_forum_topic','pin_chat_message','delete_message','answer_callback_query')
    try:
        with patch.multiple(q.telebot.TeleBot,**{n:q.fake_api(n) for n in methods}):run()
    except Exception:traceback.print_exc();check('нет необработанных исключений',False)
    finally:
        for b in q.BOTS:pb._close_bot(b)
    print(f'ИТОГО: {len(q.OK)} OK, {len(q.FAIL)} FAIL');sys.exit(bool(q.FAIL))
