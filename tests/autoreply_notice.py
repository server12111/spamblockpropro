"""Автоответы: ввод entities, полные заметки и продолжение после 429."""
import importlib.util
import sys
import traceback
from html.parser import HTMLParser
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('queue_harness', Path(__file__).with_name('delivery_queue.py'))
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)
db, pb, fs = q.db, q.pb, q.fs
check = q.check


def plain(text):
    class Reader(HTMLParser):
        def __init__(self):
            super().__init__()
            self.text = ''
        def handle_data(self, data):
            self.text += data
    reader = Reader()
    reader.feed(text)
    return reader.text


def run():
    db.init_db()
    bid, bot, _ = q.fresh()
    text = '🤖 Привет, актуальная цена в боте, в разделе подписки'
    units = len(text.encode('utf-16-le')) // 2
    entities = [dict(type='custom_emoji', offset=0, length=2, custom_emoji_id='123'),
                dict(type='bold', offset=0, length=units-3),
                dict(type='italic', offset=3, length=units-3)]
    def enter():
        q.dispatch(bot, q.message(777, 1, text, entities=entities))
    q.click(bot, 777, 'ps_add')
    q.dispatch(bot, q.message(777, 2, 'сколько стоит, цена, ёлка'))
    enter()
    rule = db.db_auto_rules(bid)[0]
    check('пересекающиеся entities с premium emoji сохраняются без дубля', plain(rule['text']) == text)
    check('все стили и premium emoji сохранены', all(s in rule['text'] for s in ('<b>', '<i>', '<tg-emoji emoji-id="123">🤖</tg-emoji>')))
    class Styles(HTMLParser):
        def __init__(self):
            super().__init__()
            self.active, self.characters = [], []
        def handle_starttag(self, tag, attrs):
            self.active.append(tag)
        def handle_endtag(self, tag):
            assert self.active.pop() == tag
        def handle_data(self, data):
            self.characters.extend((char, set(self.active)) for char in data)
    rendered = Styles()
    rendered.feed(rule['text'])
    pos = 0
    correct_styles = True
    for char, styles in rendered.characters:
        correct_styles &= ('b' in styles) == (pos < units-3)
        correct_styles &= ('i' in styles) == (pos >= 3)
        correct_styles &= ('tg-emoji' in styles) == (pos == 0)
        pos += len(char.encode('utf-16-le')) // 2
    check('форматирование каждого символа совпадает с исходными entities', correct_styles and pos == units and not rendered.active)
    raw = q.message(777, 1, None, caption=text, caption_entities=entities).json
    check('подпись использует тот же безопасный конвертер', pb.message_html(raw, caption=True) == rule['text'])
    linked = q.message(777, 1, '🙂 <&> ссылка', entities=[
        dict(type='custom_emoji', offset=0, length=2, custom_emoji_id='123'),
        dict(type='text_link', offset=8, length=6, url='https://example.com/?a="x"&b=2')])
    linked_html = pb.message_html(linked.json)
    check('UTF-16, текст и атрибуты ссылок экранированы', plain(linked_html) == linked.text and
          'href="https://example.com/?a=&quot;x&quot;&amp;b=2"' in linked_html and '&lt;&amp;&gt;' in linked_html)
    for key in ('welcome', 'txt_sent', 'txt_offhours'):
        q.click(bot, 777, 'ps_edit_' + key)
        enter()
        check(key + ': общий ввод без дубля', plain(db.db_get_bot_setting(bid, key)) == text)
    q.click(bot, 777, 'fq_add_0')
    q.dispatch(bot, q.message(777, 3, 'Вопрос'))
    enter()
    check('FAQ: общий ввод без дубля', plain(db.db_faq_nodes(bid)[0]['text']) == text)
    q.click(bot, 777, 'p_add_tpl_8000')
    enter()
    template = db.db_get_templates(bid)[0][1]
    check('шаблон из лички: исходный текст без дубля', template == text)
    # Редактор шаблонов в группе тоже сохраняет исходный m.text.
    row = fs._mgmt_row(bid)
    cb = q.callback(777, 'fs_mgmt_add')
    cb.message = q.message(row[0], row[2], message_thread_id=row[1])
    fs.fs_management_callback(bot, bid, cb)
    m = q.message(row[0], 5, text, message_thread_id=row[1], entities=entities)
    q.dispatch(bot, m)
    check('шаблон из темы: исходный текст без дубля', [body for tid, body in db.db_get_templates(bid)] == [text, text])
    db.db_set_bot_setting(bid, 'txt_sent', pb.DEFAULT_SENT)
    for group in (True, False):
        if not group:
            bid, bot, _ = q.fresh(False)
            db.db_add_bot_admin(bid, 778)
            db.db_auto_save(bid, rule['phrases'], rule['text'])
        target = fs.fs_get_group(bid)[1] if group else 777
        label = 'тема' if group else 'личный режим'
        def deliver(uid, query):
            start = len(q.SENT)
            q.dispatch(bot, q.message(uid, 10, query))
            q.drain(bot, bid)
            return q.SENT[start:]
        sent = deliver(8001, 'ЦЕНА! Сколько, стоит?')
        expected = f'{pb.TE_ROBOT} <b>Автоответ на «сколько стоит»</b>\n<blockquote>{rule["text"]}</blockquote>'
        notices = [s for s in sent if s[0] == 'send_message' and s[1][0] == target and 'Автоответ' in s[1][1]]
        check(label + ': первая совпавшая фраза и полный HTML', len(notices) == 1 and notices[0][1][1] == expected)
        answer = [s for s in sent if s[0] == 'send_message' and s[1][0] == 8001]
        check(label + ': цитата совпадает с ответом пользователя', any(s[1][1] == rule['text'] for s in answer))
        if not group:
            check('заметка каждому админу', any(s[0] == 'send_message' and s[1][0] == 778 and s[1][1] == expected for s in sent))
        sent = deliver(8002, 'ЕЛКА!')
        check(label + ': исходная фраза после нормализации ё/е', any(s[0] == 'send_message' and s[1][0] == target and 'Автоответ на «ёлка»' in s[1][1] for s in sent))
        # Длинный ответ уже в хранилище: лимиты редактора не меняем.
        long = '<b>' + 'я' * 2500 + '<i>' + '🙂' * 1250 + '</i>' + 'я' * 1250 + '</b>'
        current = db.db_auto_rules(bid)[0]
        db.db_auto_save(bid, ['цена < 5 & 7'], long, current['id'])
        sent = deliver(8003, 'цена 5 7')
        parts = [s for s in sent if s[0] == 'send_message' and s[1][0] == target and '<blockquote>' in s[1][1]]
        header = plain(f'{pb.TE_ROBOT} <b>Автоответ на «цена &lt; 5 &amp; 7»</b>\n')
        check(label + ': 5000 символов без потерь и обрезки', len(plain(long)) == 5000 and len(parts) >= 2 and ''.join(plain(s[1][1]) for s in parts) == header + plain(long))
        valid = True
        for s in parts:
            try:
                pb.validate_support_html(s[1][1])
            except ValueError:
                valid = False
        check(label + ': части валидны, форматирование и лимит сохранены', bool(parts) and valid and all('<b>' in s[1][1] and s[2].get('parse_mode') == 'HTML' for s in parts))
        if group:
            c = db._conn()
            mapped = [c.execute('SELECT 1 FROM fs_msgs WHERE bot_id=? AND group_msg_id=?', (bid, s[3])).fetchone() for s in parts]
            replies = c.execute('SELECT COUNT(*) FROM fs_admin_msgs WHERE bot_id=?', (bid,)).fetchone()[0]
            c.close()
            check('все части служебные, в нужной теме и без соответствий', bool(parts) and not any(mapped) and all(s[2].get('message_thread_id') == fs.fs_get_topic(bid, 8003)[0] for s in parts))
            check('заметка не считается ответом администратора', replies == 0)
        # Повтор второй части не повторяет первую и ответ пользователю.
        attempts = [0]
        def fault(name, args, kwargs):
            if name == 'send_message' and args[0] == target and '<blockquote>' in args[1]:
                attempts[0] += 1
                if attempts[0] == 2:
                    raise q.error(429, 'retry', 30)
        q.FAULT = fault
        start = len(q.SENT)
        deliver(8004, 'цена 5 7')
        q.FAULT = None
        q.NOW[0] += 30
        restarted = pb.make_purchased_bot(bid, '100000:TEST', 777)
        q.BOTS.append(restarted)
        q.drain(restarted, bid)
        retried = [s for s in q.SENT[start:] if s[0] == 'send_message' and s[1][0] == target and '<blockquote>' in s[1][1]]
        user_answer = ''.join(plain(s[1][1]) for s in q.SENT[start:] if s[0] == 'send_message' and s[1][0] == 8004 and 'яя' in s[1][1])
        check(label + ': 429 второй части и рестарт без дублей', ''.join(plain(s[1][1]) for s in retried) == header + plain(long) and user_answer == plain(long) and not db.db_support_notifications(bid, q.NOW[0]))


if __name__ == '__main__':
    methods = ('get_me', 'get_chat', 'send_message', 'copy_message', 'copy_messages', 'forward_message', 'forward_messages', 'send_media_group', 'send_photo', 'edit_message_text', 'edit_forum_topic', 'create_forum_topic', 'pin_chat_message', 'delete_message', 'answer_callback_query')
    try:
        with patch.multiple(q.telebot.TeleBot, **{n: q.fake_api(n) for n in methods}), patch.object(q.dq, 'time', q.CLOCK), patch.object(db, 'time', q.CLOCK), patch.object(pb, 'time', q.CLOCK), patch.object(fs, 'RATE_LIMIT_SEC', 0):
            run()
    except Exception:
        traceback.print_exc()
        check('нет необработанных исключений', False)
    finally:
        q.FAULT = None
        for bot in q.BOTS:
            pb._close_bot(bot)
    print(f'ИТОГО: {len(q.OK)} OK, {len(q.FAIL)} FAIL')
    sys.exit(bool(q.FAIL))
