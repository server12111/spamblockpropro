"""Форматированные вводы: настоящие обработчики и SQLite, эмуляция только Telegram."""
import copy
import importlib.util
import json
import os
import sys
import traceback
from html.parser import HTMLParser
from pathlib import Path
from unittest.mock import patch

os.environ['SUPER_ADMIN'] = '777'
spec = importlib.util.spec_from_file_location('queue_harness', Path(__file__).with_name('delivery_queue.py'))
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)
db, pb, fs, t = q.db, q.pb, q.fs, q.t
check = q.check
_callback = q.callback


def unique_callback(uid, data):
    cb = _callback(uid, data)
    cb.id = 'fmt_' + str(len(q.API))
    return cb


q.callback = unique_callback
import telegram_text as fmt
import reply_queue as rq
import service_broadcast as svc


def units(text):
    return len(text.encode('utf-16-le')) // 2


TEXT = '😀Я жир ссылка\nцитата\nтайна 🌟Ю'
ENTITIES = []
for kind, fragment, extra in (
    ('bold', 'жир', {}),
    ('text_link', 'ссылка', {'url': 'https://example.com/?a=1&b=2'}),
    ('blockquote', 'цитата', {}),
    ('spoiler', 'тайна', {}),
    ('custom_emoji', '🌟', {'custom_emoji_id': '5432109876543210'}),
):
    start = TEXT.index(fragment)
    ENTITIES.append(dict(type=kind, offset=units(TEXT[:start]), length=units(fragment), **extra))
EXPECTED = ('😀Я <b>жир</b> <a href="https://example.com/?a=1&amp;b=2">ссылка</a>\n'
            '<blockquote>цитата</blockquote>\n<tg-spoiler>тайна</tg-spoiler> '
            '<tg-emoji emoji-id="5432109876543210">🌟</tg-emoji>Ю')


class TelegramHTML(HTMLParser):
    """Независимая имитация HTML Bot API: текст и UTF-16 entities ответа API."""
    def __init__(self, value):
        super().__init__()
        self.text, self.entities, self.stack = '', [], []
        self.feed(value)
        assert not self.stack, 'Незакрытый тег'

    def handle_data(self, data):
        self.text += data

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        kind = {'b': 'bold', 'i': 'italic', 'u': 'underline', 's': 'strikethrough',
                'tg-spoiler': 'spoiler', 'code': 'code', 'pre': 'pre',
                'blockquote': 'blockquote', 'tg-emoji': 'custom_emoji', 'a': 'text_link'}[tag]
        extra = {}
        if tag == 'a': extra['url'] = attrs['href']
        if tag == 'tg-emoji': extra['custom_emoji_id'] = attrs['emoji-id']
        if tag == 'blockquote' and 'expandable' in attrs: kind = 'expandable_blockquote'
        self.stack.append((tag, dict(type=kind, offset=units(self.text), **extra)))

    def handle_endtag(self, tag):
        opening, entity = self.stack.pop()
        assert opening == tag, 'Неверная вложенность'
        entity['length'] = units(self.text) - entity['offset']
        self.entities.append(entity)


MESSAGES, DELIVERED = {}, []
FAIL_COPY = False


def api(name):
    original = q.fake_api(name)
    def call(bot, *a, **kw):
        if FAIL_COPY and name in ('copy_message', 'forward_message'):
            raise q.error(400, 'protected content')
        result = original(bot, *a, **kw)
        if name in ('send_message', 'edit_message_text', 'send_photo'):
            edit = name == 'edit_message_text'
            chat, mid = (a[1], a[2]) if edit else (a[0], result.message_id)
            value = a[0] if edit else (kw.get('caption') or '' if name == 'send_photo' else a[1])
            parsed = TelegramHTML(value) if kw.get('parse_mode') == 'HTML' else None
            body = parsed.text if parsed else value
            entities = parsed.entities if parsed else [e.to_dict() if hasattr(e, 'to_dict') else e
                for e in (kw.get('caption_entities' if name == 'send_photo' else 'entities') or [])]
            extra = dict(message_thread_id=kw.get('message_thread_id'))
            if name == 'send_photo':
                extra.update(photo=[dict(file_id='photo', file_unique_id='u', width=1, height=1)],
                             caption=body, caption_entities=entities)
                body = None
            else:
                extra['entities'] = entities
            message = q.message(chat, mid, body, **extra)
            MESSAGES[(chat, mid)] = message
            DELIVERED.append((name, chat, copy.deepcopy(message.json)))
            return message
        if name in ('copy_message', 'forward_message'):
            source = MESSAGES.get((a[1], a[2]))
            if source:
                raw = copy.deepcopy(source.json)
                raw.update(message_id=result.message_id, chat={'id': a[0], 'type': 'private'})
                MESSAGES[(a[0], result.message_id)] = t.Message.de_json(raw)
                DELIVERED.append((name, a[0], raw))
        return result
    return call


def incoming(chat=777, caption=False, **extra):
    mid = 100 + len(q.API)
    if caption:
        return q.message(chat, mid, None, caption=TEXT, caption_entities=ENTITIES,
                         photo=[dict(file_id='photo', file_unique_id='u', width=1, height=1)], **extra)
    return q.message(chat, mid, TEXT, entities=ENTITIES, **extra)


def same_body(raw, body=TEXT, entities=ENTITIES):
    text = raw.get('text') or raw.get('caption') or ''
    if text.count(body) != 1:
        return False
    shift = units(text[:text.index(body)])
    received = raw.get('entities') or raw.get('caption_entities') or []
    # telebot добавляет необязательные поля со значением None в to_dict().
    # Сравниваем точно все содержательные entities внутри проверяемого текста.
    received = [{k: v for k, v in e.items() if v is not None} for e in received
                if shift <= e['offset'] < shift + units(body)]
    expected = [dict(e, offset=e['offset'] + shift) for e in entities]
    def canonical(items):
        return sorted(json.dumps(e, sort_keys=True) for e in items)
    return canonical(received) == canonical(expected)


def last_to(chat, start=0):
    return next(raw for _, target, raw in reversed(DELIVERED[start:]) if target == chat)


def group_click(bot, bid, data, thread=None, mid=None):
    row = fs._mgmt_row(bid)
    cb = q.callback(777, data)
    cb.message = q.message(row[0], mid or row[2], message_thread_id=thread or row[1])
    for h in bot.callback_query_handlers:
        if h['filters']['func'](cb): return h['function'](cb)


def run():
    global FAIL_COPY
    db.init_db()
    bid, bot, _ = q.fresh()
    uid = 8800
    db.db_add_user(bid, uid, 'user')
    check('общий конвертер: точный HTML, UTF-16 и единственный текст',
          fmt.message_html(incoming().json) == EXPECTED)
    check('caption: тот же HTML', fmt.message_html(incoming(caption=True).json, caption=True) == EXPECTED)
    for caption in (False, True):
        for path in ('панель', 'ответ', 'Управление'):
            if path == 'панель': q.click(bot, 777, 'p_mgadd_tpl')
            elif path == 'ответ': q.click(bot, 777, f'p_add_tpl_{uid}')
            else: group_click(bot, bid, 'fs_mgmt_add')
            row = fs._mgmt_row(bid)
            m = incoming(row[0], caption, message_thread_id=row[1]) if path == 'Управление' else incoming(caption=caption)
            q.dispatch(bot, m)
            tid, saved = db.db_get_templates(bid)[-1]
            # Убираем только служебный маркер нового формата, содержимое проверяем точно.
            check(f'{path}, caption={caption}: сохранён полный HTML',
                  saved == '<!--formatted:v1-->\n' + EXPECTED)
            start = len(DELIVERED)
            q.click(bot, 777, f'p_use_tpl_{uid}_{tid}')
            check(f'{path}: пользователю только шаблон', last_to(uid, start)['text'] == TEXT
                  and same_body(last_to(uid, start)))
            admin = next(raw for _, chat, raw in DELIVERED[start:] if chat == 777 and same_body(raw))
            check(f'{path}: копия в личке с пометкой', admin['text'].startswith('📝 Шаблон\n'))
            fs.fs_claim_topic(bid, uid, 'Пользователь')
            fs.fs_set_topic_thread(bid, uid, 555)
            start = len(DELIVERED)
            group_click(bot, bid, f'fs_pin_send_{uid}_{tid}', thread=555)
            user = last_to(uid, start)
            marked = last_to(row[0], start)
            check(f'{path}: тема с пометкой, пользователь без', user['text'] == TEXT and same_body(user)
                  and marked['text'].startswith('📝 Шаблон\n') and same_body(marked))
            check(f'{path}: Reply соответствует прежнему сообщению',
                  fs.fs_get_mapping(row[0], marked['message_id']) == (bid, uid, 555, user['message_id']))

    old = 'Старый <b>шаблон</b> & ответ'
    tid = db.db_add_template(bid, old)
    q.click(bot, 777, f'p_use_tpl_{uid}_{tid}')
    check('старый шаблон остаётся буквальным текстом', last_to(uid)['text'] == old
          and not last_to(uid).get('entities'))
    group_click(bot, bid, f'fs_pin_send_{uid}_{tid}', thread=555)
    check('старый шаблон в теме: буквальный текст и пометка только админам',
          last_to(uid)['text'] == old and last_to(fs._mgmt_row(bid)[0])['text'] == '📝 Шаблон\n' + old)

    for caption in (False, True):
        for index, key in enumerate(pb.TEXTS):
            recipient = uid + 100 + 10 * int(caption) + index
            q.click(bot, 777, 'ps_edit_' + key)
            q.dispatch(bot, incoming(caption=caption))
            check(f'{key}, caption={caption}: сохранён HTML', db.db_get_bot_setting(bid, key) == EXPECTED)
            if key == 'welcome': q.dispatch(bot, q.message(recipient, 8, '/start'))
            elif key == 'txt_sent': q.dispatch(bot, q.message(recipient, 9, 'письмо'))
            else:
                db.db_set_bot_setting(bid, 'hours_enabled', '1')
                db.db_set_bot_setting(bid, 'hours_days', '')
                q.dispatch(bot, q.message(recipient, 10, 'другое письмо'))
                q.drain(bot, bid)
            check(f'{key}, caption={caption}: пользователю те же entities', same_body(last_to(recipient)))
            db.db_set_bot_setting(bid, 'hours_enabled', '0')

        phrase = 'проверка' + str(int(caption))
        q.click(bot, 777, 'ps_add'); q.dispatch(bot, q.message(777, 20, phrase))
        q.dispatch(bot, incoming(caption=caption))
        rule = db.db_auto_rules(bid)[-1]
        check(f'новый автоответ, caption={caption}: сохранён HTML', rule['text'] == EXPECTED)
        q.click(bot, 777, f"ps_answer_{rule['id']}")
        q.dispatch(bot, incoming(caption=caption))
        check(f'изменение автоответа, caption={caption}: сохранён HTML',
              next(r for r in db.db_auto_rules(bid) if r['id'] == rule['id'])['text'] == EXPECTED)
        recipient = uid + 200 + int(caption)
        q.dispatch(bot, q.message(recipient, 21, phrase)); q.drain(bot, bid)
        check(f'автоответ, caption={caption}: доставлен с entities',
              any(chat == recipient and same_body(raw) for _, chat, raw in DELIVERED))

        for rename in (False, True):
            q.click(bot, 777, f'fq_name_{nid}' if rename else 'fq_add_0')
            q.dispatch(bot, incoming(caption=caption))
            if not rename:
                q.dispatch(bot, incoming(caption=caption))
                nid = db.db_faq_nodes(bid)[-1]['id']
            q.click(bot, uid, f'fq_u_{nid}_0')
            shown = last_to(uid)
            check(f'FAQ название, rename={rename}, caption={caption}: все entities',
                  same_body(dict(text=shown['text'][:len(TEXT)], entities=shown['entities'])))
            q.click(bot, 777, f'fq_text_{nid}'); q.dispatch(bot, incoming(caption=caption))
            check(f'FAQ текст, caption={caption}: сохранён HTML', db.db_faq_get(bid, nid)['text'] == EXPECTED)
            q.click(bot, uid, f'fq_u_{nid}_0')
            both = ENTITIES + [dict(e, offset=e['offset'] + units(TEXT + '\n\n')) for e in ENTITIES]
            check(f'FAQ название и текст, caption={caption}: полные entities без потери',
                  same_body(last_to(uid), body=TEXT + '\n\n' + TEXT, entities=both))

    q.click(bot, 777, f'p_reply_{uid}'); q.dispatch(bot, incoming())
    check('ручной ответ в личке', same_body(last_to(uid)))
    q.click(bot, 777, f'p_reply_{uid}'); q.dispatch(bot, incoming(caption=True))
    check('подпись фото в личном ответе', same_body(last_to(uid)))
    for caption in (False, True):
        for fallback in (False, True):
            FAIL_COPY = fallback
            m = incoming(fs._mgmt_row(bid)[0], caption=caption, message_thread_id=555)
            MESSAGES[(m.chat.id, m.message_id)] = m
            q.dispatch(bot, m)
            FAIL_COPY = False
            check(f'ручной ответ из темы, caption={caption}, resend={fallback}', same_body(last_to(uid)))
        q.click(bot, 777, 'bcast_text_p'); q.dispatch(bot, incoming(caption=caption))
        check(f'рассылка купленного бота: текст, caption={caption}', same_body(last_to(uid)))
    q.click(bot, 777, 'bcast_photo_p'); q.dispatch(bot, incoming(caption=True))
    check('рассылка купленного бота: подпись', same_body(last_to(uid)))

    import handlers
    main = q.telebot.TeleBot('123456:TEST', threaded=False)
    handlers.register(main)
    db.DBState(0)[777] = f'await_admin_msg_{uid}'
    q.dispatch(main, incoming())
    check('ответ суперадмина', same_body(last_to(uid)))
    db.DBState(0)[777] = f'await_admin_msg_{uid}'
    q.dispatch(main, incoming(caption=True))
    check('ответ суперадмина из подписи', same_body(last_to(uid)))
    for caption in (False, True):
        jid = svc.append(main, 777, 777, 'main', incoming(caption=caption))
        svc.send(main, jid, 0, uid)
        check(f'рассылка главного бота, caption={caption}', same_body(last_to(uid)))

    # Новые типы и вложенные entities: проверяем преобразователь независимо от telebot.html_text.
    complex_text = '😀Я цитата\nкод\nблок\nимя Ю'
    entities = []
    for kind, part, extra in (
        ('expandable_blockquote', 'цитата', {}), ('code', 'код', {}),
        ('pre', 'блок', {'language': 'python'}),
        ('text_mention', 'имя', {'user': {'id': 123, 'is_bot': False, 'first_name': 'Имя'}}),
    ):
        entities.append(dict(type=kind, offset=units(complex_text[:complex_text.index(part)]), length=units(part), **extra))
    converted = fmt.message_html(q.message(777, 90, complex_text, entities=entities).json)
    check('expandable, code/pre, text_mention и UTF-16',
          '<blockquote expandable>цитата</blockquote>' in converted and '<code>код</code>' in converted
          and '<pre><code class="language-python">блок</code></pre>' in converted
          and '<a href="tg://user?id=123">имя</a>' in converted)

    q.click(bot, 777, 'p_mgadd_tpl')
    count = len(db.db_get_templates(bid))
    oversized = q.message(777, 91, 'я' * 4096, entities=[dict(type='bold', offset=0, length=4096)])
    q.dispatch(bot, oversized)
    check('лимит проверяет результат с HTML', len(db.db_get_templates(bid)) == count)
    q.click(bot, 777, 'p_cancel')

    # Пометка может вытеснить хвост максимального шаблона во второе сообщение.
    long_text = 'я' * 4080
    q.click(bot, 777, 'p_mgadd_tpl')
    q.dispatch(bot, q.message(777, 1000 + len(q.API), long_text,
                             entities=[dict(type='bold', offset=0, length=units(long_text))]))
    long_tid = db.db_get_templates(bid)[-1][0]
    start = len(DELIVERED)
    q.click(bot, 777, f'p_use_tpl_{uid}_{long_tid}')
    admin_parts = [raw for _, chat, raw in DELIVERED[start:] if chat == 777 and 'яя' in raw.get('text', '')]
    check('длинный шаблон в личке: полный текст и одна пометка',
          ''.join(raw['text'] for raw in admin_parts) == '📝 Шаблон\n' + long_text
          and last_to(uid, start)['text'] == long_text)

    gid = fs._mgmt_row(bid)[0]
    start = len(DELIVERED)
    def rate_limit_mark(name, args, kw):
        if name == 'edit_message_text' and 'Шаблон' in args[0]:
            raise q.error(429, 'retry', 30)
    with patch.object(rq, 'time', q.CLOCK), patch.object(q.dq, 'time', q.CLOCK), patch.object(db, 'time', q.CLOCK):
        q.FAULT = rate_limit_mark
        group_click(bot, bid, f'fs_pin_send_{uid}_{long_tid}', thread=555)
        q.FAULT = None
        before = len(q.API)
        rq.pump(bot, bid)
        check('пометка после 429: до retry_after запросов нет', len(q.API) == before and bool(rq.pending(bid)))
        q.NOW[0] += 31
        restarted = pb.make_purchased_bot(bid, '123457:TEST', 777)
        q.BOTS.append(restarted)
        rq.pump(restarted, bid)
    user_parts = [raw for _, chat, raw in DELIVERED[start:] if chat == uid]
    # Читаем актуальные сообщения темы: edit заменяет чистый источник помеченным.
    group_ids = list(dict.fromkeys(raw['message_id'] for _, chat, raw in DELIVERED[start:]
                                  if chat == gid and 'яя' in raw.get('text', '')))
    marked_parts = [MESSAGES[(gid, mid)].json for mid in group_ids]
    check('пометка после 429 и рестарта: пользователь получил шаблон один раз',
          len(user_parts) == 1 and user_parts[0]['text'] == long_text and not rq.pending(bid))
    check('длинный шаблон в теме: полный текст и одна пометка',
          ''.join(raw['text'] for raw in marked_parts) == '📝 Шаблон\n' + long_text)
    check('длинный шаблон: каждая часть в теме сохраняет Reply',
          all(fs.fs_get_mapping(gid, raw['message_id']) == (bid, uid, 555, user_parts[0]['message_id'])
              for raw in marked_parts))

    start = len(DELIVERED)
    FAIL_COPY = True
    q.dispatch(bot, q.message(gid, 2000 + len(q.API), 'я' * 4096, message_thread_id=555,
                             entities=[dict(type='bold', offset=0, length=4096)]))
    FAIL_COPY = False
    delivered = [raw for _, chat, raw in DELIVERED[start:] if chat == uid]
    check('resend ответа 4096 символов: одна доставка с исходными entities',
          len(delivered) == 1 and same_body(delivered[0], body='я' * 4096,
              entities=[dict(type='bold', offset=0, length=4096)]))

    start = len(DELIVERED)
    def reject_admin_copy(name, args, kw):
        if name == 'send_message' and args[0] == 777 and '<b>Шаблон</b>' in args[1]:
            raise q.error(403, 'blocked')
    q.FAULT = reject_admin_copy
    q.click(bot, 777, f'p_use_tpl_{uid}_{long_tid}')
    q.FAULT = None
    check('отказ копии админу не меняет успешную доставку пользователю',
          len([raw for _, chat, raw in DELIVERED[start:] if chat == uid]) == 1
          and last_to(uid, start)['text'] == long_text and 'Ответ отправлен' in last_to(777, start)['text'])

    # Медиа без подписи не заменяет ожидаемый текст пустой строкой.
    for flow in ('p_mgadd_tpl', f'p_add_tpl_{uid}', 'ps_edit_welcome', 'fq_add_0', 'bcast_text_p'):
        q.click(bot, 777, flow)
        before = db.DBState(bid).get(777)
        start = len(DELIVERED)
        bare = q.message(777, 3000 + len(q.API), None,
                         photo=[dict(file_id='photo', file_unique_id='u', width=1, height=1)])
        q.dispatch(bot, bare)
        check(flow + ': фото без подписи сохраняет ожидание текста',
              db.DBState(bid).get(777) == before and not any(chat == uid for _, chat, raw in DELIVERED[start:]))
    q.click(bot, 777, 'p_cancel')

    # Отказ Premium обрабатывает существующая защита, остальные стили остаются.
    def no_premium(name, args, kw):
        if name == 'send_message' and 'emoji-id="5432109876543210"' in args[1]:
            raise q.error(400, 'custom_emoji premium required')
    q.FAULT = no_premium
    fmt.send_html(bot, uid, EXPECTED)
    q.FAULT = None
    raw = last_to(uid)
    check('premium fallback: символы и остальные entities сохранены', raw['text'] == TEXT
          and same_body(raw, entities=[e for e in ENTITIES if e['type'] != 'custom_emoji'])
          and not any(e['type'] == 'custom_emoji' for e in raw['entities']))


if __name__ == '__main__':
    methods = ('get_me', 'get_chat', 'get_chat_member', 'send_message', 'send_photo', 'copy_message',
               'copy_messages', 'forward_message', 'forward_messages', 'send_media_group',
               'edit_message_text', 'edit_forum_topic', 'create_forum_topic', 'pin_chat_message',
               'delete_message', 'answer_callback_query', 'edit_message_reply_markup')
    try:
        with patch.multiple(q.telebot.TeleBot, **{name: api(name) for name in methods}): run()
    except Exception:
        traceback.print_exc()
        check('нет необработанных исключений', False)
    finally:
        for bot in q.BOTS: pb._close_bot(bot)
    print(f'ИТОГО: {len(q.OK)} OK, {len(q.FAIL)} FAIL')
    sys.exit(bool(q.FAIL))
