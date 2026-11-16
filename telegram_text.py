"""Отправка длинного HTML с сохранением тегов, сущностей и кнопок"""
import html
import json
import time
from functools import wraps
from html.parser import HTMLParser


def message_html(raw, caption=False):
    """Один проход по UTF-16: пересекающиеся entities не повторяют текст.

    На границах пересечений закрываем и переоткрываем теги, чтобы HTML
    оставался вложенным, а каждый исходный символ выводился ровно один раз.
    """
    raw = json.loads(raw) if isinstance(raw, str) else dict(raw)
    text = raw.get('caption' if caption else 'text') or ''
    encoded = text.encode('utf-16-le')
    size = len(encoded) // 2
    field = 'caption_entities' if caption else 'entities'
    simple = dict(bold='b', italic='i', underline='u', strikethrough='s',
                  spoiler='tg-spoiler', code='code', blockquote='blockquote')
    spans = []
    for entity in raw.get(field) or []:
        kind = entity['type']
        if kind in simple:
            tag = simple[kind]
            opening, closing = f'<{tag}>', f'</{tag}>'
        elif kind == 'expandable_blockquote':
            opening, closing = '<blockquote expandable>', '</blockquote>'
        elif kind == 'pre':
            language = html.escape(entity.get('language') or '', quote=True)
            opening = f'<pre><code class="language-{language}">' if language else '<pre>'
            closing = '</code></pre>' if language else '</pre>'
        elif kind in ('text_link', 'text_mention'):
            url = entity['url'] if kind == 'text_link' else f'tg://user?id={entity["user"]["id"]}'
            opening, closing = f'<a href="{html.escape(url, quote=True)}">', '</a>'
        elif kind == 'custom_emoji':
            opening = f'<tg-emoji emoji-id="{html.escape(str(entity["custom_emoji_id"]), quote=True)}">'
            closing = '</tg-emoji>'
        elif kind == 'date_time':
            opening = (f'<tg-time unix="{html.escape(str(entity["unix_time"]), quote=True)}" '
                       f'format="{html.escape(entity["date_time_format"], quote=True)}">')
            closing = '</tg-time>'
        else:
            continue  # URL, mention, hashtag и т. п. остаются исходным текстом.
        start = max(0, entity['offset'])
        end = min(size, entity['offset'] + entity['length'])
        if start < end:
            spans.append((start, end, opening, closing))
    spans.sort(key=lambda span: (span[0], -span[1]))
    boundaries = sorted({0, size} | {pos for span in spans for pos in span[:2]})
    result, previous = [], []
    for start, end in zip(boundaries, boundaries[1:]):
        active = [i for i, span in enumerate(spans) if span[0] <= start < span[1]]
        common = 0
        while common < min(len(previous), len(active)) and previous[common] == active[common]:
            common += 1
        result.extend(spans[i][3] for i in reversed(previous[common:]))
        result.extend(spans[i][2] for i in active[common:])
        result.append(html.escape(encoded[start*2:end*2].decode('utf-16-le'), quote=False))
        previous = active
    result.extend(spans[i][3] for i in reversed(previous))
    return ''.join(result)


def ordered_album(items, key=lambda message: message.message_id):
    """Порядок обработки worker-потоками не является порядком сообщений Telegram"""
    return sorted(items, key=key)


def retry_telegram(call):
    """Любая 429 повторяется только после retry_after и сохраняет результат API"""
    while True:
        try: return call()
        except Exception as exc:
            if getattr(exc, 'error_code', None) != 429: raise
            params = (getattr(exc, 'result_json', None) or {}).get('parameters', {})
            time.sleep(max(1, float(params.get('retry_after', 1))))


def html_chunks(text, limit=3800):
    if len((text or '').encode('utf-16-le')) // 2 <= limit:
        return [text] if text else []
    class Splitter(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=False)
            self.stack, self.parts, self.current = [], [], ''
        def closers(self):
            return ''.join('</' + tag + '>' for tag, raw in reversed(self.stack))
        def add(self, token):
            if len((self.current + token + self.closers()).encode('utf-16-le')) // 2 > limit:
                self.parts.append(self.current + self.closers())
                self.current = ''.join(raw for tag, raw in self.stack)
            self.current += token
        def handle_starttag(self, tag, attrs):
            raw = self.get_starttag_text()
            self.add(raw + '</' + tag + '>')
            self.current = self.current[:-(len(tag) + 3)]
            self.stack.append((tag, raw))
        def handle_endtag(self, tag):
            if self.stack and self.stack[-1][0] == tag:
                self.stack.pop()
                self.current += '</' + tag + '>'
        def handle_data(self, data):
            for char in data:
                self.add(char)
        def handle_entityref(self, name):
            self.add('&' + name + ';')
        def handle_charref(self, name):
            self.add('&#' + name + ';')
    parser = Splitter()
    parser.feed(text or '')
    parser.close()
    if parser.current:
        parser.parts.append(parser.current + parser.closers())
    return parser.parts


def protect_text_lengths(bot):
    """Один предел для всех отправок, в том числе из форумных обработчиков"""
    original = bot.send_message
    if getattr(original, '_length_guard', False):
        return
    @wraps(original)
    def send(chat_id, text, *args, **kwargs):
        if len((text or '').encode('utf-16-le')) // 2 <= 4096:
            return original(chat_id, text, *args, **kwargs)
        if kwargs.get('parse_mode') == 'HTML':
            chunks = html_chunks(text)
        else:
            chunks = [text[i:i+1900] for i in range(0, len(text), 1900)]
        result = None
        for i, chunk in enumerate(chunks):
            options = dict(kwargs)
            if i < len(chunks) - 1:
                options.pop('reply_markup', None)
            result = original(chat_id, chunk, *args, **options)
        return result
    send._length_guard = True
    bot.send_message = send


def send_html(bot, chat_id, text, reply_markup=None, parse_mode='HTML'):
    chunks = html_chunks(text, 4096) if parse_mode == 'HTML' else [text[i:i+1900] for i in range(0, len(text), 1900)]
    result = None
    for i, chunk in enumerate(chunks):
        result = bot.send_message(chat_id, chunk, parse_mode=parse_mode,
                                  reply_markup=reply_markup if i == len(chunks) - 1 else None)
    return result


def send_long(bot, chat_id, header, body, reply_markup=None):
    return send_html(bot, chat_id, header + '\n\n' + html.escape(body or ''), reply_markup)


def edit_html(bot, chat_id, message_id, text, reply_markup=None, parse_mode='HTML'):
    chunks = html_chunks(text, 4096) if parse_mode == 'HTML' else [text[i:i+1900] for i in range(0, len(text), 1900)]
    result = None
    for i, chunk in enumerate(chunks):
        markup = reply_markup if i == len(chunks) - 1 else None
        if i == 0:
            try:
                result = bot.edit_message_text(chunk, chat_id, message_id,
                                               parse_mode=parse_mode, reply_markup=markup)
                continue
            except Exception as exc:
                if 'not modified' in str(exc).lower():
                    continue
        result = bot.send_message(chat_id, chunk, parse_mode=parse_mode, reply_markup=markup)
    return result
