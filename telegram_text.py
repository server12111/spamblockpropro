"""Отправка длинного HTML с сохранением тегов, сущностей и кнопок"""
import html
import time
from functools import wraps
from html.parser import HTMLParser


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
