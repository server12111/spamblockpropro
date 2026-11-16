# -*- coding: utf-8 -*-
"""Сценарии проверки купленного бота: реальный код, временная БД, Telegram без сети"""
import atexit
import html
import io
import json
import logging
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import threading
import types
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DATA = tempfile.mkdtemp(prefix='fix_b_', dir=ROOT / 'tests')
os.environ.update(DATA_DIR=DATA, PYTHON_DOTENV_DISABLED='1', SUPER_ADMIN='900',
                  BOT_TOKEN='123456:' + 'A' * 35, CRYPTOBOT_TKN='test-api-key',
                  TONCENTER_KEY='test-ton-key', WEBHOOK_URL='')
sys.modules['dotenv'] = types.SimpleNamespace(load_dotenv=lambda *a, **k: False)
import telebot
from telebot import types as t
from telebot.apihelper import ApiTelegramException
import database as db
import forum_support as FS
import purchased_bot as PB

db.init_db()
API, BOTS = [], []
SEQ = 1000
OK = FAIL = 0


def message(uid, text='текст', chat=None, **fields):
    global SEQ
    SEQ += 1
    data = {'message_id': SEQ, 'date': 0,
            'from': {'id': uid, 'is_bot': False, 'first_name': 'Юзер'},
            'chat': {'id': chat or uid, 'type': 'supergroup' if chat and chat < 0 else 'private'}}
    if text is not None:
        data['text'] = text
    data.update(fields)
    return t.Message.de_json(data)


def callback(bot, uid, data, original=None):
    cb = t.CallbackQuery('cb', t.User(uid, False, 'Админ'), data=data,
                         chat_instance='ci', json_string={}, message=original or message(uid))
    bot.process_new_callback_query([cb])


def dispatch(bot, msg):
    bot.process_new_messages([msg])


def api(name):
    def call(self, *args, **kw):
        if name in ('send_message', 'edit_message_text'):
            text = args[1] if name == 'send_message' else args[0]
            plain = html.unescape(re.sub(r'<[^>]*>', '', text)) if kw.get('parse_mode') == 'HTML' else text
            assert len(plain.encode('utf-16-le')) // 2 <= 4096, 'message is too long'
        API.append((name, args, kw))
        if name == 'get_me':
            return t.User(999, True, 'Бот', username='test_bot')
        return message(args[0] if args and isinstance(args[0], int) else 1, 'ok')
    return call


for name in ('get_me', 'send_message', 'send_photo', 'send_video', 'send_document',
             'send_audio', 'send_voice', 'send_video_note', 'send_animation', 'send_sticker',
             'forward_message', 'copy_message', 'edit_message_text', 'delete_message',
             'answer_callback_query', 'edit_message_reply_markup', 'delete_webhook'):
    setattr(telebot.TeleBot, name, api(name))
PB.get_ton_amount = lambda: 1


def fresh():
    owner = 100 + len(BOTS) * 10
    token = str(200000 + owner) + ':TEST'
    bid = db.db_add_bot(owner, token, owner)
    bot = PB.make_purchased_bot(bid, token, owner)
    # Обработчики выполняются синхронно; настоящие worker-потоки остаются для проверки stop
    bot.threaded = False
    BOTS.append(bot)
    return bid, bot, owner, token


def cleanup():
    for bot in BOTS:
        if hasattr(bot, 'worker_pool'):
            bot.worker_pool.close()
    db.db_close_all()
    logging.shutdown()
    shutil.rmtree(DATA)


atexit.register(cleanup)


def check(name, fn):
    global OK, FAIL
    API.clear()
    try:
        fn()
    except Exception as exc:
        FAIL += 1
        import traceback
        line = traceback.extract_tb(exc.__traceback__)[-1].lineno
        print('[FAIL] ' + name + ' (' + type(exc).__name__ + f', строка {line})')
    else:
        OK += 1
        print('[OK] ' + name)


def sent_to(uid, method='send_message'):
    return [(args, kw) for name, args, kw in API if name == method and args[0] == uid]


def owner_only():
    bid, bot, owner, _ = fresh()
    admin = owner + 1
    db.db_add_bot_admin(bid, admin)
    db.db_add_bot_earnings(bid, 10)
    exp = db.db_get_bot_expires(bid)
    for data in ('p_balance', 'p_withdraw', 'p_renew_balance', 'p_renew_bal_30', 'p_renew_bal_all'):
        callback(bot, admin, data)
    for st, text in (({'step': 'p_await_ton_address', 'balance': 10}, 'U' * 48),
                     ({'step': 'p_await_withdraw_amount', 'balance': 10, 'address': 'U' * 48}, 'всё')):
        db.DBState(bid)[admin] = st
        dispatch(bot, message(admin, text))
    assert db.db_get_bot_earnings(bid)[0] == 10 and db.db_get_bot_expires(bid) == exp
    assert len([1 for n, a, k in API if 'Доступно только владельцу бота' in str(a)]) == 7
    callback(bot, owner, 'p_withdraw')
    dispatch(bot, message(owner, 'U' * 48))
    dispatch(bot, message(owner, 'всё'))
    assert db.db_get_bot_earnings(bid)[0] == 0


def masked_log():
    import requests
    import urllib3.connection
    import urllib3.exceptions
    bid, bot, owner, token = fresh()
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    logging.getLogger().addHandler(handler)
    def fail(*a, **kw):
        raise urllib3.exceptions.NewConnectionError(a[0], 'test connection refused')
    def get_me(self):
        session = requests.Session()
        session.trust_env = False
        return session.get('https://api.telegram.org/bot' + token + '/getMe')
    try:
        with patch.object(urllib3.connection.HTTPSConnection, 'connect', fail), \
                patch.object(telebot.TeleBot, 'get_me', get_me), \
                patch.object(PB, 'make_purchased_bot', return_value=bot):
            assert not PB.launch_bot(bid, token, owner)
        logging.getLogger('werkzeug').warning('POST /webhook/' + token)
        assert token not in output.getvalue(), 'secret leaked'
        assert '[СКРЫТО]' in output.getvalue(), 'request did not reach adapter'
    finally:
        logging.getLogger().removeHandler(handler)


def private_state():
    bid, bot, owner, _ = fresh()
    callback(bot, owner, 'p_reply_700')
    dispatch(bot, message(owner, 'Внутренняя заметка', chat=-100,
                          message_thread_id=55,
                          reply_to_message=message(999, 'Закреп', chat=-100).json))
    assert not sent_to(700)
    assert db.DBState(bid).get(owner) == 'await_reply_700'
    # Другие состояния личной панели также не перехватывают сообщения группы
    callback(bot, owner, 'p_mgadd_tpl')
    dispatch(bot, message(owner, 'Заметка', chat=-100))
    assert not db.db_get_templates(bid)
    dispatch(bot, message(owner, 'Шаблон из лички'))
    assert db.db_get_templates(bid)[0][1] == 'Шаблон из лички'


def media_back():
    bid, bot, owner, _ = fresh()
    original = message(owner, None, photo=[{'file_id': 'original-photo', 'file_unique_id': 'p',
                                          'width': 10, 'height': 10}], caption='Исходная подпись')
    callback(bot, owner, 'p_reply_701', original)
    callback(bot, owner, 'p_back_reply_701')
    deleted = any(n == 'delete_message' and a == (owner, original.message_id) for n, a, k in API)
    restored = any(a[1] == 'original-photo' and 'Исходная подпись' in k.get('caption', '')
                   for a, k in sent_to(owner, 'send_photo'))
    assert not deleted or restored
    callback(bot, owner, 'p_reply_701', original)
    dispatch(bot, message(owner, 'Ответ'))
    assert sent_to(701)


class DeferredThread:
    pending = []
    def __init__(self, target=None, **kw):
        self.target = target
    def start(self):
        self.pending.append(self.target)


def no_drop():
    bid, bot, owner, token = fresh()
    DeferredThread.pending.clear()
    with patch.object(PB, 'make_purchased_bot', return_value=bot), \
            patch.object(PB.threading, 'Thread', DeferredThread):
        assert PB.launch_bot(bid, token, owner)
    try:
        assert any(n == 'delete_webhook' and k.get('drop_pending_updates') is False for n, a, k in API)
    finally:
        PB.stop_bot(token)


def restart_loop():
    bid, old, owner, token = fresh()
    replacement = telebot.TeleBot(token, threaded=False)
    BOTS.append(replacement)
    DeferredThread.pending.clear()
    with patch.object(PB, 'make_purchased_bot', side_effect=[old, replacement]), \
            patch.object(PB.threading, 'Thread', DeferredThread):
        PB.launch_bot(bid, token, owner)
        old_loop = DeferredThread.pending[-1]
        PB.stop_bot(token)
        PB.launch_bot(bid, token, owner)
    calls = []
    class LoopDetected(Exception): pass
    def returned(**kw):
        calls.append(1)
        if len(calls) >= 3:
            PB.running_bot_ids.pop(bid, None)
            raise LoopDetected()
    old.infinity_polling = returned
    try:
        old_loop()
        assert not calls
    finally:
        PB.stop_bot(token)


def reply_race(kind='text', failure=False, same=False):
    bid, bot, owner, _ = fresh()
    callback(bot, owner, 'p_reply_710')
    entered, release = threading.Event(), threading.Event()
    method = 'send_photo' if kind == 'photo' else 'send_message'
    original = getattr(bot, method)
    errors = []
    def delayed(uid, *a, **kw):
        if uid == 710:
            entered.set()
            assert release.wait(5)
            if failure:
                raise RuntimeError('test failed delivery')
        return original(uid, *a, **kw)
    def run():
        try:
            if kind == 'template':
                tid = db.db_add_template(bid, 'Шаблон')
                callback(bot, owner, f'p_use_tpl_710_{tid}')
            else:
                m = message(owner, 'Ответ') if kind == 'text' else message(owner, None,
                    photo=[{'file_id': 'reply-photo', 'file_unique_id': 'r', 'width': 10, 'height': 10}])
                dispatch(bot, m)
        except Exception as exc:
            errors.append(exc)
    target = 710 if same else 711
    with patch.object(bot, method, delayed):
        thread = threading.Thread(target=run)
        thread.start()
        try:
            assert entered.wait(5)
            callback(bot, owner, f'p_reply_{target}')
        finally:
            release.set()
            thread.join(5)
    assert not thread.is_alive() and not errors
    assert db.DBState(bid).get(owner) == f'await_reply_{target}'
    dispatch(bot, message(owner, 'Следующий ответ'))
    assert any('Следующий ответ' in a[1] for a, k in sent_to(target))


def workers_stop():
    bid, bot, owner, token = fresh()
    bot.threaded = True
    workers = list(bot.worker_pool.workers)
    assert all(w.is_alive() for w in workers)
    PB.running_bots[token] = bot
    PB.running_bot_ids[bid] = token
    assert PB.stop_bot(token)
    assert all(not w.is_alive() for w in workers)


def long_delivery():
    bid, bot, owner, _ = fresh()
    dispatch(bot, message(720, 'я' * 4096))
    assert sent_to(owner)
    assert not FS.fs_get_pending(bid, 720)
    assert db.db_get_last_user_messages(bid, 720, n=2)[0][0] == 'я' * 4096


def one_queue():
    bid, bot, owner, _ = fresh()
    original = bot.send_message
    def fail(uid, *a, **kw):
        if uid == owner:
            raise RuntimeError('test unavailable')
        return original(uid, *a, **kw)
    with patch.object(bot, 'send_message', fail):
        m = message(721, 'Не терять')
        dispatch(bot, m)
    pending = FS.fs_get_pending(bid, 721)
    payload = json.loads(pending[0])
    assert len(payload) == 1 and payload[0]['mid'] == m.message_id


def album_limit():
    bid, bot, owner, _ = fresh()
    dispatch(bot, message(722, 'Сначала текст'))
    for i in range(2):
        dispatch(bot, message(722, None, media_group_id='rejected', photo=[
            {'file_id': f'p{i}', 'file_unique_id': f'u{i}', 'width': 10, 'height': 10}]))
    assert not sent_to(owner, 'send_photo')
    # Разрешённый альбом доставляется полностью и лимит проверяется один раз
    with patch.object(PB, '_is_rate_limited', return_value=False) as limiter:
        for i in range(2):
            dispatch(bot, message(723, None, media_group_id='accepted', photo=[
                {'file_id': f'a{i}', 'file_unique_id': f'v{i}', 'width': 10, 'height': 10}]))
    assert len(sent_to(owner, 'send_photo')) == 2 and limiter.call_count == 1


def broadcast_retry(photo=False):
    bid, bot, owner, _ = fresh()
    db.db_add_user(bid, 724, '')
    callback(bot, owner, 'bcast_photo_p' if photo else 'bcast_text_p')
    method = 'send_photo' if photo else 'send_message'
    original = getattr(bot, method)
    attempts = []
    def limited(uid, *a, **kw):
        if uid == 724:
            attempts.append(1)
            if len(attempts) == 1:
                raise ApiTelegramException('send', None, {'ok': False, 'error_code': 429,
                    'description': 'Too Many Requests', 'parameters': {'retry_after': 2}})
        return original(uid, *a, **kw)
    m = message(owner, 'Рассылка') if not photo else message(owner, None, photo=[
        {'file_id': 'broadcast', 'file_unique_id': 'b', 'width': 10, 'height': 10}])
    with patch.object(bot, method, limited), patch.object(PB.time, 'sleep') as sleep:
        dispatch(bot, m)
    assert len(attempts) == 2 and sleep.call_args.args[0] >= 2
    assert any('1/1' in a[1] for a, k in sent_to(owner))


def cancel_template():
    bid, bot, owner, _ = fresh()
    callback(bot, owner, 'p_mgadd_tpl')
    callback(bot, owner, 'p_admin_templates')
    dispatch(bot, message(owner, 'Случайный текст'))
    assert not db.db_get_templates(bid) and db.DBState(bid).get(owner) is None


def long_template():
    bid, bot, owner, _ = fresh()
    callback(bot, owner, 'p_mgadd_tpl')
    dispatch(bot, message(owner, 'ш' * 4096))
    tid = db.db_get_templates(bid)[0][0]
    callback(bot, owner, f'p_use_tpl_725_{tid}')
    assert sum(a[1].count('ш') for a, k in sent_to(725)) == 4096


def templates_list():
    bid, bot, owner, _ = fresh()
    for char in ('я', 'ю'):
        db.db_add_template(bid, char * 2100)
    dispatch(bot, message(owner, '/templates'))
    assert sum(a[1].count('я') for a, k in sent_to(owner)) >= 2100
    assert sum(a[1].count('ю') for a, k in sent_to(owner)) >= 2100


def long_history():
    bid, bot, owner, _ = fresh()
    for char in ('я', 'ю'):
        db.db_log_message(bid, 726, char * 2100)
    callback(bot, owner, 'p_reply_726')
    callback(bot, owner, 'p_back_reply_726')
    texts = [a[0] if n == 'edit_message_text' else a[1] for n, a, k in API
             if n in ('edit_message_text', 'send_message')]
    assert sum(text.count('я') for text in texts) >= 2100
    assert sum(text.count('ю') for text in texts) >= 2100
    assert any(k.get('reply_markup') and 'p_reply_726' in k['reply_markup'].to_json()
               for n, a, k in API)


def forwarded_reply():
    for field, value in (('contact', {'phone_number': '+123456789', 'first_name': 'Контакт'}),
                         ('location', {'longitude': 30.5, 'latitude': 50.4}),
                         ('dice', {'emoji': '🎲', 'value': 1})):
        bid, bot, owner, _ = fresh()
        dispatch(bot, message(727, None, **{field: value}))
        assert sent_to(owner, 'forward_message')
        assert any(k.get('reply_markup') and 'p_reply_727' in k['reply_markup'].to_json()
                   for a, k in sent_to(owner))
        callback(bot, owner, 'p_reply_727')
        dispatch(bot, message(owner, 'Ответ на пересылку'))
        assert sent_to(727)


for name, fn in (
    ('01 деньги доступны только владельцу', owner_only),
    ('02 секреты скрыты в исключениях requests и журнале werkzeug', masked_log),
    ('03 личные состояния не перехватывают сообщения форума', private_state),
    ('04 исходное фото и подпись сохраняются при ответе и отмене', media_back),
    ('05 запуск сохраняет накопленные обновления', no_drop),
    ('06 старый polling не оживает после перезапуска с тем же токеном', restart_loop),
    ('07 завершение первого ответа сохраняет нового адресата', reply_race),
    ('08 остановка закрывает рабочие потоки', workers_stop),
    ('09 длинное обращение доставлено и записано в историю', long_delivery),
    ('10 недоставленное сообщение занимает очередь один раз', one_queue),
    ('11 ограничение частоты применяется ко всему альбому', album_limit),
    ('12 рассылка повторяет отправку после 429', broadcast_retry),
    ('13 отмена добавления шаблона сбрасывает состояние', cancel_template),
    ('14 шаблон 4096 символов отправляется целиком', long_template),
    ('15 список длинных шаблонов доступен', templates_list),
    ('16 возврат восстанавливает длинную историю', long_history),
    ('17 контакт, геолокация и другие пересылки получают кнопку ответа', forwarded_reply),
    ('гонка при неудачной отправке текста', lambda: reply_race(failure=True)),
    ('гонка при отправке фото', lambda: reply_race(kind='photo')),
    ('гонка при неудачной отправке фото', lambda: reply_race(kind='photo', failure=True)),
    ('гонка при отправке шаблона', lambda: reply_race(kind='template')),
    ('повторное открытие того же адресата во время отправки', lambda: reply_race(same=True)),
    ('рассылка фото повторяет отправку после 429', lambda: broadcast_retry(photo=True)),
):
    check(name, fn)

print(f'ИТОГО: {OK} OK, {FAIL} FAIL')
sys.exit(1 if FAIL else 0)
