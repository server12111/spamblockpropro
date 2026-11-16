"""Круг 5б: реальные обработчики и таймеры, временная SQLite, Telegram без сети"""
import importlib.util
import sys
import threading
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('r5b_fixture', ROOT / 'tests/fix_r2.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
q, db, pb, h = r.q, r.db, r.pb, r.h
import service_broadcast as svc


def group_message(uid, text):
    return q.message(-5000, 50, text, **{'from': dict(id=uid, is_bot=False, first_name='Имя')})


def click_at(bot, cb):
    for handler in bot.callback_query_handlers:
        if bot._test_message_handler(handler, cb):
            return handler['function'](cb)


def private_reply():
    main = r.mainbot()
    q.click(main, 900, 'admin_reply_5101')
    saved = db.DBState(0).get(900)
    start = len(q.API)
    q.dispatch(main, group_message(900, 'Чужая переписка'))
    assert not any(n == 'send_message' and a[0] == 5101 for n, a, _, _ in q.API[start:]), 'групповой текст доставлен адресату'
    assert db.DBState(0).get(900) == saved, 'групповой ввод сбросил ожидание ответа'
    q.dispatch(main, q.message(900, 51, 'Личный ответ'))
    assert any(n == 'send_message' and a[0] == 5101 and 'Личный ответ' in a[1] for n, a, _, _ in q.API[start:])
    assert db.DBState(0).get(900) is None


def preserve_request(cancel=False):
    main = r.mainbot()
    uid = 5102 + int(cancel)
    q.click(main, uid, 'user_send')
    start = len(q.SENT)
    q.dispatch(main, q.message(uid, 52, 'Текст обращения'))
    request = next(s for s in q.SENT[start:] if s[0] == 'send_message' and s[1][0] == 900)
    cb = q.callback(900, f'admin_reply_{uid}')
    cb.message = q.message(900, request[3], request[1][1])
    start = len(q.API)
    sent_start = len(q.SENT)
    click_at(main, cb)
    prompts = [s for s in q.SENT[sent_start:] if s[0] == 'send_message' and s[1][0] == 900 and 'Введи ответ' in s[1][1]]
    if cancel and prompts:
        cb = q.callback(900, 'cancel')
        cb.message = q.message(900, prompts[-1][3], prompts[-1][1][1])
        click_at(main, cb)
        assert db.DBState(0).get(900) is None
        assert any(n == 'delete_message' and a[:2] == (900, prompts[-1][3]) for n, a, _, _ in q.API[start:])
    changed = any(
        n == 'edit_message_text' and a[1:3] == (900, request[3])
        or n == 'delete_message' and a[:2] == (900, request[3])
        for n, a, k, _ in q.API[start:])
    assert not changed, 'исходное обращение изменено или удалено'
    assert prompts, 'нет отдельного приглашения для ответа'


def group_dialogs():
    main = r.mainbot()
    for uid, value, text in (
        (900, 'await_new_price', '19'),
        (900, 'await_coupon_input', '5199 30'),
        (900, 'await_free_renew_input', '1 30'),
        (900, 'bcast_any_all', 'Групповая рассылка'),
        (5104, 'await_user_msg', 'Групповое обращение'),
    ):
        db.DBState(0)[uid] = value
        start = len(q.API)
        q.dispatch(main, group_message(uid, text))
        assert db.DBState(0).get(uid) == value, value
        assert not any(n in ('send_message', 'edit_message_text') for n, _, _, _ in q.API[start:]), value
        # /start из группы также не отменяет личный диалог
        q.dispatch(main, group_message(uid, '/start'))
        assert db.DBState(0).get(uid) == value, '/start сбросил личный диалог'
    db.DBState(0).pop(900, None)
    db.DBState(0).pop(5104, None)


def group_callbacks():
    main = r.mainbot()
    db.DBState(0)[900] = 'await_admin_msg_5101'
    for data in ('admin_reply_5102', 'broadcast_all', 'set_price', 'cancel', 'back_to_start'):
        cb = q.callback(900, data)
        cb.message = group_message(900, 'Кнопка из группы')
        start = len(q.API)
        click_at(main, cb)
        assert db.DBState(0).get(900) == 'await_admin_msg_5101', data
        assert len(q.API) == start, data
    db.DBState(0).pop(900, None)


def album_retry(kind):
    main = r.mainbot()
    q.click(main, 900, 'broadcast_all')
    with patch.object(main, 'get_file', return_value=r.NS(file_path='photo.jpg')), \
         patch.object(main, 'download_file', side_effect=q.error(400, 'file is too big')):
        q.dispatch(main, q.photo(900, 60, 'failed-album'))
        jid = db.DBState(0)[900]['job_id']
        timer = svc._timers[jid]
        timer.join(4)
        assert not timer.is_alive(), 'таймер завис'
    assert svc.job(jid)['status'] == 'failed'
    assert db.DBState(0).get(900) == 'bcast_any_all', 'состояние осталось привязано к старому альбому'
    with patch.object(main, 'get_file', return_value=r.NS(file_path='photo.jpg')), \
         patch.object(main, 'download_file', return_value=b'new photo'), \
         patch.object(main, 'send_media_group', side_effect=lambda uid, media:
                      [q.photo(uid, 100 + i) for i in range(len(media))]):
        if kind == 'text':
            q.dispatch(main, q.message(900, 61, 'Повторная рассылка'))
        else:
            q.dispatch(main, q.photo(900, 61, 'new-album'))
            q.dispatch(main, q.photo(900, 62, 'new-album'))
            new_jid = db.DBState(0)[900]['job_id']
            svc._timers[new_jid].join(4)
        current = db.DBState(0)[900]
        assert current['job_id'] != jid
        job = svc.job(current['job_id'])
        assert job['scope'] == 'all' and job['status'] == 'preview'
        assert [m['message_id'] for m in job['payload']] == ([61] if kind == 'text' else [61, 62])
        q.click(main, 900, f"svc_cancel_{current['job_id']}")


def stale_album_failure(action):
    main = r.mainbot()
    q.click(main, 900, 'broadcast_all')
    entered, release = threading.Event(), threading.Event()
    def download(*args):
        entered.set()
        assert release.wait(3)
        raise q.error(400, 'file is too big')
    with patch.object(main, 'get_file', return_value=r.NS(file_path='photo.jpg')), \
         patch.object(main, 'download_file', side_effect=download):
        q.dispatch(main, q.photo(900, 70, 'late-' + action))
        jid = db.DBState(0)[900]['job_id']
        timer = svc._timers[jid]
        try:
            assert entered.wait(3)
            q.click(main, 900, action)
            saved = db.DBState(0).get(900)
        finally:
            release.set()
            timer.join(4)
    assert not timer.is_alive()
    assert svc.job(jid)['status'] == 'failed'
    assert db.DBState(0).get(900) == saved, 'ошибка восстановила отменённый или заменила новый диалог'


def notification_failure():
    main = r.mainbot()
    q.click(main, 900, 'broadcast')
    send = main.send_message
    def notify(chat, text, **kwargs):
        if 'Не удалось подготовить рассылку' in text:
            raise q.error(403, 'bot was blocked by the user')
        return send(chat, text, **kwargs)
    with patch.object(main, 'get_file', side_effect=q.error(400, 'file is too big')), \
         patch.object(main, 'send_message', side_effect=notify):
        try:
            q.dispatch(main, q.photo(900, 80))
        except q.ApiTelegramException:
            pass
    assert db.DBState(0).get(900) == 'bcast_any_mine', 'ошибка уведомления помешала повторному вводу'


if __name__ == '__main__':
    try:
        with patch.multiple(q.telebot.TeleBot, **{n: q.fake_api(n) for n in r.METHODS}), \
             patch.object(svc, 'resume'):
            for name, fn in [
                ('Личный ответ не захватывает группу и сохраняет ожидание', private_reply),
                ('Ответ сохраняет исходное обращение', preserve_request),
                ('Отмена удаляет приглашение, сохраняя обращение', lambda: preserve_request(True)),
                ('Все личные диалоги игнорируют сообщения и команды группы', group_dialogs),
                ('Групповые кнопки не меняют личный диалог', group_callbacks),
                ('После ошибки таймера принимается повторный текст', lambda: album_retry('text')),
                ('После ошибки таймера принимается новый альбом', lambda: album_retry('album')),
                ('Ошибка таймера не отменяет отмену', lambda: stale_album_failure('back_to_start')),
                ('Ошибка таймера не меняет новый диалог', lambda: stale_album_failure('admin_reply_5101')),
                ('Сбой уведомления не блокирует повторный ввод', notification_failure),
            ]:
                r.check(name, fn)
    finally:
        for timer in list(svc._timers.values()):
            timer.cancel()
            timer.join(4)
        for bot in q.BOTS:
            pb._close_bot(bot)
        db.db_close_all()
    print(f'ИТОГО: {r.OK} OK, {r.FAIL} FAIL')
    sys.exit(bool(r.FAIL))
