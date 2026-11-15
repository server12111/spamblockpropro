import logging
import logging.handlers
import os
import signal
import sys
import time
import threading
import telebot
from datetime import datetime
from config import TOKEN, WEBHOOK_URL, WEBHOOK_PORT, SUPER_ADMIN
from database import init_db, db_get_all_bots_for_checker, DB_PATH
from purchased_bot import launch_bot, running_bots
from handlers import register, start_subscription_checker
from forum_support import start_weekly_scheduler

# ── Логирование с ротацией: без неё bot.log за месяцы забьёт диск ──
_data_dir = os.getenv('DATA_DIR', '.')
os.makedirs(_data_dir, exist_ok=True)
_log_file = os.path.join(_data_dir, 'bot.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.handlers.RotatingFileHandler(_log_file, maxBytes=5 * 1024 * 1024,
                                             backupCount=3, encoding='utf-8'),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)

bot = telebot.TeleBot(TOKEN)

if __name__ == '__main__':
    log.info(f'Config: TOKEN={TOKEN[:10]}... SUPER_ADMIN={SUPER_ADMIN} DATA_DIR={_data_dir}')
    init_db()
    register(bot)

    # Прогреваем соединение и кэшируем bot.get_me() ДО запуска купленных ботов,
    # пока их фоновые потоки ещё не заняли сетевые соединения long polling'ом.
    try:
        bot.get_me()
        if not WEBHOOK_URL:
            bot.delete_webhook(drop_pending_updates=True, timeout=10)
            log.info('Webhook cleared.')
    except Exception as e:
        log.warning(f'Pre-warm (get_me/delete_webhook) failed: {e}')

    now = datetime.now()
    skipped_expired = 0
    for db_bot_id, owner_id, token, admin_id, expires_at in db_get_all_bots_for_checker():
        if expires_at:
            try:
                if datetime.fromisoformat(str(expires_at)) <= now:
                    skipped_expired += 1
                    continue
            except Exception:
                pass
        launch_bot(db_bot_id, token, admin_id, bot)
        time.sleep(0.3)  # разносим старты polling-потоков, чтобы не бить залпом
    log.info(f'Started. Loaded {len(running_bots)} purchased bot(s), skipped {skipped_expired} expired.')

    start_subscription_checker(bot)

    # Единый планировщик еженедельной статистики форум-групп (один поток на все боты)
    start_weekly_scheduler()

    # ── Watchdog: мёртвый выключатель для долгой автономной работы ──
    # Каждые 30 минут проверяет доступность БД свежим (некэшированным)
    # соединением. 3 неудачи подряд -> жёсткий выход: хостинг перезапустит
    # контейнер. Так "зависший" процесс не молчит неделями, а перезапускается.
    def _start_watchdog():
        import sqlite3 as _sq

        def _probe():
            c = _sq.connect(DB_PATH, timeout=5)
            try:
                c.execute('SELECT 1')
            finally:
                c.close()

        def _loop():
            fails = 0
            heartbeat = os.path.join(_data_dir, 'heartbeat.txt')
            while True:
                time.sleep(1800)
                try:
                    _probe()
                    fails = 0
                    try:
                        with open(heartbeat, 'w') as f:
                            f.write(str(int(time.time())))
                    except Exception:
                        pass
                except Exception as e:
                    fails += 1
                    log.warning(f'Watchdog: проба БД не прошла ({fails}/3): {e}')
                    if fails >= 3:
                        log.critical('Watchdog: БД недоступна 3 пробы подряд — '
                                     'аварийный перезапуск процесса')
                        os._exit(1)

        threading.Thread(target=_loop, name='fs-watchdog', daemon=True).start()
        log.info('Watchdog запущен (проба БД каждые 30 минут).')

    _start_watchdog()

    def _run_polling():
        log.info('Running in polling mode.')

        def _shutdown(signum, frame):
            log.info(f'Signal {signum} received, stopping...')
            bot.stop_polling()
            for tok, b in list(running_bots.items()):
                try:
                    b.stop_polling()
                except Exception:
                    pass
            sys.exit(0)

        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)

        bot.infinity_polling(skip_pending=False, timeout=60, long_polling_timeout=5,
                         allowed_updates=['message', 'callback_query', 'message_reaction'])

    if WEBHOOK_URL:
        from urllib.parse import urlparse
        from flask import Flask, request, abort

        app = Flask(__name__)
        secret = TOKEN.replace(':', '_')

        def _telegram_webhook():
            if request.headers.get('content-type') == 'application/json':
                update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
                bot.process_new_updates([update])
                return ''
            abort(403)

        # Хостинг может проксировать домен с префиксом пути (например
        # https://host.bothost.tech/webhook -> контейнер). Регистрируем
        # POST-маршруты: прямой путь и catch-all, путь должен заканчиваться секретом.
        def _webhook_prefixed(_sub=None):
            if not (_sub or '').endswith(secret):
                abort(404)
            return _telegram_webhook()

        app.add_url_rule(f'/{secret}', 'hook_exact',
                         lambda: _webhook_prefixed(secret), methods=['POST'])
        app.add_url_rule('/<path:_sub>', 'hook_prefixed',
                         _webhook_prefixed, methods=['POST'])

        @app.route('/')
        def index():
            return 'OK'

        # Установка webhook с ретраями: сбой Telegram (502 и т.п.) не должен
        # ронять процесс в crash-loop — при неудаче откатываемся в polling
        base = WEBHOOK_URL.rstrip('/')
        webhook_ok = False
        for attempt in range(1, 4):
            try:
                bot.set_webhook(url=f'{base}/{secret}')
                webhook_ok = True
                # секрет (производный от токена) в лог не пишем
                log.info(f'Webhook set: {base}/***')
                break
            except Exception as e:
                log.warning(f'Set webhook attempt {attempt}/3 failed: {e}')
                time.sleep(5)

        if webhook_ok:
            try:
                app.run(host='0.0.0.0', port=WEBHOOK_PORT, threaded=True)
            except OSError as e:
                # порт занят (зомби-процесс и т.п.) — webhook всё равно не
                # примет апдейты: снимаем его и уходим в polling, бот живёт
                log.error(f'Порт {WEBHOOK_PORT} недоступен ({e}) — fallback в polling')
                try:
                    bot.delete_webhook()
                except Exception:
                    pass
                _run_polling()
        else:
            log.error('Webhook не установлен — переключаюсь в polling mode '
                      '(webhook можно включить перезапуском позже).')
            _run_polling()
    else:
        _run_polling()
