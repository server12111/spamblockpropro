import logging
import os
import signal
import sys
import time
import telebot
from datetime import datetime
from config import TOKEN, WEBHOOK_URL, WEBHOOK_PORT, SUPER_ADMIN
from database import init_db, db_get_all_bots_for_checker
from purchased_bot import launch_bot, running_bots
from handlers import register, start_subscription_checker
from forum_support import start_weekly_scheduler

# ── Логування ────────────────────────────────────────
_data_dir = os.getenv('DATA_DIR', '.')
os.makedirs(_data_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(_data_dir, 'bot.log'), encoding='utf-8'),
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

        bot.infinity_polling(skip_pending=False, timeout=60, long_polling_timeout=5)

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
                log.info(f'Webhook set: {base}/{secret}')
                break
            except Exception as e:
                log.warning(f'Set webhook attempt {attempt}/3 failed: {e}')
                time.sleep(5)

        if webhook_ok:
            app.run(host='0.0.0.0', port=WEBHOOK_PORT, threaded=True)
        else:
            log.error('Webhook не установлен — переключаюсь в polling mode '
                      '(webhook можно включить перезапуском позже).')
            _run_polling()
    else:
        _run_polling()
