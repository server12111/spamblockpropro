import logging
import os
import signal
import sys
import telebot
from config import TOKEN, WEBHOOK_URL, WEBHOOK_PORT, SUPER_ADMIN
from database import init_db, db_get_all_bots
from purchased_bot import launch_bot, running_bots
from handlers import register, start_subscription_checker

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
    for db_bot_id, token, admin_id, owner_id in db_get_all_bots():
        launch_bot(db_bot_id, token, admin_id, bot)
    log.info(f'Started. Loaded {len(running_bots)} purchased bot(s).')

    start_subscription_checker(bot)

    if WEBHOOK_URL:
        from flask import Flask, request, abort

        app = Flask(__name__)
        secret = TOKEN.replace(':', '_')

        @app.route(f'/{secret}', methods=['POST'])
        def webhook():
            if request.headers.get('content-type') == 'application/json':
                update = telebot.types.Update.de_json(request.get_data().decode('utf-8'))
                bot.process_new_updates([update])
                return ''
            abort(403)

        @app.route('/')
        def index():
            return 'OK'

        bot.remove_webhook()
        bot.set_webhook(url=f'{WEBHOOK_URL}/{secret}')
        log.info(f'Webhook set: {WEBHOOK_URL}/{secret}')

        app.run(host='0.0.0.0', port=WEBHOOK_PORT, threaded=True)
    else:
        log.info('Running in polling mode.')
        try:
            bot.delete_webhook(drop_pending_updates=True)
            log.info('Webhook cleared.')
        except Exception as e:
            log.warning(f'delete_webhook: {e}')

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

        bot.infinity_polling(skip_pending=False)
