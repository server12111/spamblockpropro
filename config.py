import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN               = os.getenv('BOT_TOKEN', '').strip()  # strip: защита от \r/пробелов в .env (иначе 502 на каждый запрос)
SUPER_ADMIN         = int(os.getenv('SUPER_ADMIN', '0'))
PRICE_USDT          = float(os.getenv('PRICE_USDT', '0.1'))
TON_WALLET          = os.getenv('TON_WALLET', '').strip()
CRYPTOBOT_TKN       = os.getenv('CRYPTOBOT_TKN', '').strip()
TONCENTER_KEY       = os.getenv('TONCENTER_KEY', '').strip()
ADMIN_USERNAME      = os.getenv('ADMIN_USERNAME', '').strip()
WEBHOOK_URL         = os.getenv('WEBHOOK_URL', '').strip()  # https://yourdomain.com  (без слеша в конце)
WEBHOOK_PORT        = int(os.getenv('WEBHOOK_PORT', '8443'))
WITHDRAWAL_CHANNEL  = int(os.getenv('WITHDRAWAL_CHANNEL', '0'))   # ID канала/чата для запросов на вывод
BOT_USERNAME        = os.getenv('BOT_USERNAME', '').strip()        # username главного бота (без @)
STATS_TZ_OFFSET     = int(os.getenv('FS_STATS_TZ', '3'))           # часовой пояс еженедельной статистики (UTC+3)
RATE_LIMIT_SEC      = int(os.getenv('FS_RATE_LIMIT', '3'))         # лимит отправки сообщений пользователем (сек)
