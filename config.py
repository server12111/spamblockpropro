import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN          = os.getenv('BOT_TOKEN', '')
SUPER_ADMIN    = int(os.getenv('SUPER_ADMIN', '0'))
PRICE_USDT     = float(os.getenv('PRICE_USDT', '0.1'))
TON_WALLET     = os.getenv('TON_WALLET', '')
CRYPTOBOT_TKN  = os.getenv('CRYPTOBOT_TKN', '')
TONCENTER_KEY  = os.getenv('TONCENTER_KEY', '')
ADMIN_USERNAME = os.getenv('ADMIN_USERNAME', '')
WEBHOOK_URL    = os.getenv('WEBHOOK_URL', '')       # https://yourdomain.com  (без слеша в конце)
WEBHOOK_PORT   = int(os.getenv('WEBHOOK_PORT', '8443'))
