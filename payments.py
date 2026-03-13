import requests
import time
import threading
import logging
from config import CRYPTOBOT_TKN, TONCENTER_KEY, TON_WALLET
from database import get_price, db_is_tx_used, db_mark_tx_used

log = logging.getLogger(__name__)

# ═══════════════════════════════════════
#  CRYPTOBOT API
# ═══════════════════════════════════════
def cb_create_invoice(user_id):
    if not CRYPTOBOT_TKN: return None
    try:
        r = requests.post('https://pay.crypt.bot/api/createInvoice',
            json={'asset': 'USDT', 'amount': str(get_price()),
                  'description': f'SpamBot #{user_id}', 'expires_in': 3600},
            headers={'Crypto-Pay-API-Token': CRYPTOBOT_TKN}, timeout=10)
        d = r.json()
        return d['result'] if d.get('ok') else None
    except Exception as e:
        log.error(f'cb_create_invoice error: {e}')
        return None

def cb_check_invoice(invoice_id):
    if not CRYPTOBOT_TKN: return None
    try:
        r = requests.get('https://pay.crypt.bot/api/getInvoices',
            params={'invoice_ids': str(invoice_id), 'status': 'paid'},
            headers={'Crypto-Pay-API-Token': CRYPTOBOT_TKN}, timeout=10)
        d = r.json()
        items = d.get('result', {}).get('items', [])
        if items:
            return items[0]
        r2 = requests.get('https://pay.crypt.bot/api/getInvoices',
            params={'invoice_ids': str(invoice_id)},
            headers={'Crypto-Pay-API-Token': CRYPTOBOT_TKN}, timeout=10)
        d2 = r2.json()
        items2 = d2.get('result', {}).get('items', [])
        return items2[0] if items2 else None
    except Exception as e:
        log.error(f'cb_check_invoice error: {e}')
        return None

# ═══════════════════════════════════════
#  TON
# ═══════════════════════════════════════
_ton_check_lock = threading.Lock()  # захист від race condition при одночасній перевірці

def get_ton_price_usd() -> float:
    try:
        r = requests.get('https://pay.crypt.bot/api/getExchangeRates',
            headers={'Crypto-Pay-API-Token': CRYPTOBOT_TKN}, timeout=10)
        d = r.json()
        if d.get('ok'):
            for rate in d['result']:
                if rate.get('source') == 'TON' and rate.get('target') == 'USD':
                    return float(rate['rate'])
    except Exception as e:
        log.error(f'get_ton_price_usd error: {e}')
    return 0.0

def get_ton_amount() -> float:
    price = get_ton_price_usd()
    if price <= 0: return 0.0
    return round(get_price() / price, 4)

def ton_payment_link(ton_amount: float, payment_code: str) -> str:
    nanotons = int(ton_amount * 1_000_000_000)
    return f"https://app.tonkeeper.com/transfer/{TON_WALLET}?amount={nanotons}&text={payment_code}"

def ton_check_transfer(user_id: int, expected_ton: float, payment_code: str) -> bool:
    # Lock гарантує що два одночасних кліки "Перевірити" не зарахують одну транзакцію двічі
    with _ton_check_lock:
        try:
            r = requests.get('https://toncenter.com/api/v2/getTransactions', params={
                'address': TON_WALLET,
                'limit':   50,
                'api_key': TONCENTER_KEY,
            }, timeout=15)
            data = r.json()
            if not data.get('ok'): return False

            now      = time.time()
            min_nano = int(expected_ton * 1_000_000_000 * 0.95)

            for tx in data.get('result', []):
                if now - tx.get('utime', 0) > 7200: continue
                in_msg  = tx.get('in_msg', {})
                value   = int(in_msg.get('value', 0))
                if value < min_nano: continue
                comment = in_msg.get('message', '')
                if payment_code not in comment: continue
                tx_hash = tx.get('transaction_id', {}).get('hash', '')
                if not tx_hash or db_is_tx_used(tx_hash): continue
                db_mark_tx_used(tx_hash, user_id)
                return True
            return False
        except Exception as e:
            log.error(f'ton_check_transfer error: {e}')
            return False
