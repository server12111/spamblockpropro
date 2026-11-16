import requests
import safe_logging
import database as db
from purchase_store import db_get_payment, db_claim_pending_payment, claim_ton_transfer
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

def get_ton_amount(quoted_price=None) -> float:
    quoted_price = get_price() if quoted_price is None else quoted_price
    price = get_ton_price_usd()
    if price <= 0: return 0.0
    return round(quoted_price / price, 4)

def ton_payment_link(ton_amount: float, payment_code: str) -> str:
    nanotons = int(ton_amount * 1_000_000_000)
    return f"https://app.tonkeeper.com/transfer/{TON_WALLET}?amount={nanotons}&text={payment_code}"

def ton_check_transfer(user_id: int, expected_ton: float, payment_code: str, *, return_tx=False, expires_at=None):
    # Lock гарантує що два одночасних кліки "Перевірити" не зарахують одну транзакцію двічі
    with _ton_check_lock:
        try:
            # Срок ограничивает дату перевода; оплата вовремя доступна и после перезапуска
            end = expires_at if expires_at is not None else time.time()
            start = end - 7200
            min_nano = int(expected_ton * 1_000_000_000 * 0.95)
            for tx in _ton_transactions(start):
                if not start <= tx.get('utime', 0) <= end: continue
                in_msg  = tx.get('in_msg', {})
                value   = int(in_msg.get('value', 0))
                if value < min_nano: continue
                comment = in_msg.get('message', '')
                # ТОЧНОЕ совпадение (после strip): подстрока 'SPB123' была бы
                # внутри 'SPB123SPB456' и засчитала бы чужую транзакцию.
                # Инструкция юзеру: комментарий = только код
                if comment.strip() != payment_code: continue
                tx_hash = tx.get('transaction_id', {}).get('hash', '')
                if not tx_hash or db_is_tx_used(tx_hash): continue
                if return_tx:
                    return {'tx_hash': tx_hash, 'ton_amount': value / 1_000_000_000, 'utime': tx['utime']}
                db_mark_tx_used(tx_hash, user_id)
                return True
            return False
        except Exception as e:
            log.error(f'ton_check_transfer error: {e}')
            return False


def _ton_transactions(start):
    params = dict(address=TON_WALLET, limit=50, api_key=TONCENTER_KEY, archival=True)
    cursors = set()
    while True:
        data = requests.get('https://toncenter.com/api/v2/getTransactions', params=dict(params), timeout=15).json()
        if not data.get('ok'):
            return
        rows = data.get('result') or []
        for tx in rows:
            yield tx
        if not rows or min(tx.get('utime', 0) for tx in rows) < start:
            return
        cursor = data.get('previous_transaction') or rows[-1].get('transaction_id', {})
        key = (cursor.get('lt'), cursor.get('hash'))
        if not all(key) or key in cursors or (len(rows) < 50 and not data.get('previous_transaction')):
            return
        cursors.add(key)
        params.update(lt=key[0], hash=key[1])


def reconcile_payments(main_bot):
    """Проверка всех сохранённых счетов независимо от текущего экрана пользователя"""
    c = db._conn()
    try:
        rows = c.execute('SELECT provider,invoice_id,user_id FROM payment_orders '
                         'WHERE settled=0 AND next_check<=? ORDER BY next_check LIMIT 100', (time.time(),)).fetchall()
    finally: c.close()
    count = 0
    for provider, invoice_id, uid in rows:
        paid = False
        try:
            order = db_get_payment(uid, invoice_id, provider)
            if provider == 'crypto':
                invoice = cb_check_invoice(invoice_id)
                if invoice and invoice.get('status') == 'paid':
                    paid = db_claim_pending_payment(uid, invoice_id, paid_amount=invoice.get('amount') or order['amount'])
            else:
                found = ton_check_transfer(uid, order['ton'], invoice_id, return_tx=True, expires_at=order['expires_at'])
                if found:
                    paid = claim_ton_transfer(uid, invoice_id, found)
            if paid:
                count += 1
                order = db_get_payment(uid, invoice_id, provider)
                if order['receipt'].get('setup_grant'): order['bot_id'] = None
                from keyboards import TE_CHECK, _btn
                from telebot.types import InlineKeyboardMarkup
                kb = InlineKeyboardMarkup()
                kb.add(_btn('Купить бота', icon='💎', callback_data='buy_bot'))
                if order['bot_id']:
                    from purchased_bot import launch_bot, _lifecycle_lock
                    with _lifecycle_lock:
                        info = db.db_get_bot_info(order['bot_id'])
                        if info: launch_bot(order['bot_id'], info[0], info[1], main_bot)
                try:
                    main_bot.send_message(uid, f'{TE_CHECK} Оплата зачтена' if order['bot_id'] else
                        f'{TE_CHECK} Оплата зачтена, можно подключить бота', parse_mode='HTML',
                        reply_markup=None if order['bot_id'] else kb)
                except Exception: pass
        except Exception:
            log.exception('Не удалось проверить сохранённый счёт')
        finally:
            c = db._conn()
            try:
                c.execute('UPDATE payment_orders SET next_check=? WHERE provider=? AND invoice_id=?',
                          (time.time() + (60 if paid or provider == 'crypto' else 300), provider, invoice_id))
                c.commit()
            finally: c.close()
    return count


def start_payment_checker(main_bot):
    def run():
        while True:
            try: reconcile_payments(main_bot)
            except Exception: log.exception('Проверка сохранённых счетов прервана')
            time.sleep(30)
    threading.Thread(target=run, name='payment-checker', daemon=True).start()
