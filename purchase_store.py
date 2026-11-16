"""Транзакции покупок: счёт, начисления и право настройки сохраняются вместе"""
from datetime import datetime, timedelta
import math
import json
import time

def _conn():
    import database
    return database._conn()


def purchase_context(value):
    value = value if isinstance(value, dict) else {}
    target = value.get('bot_id') if value.get('step') == 'renewing' or value.get('renewing') else None
    if value.get('receipt', {}).get('setup_grant'): target = None
    return target, value.get('via_bot_id')


def db_set_pending(user_id, invoice_id, *, context=None, amount=None,
                   expires_at=None, provider='crypto', expected_ton=None):
    target, via = purchase_context(context)
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        if target: require_renewable(c, target, user_id)
        c.execute('INSERT OR IGNORE INTO payment_orders '
                  '(provider, invoice_id, user_id, amount, target_bot_id, via_bot_id, expires_at, expected_ton) '
                  'VALUES (?,?,?,?,?,?,?,?)',
                  (provider, str(invoice_id), user_id, amount, target, via, expires_at, expected_ton))
        if provider == 'crypto':
            c.execute('INSERT OR REPLACE INTO pending_payments (user_id, invoice_id) VALUES (?,?)',
                      (user_id, invoice_id))
        c.commit()
    finally:
        c.close()


def db_del_pending(user_id):
    """Удаляет только старый указатель, действующие счета остаются в журнале"""
    c = _conn()
    c.execute('DELETE FROM pending_payments WHERE user_id=?', (user_id,))
    c.commit(); c.close()


def db_get_payment(user_id, invoice_id, provider='crypto'):
    c = _conn()
    row = c.execute('SELECT amount, target_bot_id, via_bot_id, expires_at, settled, expected_ton, receipt '
                    'FROM payment_orders WHERE provider=? AND invoice_id=? AND user_id=?',
                    (provider, str(invoice_id), user_id)).fetchone()
    c.close()
    if not row:
        return None
    result = dict(zip(('amount', 'bot_id', 'via_bot_id', 'expires_at', 'settled', 'ton', 'receipt'), row))
    result['receipt'] = json.loads(result['receipt'] or '{}')
    return result


def db_get_pending(user_id):
    c = _conn()
    row = c.execute("SELECT invoice_id FROM payment_orders WHERE user_id=? AND provider='crypto' "
                    'AND settled=0 ORDER BY rowid DESC LIMIT 1', (user_id,)).fetchone()
    c.close()
    return int(row[0]) if row else None


def _sync_grants(c, user_id):
    row = c.execute('SELECT days FROM setup_grants WHERE user_id=? ORDER BY id LIMIT 1',
                    (user_id,)).fetchone()
    if row:
        c.execute('INSERT OR IGNORE INTO paid_setups (user_id) VALUES (?)', (user_id,))
        c.execute('INSERT OR REPLACE INTO bot_grants (user_id, days) VALUES (?,?)', (user_id, row[0]))
    else:
        c.execute('DELETE FROM paid_setups WHERE user_id=?', (user_id,))
        c.execute('DELETE FROM bot_grants WHERE user_id=?', (user_id,))


def _grant(c, user_id, days):
    c.execute('INSERT INTO setup_grants (user_id, days) VALUES (?,?)', (user_id, max(int(days), 1)))
    _sync_grants(c, user_id)


def require_renewable(c, bot_id, user_id=None):
    row = c.execute('SELECT expires_at, deleted, owner_id, admin_id FROM bots WHERE id=?', (bot_id,)).fetchone()
    if not row: raise ValueError('Бот не найден')
    if row[1]: raise ValueError('Бот удалён, продление недоступно')
    if user_id is not None and user_id not in row[2:]: raise ValueError('Этот бот тебе недоступен')
    return row


def renewal_error(bot_id, user_id=None):
    c = _conn()
    try:
        require_renewable(c, bot_id, user_id)
    except ValueError as exc:
        return str(exc)
    finally: c.close()


def renew_in_transaction(c, bot_id, days):
    row = require_renewable(c, bot_id)
    try:
        current = datetime.fromisoformat(str(row[0]))
    except (ValueError, TypeError):
        current = datetime.now()
    expires = max(current, datetime.now()) + timedelta(days=days)
    c.execute('UPDATE bots SET expires_at=?, active=1 WHERE id=?', (expires.isoformat(), bot_id))
    return expires


def _apply_purchase(c, user_id, days, target=None, via=None, amount=None, paid_order=False):
    receipt = {}
    if target:
        if paid_order:
            row = c.execute('SELECT deleted FROM bots WHERE id=?', (target,)).fetchone()
            if not row or row[0]:
                _grant(c, user_id, days)
                receipt['setup_grant'] = True
            else:
                # Доступ проверен при привязке счёта, передача роли не отменяет оплату
                renew_in_transaction(c, target, days)
        else:
            require_renewable(c, target, user_id)
            renew_in_transaction(c, target, days)
    else:
        _grant(c, user_id, days)
    if amount is None:
        return receipt
    amount = float(amount)
    if not math.isfinite(amount) or amount <= 0:
        raise ValueError('Invalid payment amount')
    if via:
        row = c.execute('SELECT b.owner_id FROM bots b JOIN bot_marketplace m ON m.bot_id=b.id '
                        'WHERE b.id=? AND m.enabled=1 AND b.active=1', (via,)).fetchone()
        if row and row[0] != user_id:
            commission = round(amount * 0.5, 4)
            receipt.update(commission=commission, commission_owner=row[0])
            c.execute('INSERT INTO bot_earnings (bot_id, balance_usdt, total_earned) VALUES (?,?,?) '
                      'ON CONFLICT(bot_id) DO UPDATE SET balance_usdt=balance_usdt+excluded.balance_usdt, '
                      'total_earned=total_earned+excluded.total_earned', (via, commission, commission))
    ref = c.execute('SELECT referrer_id FROM referrals WHERE referred_id=? AND paid=0',
                    (user_id,)).fetchone()
    if ref:
        receipt['referrer'] = ref[0]
        c.execute('UPDATE referrals SET paid=1 WHERE referred_id=?', (user_id,))
        c.execute('INSERT INTO referral_discounts (user_id, discount_count) VALUES (?,1) '
                  'ON CONFLICT(user_id) DO UPDATE SET discount_count=discount_count+1', (ref[0],))
    return receipt


def db_claim_pending_payment(user_id, invoice_id, *, paid_amount=None, provider='crypto', tx_hash=None, paid_at=None):
    """Зачёт и начисления неделимы; повтор после сбоя безопасен"""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        row = c.execute('SELECT amount, target_bot_id, via_bot_id, settled, expires_at FROM payment_orders '
                        'WHERE provider=? AND invoice_id=? AND user_id=?',
                        (provider, str(invoice_id), user_id)).fetchone()
        if not row or row[3]:
            return False
        if provider == 'ton' and (row[4] is None or paid_at is not None and not row[4] - 7200 <= paid_at <= row[4]):
            return False
        if tx_hash and not c.execute('INSERT OR IGNORE INTO used_txs (tx_hash, user_id) VALUES (?,?)',
                                     (tx_hash, user_id)).rowcount:
            return False
        amount = paid_amount if paid_amount is not None else row[0]
        receipt = _apply_purchase(c, user_id, 30, target=row[1], via=row[2], amount=amount, paid_order=True)
        c.execute('UPDATE payment_orders SET settled=1, receipt=? WHERE provider=? AND invoice_id=?',
                  (json.dumps(receipt), provider, str(invoice_id)))
        if provider == 'crypto':
            c.execute('DELETE FROM pending_payments WHERE user_id=? AND invoice_id=?', (user_id, invoice_id))
        c.commit()
        return True
    finally:
        c.close()


def claim_ton_transfer(user_id, code, found):
    order = db_get_payment(user_id, code, 'ton')
    if not order or not found: return False
    amount = order['amount']
    if isinstance(found, dict):
        if amount is not None: amount *= found['ton_amount'] / order['ton']
        tx_hash, paid_at = found['tx_hash'], found.get('utime', time.time())
    else:
        tx_hash, paid_at = found, time.time()
    return db_claim_pending_payment(user_id, code, provider='ton', tx_hash=tx_hash,
                                    paid_amount=amount, paid_at=paid_at)


def db_use_purchase_discount(user_id, context=None):
    target, _ = purchase_context(context)
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        if not target and c.execute('SELECT 1 FROM paid_setups WHERE user_id=?', (user_id,)).fetchone():
            return False
        if not c.execute('UPDATE referral_discounts SET discount_count=discount_count-1 '
                         'WHERE user_id=? AND discount_count>0', (user_id,)).rowcount:
            return False
        _apply_purchase(c, user_id, 10, target=target)
        c.commit()
        return True
    finally:
        c.close()


def db_activate_trial(user_id):
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        if c.execute('SELECT 1 FROM paid_setups WHERE user_id=?', (user_id,)).fetchone():
            return False
        if not c.execute('INSERT OR IGNORE INTO trial_used (user_id) VALUES (?)', (user_id,)).rowcount:
            return False
        _grant(c, user_id, 3)
        c.commit()
        return True
    finally:
        c.close()


def db_claim_coupon(user_id):
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        row = c.execute('SELECT days FROM free_coupons WHERE user_id=? AND used=0', (user_id,)).fetchone()
        if not row:
            return None
        c.execute('UPDATE free_coupons SET used=1 WHERE user_id=? AND used=0', (user_id,))
        _grant(c, user_id, row[0])
        c.commit()
        return row[0]
    finally:
        c.close()


def db_mark_paid(user_id, days=30):
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        _grant(c, user_id, days)
        c.commit()
    finally:
        c.close()


def db_peek_grant_days(user_id):
    c = _conn()
    row = c.execute('SELECT days FROM setup_grants WHERE user_id=? ORDER BY id LIMIT 1', (user_id,)).fetchone()
    c.close()
    return row[0] if row else None


def _take_grant(c, user_id):
    row = c.execute('SELECT id, days FROM setup_grants WHERE user_id=? ORDER BY id LIMIT 1', (user_id,)).fetchone()
    if row:
        c.execute('DELETE FROM setup_grants WHERE id=?', (row[0],))
    _sync_grants(c, user_id)
    return row[1] if row else None


def db_take_grant_days(user_id):
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        days = _take_grant(c, user_id)
        c.commit()
        return days
    finally:
        c.close()


def db_is_paid(user_id):
    return db_peek_grant_days(user_id) is not None


def db_clear_paid(user_id):
    c = _conn()
    _sync_grants(c, user_id)
    c.commit(); c.close()


def db_register_paid_bot(user_id, token, admin_id, *, revision=None):
    """Создание бота и расход одного гранта неделимы, запуск выполняется после"""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        if revision is not None and not c.execute(
                'DELETE FROM states WHERE bot_id=0 AND user_id=? AND revision=?',
                (user_id, revision)).rowcount:
            return None
        from database import ensure_bot_identity
        ensure_bot_identity(c, token)
        days = _take_grant(c, user_id)
        if days is None:
            raise ValueError('Setup grant missing')
        expires = (datetime.now() + timedelta(days=days)).isoformat()
        cur = c.execute('INSERT INTO bots (owner_id, token, admin_id, expires_at) VALUES (?,?,?,?)',
                        (user_id, token, admin_id, expires))
        bot_id = cur.lastrowid
        if revision is None:
            c.execute('DELETE FROM states WHERE bot_id=0 AND user_id=?', (user_id,))
        c.commit()
        return bot_id
    finally:
        c.close()
