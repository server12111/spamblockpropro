import sqlite3
import json
from datetime import datetime, timedelta
from config import PRICE_USDT

DB_PATH = 'spambots.db'

def _conn():
    return sqlite3.connect(DB_PATH)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS bots (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id   INTEGER NOT NULL,
            token      TEXT    UNIQUE NOT NULL,
            admin_id   INTEGER NOT NULL,
            username   TEXT    DEFAULT '',
            active     INTEGER DEFAULT 1,
            expires_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS bot_admins (
            bot_id   INTEGER NOT NULL,
            admin_id INTEGER NOT NULL,
            PRIMARY KEY (bot_id, admin_id)
        );
        CREATE TABLE IF NOT EXISTS bot_users (
            bot_id  INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS used_txs (
            tx_hash    TEXT PRIMARY KEY,
            user_id    INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS states (
            bot_id  INTEGER NOT NULL DEFAULT 0,
            user_id INTEGER NOT NULL,
            state   TEXT    NOT NULL,
            PRIMARY KEY (bot_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS pending_payments (
            user_id    INTEGER PRIMARY KEY,
            invoice_id INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS blocked_users (
            bot_id  INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS bot_settings (
            bot_id  INTEGER NOT NULL,
            key     TEXT    NOT NULL,
            value   TEXT    NOT NULL,
            PRIMARY KEY (bot_id, key)
        );
        CREATE TABLE IF NOT EXISTS paid_setups (
            user_id INTEGER PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS bot_templates (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id     INTEGER NOT NULL,
            text       TEXT    NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS trial_used (
            user_id    INTEGER PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS referrals (
            referrer_id INTEGER NOT NULL,
            referred_id INTEGER NOT NULL PRIMARY KEY,
            paid        INTEGER DEFAULT 0,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS referral_discounts (
            user_id        INTEGER PRIMARY KEY,
            discount_count INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS bot_messages (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id    INTEGER NOT NULL,
            user_id   INTEGER NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    conn.commit()
    # Миграции для старых схем
    for stmt in [
        'ALTER TABLE bots ADD COLUMN username TEXT DEFAULT ""',
        'ALTER TABLE bots ADD COLUMN expires_at TIMESTAMP',
    ]:
        try:
            conn.execute(stmt); conn.commit()
        except Exception:
            pass
    conn.close()

# ── Bots ──────────────────────────────────────────────
def db_add_bot(owner_id, token, admin_id, days: int = 30):
    expires = datetime.now() + timedelta(days=days)
    c = _conn()
    c.execute(
        'INSERT INTO bots (owner_id, token, admin_id, expires_at) VALUES (?,?,?,?)',
        (owner_id, token, admin_id, expires.isoformat())
    )
    row_id = c.execute('SELECT last_insert_rowid()').fetchone()[0]
    c.commit(); c.close()
    return row_id

def db_get_all_bots():
    c = _conn()
    rows = c.execute('SELECT id, token, admin_id, owner_id FROM bots WHERE active=1').fetchall()
    c.close(); return rows

def db_get_owner_bot(owner_id):
    c = _conn()
    row = c.execute('SELECT id FROM bots WHERE owner_id=? AND active=1', (owner_id,)).fetchone()
    c.close(); return row[0] if row else None

def db_get_owner_bots(owner_id):
    """Returns [(id, token, username, created_at, expires_at), ...]"""
    c = _conn()
    rows = c.execute(
        'SELECT id, token, username, created_at, expires_at FROM bots WHERE owner_id=? AND active=1',
        (owner_id,)
    ).fetchall()
    c.close(); return rows

def db_update_bot_username(bot_id: int, username: str):
    c = _conn()
    c.execute('UPDATE bots SET username=? WHERE id=?', (username or '', bot_id))
    c.commit(); c.close()

def db_deactivate_bot(bot_id: int):
    c = _conn()
    c.execute('UPDATE bots SET active=0 WHERE id=?', (bot_id,))
    c.commit(); c.close()

def db_get_bot_info(bot_id: int):
    """Returns (token, admin_id, owner_id, created_at) or None."""
    c = _conn()
    row = c.execute(
        'SELECT token, admin_id, owner_id, created_at FROM bots WHERE id=?', (bot_id,)
    ).fetchone()
    c.close(); return row

def db_get_bot_expires(bot_id: int):
    """Returns expires_at string or None."""
    c = _conn()
    row = c.execute('SELECT expires_at FROM bots WHERE id=?', (bot_id,)).fetchone()
    c.close(); return row[0] if row else None

def db_get_active_bots_list():
    """Returns [(id, owner_id, admin_id, created_at, expires_at), ...]"""
    c = _conn()
    rows = c.execute(
        'SELECT id, owner_id, admin_id, created_at, expires_at FROM bots WHERE active=1'
    ).fetchall()
    c.close(); return rows

def db_get_all_bots_for_checker():
    """Returns [(id, owner_id, token, admin_id, expires_at), ...]"""
    c = _conn()
    rows = c.execute(
        'SELECT id, owner_id, token, admin_id, expires_at FROM bots WHERE active=1'
    ).fetchall()
    c.close(); return rows

# ── Subscription ──────────────────────────────────────
def db_renew_bot(bot_id: int, days: int = 30):
    c = _conn()
    row = c.execute('SELECT expires_at FROM bots WHERE id=?', (bot_id,)).fetchone()
    try:
        current = datetime.fromisoformat(str(row[0])) if row and row[0] else datetime.now()
        base    = max(current, datetime.now())
    except Exception:
        base = datetime.now()
    new_expires = base + timedelta(days=days)
    c.execute('UPDATE bots SET expires_at=?, active=1 WHERE id=?',
              (new_expires.isoformat(), bot_id))
    c.commit(); c.close()
    return new_expires

# ── Bot Admins ────────────────────────────────────────
def db_get_bot_admins(bot_id: int) -> list:
    """Returns list of all admin_ids (primary + additional)."""
    c = _conn()
    primary    = c.execute('SELECT admin_id FROM bots WHERE id=?', (bot_id,)).fetchone()
    additional = c.execute('SELECT admin_id FROM bot_admins WHERE bot_id=?', (bot_id,)).fetchall()
    c.close()
    result = [primary[0]] if primary else []
    for r in additional:
        if r[0] not in result:
            result.append(r[0])
    return result

def db_add_bot_admin(bot_id: int, admin_id: int):
    c = _conn()
    c.execute('INSERT OR IGNORE INTO bot_admins (bot_id, admin_id) VALUES (?,?)', (bot_id, admin_id))
    c.commit(); c.close()

def db_remove_bot_admin(bot_id: int, admin_id: int):
    c = _conn()
    c.execute('DELETE FROM bot_admins WHERE bot_id=? AND admin_id=?', (bot_id, admin_id))
    c.commit(); c.close()

def db_set_primary_admin(bot_id: int, new_admin_id: int):
    c = _conn()
    c.execute('UPDATE bots SET admin_id=? WHERE id=?', (new_admin_id, bot_id))
    c.commit(); c.close()

# ── Users ─────────────────────────────────────────────
def db_add_user(bot_id, user_id):
    c = _conn()
    c.execute('INSERT OR IGNORE INTO bot_users (bot_id, user_id) VALUES (?,?)', (bot_id, user_id))
    c.commit(); c.close()

def db_get_bot_users(bot_id):
    c = _conn()
    rows = c.execute('SELECT user_id FROM bot_users WHERE bot_id=?', (bot_id,)).fetchall()
    c.close(); return [r[0] for r in rows]

def db_get_all_users():
    c = _conn()
    rows = c.execute('SELECT DISTINCT user_id FROM bot_users').fetchall()
    c.close(); return [r[0] for r in rows]

# ── Blocking ──────────────────────────────────────────
def db_block_user(bot_id: int, user_id: int):
    c = _conn()
    c.execute('INSERT OR IGNORE INTO blocked_users (bot_id, user_id) VALUES (?,?)', (bot_id, user_id))
    c.commit(); c.close()

def db_unblock_user(bot_id: int, user_id: int):
    c = _conn()
    c.execute('DELETE FROM blocked_users WHERE bot_id=? AND user_id=?', (bot_id, user_id))
    c.commit(); c.close()

def db_is_blocked(bot_id: int, user_id: int) -> bool:
    c = _conn()
    row = c.execute('SELECT 1 FROM blocked_users WHERE bot_id=? AND user_id=?', (bot_id, user_id)).fetchone()
    c.close(); return row is not None

def db_get_blocked_list(bot_id: int):
    c = _conn()
    rows = c.execute('SELECT user_id FROM blocked_users WHERE bot_id=?', (bot_id,)).fetchall()
    c.close(); return [r[0] for r in rows]

# ── Bot Settings ──────────────────────────────────────
def db_get_bot_setting(bot_id: int, key: str, default: str = '') -> str:
    c = _conn()
    row = c.execute('SELECT value FROM bot_settings WHERE bot_id=? AND key=?', (bot_id, key)).fetchone()
    c.close(); return row[0] if row else default

def db_set_bot_setting(bot_id: int, key: str, value: str):
    c = _conn()
    c.execute('INSERT OR REPLACE INTO bot_settings (bot_id, key, value) VALUES (?,?,?)',
              (bot_id, key, value))
    c.commit(); c.close()

# ── Transactions ──────────────────────────────────────
def db_is_tx_used(tx_hash: str) -> bool:
    c = _conn()
    row = c.execute('SELECT 1 FROM used_txs WHERE tx_hash=?', (tx_hash,)).fetchone()
    c.close(); return row is not None

def db_mark_tx_used(tx_hash: str, user_id: int):
    c = _conn()
    c.execute('INSERT OR IGNORE INTO used_txs (tx_hash, user_id) VALUES (?,?)', (tx_hash, user_id))
    c.commit(); c.close()

# ── Settings / Price ──────────────────────────────────
def get_price() -> float:
    c = _conn()
    row = c.execute("SELECT value FROM settings WHERE key='price'").fetchone()
    c.close()
    return float(row[0]) if row else PRICE_USDT

def set_price(price: float):
    c = _conn()
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('price', ?)", (str(price),))
    c.commit(); c.close()

# ── Persistent States ─────────────────────────────────
def db_set_state(bot_id: int, user_id: int, value):
    c = _conn()
    c.execute('INSERT OR REPLACE INTO states (bot_id, user_id, state) VALUES (?,?,?)',
              (bot_id, user_id, json.dumps(value, ensure_ascii=False)))
    c.commit(); c.close()

def db_get_state(bot_id: int, user_id: int):
    c = _conn()
    row = c.execute('SELECT state FROM states WHERE bot_id=? AND user_id=?', (bot_id, user_id)).fetchone()
    c.close()
    return json.loads(row[0]) if row else None

def db_del_state(bot_id: int, user_id: int):
    c = _conn()
    c.execute('DELETE FROM states WHERE bot_id=? AND user_id=?', (bot_id, user_id))
    c.commit(); c.close()

class DBState:
    def __init__(self, bot_id: int = 0):
        self.bot_id = bot_id

    def get(self, user_id: int, default=None):
        val = db_get_state(self.bot_id, user_id)
        return val if val is not None else default

    def __setitem__(self, user_id: int, value):
        db_set_state(self.bot_id, user_id, value)

    def __getitem__(self, user_id: int):
        val = db_get_state(self.bot_id, user_id)
        if val is None:
            raise KeyError(user_id)
        return val

    def pop(self, user_id: int, default=None):
        val = db_get_state(self.bot_id, user_id)
        db_del_state(self.bot_id, user_id)
        return val if val is not None else default

    def __contains__(self, user_id: int):
        return db_get_state(self.bot_id, user_id) is not None

# ── Pending Payments ──────────────────────────────────
def db_set_pending(user_id: int, invoice_id: int):
    c = _conn()
    c.execute('INSERT OR REPLACE INTO pending_payments (user_id, invoice_id) VALUES (?,?)',
              (user_id, invoice_id))
    c.commit(); c.close()

def db_del_pending(user_id: int):
    c = _conn()
    c.execute('DELETE FROM pending_payments WHERE user_id=?', (user_id,))
    c.commit(); c.close()

# ── Paid Setups ───────────────────────────────────────
def db_mark_paid(user_id: int):
    c = _conn()
    c.execute('INSERT OR IGNORE INTO paid_setups (user_id) VALUES (?)', (user_id,))
    c.commit(); c.close()

def db_is_paid(user_id: int) -> bool:
    c = _conn()
    row = c.execute('SELECT 1 FROM paid_setups WHERE user_id=?', (user_id,)).fetchone()
    c.close(); return row is not None

def db_clear_paid(user_id: int):
    c = _conn()
    c.execute('DELETE FROM paid_setups WHERE user_id=?', (user_id,))
    c.commit(); c.close()

# ── Bot Templates ─────────────────────────────────────
def db_get_templates(bot_id: int) -> list:
    c = _conn()
    rows = c.execute(
        'SELECT id, text FROM bot_templates WHERE bot_id=? ORDER BY created_at',
        (bot_id,)
    ).fetchall()
    c.close(); return rows

def db_add_template(bot_id: int, text: str) -> int:
    c = _conn()
    c.execute('INSERT INTO bot_templates (bot_id, text) VALUES (?,?)', (bot_id, text))
    row_id = c.execute('SELECT last_insert_rowid()').fetchone()[0]
    c.commit(); c.close(); return row_id

def db_del_template(template_id: int, bot_id: int):
    c = _conn()
    c.execute('DELETE FROM bot_templates WHERE id=? AND bot_id=?', (template_id, bot_id))
    c.commit(); c.close()

def db_get_template(template_id: int, bot_id: int):
    c = _conn()
    row = c.execute(
        'SELECT text FROM bot_templates WHERE id=? AND bot_id=?', (template_id, bot_id)
    ).fetchone()
    c.close(); return row[0] if row else None

# ── Statistics ────────────────────────────────────────
def db_get_stats():
    c = _conn()
    bots_count  = c.execute('SELECT COUNT(*) FROM bots WHERE active=1').fetchone()[0]
    main_users  = c.execute('SELECT COUNT(*) FROM bot_users WHERE bot_id=0').fetchone()[0]
    total_users = c.execute('SELECT COUNT(DISTINCT user_id) FROM bot_users').fetchone()[0]
    breakdown   = c.execute(
        'SELECT b.owner_id, b.id, COUNT(bu.user_id), b.created_at, b.expires_at '
        'FROM bots b LEFT JOIN bot_users bu ON b.id=bu.bot_id '
        'WHERE b.active=1 GROUP BY b.id ORDER BY COUNT(bu.user_id) DESC'
    ).fetchall()
    c.close()
    return bots_count, main_users, total_users, breakdown

# ── Trial ─────────────────────────────────────────────
def db_has_used_trial(user_id: int) -> bool:
    c = _conn()
    row = c.execute('SELECT 1 FROM trial_used WHERE user_id=?', (user_id,)).fetchone()
    c.close(); return row is not None

def db_mark_trial_used(user_id: int):
    c = _conn()
    c.execute('INSERT OR IGNORE INTO trial_used (user_id) VALUES (?)', (user_id,))
    c.commit(); c.close()

# ── Referrals ─────────────────────────────────────────
def db_set_referral(referrer_id: int, referred_id: int):
    c = _conn()
    c.execute('INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?,?)',
              (referrer_id, referred_id))
    c.commit(); c.close()

def db_get_referrer(referred_id: int):
    c = _conn()
    row = c.execute('SELECT referrer_id FROM referrals WHERE referred_id=? AND paid=0',
                    (referred_id,)).fetchone()
    c.close(); return row[0] if row else None

def db_mark_referral_paid(referred_id: int):
    c = _conn()
    row = c.execute('SELECT referrer_id FROM referrals WHERE referred_id=?',
                    (referred_id,)).fetchone()
    if row:
        referrer_id = row[0]
        c.execute('UPDATE referrals SET paid=1 WHERE referred_id=?', (referred_id,))
        c.execute('''INSERT INTO referral_discounts (user_id, discount_count) VALUES (?,1)
                     ON CONFLICT(user_id) DO UPDATE SET discount_count=discount_count+1''',
                  (referrer_id,))
    c.commit(); c.close()

def db_get_discount_count(user_id: int) -> int:
    c = _conn()
    row = c.execute('SELECT discount_count FROM referral_discounts WHERE user_id=?',
                    (user_id,)).fetchone()
    c.close(); return row[0] if row else 0

def db_use_discount(user_id: int):
    c = _conn()
    c.execute('''UPDATE referral_discounts SET discount_count=MAX(0, discount_count-1)
                 WHERE user_id=?''', (user_id,))
    c.commit(); c.close()

# ── Bot Message Stats ─────────────────────────────────
def db_log_message(bot_id: int, user_id: int):
    c = _conn()
    c.execute('INSERT INTO bot_messages (bot_id, user_id) VALUES (?,?)', (bot_id, user_id))
    c.commit(); c.close()

def db_get_bot_msg_stats(bot_id: int) -> dict:
    c = _conn()
    today = c.execute(
        "SELECT COUNT(*) FROM bot_messages WHERE bot_id=? AND timestamp >= date('now')",
        (bot_id,)
    ).fetchone()[0]
    week = c.execute(
        "SELECT COUNT(*) FROM bot_messages WHERE bot_id=? AND timestamp >= date('now','-7 days')",
        (bot_id,)
    ).fetchone()[0]
    total_users = c.execute(
        'SELECT COUNT(*) FROM bot_users WHERE bot_id=?', (bot_id,)
    ).fetchone()[0]
    blocked = c.execute(
        'SELECT COUNT(*) FROM blocked_users WHERE bot_id=?', (bot_id,)
    ).fetchone()[0]
    c.close()
    return {'today': today, 'week': week, 'total_users': total_users, 'blocked': blocked}
