import sqlite3
import json
import logging
import os
import threading
import time
import weakref
import math
import uuid
from datetime import datetime, timedelta
from config import PRICE_USDT

log = logging.getLogger(__name__)

_data_dir = os.getenv('DATA_DIR', '.')
DATA_DIR = _data_dir  # публичный экспорт (main.py, forum_support)
os.makedirs(_data_dir, exist_ok=True)
DB_PATH = os.path.join(_data_dir, 'spambots.db')

# ── Пул соединений (thread-local) ─────────────────────────────
# Каждый поток переиспользует одно соединение: WAL допускает параллельные
# читатели, busy_timeout ждёт при конфликте записи. Кэш ограничен числом
# потоков, утечек нет. Ключ кэша — путь к БД (совместимо с подменой в тестах).
_tls = threading.local()

# Гейт восстановления: на время подмены файла БД (restore) ВСЕ обращения
# через пул блокируются — параллельные операции не пишут в старый файл
# и не создают соединения посреди замены (см. db_restore_section).
_restore_gate = threading.RLock()

class _CachedConn:
    """Обёртка: close() не закрывает кэшированное соединение, а откатывает
    незавершённую транзакцию (чтобы чужая транзакция не держала блокировку).
    При 'disk I/O error' или закрытии извне (restore_db) соединение
    пересоздаётся, запрос повторяется."""
    __slots__ = ('_c', '_file_sig', '_path', '__weakref__')
    def __init__(self, c, path):
        self._c = c
        self._path = path
        self._file_sig = None
    def _reopen(self):
        try:
            self._c.close()
        except Exception:
            pass
        self._c = sqlite3.connect(self._path, timeout=15, check_same_thread=False)
        try:
            self._c.execute('PRAGMA busy_timeout=15000')
        except Exception:
            pass
        self._file_sig = _file_signature(self._path)
    def execute(self, *args, **kwargs):
        try:
            transaction_open = self._c.in_transaction
        except sqlite3.ProgrammingError:
            transaction_open = False
        try:
            return self._c.execute(*args, **kwargs)
        except sqlite3.OperationalError as e:
            # Нельзя повторять один запрос из середины транзакции на новом
            # соединении: предыдущие начисления откатятся, а отметка зачёта
            # может сохраниться отдельно от них
            if transaction_open or self._c.in_transaction:
                raise
            low = str(e).lower()
            if 'disk i/o' in low:
                # файл/хранилище моргнуло: пересоздаём соединение и повторяем раз
                log.warning('SQLite disk I/O error — пересоздаю соединение')
                self._reopen()
                return self._c.execute(*args, **kwargs)
            if 'database is locked' in low:
                # busy_timeout не помог — открываем заново и пробуем ещё раз
                time.sleep(0.5)
                self._reopen()
                return self._c.execute(*args, **kwargs)
            raise
        except sqlite3.ProgrammingError as e:
            if 'closed database' not in str(e).lower():
                raise
            # соединение закрыто извне (db_close_all после restore) — переоткрываем
            self._reopen()
            return self._c.execute(*args, **kwargs)
    def commit(self):
        self._c.commit()
    def close(self):
        try:
            if self._c.in_transaction:
                self._c.rollback()
        except Exception:
            pass
    def __getattr__(self, name):
        return getattr(self._c, name)

    def __del__(self):
        # Завершившийся поток удаляет TLS; дескриптор закрываем сразу,
        # не дожидаясь сборки внутренних циклов sqlite3.Connection
        try:
            self._c.close()
        except Exception:
            pass


class _ConnLease:
    """Держит гейт до окончания работы вызывающего кода, включая fetch/commit"""
    def __init__(self, cached):
        self._cached = cached
        self._closed = False

    def __getattr__(self, name):
        return getattr(self._cached, name)

    def close(self):
        if not self._closed:
            self._closed = True
            try:
                self._cached.close()
            finally:
                _restore_gate.release()

    def __del__(self):
        self.close()

# Реестр всех соединений: db_close_all() закрывает их перед подменой файла БД
# (restore_db), иначе на Windows файл не удалить под открытым дескриптором.
_conns_registry = weakref.WeakSet()

def db_close_all():
    """Закрывает ВСЕ кэшированные соединения любых потоков. После вызова
    кэшированные обёртки переоткрываются автоматически при следующем запросе."""
    with _restore_gate:
        for c in list(_conns_registry):
            c._c.close()

def _file_signature(path):
    # только идентификация файла (inode/dev): mtime/size меняются при КАЖДОЙ
    # записи и заставляли бы пересоздавать соединение после любого INSERT
    try:
        st = os.stat(path)
        return (st.st_dev, st.st_ino)
    except OSError:
        return None

def db_restore_section():
    """Контекстный менеджер-гейт: `with db_restore_section():` останавливает
    доступ к БД всех остальных потоков пула на время restore (close_all →
    подмена файла → init_db). Потоки просто дождутся своей очереди и
    переоткроют соединения уже с новой БД — без режима обслуживания."""
    return _restore_gate

def _conn():
    # во время restore ждём завершения замены файла (см. db_restore_section)
    _restore_gate.acquire()
    try:
        return _ConnLease(_conn_locked())
    except BaseException:
        _restore_gate.release()
        raise

def _conn_locked():
    path = DB_PATH
    conns = getattr(_tls, 'conns', None)
    if conns is None:
        conns = {}
        _tls.conns = conns
    c = conns.get(path)
    if c is not None:
        # файл подменили (restore_db/бэкап)? — пересоздаём соединение
        if _file_signature(path) != c._file_sig:
            try:
                c._c.close()
            except Exception:
                pass
            c = None
    if c is None:
        raw = sqlite3.connect(path, timeout=15, check_same_thread=False)
        try:
            raw.execute('PRAGMA busy_timeout=15000')
            raw.execute('PRAGMA synchronous=NORMAL')
        except Exception:
            pass
        c = _CachedConn(raw, path)
        _conns_registry.add(c)
        c._file_sig = _file_signature(path)
        conns[path] = c
    return c

def _utc_now_str() -> str:
    """Единый формат UTC-времени для всех новых таблиц (CURRENT_TIMESTAMP тоже UTC)."""
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')


def subscription_days_left(expires_at, now=None):
    """Положительный остаток, даже меньше суток, означает действующую подписку"""
    expires = datetime.fromisoformat(str(expires_at))
    return math.ceil((expires - (now or datetime.now())).total_seconds() / 86400)

def init_db(path=None, *, schema=False):
    conn = sqlite3.connect(path or DB_PATH, timeout=15)
    try:
        return _init_db_connection(conn, schema=schema)
    finally:
        conn.close()


def _init_db_connection(conn, *, schema=False):
    try:
        # WAL требует mmap (-shm) и на некоторых хостингах даёт 'disk I/O error' —
        # используем классический journal-режим: надёжно на любом хранилище.
        conn.execute('PRAGMA journal_mode=DELETE')
        conn.execute('PRAGMA busy_timeout=15000')
    except Exception:
        pass
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
            bot_id   INTEGER NOT NULL,
            user_id  INTEGER NOT NULL,
            username TEXT DEFAULT '',
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
        CREATE TABLE IF NOT EXISTS payment_orders (
            provider TEXT NOT NULL,
            invoice_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            amount REAL,
            target_bot_id INTEGER,
            via_bot_id INTEGER,
            expires_at REAL,
            settled INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (provider, invoice_id)
        );
        CREATE TABLE IF NOT EXISTS setup_grants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            days INTEGER NOT NULL
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
        CREATE TABLE IF NOT EXISTS bot_marketplace (
            bot_id  INTEGER PRIMARY KEY,
            enabled INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS bot_earnings (
            bot_id        INTEGER PRIMARY KEY,
            balance_usdt  REAL DEFAULT 0.0,
            total_earned  REAL DEFAULT 0.0
        );
        CREATE TABLE IF NOT EXISTS withdrawal_requests (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id      INTEGER NOT NULL,
            owner_id    INTEGER NOT NULL,
            ton_address TEXT NOT NULL,
            usdt_amount REAL NOT NULL,
            status      TEXT DEFAULT 'pending',
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS bot_grants (
            user_id  INTEGER PRIMARY KEY,
            days     INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS free_coupons (
            user_id    INTEGER PRIMARY KEY,
            days       INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            used       INTEGER DEFAULT 0
        );
        -- ── Forum Topics: группы техподдержки ──────────────
        CREATE TABLE IF NOT EXISTS fs_connect_requests (
            bot_id     INTEGER PRIMARY KEY,
            user_id    INTEGER NOT NULL,
            token      TEXT NOT NULL,
            request_id INTEGER NOT NULL,
            expires_at REAL NOT NULL,
            mode       TEXT NOT NULL DEFAULT 'select',
            chat_id    TEXT,
            title      TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS fs_groups (
            bot_id       INTEGER PRIMARY KEY,
            chat_id      INTEGER NOT NULL UNIQUE,
            title        TEXT DEFAULT '',
            thread_stats INTEGER,
            connected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        -- Пользовательские темы: 1 пользователь = 1 тема на бота
        CREATE TABLE IF NOT EXISTS fs_topics (
            bot_id        INTEGER NOT NULL,
            user_id       INTEGER NOT NULL,
            thread_id     INTEGER NOT NULL,
            title         TEXT DEFAULT '',
            pinned_msg_id INTEGER,
            info_hash     TEXT DEFAULT '',
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at    TIMESTAMP,
            PRIMARY KEY (bot_id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_fs_topics_thread ON fs_topics(bot_id, thread_id);
        -- Соответствие исходных и групповых сообщений (для ответов через Reply)
        CREATE TABLE IF NOT EXISTS fs_msgs (
            chat_id      INTEGER NOT NULL,
            group_msg_id INTEGER NOT NULL,
            bot_id       INTEGER NOT NULL,
            user_id      INTEGER NOT NULL,
            thread_id    INTEGER NOT NULL,
            src_msg_id   INTEGER,
            media_group  TEXT DEFAULT '',
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chat_id, group_msg_id)
        );
        CREATE INDEX IF NOT EXISTS idx_fs_msgs_user ON fs_msgs(bot_id, user_id);
        -- История обнаруженных username
        CREATE TABLE IF NOT EXISTS fs_username_history (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id     INTEGER NOT NULL,
            user_id    INTEGER NOT NULL,
            username   TEXT NOT NULL,
            first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_seen  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (bot_id, user_id, username)
        );
        -- Обращения: серия сообщений = 1 обращение (разрыв > TICKET_GAP_HOURS открывает новое)
        CREATE TABLE IF NOT EXISTS fs_tickets (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id    INTEGER NOT NULL,
            user_id   INTEGER NOT NULL,
            opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_at   TIMESTAMP,
            closed_at TIMESTAMP,
            msgs      INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_fs_tickets_bot ON fs_tickets(bot_id, opened_at);
        CREATE INDEX IF NOT EXISTS idx_fs_tickets_user ON fs_tickets(bot_id, user_id, closed_at);
        -- Сообщения, отправленные админами пользователям (для статистики)
        CREATE TABLE IF NOT EXISTS fs_admin_msgs (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id    INTEGER NOT NULL,
            user_id   INTEGER NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_fs_admin_msgs_bot ON fs_admin_msgs(bot_id, timestamp);
        -- Очередь подтверждения отправки случайных сообщений пользователя
        CREATE TABLE IF NOT EXISTS fs_pending (
            bot_id        INTEGER NOT NULL,
            user_id       INTEGER NOT NULL,
            payload       TEXT NOT NULL DEFAULT '[]',
            status        TEXT DEFAULT 'pending',
            prompt_msg_id INTEGER,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at    TIMESTAMP,
            PRIMARY KEY (bot_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS delivery_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            media_group_id TEXT NOT NULL DEFAULT '',
            kind TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at REAL NOT NULL,
            ready_at REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            next_at REAL NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            progress TEXT NOT NULL DEFAULT '{}',
            finished_at REAL,
            UNIQUE(bot_id, chat_id, message_id)
        );
        CREATE INDEX IF NOT EXISTS idx_delivery_ready
            ON delivery_queue(bot_id, status, user_id, id);
        CREATE TABLE IF NOT EXISTS delivery_limits (
            bot_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            not_before REAL NOT NULL,
            PRIMARY KEY(bot_id, chat_id)
        );
        CREATE TABLE IF NOT EXISTS delivery_sources (
            bot_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL,
            group_msg_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            src_msg_id INTEGER NOT NULL,
            created_at REAL NOT NULL,
            PRIMARY KEY(bot_id, chat_id, group_msg_id, src_msg_id)
        );
        CREATE TABLE IF NOT EXISTS faq_nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id INTEGER NOT NULL,
            parent_id INTEGER,
            title TEXT NOT NULL,
            text TEXT NOT NULL DEFAULT '',
            position INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_faq_parent ON faq_nodes(bot_id, parent_id, position, id);
        CREATE TABLE IF NOT EXISTS auto_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id INTEGER NOT NULL,
            phrases TEXT NOT NULL,
            text TEXT NOT NULL,
            offhours INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_auto_rules_bot ON auto_rules(bot_id, id);
        CREATE TABLE IF NOT EXISTS support_cooldowns (
            bot_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
            kind TEXT NOT NULL, shown_at REAL NOT NULL,
            PRIMARY KEY(bot_id, user_id, kind)
        );
        CREATE TABLE IF NOT EXISTS support_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bot_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
            kind TEXT NOT NULL, text TEXT NOT NULL,
            notice TEXT NOT NULL DEFAULT '[]', next_at REAL NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            stage INTEGER NOT NULL DEFAULT 0,
            UNIQUE(bot_id, user_id, kind)
        );
        CREATE TABLE IF NOT EXISTS support_album_replies (
            bot_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
            album TEXT NOT NULL, reserved_at REAL NOT NULL,
            PRIMARY KEY(bot_id,user_id,album)
        );
        -- Еженедельные отчёты (защита от дублей по периоду)
        CREATE TABLE IF NOT EXISTS fs_weekly_reports (
            bot_id       INTEGER NOT NULL,
            period_start TEXT NOT NULL,
            period_end   TEXT NOT NULL,
            status       TEXT DEFAULT 'queued',
            message_id   INTEGER,
            published_at TIMESTAMP,
            PRIMARY KEY (bot_id, period_start)
        );
    ''')
    conn.execute('CREATE TABLE IF NOT EXISTS balance_operations ('
                 'bot_id INTEGER, operation_key TEXT, result TEXT NOT NULL, PRIMARY KEY(bot_id,operation_key))')
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS service_broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, creator INTEGER NOT NULL, chat_id INTEGER NOT NULL,
            scope TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft', payload TEXT NOT NULL DEFAULT '[]',
            updated_at REAL NOT NULL, notified INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS service_recipients (
            job_id INTEGER NOT NULL, user_id INTEGER NOT NULL, bot_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', next_at REAL NOT NULL DEFAULT 0,
            PRIMARY KEY(job_id,user_id));
        CREATE TABLE IF NOT EXISTS service_files (
            job_id INTEGER NOT NULL, source_id TEXT NOT NULL, name TEXT NOT NULL, data BLOB NOT NULL,
            PRIMARY KEY(job_id,source_id));
        CREATE TABLE IF NOT EXISTS service_file_ids (
            job_id INTEGER NOT NULL, bot_id INTEGER NOT NULL, source_id TEXT NOT NULL, file_id TEXT NOT NULL,
            PRIMARY KEY(job_id,bot_id,source_id));
    ''')
    conn.commit()
    conn.execute('CREATE TABLE IF NOT EXISTS reply_queue ('
                 'id INTEGER PRIMARY KEY AUTOINCREMENT, bot_id INTEGER NOT NULL, '
                 'source_key TEXT NOT NULL, payload TEXT NOT NULL, progress TEXT DEFAULT "{}", '
                 'next_at REAL DEFAULT 0, attempts INTEGER DEFAULT 0, status TEXT DEFAULT "pending", '
                 'UNIQUE(bot_id,source_key))')
    # Миграции для старых схем (безопасны и повторяемы)
    for stmt in [
        'ALTER TABLE bots ADD COLUMN username TEXT DEFAULT ""',
        'ALTER TABLE bots ADD COLUMN expires_at TIMESTAMP',
        "ALTER TABLE bot_messages ADD COLUMN text TEXT DEFAULT ''",
        "ALTER TABLE bot_users ADD COLUMN username TEXT DEFAULT ''",
        # Расширение bot_users для forum-режима (старые строки получают NULL = «Неизвестно»)
        "ALTER TABLE bot_users ADD COLUMN first_name TEXT DEFAULT ''",
        'ALTER TABLE bot_users ADD COLUMN registered_at TIMESTAMP',
        'ALTER TABLE bot_users ADD COLUMN first_contact_at TIMESTAMP',
        'ALTER TABLE bot_users ADD COLUMN contact_count INTEGER DEFAULT 0',
        'ALTER TABLE states ADD COLUMN updated_at TIMESTAMP',
        "ALTER TABLE states ADD COLUMN revision TEXT DEFAULT ''",
        'ALTER TABLE fs_groups ADD COLUMN thread_mgmt INTEGER',
        'ALTER TABLE fs_groups ADD COLUMN mgmt_msg_id INTEGER',
        'ALTER TABLE fs_groups ADD COLUMN mgmt_wait_admin INTEGER',
        'ALTER TABLE fs_groups ADD COLUMN mgmt_claim TEXT',
        'ALTER TABLE fs_groups ADD COLUMN mgmt_claim_at REAL',
        'ALTER TABLE fs_pending ADD COLUMN claim_session TEXT',
        'ALTER TABLE support_notifications ADD COLUMN stage INTEGER NOT NULL DEFAULT 0',
        'ALTER TABLE payment_orders ADD COLUMN expected_ton REAL',
        'ALTER TABLE payment_orders ADD COLUMN next_check REAL NOT NULL DEFAULT 0',
        "ALTER TABLE payment_orders ADD COLUMN receipt TEXT DEFAULT '{}'",
    ]:
        try:
            conn.execute(stmt); conn.commit()
        except Exception:
            pass
    # Только добавление: прежние таблицы остаются совместимыми
    for user_id, invoice_id, state_json in conn.execute(
            'SELECT p.user_id, p.invoice_id, s.state FROM pending_payments p '
            'LEFT JOIN states s ON s.bot_id=0 AND s.user_id=p.user_id').fetchall():
        try:
            context = json.loads(state_json) if state_json else None
        except (ValueError, TypeError):
            context = None
        from purchase_store import purchase_context
        target, via = purchase_context(context)
        conn.execute('INSERT OR IGNORE INTO payment_orders '
                     '(provider, invoice_id, user_id, target_bot_id, via_bot_id) VALUES (?,?,?,?,?)',
                     ('crypto', str(invoice_id), user_id, target, via))
    conn.execute('INSERT INTO setup_grants (user_id, days) '
                 'SELECT p.user_id, COALESCE(g.days, 30) FROM paid_setups p '
                 'LEFT JOIN bot_grants g ON g.user_id=p.user_id '
                 'WHERE NOT EXISTS (SELECT 1 FROM setup_grants s WHERE s.user_id=p.user_id)')
    conn.commit()
    result = {name: {r[1] for r in conn.execute('PRAGMA table_info("' + name + '")')}
              for name, in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if schema:
        return result


def validate_restore(path):
    """Миграции и проверка всех столбцов выполняются до замены рабочей БД"""
    expected = init_db(':memory:', schema=True)
    actual = init_db(path, schema=True)
    if any(not columns <= actual.get(table, set()) for table, columns in expected.items()):
        raise ValueError('Несовместимая база данных')


def db_bot_can_run(bot_id):
    c = _conn()
    try:
        row = c.execute('SELECT active,expires_at FROM bots WHERE id=?', (bot_id,)).fetchone()
        if not row or not row[0]: return False
        return not row[1] or datetime.fromisoformat(str(row[1])) > datetime.now()
    finally: c.close()


def ensure_bot_identity(c, token, exclude=0):
    if c.execute('SELECT 1 FROM bots WHERE id<>? AND (token=? OR token LIKE ?)',
                 (exclude, token, token.split(':', 1)[0] + ':%')).fetchone():
        raise sqlite3.IntegrityError('Этот бот уже подключён')

# ── Устойчивая очередь доставки ─────────────────────────
def db_delivery_rows(bot_id, active=True):
    c = _conn()
    try:
        cur = c.execute('SELECT * FROM delivery_queue WHERE bot_id=? ' +
                        ("AND status IN ('accepting','pending','working','fallback') " if active else '') +
                        'ORDER BY id', (bot_id,))
        names = [col[0] for col in cur.description]
        return [dict(zip(names, row)) for row in cur.fetchall()]
    finally:
        c.close()


def db_delivery_enqueue(bot_id, m, payload, ready_at):
    c = _conn()
    try:
        cur = c.execute('INSERT OR IGNORE INTO delivery_queue '
                        '(bot_id,user_id,chat_id,message_id,media_group_id,kind,payload,created_at,ready_at,status) '
                        "VALUES (?,?,?,?,?,?,?,?,?,'accepting')",
                        (bot_id, m.from_user.id, m.chat.id, m.message_id,
                         m.media_group_id or '', m.content_type, payload, time.time(), ready_at))
        c.commit()
        return cur.lastrowid if cur.rowcount > 0 else None
    finally:
        c.close()


def db_delivery_update(ids, **values):
    allowed = {'status', 'next_at', 'attempts', 'progress', 'finished_at', 'ready_at'}
    if not ids or not values or not values.keys() <= allowed:
        raise ValueError('invalid delivery update')
    c = _conn()
    try:
        c.execute('UPDATE delivery_queue SET ' + ','.join(k + '=?' for k in values) +
                  ' WHERE id IN (' + ','.join('?' for _ in ids) + ')',
                  (*values.values(), *ids))
        c.commit()
    finally:
        c.close()


def db_delivery_limit(bot_id, chat_id, until=None):
    c = _conn()
    try:
        if until is not None:
            c.execute('INSERT INTO delivery_limits VALUES (?,?,?) '
                      'ON CONFLICT(bot_id,chat_id) DO UPDATE SET '
                      'not_before=MAX(not_before,excluded.not_before)', (bot_id, chat_id, until))
            c.commit()
        row = c.execute('SELECT not_before FROM delivery_limits WHERE bot_id=? AND chat_id=?',
                        (bot_id, chat_id)).fetchone()
        return row[0] if row else 0
    finally:
        c.close()


def db_delivery_finish(rows, mappings):
    """Соответствия, статистика и отметка доставки — одна транзакция.

    mappings: (chat_id, sent_id, thread_id, [исходные message_id]). Для склейки
    основной Reply указывает на последний исходный, обратный поиск хранит все.
    """
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        now = time.time()
        bot_id, uid = rows[0]['bot_id'], rows[0]['user_id']
        for chat, mid, thread, sources in mappings:
            c.execute('INSERT OR REPLACE INTO fs_msgs '
                      '(chat_id,group_msg_id,bot_id,user_id,thread_id,src_msg_id,media_group) '
                      'VALUES (?,?,?,?,?,?,?)',
                      (chat, mid, bot_id, uid, thread, sources[-1], rows[0]['media_group_id']))
            for src in sources:
                c.execute('INSERT OR IGNORE INTO delivery_sources VALUES (?,?,?,?,?,?)',
                          (bot_id, chat, mid, uid, src, now))
        for row in rows:
            changed = c.execute("UPDATE delivery_queue SET status='delivered',finished_at=? "
                                "WHERE id=? AND status IN ('pending','working')",
                                (now, row['id'])).rowcount
            if not changed:
                continue
            stamp = datetime.utcfromtimestamp(row['created_at']).strftime('%Y-%m-%d %H:%M:%S')
            ticket = c.execute('SELECT id,last_at FROM fs_tickets WHERE bot_id=? AND user_id=? '
                               'AND closed_at IS NULL ORDER BY id DESC LIMIT 1', (bot_id, uid)).fetchone()
            if ticket and ticket[1] and (datetime.fromisoformat(stamp) -
                                        datetime.fromisoformat(ticket[1])).total_seconds() < 21600:
                c.execute('UPDATE fs_tickets SET msgs=msgs+1,last_at=? WHERE id=?', (stamp, ticket[0]))
            else:
                if ticket:
                    c.execute('UPDATE fs_tickets SET closed_at=last_at WHERE id=?', (ticket[0],))
                c.execute('INSERT INTO fs_tickets (bot_id,user_id,opened_at,last_at,msgs) '
                          'VALUES (?,?,?,?,1)', (bot_id, uid, stamp, stamp))
            c.execute('UPDATE bot_users SET contact_count=contact_count+1,'
                      'first_contact_at=COALESCE(first_contact_at,?) WHERE bot_id=? AND user_id=?',
                      (stamp, bot_id, uid))
            message = json.loads(row['payload'])
            c.execute('INSERT INTO bot_messages (bot_id,user_id,timestamp,text) VALUES (?,?,?,?)',
                      (bot_id, uid, stamp, message.get('text') or '[' + row['kind'] + ']'))
        c.commit()
    finally:
        c.close()


def db_delivery_cleanup(bot_id):
    c = _conn()
    try:
        cutoff = time.time() - 7 * 86400
        c.execute('DELETE FROM delivery_queue WHERE bot_id=? AND finished_at<? '
                  "AND status IN ('delivered','deleted','cancelled')", (bot_id, cutoff))
        c.execute('DELETE FROM delivery_limits WHERE bot_id=? AND not_before<?', (bot_id, time.time()))
        c.execute('DELETE FROM delivery_sources WHERE bot_id=? AND NOT EXISTS '
                  '(SELECT 1 FROM fs_msgs m WHERE m.bot_id=delivery_sources.bot_id '
                  'AND m.chat_id=delivery_sources.chat_id AND m.group_msg_id=delivery_sources.group_msg_id)',
                  (bot_id,))
        c.commit()
    finally:
        c.close()


def db_delivery_resume(bot_id, user_id, cancel=False):
    """Окно страховки управляет всеми сохранёнными обращениями пользователя."""
    c = _conn()
    try:
        rows = c.execute("SELECT id,progress FROM delivery_queue WHERE bot_id=? AND user_id=? "
                         "AND status='fallback' ORDER BY id", (bot_id, user_id)).fetchall()
        for ident, saved in rows:
            progress = json.loads(saved)
            progress.pop('unavailable', None)
            c.execute('UPDATE delivery_queue SET status=?,next_at=0,progress=?,finished_at=? WHERE id=?',
                      ('cancelled' if cancel else 'pending', json.dumps(progress),
                       time.time() if cancel else None, ident))
        c.commit()
        return len(rows)
    finally:
        c.close()


def db_delivery_fallback(rows):
    """Переход в страховку и её durable указатель сохраняются атомарно."""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        bot_id, uid = rows[0]['bot_id'], rows[0]['user_id']
        old = c.execute('SELECT payload FROM fs_pending WHERE bot_id=? AND user_id=?', (bot_id, uid)).fetchone()
        items = json.loads(old[0]) if old else []
        for row in rows:
            c.execute("UPDATE delivery_queue SET status='fallback' WHERE id=?", (row['id'],))
            existing = next((it for it in items if it['mid'] == row['message_id']), None)
            if existing is not None:
                existing['delivery_queue'] = True
            elif len(items) < 20:
                items.append(dict(mid=row['message_id'], type=row['kind'], mg=row['media_group_id'], delivery_queue=True))
        stamp = _utc_now_str()
        if old:
            c.execute('UPDATE fs_pending SET payload=?,updated_at=? WHERE bot_id=? AND user_id=?',
                      (json.dumps(items), stamp, bot_id, uid))
        else:
            c.execute('INSERT INTO fs_pending (bot_id,user_id,payload,updated_at) VALUES (?,?,?,?)',
                      (bot_id, uid, json.dumps(items), stamp))
        c.commit()
    finally:
        c.close()


# ── Bots ──────────────────────────────────────────────
def db_token_in_use(token, exclude_bot_id):
    c = _conn()
    try:
        return bool(c.execute('SELECT 1 FROM bots WHERE id<>? AND (token=? OR token LIKE ?)',
                              (exclude_bot_id, token, token.split(':', 1)[0] + ':%')).fetchone())
    finally:
        c.close()


def db_replace_bot_token(bot_id, expected, token, username, reset_group=False):
    """Compare-and-swap токена и снимок привязки для отката неудачного запуска."""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        previous = c.execute('SELECT token,username FROM bots WHERE id=?', (bot_id,)).fetchone()
        if not previous or previous[0] != expected: raise ValueError('Токен уже изменён. Открой список ботов заново')
        ensure_bot_identity(c, token, bot_id)
        snapshot = dict(previous=previous, tables={})
        if reset_group:
            for table in ('fs_groups', 'fs_topics', 'fs_msgs', 'delivery_sources'):
                cursor = c.execute(f'SELECT * FROM {table} WHERE bot_id=?', (bot_id,))
                snapshot['tables'][table] = ([col[0] for col in cursor.description], cursor.fetchall())
                c.execute(f'DELETE FROM {table} WHERE bot_id=?', (bot_id,))
            snapshot['notices'] = c.execute('SELECT id,notice FROM support_notifications WHERE bot_id=?', (bot_id,)).fetchall()
            c.execute("UPDATE support_notifications SET notice='[]' WHERE bot_id=?", (bot_id,))
        c.execute('UPDATE bots SET token=?,username=? WHERE id=?', (token, username, bot_id))
        c.commit()
        return snapshot
    finally:
        c.close()


def db_restore_bot_token(bot_id, expected, snapshot):
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        if not c.execute('UPDATE bots SET token=?,username=? WHERE id=? AND token=?',
                         (*snapshot['previous'], bot_id, expected)).rowcount:
            raise ValueError('Токен уже изменён')
        for table, (columns, rows) in snapshot['tables'].items():
            c.execute(f'DELETE FROM {table} WHERE bot_id=?', (bot_id,))
            names, placeholders = ','.join(columns), ','.join('?' for _ in columns)
            c.executemany(f'INSERT INTO {table} ({names}) VALUES ({placeholders})', rows)
        for row_id, notice in snapshot.get('notices', []):
            c.execute('UPDATE support_notifications SET notice=? WHERE bot_id=? AND id=?', (notice, bot_id, row_id))
        c.commit()
    finally:
        c.close()


def db_faq_nodes(bot_id, parent_id=None):
    c = _conn()
    try:
        return [dict(zip(('id', 'parent_id', 'title', 'text', 'position'), r)) for r in c.execute(
            'SELECT id,parent_id,title,text,position FROM faq_nodes WHERE bot_id=? AND parent_id IS ? ORDER BY position,id',
            (bot_id, parent_id))]
    finally:
        c.close()


def db_faq_get(bot_id, node_id):
    c = _conn()
    try:
        row = c.execute('SELECT id,parent_id,title,text,position FROM faq_nodes WHERE bot_id=? AND id=?', (bot_id, node_id)).fetchone()
        return dict(zip(('id', 'parent_id', 'title', 'text', 'position'), row)) if row else None
    finally:
        c.close()


FAQ_TEXT_LIMIT = 3847  # 4096 минус 40*6 на название, <b></b> и два перевода строки


def validate_faq_text(text):
    if len(text.encode('utf-16-le')) // 2 > FAQ_TEXT_LIMIT:
        raise ValueError(f'Текст раздела — не больше {FAQ_TEXT_LIMIT} символов')


def db_faq_add(bot_id, parent_id, title, text=''):
    validate_faq_text(text)
    if not 1 <= len(title.strip()) <= 40:
        raise ValueError('Название должно содержать от 1 до 40 символов')
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        ancestor, depth = parent_id, 1
        while ancestor is not None:
            row = c.execute('SELECT parent_id FROM faq_nodes WHERE bot_id=? AND id=?', (bot_id, ancestor)).fetchone()
            if not row: raise ValueError('Раздел больше недоступен')
            ancestor = row[0]
            depth += 1
            if depth > 4: raise ValueError('Максимальная глубина FAQ — 4 уровня')
        if c.execute('SELECT COUNT(*) FROM faq_nodes WHERE bot_id=?', (bot_id,)).fetchone()[0] >= 300:
            raise ValueError('В FAQ может быть не больше 300 кнопок')
        count, pos = c.execute('SELECT COUNT(*),COALESCE(MAX(position),-1)+1 FROM faq_nodes '
                               'WHERE bot_id=? AND parent_id IS ?', (bot_id, parent_id)).fetchone()
        if count >= 50: raise ValueError('В разделе может быть не больше 50 кнопок')
        cur = c.execute('INSERT INTO faq_nodes(bot_id,parent_id,title,text,position) VALUES(?,?,?,?,?)',
                        (bot_id, parent_id, title.strip(), text, pos))
        c.commit()
        return cur.lastrowid
    finally:
        c.close()


def db_faq_edit(bot_id, node_id, field, value):
    if field == 'text': validate_faq_text(value)
    if field not in ('title', 'text'): raise ValueError('Неизвестное поле')
    if field == 'title' and not 1 <= len(value.strip()) <= 40:
        raise ValueError('Название должно содержать от 1 до 40 символов')
    c = _conn()
    try:
        if not c.execute(f'UPDATE faq_nodes SET {field}=? WHERE bot_id=? AND id=?', (value, bot_id, node_id)).rowcount:
            raise ValueError('Раздел больше недоступен')
        c.commit()
    finally:
        c.close()


def db_faq_subtree(bot_id, node_id, delete=False):
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE' if delete else 'BEGIN')
        ids = [r[0] for r in c.execute('WITH RECURSIVE tree(id) AS '
            '(SELECT id FROM faq_nodes WHERE bot_id=? AND id=? UNION ALL '
            'SELECT n.id FROM faq_nodes n JOIN tree t ON n.parent_id=t.id WHERE n.bot_id=?) SELECT id FROM tree',
            (bot_id, node_id, bot_id))]
        if delete:
            c.executemany('DELETE FROM faq_nodes WHERE bot_id=? AND id=?', [(bot_id, i) for i in ids])
        c.commit()
        return len(ids)
    finally:
        c.close()


def db_faq_move(bot_id, node_id, direction):
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        node = c.execute('SELECT parent_id FROM faq_nodes WHERE bot_id=? AND id=?', (bot_id, node_id)).fetchone()
        if not node: raise ValueError('Раздел больше недоступен')
        siblings = c.execute('SELECT id,position FROM faq_nodes WHERE bot_id=? AND parent_id IS ? ORDER BY position,id', (bot_id, node[0])).fetchall()
        index = next(i for i, r in enumerate(siblings) if r[0] == node_id)
        other = index + direction
        if 0 <= other < len(siblings):
            a, b = siblings[index], siblings[other]
            c.execute('UPDATE faq_nodes SET position=? WHERE bot_id=? AND id=?', (b[1], bot_id, a[0]))
            c.execute('UPDATE faq_nodes SET position=? WHERE bot_id=? AND id=?', (a[1], bot_id, b[0]))
        c.commit()
    finally:
        c.close()


def db_auto_rules(bot_id):
    c = _conn()
    try:
        return [dict(id=r[0], phrases=json.loads(r[1]), text=r[2], offhours=bool(r[3]))
                for r in c.execute('SELECT id,phrases,text,offhours FROM auto_rules WHERE bot_id=? ORDER BY id', (bot_id,))]
    finally:
        c.close()


def db_auto_save(bot_id, phrases, text, rule_id=None, offhours=False):
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        if rule_id is None:
            if c.execute('SELECT COUNT(*) FROM auto_rules WHERE bot_id=?', (bot_id,)).fetchone()[0] >= 30:
                raise ValueError('Можно добавить не больше 30 правил')
            cur = c.execute('INSERT INTO auto_rules(bot_id,phrases,text,offhours) VALUES(?,?,?,?)',
                            (bot_id, json.dumps(phrases, ensure_ascii=False), text, int(offhours)))
            rule_id = cur.lastrowid
        else:
            if not c.execute('UPDATE auto_rules SET phrases=?,text=?,offhours=? WHERE bot_id=? AND id=?',
                             (json.dumps(phrases, ensure_ascii=False), text, int(offhours), bot_id, rule_id)).rowcount:
                raise ValueError('Правило больше недоступно')
        c.commit()
        return rule_id
    finally:
        c.close()


def db_auto_delete(bot_id, rule_id):
    c = _conn()
    try:
        c.execute('DELETE FROM auto_rules WHERE bot_id=? AND id=?', (bot_id, rule_id))
        c.commit()
    finally:
        c.close()


def db_support_notify(bot_id, uid, kind, text, notice, now, cooldown, album=None):
    """Резервируем ответ до отправки; кулдаун отсчитывается от успешного показа."""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        last = c.execute('SELECT shown_at FROM support_cooldowns WHERE bot_id=? AND user_id=? AND kind=?',
                         (bot_id, uid, kind)).fetchone()
        if last and now - last[0] < cooldown:
            return False
        if album:
            c.execute('DELETE FROM support_album_replies WHERE reserved_at<?', (now - 7*86400,))
            if c.execute('SELECT 1 FROM support_album_replies WHERE bot_id=? AND user_id=? AND album=?',
                         (bot_id, uid, album)).fetchone(): return False
        cur = c.execute('INSERT OR IGNORE INTO support_notifications(bot_id,user_id,kind,text,notice) VALUES(?,?,?,?,?)',
                        (bot_id, uid, kind, text, json.dumps(notice, ensure_ascii=False)))
        if cur.rowcount and album:
            c.execute('INSERT INTO support_album_replies VALUES(?,?,?,?)', (bot_id, uid, album, now))
        c.commit()
        return bool(cur.rowcount)
    finally:
        c.close()


def db_support_notifications(bot_id, now):
    c = _conn()
    try:
        return c.execute('SELECT id,user_id,kind,text,notice,attempts,stage FROM support_notifications '
                         'WHERE bot_id=? AND next_at<=? ORDER BY id', (bot_id, now)).fetchall()
    finally:
        c.close()


def db_support_notification_sent(bot_id, row, now):
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        c.execute('INSERT OR REPLACE INTO support_cooldowns VALUES(?,?,?,?)', (bot_id, row[1], row[2], now))
        c.execute('UPDATE support_notifications SET stage=1 WHERE id=? AND bot_id=?', (row[0], bot_id))
        c.commit()
    finally:
        c.close()


def db_support_notification_done(bot_id, row, remaining=None):
    c = _conn()
    try:
        if remaining is None:
            c.execute('DELETE FROM support_notifications WHERE id=? AND bot_id=?', (row[0], bot_id))
        else:
            c.execute('UPDATE support_notifications SET notice=? WHERE id=? AND bot_id=?',
                      (json.dumps(remaining, ensure_ascii=False), row[0], bot_id))
        c.commit()
    finally:
        c.close()


def db_support_notification_retry(bot_id, row, next_at):
    c = _conn()
    try:
        c.execute('UPDATE support_notifications SET next_at=?,attempts=attempts+1 WHERE id=? AND bot_id=?',
                  (next_at, row[0], bot_id))
        c.commit()
    finally:
        c.close()


def db_add_bot(owner_id, token, admin_id, days: int = 30):
    expires = datetime.now() + timedelta(days=days)
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        ensure_bot_identity(c, token)
        cur = c.execute('INSERT INTO bots (owner_id, token, admin_id, expires_at) VALUES (?,?,?,?)',
                        (owner_id, token, admin_id, expires.isoformat()))
        c.commit()
        return cur.lastrowid
    finally: c.close()

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

def db_deactivate_bot(bot_id: int, expected_expires=None):
    c = _conn()
    try:
        if expected_expires is None:
            cur = c.execute('UPDATE bots SET active=0 WHERE id=?', (bot_id,))
        else:
            cur = c.execute('UPDATE bots SET active=0 WHERE id=? AND expires_at=?', (bot_id, expected_expires))
        c.commit()
        return cur.rowcount > 0
    finally:
        c.close()

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
    """Продление. BEGIN IMMEDIATE: чтение expires + вычисление + запись —
    одна атомарная транзакция, иначе два параллельных вызова читали одну
    дату и второй терял свои дни (продлено +30 вместо +60)."""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        new_expires = renew_in_transaction(c, bot_id, days)
        c.commit()
        return new_expires
    except Exception:
        try:
            c.execute('ROLLBACK')
        except Exception:
            pass
        c.close(); raise
    finally:
        c.close()

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
def db_add_user(bot_id, user_id, username='', first_name=''):
    """Создаёт/обновляет пользователя. Дата регистрации ставится только при создании
    записи (для существующих пользователей остаётся NULL = «Неизвестно»).
    Возвращает True, если запись создана сейчас."""
    c = _conn()
    cur = c.execute(
        'INSERT OR IGNORE INTO bot_users (bot_id, user_id, username, first_name, registered_at) '
        'VALUES (?,?,?,?,?)',
        (bot_id, user_id, username or '', first_name or '', _utc_now_str())
    )
    is_new = cur.rowcount > 0
    c.execute(
        "UPDATE bot_users SET username=?, "
        "first_name=CASE WHEN ? != '' THEN ? ELSE first_name END "
        'WHERE bot_id=? AND user_id=?',
        (username or '', first_name or '', first_name or '', bot_id, user_id)
    )
    c.commit(); c.close()
    return is_new

def db_get_user_info(bot_id, user_id) -> dict:
    """Расширенная информация о пользователе бота."""
    c = _conn()
    row = c.execute(
        'SELECT username, first_name, registered_at, first_contact_at, contact_count '
        'FROM bot_users WHERE bot_id=? AND user_id=?', (bot_id, user_id)
    ).fetchone()
    c.close()
    if not row:
        return None
    return {'username': row[0] or '', 'first_name': row[1] or '',
            'registered_at': row[2], 'first_contact_at': row[3],
            'contact_count': row[4] or 0}

def db_bump_user_contact(bot_id, user_id):
    """Отмечает обращение пользователя: дата первого обращения + счётчик сообщений."""
    c = _conn()
    c.execute(
        'UPDATE bot_users SET contact_count=contact_count+1, '
        'first_contact_at=COALESCE(first_contact_at, ?) '
        'WHERE bot_id=? AND user_id=?',
        (_utc_now_str(), bot_id, user_id)
    )
    c.commit(); c.close()

def db_add_username_history(bot_id, user_id, username):
    """Запоминает обнаруженный username (с датами первой/последней встречи)."""
    if not username:
        return
    c = _conn()
    c.execute(
        'INSERT INTO fs_username_history (bot_id, user_id, username, first_seen, last_seen) '
        'VALUES (?,?,?,?,?) '
        'ON CONFLICT(bot_id, user_id, username) DO UPDATE SET last_seen=excluded.last_seen',
        (bot_id, user_id, username, _utc_now_str(), _utc_now_str())
    )
    c.commit(); c.close()

def db_get_username_history(bot_id, user_id) -> list:
    """Список username в порядке первого обнаружения."""
    c = _conn()
    rows = c.execute(
        'SELECT username FROM fs_username_history WHERE bot_id=? AND user_id=? '
        'ORDER BY id', (bot_id, user_id)
    ).fetchall()
    c.close()
    return [r[0] for r in rows]

def db_get_bot_users(bot_id):
    c = _conn()
    rows = c.execute('SELECT user_id FROM bot_users WHERE bot_id=?', (bot_id,)).fetchall()
    c.close(); return [r[0] for r in rows]

def db_get_username(bot_id, user_id) -> str:
    c = _conn()
    row = c.execute('SELECT username FROM bot_users WHERE bot_id=? AND user_id=?',
                     (bot_id, user_id)).fetchone()
    c.close(); return (row[0] if row and row[0] else '')

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
    c.execute('INSERT OR REPLACE INTO states (bot_id, user_id, state, updated_at, revision) '
              'VALUES (?,?,?,?,?)',
              (bot_id, user_id, json.dumps(value, ensure_ascii=False), _utc_now_str(), uuid.uuid4().hex))
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

    def snapshot(self, user_id):
        c = _conn()
        try:
            row = c.execute('SELECT state,revision FROM states WHERE bot_id=? AND user_id=?',
                            (self.bot_id, user_id)).fetchone()
            return (json.loads(row[0]), row[1]) if row else (None, None)
        finally: c.close()

    def finish(self, user_id, revision):
        """Завершение долгой проверки не удаляет более новый ввод даже с тем же содержимым"""
        c = _conn()
        try:
            cur = c.execute('DELETE FROM states WHERE bot_id=? AND user_id=? AND revision=?',
                            (self.bot_id, user_id, revision))
            c.commit()
            return bool(cur.rowcount)
        finally: c.close()

    def transition(self, user_id, revision, value):
        """CAS переход: запоздавшая операция не восстанавливает отменённый диалог"""
        c = _conn()
        try:
            cur = c.execute('UPDATE states SET state=?,revision=?,updated_at=CURRENT_TIMESTAMP '
                            'WHERE bot_id=? AND user_id=? AND revision=?',
                            (json.dumps(value), uuid.uuid4().hex, self.bot_id, user_id, revision))
            c.commit()
            return bool(cur.rowcount)
        finally: c.close()

    def __setitem__(self, user_id: int, value):
        db_set_state(self.bot_id, user_id, value)

    def __getitem__(self, user_id: int):
        val = db_get_state(self.bot_id, user_id)
        if val is None:
            raise KeyError(user_id)
        return val

    def pop(self, user_id: int, default=None):
        return self.take(user_id, default=default)

    def take(self, user_id, predicate=lambda value: True, default=None):
        """Захват до сетевого вызова не стирает следующий диалог"""
        c = _conn()
        try:
            c.execute('BEGIN IMMEDIATE')
            row = c.execute('SELECT state FROM states WHERE bot_id=? AND user_id=?',
                            (self.bot_id, user_id)).fetchone()
            value = json.loads(row[0]) if row else None
            if row and predicate(value):
                c.execute('DELETE FROM states WHERE bot_id=? AND user_id=?', (self.bot_id, user_id))
                c.commit()
                return value
            return default
        finally:
            c.close()

    def __contains__(self, user_id: int):
        return db_get_state(self.bot_id, user_id) is not None

# ── Pending Payments ──────────────────────────────────
from purchase_store import (db_set_pending, db_del_pending, db_get_pending,
                            db_get_payment, db_claim_pending_payment,
                            db_mark_paid, db_peek_grant_days, db_take_grant_days,
                            db_is_paid, db_clear_paid, db_register_paid_bot,
                            db_activate_trial, db_use_purchase_discount,
                            renew_in_transaction)


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
    """Начисляет скидку рефереру ОДИН раз: условный UPDATE по paid=0,
    начисление только если rowcount==1 (защита от повторного зачёта)."""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        row = c.execute('SELECT referrer_id FROM referrals '
                        'WHERE referred_id=? AND paid=0', (referred_id,)).fetchone()
        if not row:
            c.execute('ROLLBACK'); c.close()
            return
        referrer_id = row[0]
        c.execute('UPDATE referrals SET paid=1 WHERE referred_id=? AND paid=0', (referred_id,))
        existing = c.execute('SELECT 1 FROM referral_discounts WHERE user_id=?',
                             (referrer_id,)).fetchone()
        if existing:
            c.execute('UPDATE referral_discounts SET discount_count=discount_count+1 WHERE user_id=?',
                      (referrer_id,))
        else:
            c.execute('INSERT INTO referral_discounts (user_id, discount_count) VALUES (?,1)',
                      (referrer_id,))
        c.commit()
    except Exception:
        try:
            c.execute('ROLLBACK')
        except Exception:
            pass
        raise
    finally:
        c.close()

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

def db_use_discount_atomic(user_id: int) -> bool:
    """Атомарное списание скидки: True только если она была (discount_count>0).
    Два параллельных нажатия при одной скидке дают ровно одно продление."""
    c = _conn()
    try:
        cur = c.execute('UPDATE referral_discounts SET discount_count=discount_count-1 '
                        'WHERE user_id=? AND discount_count>0', (user_id,))
        c.commit()
        return cur.rowcount > 0
    except Exception:
        c.rollback(); raise
    finally:
        c.close()

# ── Bot Message Stats ─────────────────────────────────
def db_log_message(bot_id: int, user_id: int, text: str = ''):
    c = _conn()
    c.execute('INSERT INTO bot_messages (bot_id, user_id, text) VALUES (?,?,?)',
              (bot_id, user_id, text or ''))
    c.commit(); c.close()

def db_get_last_user_messages(bot_id: int, user_id: int, n: int = 2) -> list:
    """Returns last n non-empty messages from user as [(text, timestamp), ...] oldest-first."""
    c = _conn()
    rows = c.execute(
        "SELECT text, timestamp FROM bot_messages "
        "WHERE bot_id=? AND user_id=? AND text != '' "
        "ORDER BY id DESC LIMIT ?",
        (bot_id, user_id, n)
    ).fetchall()
    c.close()
    return list(reversed(rows))

def db_get_bot_msg_stats(bot_id: int) -> dict:
    # границы «сегодня»/«неделя» по часовому поясу статистики (FS_STATS_TZ),
    # неделя — календарная (с понедельника), как в еженедельных отчётах
    from config import STATS_TZ_OFFSET
    now_utc = datetime.utcnow()
    tz_now = now_utc + timedelta(hours=STATS_TZ_OFFSET)
    day_start_tz = tz_now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start_tz = day_start_tz - timedelta(days=tz_now.weekday())
    b_today = (day_start_tz - timedelta(hours=STATS_TZ_OFFSET)).strftime('%Y-%m-%d %H:%M:%S')
    b_week = (week_start_tz - timedelta(hours=STATS_TZ_OFFSET)).strftime('%Y-%m-%d %H:%M:%S')
    c = _conn()
    today = c.execute(
        'SELECT COUNT(*) FROM bot_messages WHERE bot_id=? AND timestamp >= ?',
        (bot_id, b_today)
    ).fetchone()[0]
    week = c.execute(
        'SELECT COUNT(*) FROM bot_messages WHERE bot_id=? AND timestamp >= ?',
        (bot_id, b_week)
    ).fetchone()[0]
    total_users = c.execute(
        'SELECT COUNT(*) FROM bot_users WHERE bot_id=?', (bot_id,)
    ).fetchone()[0]
    blocked = c.execute(
        'SELECT COUNT(*) FROM blocked_users WHERE bot_id=?', (bot_id,)
    ).fetchone()[0]
    c.close()
    return {'today': today, 'week': week, 'total_users': total_users, 'blocked': blocked}

# ── Marketplace ───────────────────────────────────────
def db_set_marketplace(bot_id: int, enabled: bool):
    c = _conn()
    c.execute('INSERT OR REPLACE INTO bot_marketplace (bot_id, enabled) VALUES (?,?)',
              (bot_id, 1 if enabled else 0))
    c.commit(); c.close()

def db_is_marketplace_enabled(bot_id: int) -> bool:
    c = _conn()
    row = c.execute('SELECT enabled FROM bot_marketplace WHERE bot_id=?', (bot_id,)).fetchone()
    c.close(); return bool(row and row[0])

def db_get_marketplace_bots() -> list:
    """Returns [(bot_id, enabled), ...] for all bots that have marketplace record."""
    c = _conn()
    rows = c.execute('SELECT bot_id, enabled FROM bot_marketplace').fetchall()
    c.close(); return rows

# ── Bot Earnings ──────────────────────────────────────
def db_add_bot_earnings(bot_id: int, usdt: float):
    c = _conn()
    c.execute('''
        INSERT INTO bot_earnings (bot_id, balance_usdt, total_earned) VALUES (?,?,?)
        ON CONFLICT(bot_id) DO UPDATE SET
            balance_usdt = balance_usdt + excluded.balance_usdt,
            total_earned = total_earned + excluded.total_earned
    ''', (bot_id, usdt, usdt))
    c.commit(); c.close()

def db_get_bot_earnings(bot_id: int) -> tuple:
    """Returns (balance_usdt, total_earned)."""
    c = _conn()
    row = c.execute('SELECT balance_usdt, total_earned FROM bot_earnings WHERE bot_id=?',
                    (bot_id,)).fetchone()
    c.close(); return (row[0], row[1]) if row else (0.0, 0.0)

def db_deduct_bot_earnings(bot_id: int, usdt: float):
    c = _conn()
    c.execute('UPDATE bot_earnings SET balance_usdt = MAX(0, balance_usdt - ?) WHERE bot_id=?',
              (usdt, bot_id))
    c.commit(); c.close()

def db_deduct_bot_earnings_atomic(bot_id: int, usdt: float) -> bool:
    """Атомарное списание: True только если баланса хватило.
    Двойные конкурентные заявки на вывод не могут уйти в минус."""
    c = _conn()
    try:
        cur = c.execute('UPDATE bot_earnings SET balance_usdt = balance_usdt - ? '
                        'WHERE bot_id=? AND balance_usdt >= ?',
                        (usdt, bot_id, usdt))
        c.commit()
        return cur.rowcount > 0
    except Exception:
        c.rollback(); raise
    finally:
        c.close()

def db_return_bot_earnings(bot_id: int, usdt: float):
    """Return earnings (e.g. after rejected withdrawal)."""
    c = _conn()
    c.execute('''
        INSERT INTO bot_earnings (bot_id, balance_usdt, total_earned) VALUES (?,?,0)
        ON CONFLICT(bot_id) DO UPDATE SET balance_usdt = balance_usdt + excluded.balance_usdt
    ''', (bot_id, usdt))
    c.commit(); c.close()

# ── Withdrawal Requests ───────────────────────────────
def db_create_withdrawal(bot_id: int, owner_id: int, ton_address: str, usdt_amount: float) -> int:
    c = _conn()
    c.execute(
        'INSERT INTO withdrawal_requests (bot_id, owner_id, ton_address, usdt_amount) VALUES (?,?,?,?)',
        (bot_id, owner_id, ton_address, usdt_amount)
    )
    row_id = c.execute('SELECT last_insert_rowid()').fetchone()[0]
    c.commit(); c.close(); return row_id

def db_get_pending_withdrawals() -> list:
    """Returns [(id, bot_id, owner_id, ton_address, usdt_amount, created_at), ...]"""
    c = _conn()
    rows = c.execute(
        "SELECT id, bot_id, owner_id, ton_address, usdt_amount, created_at "
        "FROM withdrawal_requests WHERE status='pending' ORDER BY created_at"
    ).fetchall()
    c.close(); return rows

def db_get_withdrawal(wr_id: int):
    """Returns (bot_id, owner_id, ton_address, usdt_amount, status) or None."""
    c = _conn()
    row = c.execute(
        'SELECT bot_id, owner_id, ton_address, usdt_amount, status FROM withdrawal_requests WHERE id=?',
        (wr_id,)
    ).fetchone()
    c.close(); return row

def db_update_withdrawal_status(wr_id: int, status: str):
    c = _conn()
    c.execute('UPDATE withdrawal_requests SET status=? WHERE id=?', (status, wr_id))
    c.commit(); c.close()

def db_claim_withdrawal(wr_id: int, status: str) -> bool:
    """Атомарный перевод заявки в новый статус: True только если была 'pending'.
    Защита от двойного confirm/reject двумя обработчиками."""
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        row = c.execute("SELECT bot_id,usdt_amount FROM withdrawal_requests WHERE id=? AND status='pending'",
                        (wr_id,)).fetchone()
        if not row:
            return False
        cur = c.execute("UPDATE withdrawal_requests SET status=? WHERE id=? AND status='pending'",
                        (status, wr_id))
        if status == 'rejected':
            c.execute('INSERT INTO bot_earnings(bot_id,balance_usdt) VALUES(?,?) '
                      'ON CONFLICT(bot_id) DO UPDATE SET balance_usdt=balance_usdt+excluded.balance_usdt', row)
        c.commit()
        return cur.rowcount > 0
    except Exception:
        c.rollback(); raise
    finally:
        c.close()


def db_spend_balance(bot_id, owner_id, amount, operation_key, *, address=None, days=None, state=None):
    """Списание, результат и ключ повтора фиксируются одной транзакцией"""
    if not math.isfinite(amount) or amount <= 0:
        raise ValueError('Неверная сумма')
    c = _conn()
    try:
        c.execute('BEGIN IMMEDIATE')
        row = c.execute('SELECT owner_id FROM bots WHERE id=?', (bot_id,)).fetchone()
        if not row or row[0] != owner_id:
            raise ValueError('Доступно только владельцу бота')
        previous = c.execute('SELECT result FROM balance_operations WHERE bot_id=? AND operation_key=?',
                             (bot_id, operation_key)).fetchone()
        if previous:
            return json.loads(previous[0])
        if state is not None:
            current = c.execute('SELECT state FROM states WHERE bot_id=? AND user_id=?', (bot_id, owner_id)).fetchone()
            if not current or json.loads(current[0]) != state:
                return None
        if not c.execute('UPDATE bot_earnings SET balance_usdt=ROUND(balance_usdt-?,10) '
                         'WHERE bot_id=? AND ROUND(balance_usdt,10)>=?', (amount, bot_id, amount)).rowcount:
            return None
        if days is not None:
            result = renew_in_transaction(c, bot_id, days).isoformat()
        else:
            result = c.execute('INSERT INTO withdrawal_requests(bot_id,owner_id,ton_address,usdt_amount) '
                               'VALUES(?,?,?,?)', (bot_id, owner_id, address, amount)).lastrowid
        c.execute('INSERT INTO balance_operations(bot_id,operation_key,result) VALUES(?,?,?)',
                  (bot_id, operation_key, json.dumps(result)))
        if state is not None:
            c.execute('DELETE FROM states WHERE bot_id=? AND user_id=?', (bot_id, owner_id))
        c.commit()
        return result
    finally:
        c.close()

# ── Free Coupons ──────────────────────────────────────
def db_set_free_coupon(user_id: int, days: int):
    c = _conn()
    c.execute('INSERT OR REPLACE INTO free_coupons (user_id, days, used) VALUES (?,?,0)',
              (user_id, days))
    c.commit(); c.close()

def db_get_free_coupon(user_id: int):
    """Returns days or None if no unused coupon."""
    c = _conn()
    row = c.execute('SELECT days FROM free_coupons WHERE user_id=? AND used=0',
                    (user_id,)).fetchone()
    c.close(); return row[0] if row else None

def db_mark_coupon_used(user_id: int):
    c = _conn()
    c.execute('UPDATE free_coupons SET used=1 WHERE user_id=?', (user_id,))
    c.commit(); c.close()

# ── Forum Support: агрегаты для еженедельных отчётов ──
# Все метрики считаются точечными запросами по индексам за конкретный
# период [from_ts, to_ts) (UTC, формат 'YYYY-MM-DD HH:MM:SS'), без полного скана.
def db_fs_count_range(table: str, bot_id: int, from_ts: str, to_ts: str) -> int:
    """Количество строк таблицы (bot_messages / fs_tickets / fs_admin_msgs) за период."""
    if table not in ('bot_messages', 'fs_tickets', 'fs_admin_msgs'):
        raise ValueError('bad table')
    col = 'opened_at' if table == 'fs_tickets' else 'timestamp'
    c = _conn()
    n = c.execute(
        f'SELECT COUNT(*) FROM {table} WHERE bot_id=? AND {col}>=? AND {col}<?',
        (bot_id, from_ts, to_ts)
    ).fetchone()[0]
    c.close(); return n

def db_fs_tickets_total(bot_id: int) -> int:
    c = _conn()
    n = c.execute('SELECT COUNT(*) FROM fs_tickets WHERE bot_id=?', (bot_id,)).fetchone()[0]
    c.close(); return n

def db_fs_new_users_range(bot_id: int, from_ts: str, to_ts: str) -> int:
    c = _conn()
    n = c.execute(
        'SELECT COUNT(*) FROM bot_users WHERE bot_id=? AND registered_at>=? AND registered_at<?',
        (bot_id, from_ts, to_ts)
    ).fetchone()[0]
    c.close(); return n

def db_fs_users_total(bot_id: int) -> int:
    c = _conn()
    n = c.execute('SELECT COUNT(*) FROM bot_users WHERE bot_id=?', (bot_id,)).fetchone()[0]
    c.close(); return n

def db_get_topic_copies_by_src(bot_id: int, user_id: int, src_msg_id: int) -> list:
    """Обратный поиск: копии сообщения юзера в теме по оригинальному message_id.
    Возвращает [(chat_id, group_msg_id, thread_id), ...] — может быть несколько (альбом)."""
    c = _conn()
    rows = c.execute(
        'SELECT chat_id, group_msg_id, thread_id FROM fs_msgs '
        'WHERE bot_id=? AND user_id=? AND src_msg_id=? '
        'UNION SELECT s.chat_id, s.group_msg_id, m.thread_id FROM delivery_sources s '
        'JOIN fs_msgs m ON m.chat_id=s.chat_id AND m.group_msg_id=s.group_msg_id '
        'WHERE s.bot_id=? AND s.user_id=? AND s.src_msg_id=?',
        (bot_id, user_id, src_msg_id, bot_id, user_id, src_msg_id)
    ).fetchall()
    c.close()
    return rows

def db_fs_user_tickets_total(bot_id: int, user_id: int) -> int:
    c = _conn()
    n = c.execute('SELECT COUNT(*) FROM fs_tickets WHERE bot_id=? AND user_id=?',
                  (bot_id, user_id)).fetchone()[0]
    c.close(); return n


# ── Еженедельное обслуживание БД (долгий срок) ────────────────
BACKUPS_TO_KEEP = 4

def db_maintenance():
    """Чистка служебных таблиц, которые иначе растут месяцами:
    - fs_msgs (соответствия сообщений) старше 90 дней;
    - fs_admin_msgs (статистика ответов) старше 180 дней;
    - fs_pending (несоотв. очереди) старше 7 дней;
    - states (состояния пользователей) старше 30 дней.
    Пользовательские данные (bot_users, bot_messages, fs_tickets,
    fs_username_history, fs_weekly_reports) НЕ трогаются."""
    cutoff90 = (datetime.utcnow() - timedelta(days=90)).strftime('%Y-%m-%d %H:%M:%S')
    cutoff180 = (datetime.utcnow() - timedelta(days=180)).strftime('%Y-%m-%d %H:%M:%S')
    cutoff7 = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    cutoff30 = (datetime.utcnow() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    c = _conn()
    try:
        c.execute('DELETE FROM fs_msgs WHERE created_at < ?', (cutoff90,))
        c.execute('DELETE FROM fs_admin_msgs WHERE timestamp < ?', (cutoff180,))
        c.execute("DELETE FROM fs_pending WHERE created_at < ? AND status='pending'", (cutoff7,))
        c.execute('DELETE FROM states WHERE updated_at IS NOT NULL AND updated_at < ?', (cutoff30,))
        c.commit()
        log.info('db_maintenance: служебные таблицы почищены')
    except Exception as e:
        log.warning(f'db_maintenance: {e}')
    finally:
        c.close()

def db_backup(dest_dir: str) -> str:
    """Онлайн-бэкап через sqlite3 backup API (безопасен при живых соединениях).
    Возвращает путь к файлу. Старые бэкапы: остаются BACKUPS_TO_KEEP шт."""
    import glob as _glob
    os.makedirs(dest_dir, exist_ok=True)
    stamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    dest_path = os.path.join(dest_dir, f'spambots_{stamp}.db')
    src = sqlite3.connect(DB_PATH, timeout=15)
    dst = sqlite3.connect(dest_path)
    try:
        src.backup(dst)   # копирует согласованное состояние, не блокируя надолго
        log.info(f'Бэкап БД создан: {dest_path}')
    finally:
        # закрываем ОБЕ соединения: иначе при сбое дескриптор dst утечёт
        # и файл бэкапа останется залоченным (критично на Windows)
        try:
            dst.close()
        except Exception:
            pass
        src.close()
    # ротация: оставляем последние BACKUPS_TO_KEEP
    backups = sorted(_glob.glob(os.path.join(dest_dir, 'spambots_*.db')))
    for old in backups[:-BACKUPS_TO_KEEP]:
        try:
            os.remove(old)
        except OSError:
            pass
    return dest_path
