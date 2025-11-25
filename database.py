import os
import sqlite3
import json
import threading
import logging
import asyncio
import copy
import time
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Any

from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo.uri_parser import parse_uri
from pymongo.errors import ConfigurationError
from diskcache import Cache

logger = logging.getLogger("database")
if not logger.handlers:
    logger.addHandler(logging.NullHandler())
logger.setLevel(logging.ERROR)
logger.propagate = False


def _ensure_autoreply_defaults(ar_cfg: Optional[dict]) -> dict:
    if not isinstance(ar_cfg, dict):
        ar_cfg = {}
    for key in ("replied", "error", "pending"):
        value = ar_cfg.get(key)
        if isinstance(value, list):
            ar_cfg[key] = value
        elif value is None:
            ar_cfg[key] = []
        else:
            ar_cfg[key] = [value]
    if 'isactive' not in ar_cfg:
        ar_cfg['isactive'] = False
    return ar_cfg


def configure_dns():
    try:
        import dns.resolver
        resolver = dns.resolver.Resolver(configure=False)
        resolver.nameservers = ['1.1.1.1', '1.0.0.1', '8.8.8.8', '8.8.4.4']
        resolver.timeout = 5
        resolver.lifetime = 15
        dns.resolver.default_resolver = resolver
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)


def configure_ssl():
    try:
        import ssl
        import certifi
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        ssl._create_default_https_context = lambda: ssl_context
        os.environ['SSL_CERT_FILE'] = certifi.where()
        os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)


DB_SQLITE_FILENAME = "autopost.db"
DB_JSON_FILENAME = "autopostdb.json"
SQLITE_SCHEMA_VERSION = "sqlite_v1.0"

CACHE_BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
try:
    os.makedirs(CACHE_BASE_DIR, exist_ok=True)
except Exception:
    logger.exception('Unhandled exception in %s', __name__)
    CACHE_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    _db_cache = Cache(os.path.join(CACHE_BASE_DIR, "db_cache"), size_limit=512 * 1024 * 1024)
except Exception:
    logger.exception('Unhandled exception in %s', __name__)
    _db_cache = None

def _cache_get(key):
    if _db_cache is None:
        return None
    try:
        value = _db_cache.get(key)
        if value is None:
            return None
        return copy.deepcopy(value)
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)
        return None

def _cache_set(key, value, ttl=15):
    if _db_cache is None:
        return
    try:
        _db_cache.set(key, copy.deepcopy(value), expire=ttl)
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)

def _cache_delete(*keys):
    if _db_cache is None:
        return
    for key in keys:
        try:
            _db_cache.delete(key)
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)

def _cache_clear_all():
    if _db_cache is None:
        return
    try:
        _db_cache.clear()
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)

def _user_cache_key(user_id) -> str:
    return f"user::{user_id}"

def _invalidate_user_cache(user_id):
    _cache_delete(_user_cache_key(user_id))
    _cache_delete("users::all", "users::count")

def _invalidate_global_user_cache():
    _cache_delete("users::all", "users::count")

if hasattr(sqlite3, "enable_shared_cache"):
    sqlite3.enable_shared_cache(True)


def _dt_to_iso(value):
    if not value:
        return None
    if isinstance(value, str):
        return value
    try:
        return value.isoformat()
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)
        return str(value)


def _parse_datetime(value):
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)
        return None


def _remove_file_if_exists(path: str):
    if not path:
        return
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)


def _load_state_from_json(path: str) -> Optional[dict]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as handle:
            return json.load(handle)
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)
        return None


class SQLiteDatabase:
    SCHEMA_VERSION = SQLITE_SCHEMA_VERSION

    def __init__(self, db_path: str = DB_SQLITE_FILENAME):
        self.db_path = os.path.abspath(db_path)
        self._write_lock = threading.RLock()
        self._read_lock = threading.RLock()
        self._conn = self._create_connection(read_only=False)
        self._setup_schema()
        try:
            self._read_conn = self._create_connection(read_only=True)
        except sqlite3.OperationalError:
            self._read_conn = self._create_connection(read_only=False)

    def _create_connection(self, *, read_only: bool):
        mode = "ro" if read_only else "rwc"
        uri = f"file:{self.db_path}?cache=shared&mode={mode}"
        conn = sqlite3.connect(uri, uri=True, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        if not read_only:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA wal_autocheckpoint=500")
        cursor.execute("PRAGMA cache_size=-64000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.execute("PRAGMA busy_timeout=5000")
        cursor.close()
        return conn

    def _setup_schema(self):
        with self._write_lock:
            cursor = self._conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_version (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    version TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    package_type TEXT,
                    expiry_date TEXT,
                    accounts_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    webhookurl TEXT,
                    limit_value INTEGER,
                    is_active INTEGER NOT NULL DEFAULT 1
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS config (
                    id TEXT PRIMARY KEY,
                    data_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cursor.execute("INSERT OR IGNORE INTO schema_version (id, version) VALUES (1, ?)", (self.SCHEMA_VERSION,))
            self._conn.commit()

    def close_all_connections(self):
        with self._write_lock:
            try:
                self._conn.close()
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
            try:
                if hasattr(self, "_read_conn"):
                    self._read_conn.close()
            except Exception:
                logger.exception('Unhandled exception in %s', __name__)
        return True

    def _prepare_accounts_for_storage(self, accounts):
        sanitized = []
        for account in accounts or []:
            if not isinstance(account, dict):
                continue
            acc_copy = copy.deepcopy(account)
            acc_copy.pop('_validation_status', None)
            acc_copy['autoreply'] = _ensure_autoreply_defaults(acc_copy.get('autoreply'))
            acc_copy['created_at'] = _dt_to_iso(acc_copy.get('created_at'))
            acc_copy['added_at'] = _dt_to_iso(acc_copy.get('added_at') or datetime.now(timezone.utc))
            for channel in acc_copy.get('channels', []) or []:
                if isinstance(channel, dict):
                    channel.pop('attachments', None)
            sanitized.append(acc_copy)
        return sanitized

    def _prepare_accounts_for_runtime(self, accounts):
        runtime_accounts = []
        for account in accounts or []:
            if not isinstance(account, dict):
                continue
            acc_copy = copy.deepcopy(account)
            acc_copy.pop('_validation_status', None)
            acc_copy['autoreply'] = _ensure_autoreply_defaults(acc_copy.get('autoreply'))
            for channel in acc_copy.get('channels', []) or []:
                if isinstance(channel, dict):
                    channel.pop('attachments', None)
            runtime_accounts.append(acc_copy)
        return runtime_accounts

    def _serialize_user(self, user_data):
        if not isinstance(user_data, dict):
            return None
        user_copy = copy.deepcopy(user_data)
        user_id = user_copy.get('user_id')
        if user_id is None:
            return None
        user_id = str(user_id)
        expiry = _dt_to_iso(user_copy.get('expiry_date'))
        updated_at = _dt_to_iso(user_copy.get('updated_at') or datetime.now(timezone.utc))
        serialized = {
            'user_id': user_id,
            'package_type': user_copy.get('package_type'),
            'expiry_date': expiry,
            'accounts': self._prepare_accounts_for_storage(user_copy.get('accounts') or []),
            'updated_at': updated_at,
            'webhookurl': user_copy.get('webhookurl'),
            'limit': user_copy.get('limit'),
            'is_active': bool(user_copy.get('is_active', True))
        }
        return serialized

    def _deserialize_user(self, stored):
        if not isinstance(stored, dict):
            return None
        user = copy.deepcopy(stored)
        user['expiry_date'] = _parse_datetime(user.get('expiry_date'))
        user['updated_at'] = _parse_datetime(user.get('updated_at'))
        user['accounts'] = self._prepare_accounts_for_runtime(user.get('accounts') or [])
        user['is_active'] = bool(user.get('is_active', True))
        return user

    def _row_to_serialized_user(self, row: sqlite3.Row) -> dict:
        return {
            'user_id': row['user_id'],
            'package_type': row['package_type'],
            'expiry_date': row['expiry_date'],
            'accounts': json.loads(row['accounts_json']) if row['accounts_json'] else [],
            'updated_at': row['updated_at'],
            'webhookurl': row['webhookurl'],
            'limit': row['limit_value'],
            'is_active': bool(row['is_active'])
        }

    def _user_payload(self, serialized: dict) -> tuple:
        return (
            serialized['user_id'],
            serialized.get('package_type'),
            serialized.get('expiry_date'),
            json.dumps(serialized.get('accounts', []), ensure_ascii=False),
            serialized.get('updated_at') or _dt_to_iso(datetime.now(timezone.utc)),
            serialized.get('webhookurl'),
            serialized.get('limit'),
            1 if serialized.get('is_active', True) else 0
        )

    def _normalize_state(self, state: Optional[dict]) -> dict:
        normalized = {
            'schema_version': self.SCHEMA_VERSION,
            'users': {},
            'config': {}
        }
        if not isinstance(state, dict):
            return normalized
        normalized['schema_version'] = state.get('schema_version') or self.SCHEMA_VERSION
        raw_users = state.get('users') or {}
        iterable: List[dict]
        if isinstance(raw_users, dict):
            iterable = []
            for key, value in raw_users.items():
                if isinstance(value, dict):
                    value = copy.deepcopy(value)
                    value.setdefault('user_id', key)
                    iterable.append(value)
        elif isinstance(raw_users, list):
            iterable = raw_users
        else:
            iterable = []
        for item in iterable:
            serialized = self._serialize_user(item)
            if serialized:
                normalized['users'][serialized['user_id']] = serialized
        config = state.get('config') or {}
        if isinstance(config, dict):
            normalized['config'] = copy.deepcopy(config)
        return normalized

    def replace_state(self, state: Optional[dict]):
        normalized = self._normalize_state(state)
        with self._write_lock:
            cursor = self._conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("DELETE FROM users")
            cursor.execute("DELETE FROM config")
            for serialized in normalized['users'].values():
                cursor.execute(
                    """
                    INSERT INTO users (user_id, package_type, expiry_date, accounts_json, updated_at, webhookurl, limit_value, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        package_type=excluded.package_type,
                        expiry_date=excluded.expiry_date,
                        accounts_json=excluded.accounts_json,
                        updated_at=excluded.updated_at,
                        webhookurl=excluded.webhookurl,
                        limit_value=excluded.limit_value,
                        is_active=excluded.is_active
                    """,
                    self._user_payload(serialized)
                )
            for config_id, config_data in normalized['config'].items():
                cursor.execute(
                    """
                    INSERT INTO config (id, data_json, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        data_json=excluded.data_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        config_id,
                        json.dumps(config_data, ensure_ascii=False),
                        _dt_to_iso(datetime.now(timezone.utc))
                    )
                )
            cursor.execute("UPDATE schema_version SET version = ? WHERE id = 1", (normalized['schema_version'],))
            self._conn.commit()
        _cache_clear_all()

    def export_state(self) -> dict:
        with self._read_lock:
            cursor = self._read_conn.cursor()
            cursor.execute("SELECT version FROM schema_version WHERE id = 1")
            row = cursor.fetchone()
            state = {
                'schema_version': row['version'] if row and row['version'] else self.SCHEMA_VERSION,
                'users': {},
                'config': {}
            }
            cursor.execute("SELECT * FROM users")
            for user_row in cursor.fetchall():
                serialized = self._row_to_serialized_user(user_row)
                state['users'][serialized['user_id']] = serialized
            cursor.execute("SELECT * FROM config")
            for cfg_row in cursor.fetchall():
                data = json.loads(cfg_row['data_json']) if cfg_row['data_json'] else None
                state['config'][cfg_row['id']] = data
            return state

    def get_schema_version(self) -> str:
        with self._read_lock:
            cursor = self._read_conn.cursor()
            cursor.execute("SELECT version FROM schema_version WHERE id = 1")
            row = cursor.fetchone()
            return row['version'] if row and row['version'] else self.SCHEMA_VERSION

    def get_user(self, user_id):
        user_id = str(user_id)
        cache_key = _user_cache_key(user_id)
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        with self._read_lock:
            cursor = self._read_conn.cursor()
            cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
        if not row:
            return None
        serialized = self._row_to_serialized_user(row)
        user = self._deserialize_user(serialized)
        if user:
            _cache_set(cache_key, user, ttl=30)
        return user

    def get_all_users(self) -> List[dict]:
        cached = _cache_get("users::all")
        if cached is not None:
            return cached
        with self._read_lock:
            cursor = self._read_conn.cursor()
            cursor.execute("SELECT * FROM users")
            rows = cursor.fetchall()
        users = []
        for row in rows:
            serialized = self._row_to_serialized_user(row)
            deserialized = self._deserialize_user(serialized)
            if deserialized:
                users.append(deserialized)
        if users:
            _cache_set("users::all", users, ttl=10)
        return users

    def count_users_fast(self) -> int:
        cached = _cache_get("users::count")
        if cached is not None:
            try:
                return int(cached)
            except Exception:
                pass
        with self._read_lock:
            cursor = self._read_conn.cursor()
            cursor.execute("SELECT COUNT(1) AS total FROM users")
            row = cursor.fetchone()
            total = int(row['total']) if row else 0
        _cache_set("users::count", total, ttl=10)
        return total

    def add_user(self, user_data):
        serialized = self._serialize_user(user_data)
        if not serialized:
            return False
        with self._write_lock:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                INSERT INTO users (user_id, package_type, expiry_date, accounts_json, updated_at, webhookurl, limit_value, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    package_type=excluded.package_type,
                    expiry_date=excluded.expiry_date,
                    accounts_json=excluded.accounts_json,
                    updated_at=excluded.updated_at,
                    webhookurl=excluded.webhookurl,
                    limit_value=excluded.limit_value,
                    is_active=excluded.is_active
                """,
                self._user_payload(serialized)
            )
            self._conn.commit()
        user_obj = self._deserialize_user(serialized)
        if user_obj:
            _cache_set(_user_cache_key(serialized['user_id']), user_obj, ttl=30)
        _invalidate_global_user_cache()
        return True

    def delete_user(self, user_id):
        with self._write_lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM users WHERE user_id = ?", (str(user_id),))
            deleted = cursor.rowcount
            self._conn.commit()
        _invalidate_user_cache(str(user_id))
        return deleted

    def get_all_active_channels(self):
        active_channels = []
        for user in self.get_all_users():
            if not user or not user.get('is_active', True):
                continue
            for account in user.get('accounts', []) or []:
                for channel in account.get('channels', []) or []:
                    if channel.get('is_active'):
                        active_channels.append({
                            'user_id': user['user_id'],
                            'account': account,
                            'channel': channel
                        })
        return active_channels

    def _update_user_accounts(self, user_id, updater):
        user = self.get_user(user_id)
        if not user:
            return False
        if updater(user):
            return self.add_user(user)
        return False

    def update_account_autoreply_settings(self, user_id, token, new_config):
        def _updater(user):
            changed = False
            for account in user.get('accounts', []) or []:
                if account.get('token') == token:
                    ar_cfg = _ensure_autoreply_defaults(account.setdefault('autoreply', {'isactive': False}))
                    ar_cfg.update(new_config)
                    changed = True
                    break
            return changed
        return self._update_user_accounts(user_id, _updater)

    def update_autoreply_status(self, user_id, token, is_active):
        return self._update_user_accounts(
            user_id,
            lambda user: self._set_autoreply_flag(user, token, is_active)
        )

    def _set_autoreply_flag(self, user, token, is_active):
        for account in user.get('accounts', []) or []:
            if account.get('token') == token:
                ar_cfg = _ensure_autoreply_defaults(account.setdefault('autoreply', {'isactive': False}))
                ar_cfg['isactive'] = is_active
                return True
        return False

    def add_replied_user(self, user_id, token, channel_id):
        return self._update_user_accounts(user_id, lambda user: self._append_unique(user, token, 'replied', channel_id))

    def add_autoreply_error(self, user_id, token, channel_id):
        return self._update_user_accounts(user_id, lambda user: self._append_unique(user, token, 'error', channel_id))

    def add_pending_channel(self, user_id, token, channel_id):
        return self._update_user_accounts(user_id, lambda user: self._append_unique(user, token, 'pending', channel_id))

    def _append_unique(self, user, token, list_name, value):
        for account in user.get('accounts', []) or []:
            if account.get('token') == token:
                ar_cfg = _ensure_autoreply_defaults(account.setdefault('autoreply', {'isactive': False}))
                if value not in ar_cfg[list_name]:
                    ar_cfg[list_name].append(value)
                return True
        return False

    def remove_pending_channel(self, user_id, token, channel_id):
        def _updater(user):
            for account in user.get('accounts', []) or []:
                if account.get('token') == token:
                    ar_cfg = _ensure_autoreply_defaults(account.setdefault('autoreply', {'isactive': False}))
                    before = len(ar_cfg['pending'])
                    ar_cfg['pending'] = [ch for ch in ar_cfg['pending'] if ch != channel_id]
                    return before != len(ar_cfg['pending'])
            return False
        return self._update_user_accounts(user_id, _updater)

    def increment_sent_count(self, user_id, account_user_id, channel_id):
        return self.increment_sent_count_by(user_id, account_user_id, channel_id, 1)

    def increment_sent_count_by(self, user_id, account_user_id, channel_id, amount: int):
        try:
            amount = int(amount)
        except Exception:
            amount = 1
        if amount <= 0:
            amount = 1

        def _updater(user):
            for account in user.get('accounts', []) or []:
                if str(account.get('user_id')) == str(account_user_id):
                    for channel in account.get('channels', []) or []:
                        if str(channel.get('channel_id')) == str(channel_id):
                            channel['sent_count'] = channel.get('sent_count', 0) + amount
                            return True
            return False
        return self._update_user_accounts(user_id, _updater)

    def stop_all_active_channels(self):
        for user in self.get_all_users():
            changed = False
            for account in user.get('accounts', []) or []:
                for channel in account.get('channels', []) or []:
                    if channel.get('is_active'):
                        channel['is_active'] = False
                        changed = True
            if changed:
                self.add_user(user)

    def stop_all_active_autoreplies(self):
        for user in self.get_all_users():
            changed = False
            for account in user.get('accounts', []) or []:
                ar_cfg = _ensure_autoreply_defaults(account.setdefault('autoreply', {'isactive': False}))
                if ar_cfg.get('isactive'):
                    ar_cfg['isactive'] = False
                    changed = True
            if changed:
                self.add_user(user)

    def reset_all_replied_lists(self):
        for user in self.get_all_users():
            changed = False
            for account in user.get('accounts', []) or []:
                ar_cfg = _ensure_autoreply_defaults(account.setdefault('autoreply', {}))
                if ar_cfg.get('replied'):
                    ar_cfg['replied'] = []
                    changed = True
            if changed:
                self.add_user(user)

    def reset_replied_list_for_account(self, user_id, token):
        return self._update_user_accounts(
            user_id,
            lambda user: self._reset_replied(user, token)
        )

    def _reset_replied(self, user, token):
        for account in user.get('accounts', []) or []:
            if account.get('token') == token:
                ar_cfg = _ensure_autoreply_defaults(account.setdefault('autoreply', {}))
                if ar_cfg.get('replied'):
                    ar_cfg['replied'] = []
                return True
        return False

    def update_discount(self, discount_data):
        if discount_data is None:
            return False
        discount_copy = copy.deepcopy(discount_data)
        if discount_copy.get('expiry'):
            discount_copy['expiry'] = _dt_to_iso(discount_copy['expiry'])
        return self._upsert_config('discount_config', discount_copy)

    def get_discount(self):
        data = self._load_config('discount_config')
        if data and data.get('expiry'):
            data['expiry'] = _parse_datetime(data['expiry'])
        return data

    def remove_discount(self):
        return self._delete_config('discount_config')

    def get_status_log_config(self):
        return self._load_config('status_log_config')

    def update_status_log_config(self, message_id):
        payload = {'logmessageid': message_id}
        return self._upsert_config('status_log_config', payload)

    def _upsert_config(self, config_id: str, data: dict):
        with self._write_lock:
            cursor = self._conn.cursor()
            cursor.execute(
                """
                INSERT INTO config (id, data_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    data_json=excluded.data_json,
                    updated_at=excluded.updated_at
                """,
                (config_id, json.dumps(data, ensure_ascii=False), _dt_to_iso(datetime.now(timezone.utc)))
            )
            self._conn.commit()
        return True

    def _load_config(self, config_id: str):
        with self._read_lock:
            cursor = self._read_conn.cursor()
            cursor.execute("SELECT data_json FROM config WHERE id = ?", (config_id,))
            row = cursor.fetchone()
        if not row:
            return None
        data_json = row['data_json']
        if not data_json:
            return None
        try:
            return json.loads(data_json)
        except json.JSONDecodeError:
            logger.exception('Unhandled exception in %s', __name__)
            return None

    def _delete_config(self, config_id: str):
        with self._write_lock:
            cursor = self._conn.cursor()
            cursor.execute("DELETE FROM config WHERE id = ?", (config_id,))
            deleted = cursor.rowcount
            self._conn.commit()
        return deleted

    def delete_expired_trial_users(self):
        now = datetime.now(timezone.utc)
        updated = 0
        for user in self.get_all_users():
            expiry = user.get('expiry_date')
            package = (user.get('package_type') or '').lower()
            if 'trial' in package and expiry and expiry < now:
                if user.get('is_active', True):
                    user['is_active'] = False
                    self.add_user(user)
                    updated += 1
        return updated

    def get_expiring_trial_users(self):
        now = datetime.now(timezone.utc)
        limit_time = now + timedelta(days=1)
        expiring = []
        for user in self.get_all_users():
            expiry = user.get('expiry_date')
            package = (user.get('package_type') or '').lower()
            if 'trial' in package and expiry and now <= expiry < limit_time:
                expiring.append(user)
        return expiring


class MongoBackup:
    def __init__(self, mongo_uri: str, db_name: Optional[str] = None):
        configure_dns()
        configure_ssl()
        client_kwargs = dict(
            server_api=ServerApi('1'),
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=8000,
            socketTimeoutMS=10000,
            maxPoolSize=10,
            minPoolSize=1,
            retryWrites=True,
            appname="Cluster0",
        )
        if mongo_uri.startswith("mongodb+srv://"):
            client_kwargs["tls"] = True
        self.client = MongoClient(mongo_uri, **client_kwargs)
        self.db = None
        target_db = db_name
        if not target_db:
            try:
                parsed = parse_uri(mongo_uri, warn=False)
                target_db = parsed.get('database')
            except Exception as exc:
                logger.exception("Failed to parse Mongo URI: %s", exc)
        if target_db:
            self.db = self.client[target_db]
        else:
            try:
                self.db = self.client.get_default_database()
            except ConfigurationError:
                logger.error("Mongo URI missing default database, using fallback 'autopostdbuser'.")
                self.db = self.client['autopostdbuser']
            except Exception as exc:
                logger.exception("Unexpected error selecting default Mongo database: %s", exc)
                self.db = self.client['autopostdbuser']
        self.users = self.db['users']
        self.config = self.db['config']

    def close(self):
        try:
            self.client.close()
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)

    def get_backup_state(self):
        try:
            return self.config.find_one({'_id': 'backup_state'})
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            return None

    def clear_all_for_fresh_backup(self):
        try:
            self.users.delete_many({})
            self.config.delete_many({'_id': {'$ne': 'backup_state'}})
            return True
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            return False

    def mark_backup_initialized(self, schema_version=SQLITE_SCHEMA_VERSION):
        try:
            self.config.update_one(
                {'_id': 'backup_state'},
                {'$set': {'initialized': True, 'schema_version': schema_version, 'updated_at': datetime.now(timezone.utc)}},
                upsert=True
            )
            return True
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            return False

    def backup_json_state(self, json_state: dict):
        if not isinstance(json_state, dict):
            return False
        try:
            payload = {
                '_id': 'autopostdb_json',
                'data': json_state,
                'updated_at': datetime.now(timezone.utc)
            }
            self.config.replace_one({'_id': 'autopostdb_json'}, payload, upsert=True)
            return True
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            return False

    def fetch_json_state(self):
        try:
            doc = self.config.find_one({'_id': 'autopostdb_json'})
            if doc and isinstance(doc.get('data'), dict):
                return doc['data']
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
        return None

    def import_all_users_from_mongo(self):
        try:
            users = []
            for doc in self.users.find({}):
                doc.pop('_id', None)
                users.append(doc)
            return users
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            return []

    def import_config_from_mongo(self):
        try:
            configs: Dict[str, Any] = {}
            for doc in self.config.find({}):
                cfg_id = doc.get('_id')
                if not cfg_id or cfg_id == 'autopostdb_json':
                    continue
                clean_doc = {k: v for k, v in doc.items() if k != '_id'}
                configs[cfg_id] = clean_doc
            return configs
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            return {}

    def backup_user_to_mongo(self, user_data: dict):
        if not isinstance(user_data, dict):
            return False
        user_backup = copy.deepcopy(user_data)
        try:
            if user_backup.get('expiry_date') and isinstance(user_backup['expiry_date'], datetime):
                user_backup['expiry_date'] = user_backup['expiry_date'].isoformat()
            if user_backup.get('updated_at') and isinstance(user_backup['updated_at'], datetime):
                user_backup['updated_at'] = user_backup['updated_at'].isoformat()
            self.users.replace_one({'user_id': user_backup['user_id']}, user_backup, upsert=True)
            return True
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            return False

    def backup_all_users_to_mongo(self, users_data: List[dict]):
        try:
            for user in users_data:
                self.backup_user_to_mongo(user)
            return True
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            return False

    def backup_config_to_mongo(self, config_data: Dict[str, dict]):
        try:
            for config_id, config_item in config_data.items():
                payload = copy.deepcopy(config_item)
                if isinstance(payload, dict) and payload.get('expiry') and isinstance(payload['expiry'], datetime):
                    payload['expiry'] = payload['expiry'].isoformat()
                self.config.replace_one({'_id': config_id}, payload, upsert=True)
            return True
        except Exception:
            logger.exception('Unhandled exception in %s', __name__)
            return False


_db_instance: Optional[SQLiteDatabase] = None
_mongo_uri_cfg: Optional[str] = None
_mongo_db_name_cfg: Optional[str] = None
_mongo_backup_enabled: bool = False


def _to_bool(val, default=False):
    try:
        if isinstance(val, bool):
            return val
        if val is None:
            return default
        return str(val).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        logger.exception('Unhandled exception in %s', __name__)
        return default


def init_database(config):
    global _db_instance, _mongo_uri_cfg, _mongo_db_name_cfg, _mongo_backup_enabled

    _mongo_uri_cfg = config.get('mongo_uri') or os.getenv('MONGO_URI') or ""
    _mongo_db_name_cfg = config.get('mongo_db_name')
    _mongo_backup_enabled = _to_bool(config.get('mongo_backup_enabled', os.getenv('MONGO_BACKUP_ENABLED', False))) and bool(_mongo_uri_cfg)

    sqlite_exists_before = os.path.exists(DB_SQLITE_FILENAME)
    json_exists_before = os.path.exists(DB_JSON_FILENAME)

    state_from_json = _load_state_from_json(DB_JSON_FILENAME) if json_exists_before else None
    state_from_mongo = None
    mongo_users: List[dict] = []
    mongo_config: Dict[str, dict] = {}
    restored_from_mongo_records = False
    mongo_cleanup_done = False

    if not sqlite_exists_before:
        print("autopost.db not found. Preparing fresh SQLite database ...")
        if state_from_json:
            print("Legacy autopostdb.json detected. Migrating data to SQLite ...")
        elif _mongo_backup_enabled and _mongo_uri_cfg:
            print("Attempting to restore from MongoDB backup ...")
            mongo_client = None
            try:
                mongo_db = MongoBackup(_mongo_uri_cfg, _mongo_db_name_cfg)
                mongo_client = mongo_db
                mongo_db.client.admin.command('ping', maxTimeMS=5000)
                backup_state = mongo_db.get_backup_state()
                if backup_state and backup_state.get('initialized'):
                    state_from_mongo = mongo_db.fetch_json_state()
                    if state_from_mongo:
                        print("Fetched JSON snapshot from MongoDB backup.")
                    else:
                        mongo_users = mongo_db.import_all_users_from_mongo()
                        mongo_config = mongo_db.import_config_from_mongo()
                        restored_from_mongo_records = True
                        print("Fetched structured backup documents from MongoDB.")
                else:
                    print("No initialized backup found in MongoDB.")
                if state_from_mongo or restored_from_mongo_records:
                    mongo_db.clear_all_for_fresh_backup()
                    mongo_cleanup_done = True
            except Exception as exc:
                logger.exception("MongoDB restore failed: %s", exc)
            finally:
                if mongo_client:
                    mongo_client.close()
        else:
            print("MongoDB backup not enabled. Creating brand new SQLite database.")
    else:
        print("autopost.db detected. Skipping migration step.")

    _db_instance = SQLiteDatabase(DB_SQLITE_FILENAME)

    if state_from_json and not sqlite_exists_before:
        _db_instance.replace_state(state_from_json)
        _remove_file_if_exists(DB_JSON_FILENAME)
        print("Migrated data from autopostdb.json into SQLite.")
    elif state_from_mongo:
        _db_instance.replace_state(state_from_mongo)
        print("Restored SQLite database from MongoDB JSON snapshot.")
    elif restored_from_mongo_records:
        for user_data in mongo_users:
            _db_instance.add_user(user_data)
        for config_id, config_data in mongo_config.items():
            clean_config = config_data.copy() if isinstance(config_data, dict) else {}
            if config_id == 'discount_config':
                _db_instance.update_discount(clean_config)
            elif config_id == 'status_log_config':
                _db_instance.update_status_log_config(clean_config.get('logmessageid'))
            else:
                _db_instance._upsert_config(config_id, clean_config)
        print("Reconstructed SQLite database from MongoDB collections.")
    elif not sqlite_exists_before:
        print("Created fresh SQLite database (autopost.db).")
    elif state_from_json and sqlite_exists_before:
        _remove_file_if_exists(DB_JSON_FILENAME)
        print("Ignored legacy autopostdb.json because autopost.db already exists. Removed legacy file.")

    if mongo_cleanup_done:
        print("Legacy MongoDB backup cleared to prepare for the new SQLite snapshot.")

    if _mongo_backup_enabled and _mongo_uri_cfg:
        print("Triggering MongoDB backup for SQLite state ...")
        success = backup_to_mongo()
        if success:
            print("MongoDB backup succeeded.")
        else:
            logger.error("MongoDB backup failed after initialization.")


def get_db() -> SQLiteDatabase:
    if _db_instance is None:
        raise RuntimeError("Database has not been initialized. Call init_database() first.")
    return _db_instance


def describe_backend() -> str:
    if _db_instance is None:
        return "uninitialized"
    return type(_db_instance).__name__


def get_mongo_db():
    return None


def backup_to_mongo():
    if not _db_instance or not _mongo_backup_enabled or not _mongo_uri_cfg:
        return False
    max_retries = 5
    retry_delay = 5
    last_error = None
    for attempt in range(1, max_retries + 1):
        mongo_db = None
        try:
            mongo_db = MongoBackup(_mongo_uri_cfg, _mongo_db_name_cfg)
            mongo_db.client.admin.command('ping', maxTimeMS=5000)
            local_schema_version = _db_instance.get_schema_version()
            backup_state = mongo_db.get_backup_state()
            schema_match = backup_state and backup_state.get('schema_version') == local_schema_version
            if not schema_match:
                mongo_db.clear_all_for_fresh_backup()
            users = _db_instance.get_all_users()
            mongo_db.backup_all_users_to_mongo(users)
            config_payload = {}
            discount = _db_instance.get_discount()
            if discount:
                discount_copy = discount.copy()
                if discount_copy.get('expiry') and isinstance(discount_copy['expiry'], datetime):
                    discount_copy['expiry'] = discount_copy['expiry']
                config_payload['discount_config'] = discount_copy
            status_config = _db_instance.get_status_log_config()
            if status_config:
                config_payload['status_log_config'] = status_config
            if config_payload:
                mongo_db.backup_config_to_mongo(config_payload)
            mongo_db.backup_json_state(_db_instance.export_state())
            mongo_db.mark_backup_initialized(local_schema_version)
            return True
        except Exception as exc:
            last_error = exc
            logger.exception("MongoDB backup attempt %d failed: %s", attempt, exc)
            if attempt < max_retries:
                time.sleep(retry_delay)
                continue
            return False
        finally:
            if mongo_db:
                mongo_db.close()
    if last_error:
        logger.error("MongoDB backup failed after %d attempts: %s", max_retries, last_error)
    return False


async def run_db(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)
