import json
import hashlib
import sqlite3
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from src.output.formatters import FormattedTrade

def event_key(trade: FormattedTrade) -> str:
    parts = {
        'proxyWallet': trade.wallet,
        'timestamp': trade.timestamp,
        'transactionHash': trade.tx_hash,
        'asset': trade.token_id,
        'side': trade.side,
        'size': trade.tokens,
        'usdcSize': trade.total_usdc,
        'price': trade.price,
    }
    raw = json.dumps(parts, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw.encode()).hexdigest()

with open("wallets.json", 'r') as f:
    wallets = json.load(f)

class TrackerDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self.init_db()

    def init_db(self) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                '''
                CREATE TABLE IF NOT EXISTS events (
                    event_key TEXT PRIMARY KEY,
                    wallet TEXT NOT NULL,
                    user_name TEXT,
                    received_at_utc TEXT NOT NULL,
                    received_at_unix INTEGER NOT NULL,
                    api_timestamp INTEGER,
                    api_timestamp_utc TEXT,
                    latency_seconds INTEGER,
                    type TEXT,
                    side TEXT,
                    condition_id TEXT,
                    transaction_hash TEXT,
                    asset TEXT,
                    outcome_index INTEGER,
                    outcome TEXT,
                    title TEXT,
                    slug TEXT,
                    event_slug TEXT,
                    icon TEXT,
                    size REAL,
                    usdc_size REAL,
                    price REAL,
                    raw_json TEXT NOT NULL
                )
                '''
            )
            cur.execute('CREATE INDEX IF NOT EXISTS idx_events_wallet_api_ts ON events(wallet, api_timestamp DESC)')
            cur.execute(
                '''
                CREATE TABLE IF NOT EXISTS poll_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    wallet TEXT NOT NULL,
                    user_name TEXT,
                    polled_at_utc TEXT NOT NULL,
                    page_index INTEGER NOT NULL,
                    offset_value INTEGER NOT NULL,
                    returned_count INTEGER NOT NULL,
                    inserted_count INTEGER NOT NULL,
                    newest_api_timestamp INTEGER,
                    oldest_api_timestamp INTEGER,
                    status TEXT NOT NULL,
                    error TEXT
                )
                '''
            )
            cur.execute(
                '''
                CREATE TABLE IF NOT EXISTS tracker_state (
                    wallet TEXT PRIMARY KEY,
                    user_name TEXT,
                    last_polled_at_utc TEXT,
                    last_max_api_timestamp INTEGER,
                    last_new_count INTEGER DEFAULT 0,
                    total_events INTEGER DEFAULT 0
                )
                '''
            )
            self.conn.commit()

    def get_wallet_state(self, wallet: str) -> Optional[sqlite3.Row]:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT * FROM tracker_state WHERE wallet = ?', (wallet,))
            return cur.fetchone()

    def upsert_wallet_state(self, wallet: str, user_name: str, last_polled_at_utc: str, last_max_api_timestamp: Optional[int], last_new_count: int) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute('SELECT COUNT(*) FROM events WHERE wallet = ?', (wallet,))
            total_events = cur.fetchone()[0]
            cur.execute(
                '''
                INSERT INTO tracker_state (wallet, user_name, last_polled_at_utc, last_max_api_timestamp, last_new_count, total_events)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    user_name=excluded.user_name,
                    last_polled_at_utc=excluded.last_polled_at_utc,
                    last_max_api_timestamp=excluded.last_max_api_timestamp,
                    last_new_count=excluded.last_new_count,
                    total_events=excluded.total_events
                ''',
                (wallet, user_name, last_polled_at_utc, last_max_api_timestamp, last_new_count, total_events),
            )
            self.conn.commit()

    def insert_event(self, trade: FormattedTrade, received_at) -> bool:
        key = event_key(trade)
        received_unix = int(received_at.timestamp())
        api_ts = datetime.fromisoformat(trade.timestamp)
        api_ts_int = int(api_ts.timestamp()) if api_ts is not None else None
        api_ts_iso = datetime.fromtimestamp(api_ts_int, tz=timezone.utc).isoformat().replace('+00:00', 'Z')
        latency = received_unix - api_ts_int if api_ts_int is not None else None
        userName = wallets.get(trade.wallet)['userName']
        with self.lock:
            cur = self.conn.cursor()
            cur.execute(
                '''
                INSERT OR IGNORE INTO events (
                    event_key, wallet, user_name, received_at_utc, received_at_unix,
                    api_timestamp, api_timestamp_utc, latency_seconds,
                    type, side, condition_id, transaction_hash, asset,
                    outcome_index, outcome, title, slug, event_slug, icon,
                    size, usdc_size, price, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    key,
                    trade.wallet,
                    userName,
                    received_at.isoformat().replace('+00:00', 'Z'),
                    received_unix,
                    api_ts_int,
                    api_ts_iso,
                    latency,
                    'Trade',
                    trade.side,
                    "NA",
                    trade.tx_hash,
                    trade.token_id,
                    "NA",
                    "NA",
                    "NA",
                    "NA",
                    "NA",
                    "NA",
                    trade.tokens,
                    trade.total_usdc,
                    trade.price,
                    json.dumps({}),
                ),
            )
            self.conn.commit()
            return cur.rowcount > 0