# tickets_db.py
import os, sys, json, sqlite3
from pathlib import Path
from typing import Dict, Any, List
import datetime as dt
from datetime import timezone, timedelta

# Fixed CST (no DST)
FIXED_CST = timezone(timedelta(hours=-6), name="CST")


def _appdata_dir() -> Path:
    if sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support" / "TradeHub"
    elif os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home())) / "TradeHub"
    else:
        base = Path.home() / ".local" / "share" / "tradehub"
    base.mkdir(parents=True, exist_ok=True)
    return base


DB_PATH = _appdata_dir() / "tradehub.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS tickets(
  id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  strategy TEXT NOT NULL,
  side TEXT,
  opened_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  params_json TEXT NOT NULL,
  site_score REAL,
  source TEXT
);
CREATE TABLE IF NOT EXISTS fills(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticket_id TEXT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
  ts TEXT NOT NULL,
  action TEXT NOT NULL,
  qty INTEGER NOT NULL,
  price REAL NOT NULL,
  notes TEXT
);
CREATE TABLE IF NOT EXISTS notes(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ticket_id TEXT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
  ts TEXT NOT NULL,
  body TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tickets_symbol ON tickets(symbol);
CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(status);
"""


def init_db() -> None:
    with _connect() as c:
        for stmt in SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                c.execute(s + ";")


def _now_ct_str() -> str:
    return dt.datetime.now(FIXED_CST).strftime("%Y-%m-%d %I:%M %p CT")


def create_ticket(
    ticket_id: str,
    symbol: str,
    strategy: str,
    params: Dict[str, Any],
    site_score: float | None,
    source: str,
    side: str | None = None,
) -> None:
    with _connect() as c:
        c.execute(
            "INSERT OR REPLACE INTO tickets(id, symbol, strategy, side, opened_at, status, params_json, site_score, source) "
            "VALUES (?, ?, ?, ?, ?, 'open', ?, ?, ?)",
            (
                ticket_id,
                symbol.upper(),
                strategy,
                side,
                _now_ct_str(),
                json.dumps(params),
                site_score,
                source,
            ),
        )


def list_tickets(limit: int = 500) -> List[Dict[str, Any]]:
    with _connect() as c:
        rows = c.execute(
            "SELECT id, symbol, strategy, status, opened_at FROM tickets "
            "ORDER BY opened_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [
        {
            "id": r[0],
            "symbol": r[1],
            "strategy": r[2],
            "status": r[3],
            "opened_at": r[4],
        }
        for r in rows
    ]


def add_fill(
    ticket_id: str, action: str, qty: int, price: float, notes: str = ""
) -> None:
    with _connect() as c:
        c.execute(
            "INSERT INTO fills(ticket_id, ts, action, qty, price, notes) VALUES(?,?,?,?,?,?)",
            (ticket_id, _now_ct_str(), action, qty, price, notes),
        )


def add_note(ticket_id: str, body: str) -> None:
    with _connect() as c:
        c.execute(
            "INSERT INTO notes(ticket_id, ts, body) VALUES(?,?,?)",
            (ticket_id, _now_ct_str(), body),
        )
