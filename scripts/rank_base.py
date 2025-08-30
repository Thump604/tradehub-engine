# scripts/rank_base.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, List, Sequence

import pandas as pd

# ---- Paths (shared)
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
L1_DIR = DATA_DIR / "l1"
WEB_FEED = ROOT / "outputs" / "web_feed"

def ensure_paths() -> None:
    WEB_FEED.mkdir(parents=True, exist_ok=True)
    L1_DIR.mkdir(parents=True, exist_ok=True)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def read_items_forgiving(path: Path) -> List[dict]:
    """
    Returns a list of dicts whether file is:
      - a JSON array, or
      - an object with 'items', or
      - newline-delimited JSON objects.
    """
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text[0] == "[":
        return json.loads(text)
    if text[0] == "{":
        obj = json.loads(text)
        if isinstance(obj, dict) and "items" in obj and isinstance(obj["items"], list):
            return obj["items"]
        # Might be a single object; wrap it
        return [obj]
    # Fallback: JSONL
    items = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            items.append(json.loads(line))
        except Exception:
            pass
    return items

def _pick(row: dict, keys: Sequence[str], default: Any = None) -> Any:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return default

def _to_iso_date(value: Any) -> str:
    if value in (None, ""):
        return "—"
    # Try pandas for robust parsing
    try:
        dt = pd.to_datetime(value, errors="coerce", utc=False)
        if pd.isna(dt):
            return str(value)
        # date only if time isn't needed
        return str(dt.date())
    except Exception:
        return str(value)

def load_l1(kind: str) -> List[dict]:
    """
    Load the most recent Layer-1 JSON for a given kind.
    If multiple exist, pick the freshest by modtime.
    """
    dir_ = L1_DIR / kind
    if not dir_.exists():
        return []
    files = sorted(dir_.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return []
    return read_items_forgiving(files[0])

# ----- Back-compat generic suggestion builder -------------------------------
def base_suggestion_fields(data: Any, strategy: str) -> List[dict]:
    """
    Back-compatible helper used by older rank scripts:
    - If 'data' is a DataFrame -> iterate rows.
    - If list[dict] -> iterate dicts.
    Output: {symbol, strategy, expiry, score}
    No schema assumptions beyond common field names.
    """
    # normalize into list of dicts
    rows: List[dict] = []
    if isinstance(data, pd.DataFrame):
        rows = data.to_dict(orient="records")
    elif isinstance(data, list):
        rows = [r for r in data if isinstance(r, dict)]
    elif isinstance(data, dict):
        rows = [data]
    else:
        rows = []

    out: List[dict] = []
    for r in rows:
        symbol = _pick(
            r,
            ["symbol", "Symbol", "Underlying Symbol", "Underlying", "Ticker", "ticker"],
            default="",
        )
        expiry = _pick(
            r,
            [
                "expiration",
                "Expiration",
                "Expiration Date",
                "Exp Date",
                "Expiry",
                "expiry",
                "expirationDate",
            ],
            default="—",
        )
        expiry_iso = _to_iso_date(expiry)
        # score: try a few plausible numeric fields; else 0.0
        score = _pick(
            r,
            ["score", "Score", "Return", "ROI", "Edge", "edge", "Rank", "rank"],
            default=0.0,
        )
        try:
            score = float(score)
        except Exception:
            score = 0.0
        out.append(
            {
                "symbol": str(symbol).upper() if symbol else "",
                "strategy": strategy,
                "expiry": expiry_iso,
                "score": score,
            }
        )
    return out