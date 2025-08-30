from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Dict, Any, Optional

import pandas as pd


# --- Paths -------------------------------------------------------------------
ROOT: Path = Path(__file__).resolve().parents[1]
DATA: Path = ROOT / "data"
L1: Path = DATA / "l1"
WEB_FEED: Path = ROOT / "outputs" / "web_feed"
WEB_FEED.mkdir(parents=True, exist_ok=True)

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def write_json(path: Path | str, obj: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def read_items_forgiving(path: Path | str) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            if isinstance(data, list):
                return data
            # allow {"items":[...]}
            if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
                return data["items"]
        except Exception:
            return []
    return []

# --- L1 loading ---------------------------------------------------------------
def _l1_files(kind: str) -> list[Path]:
    d = L1 / kind
    if not d.exists():
        return []
    return sorted(d.glob("*.json"))

def read_l1_latest(kind: str) -> Optional[pd.DataFrame]:
    """Read latest L1 jsonl for a given kind; returns None if missing."""
    files = _l1_files(kind)
    if not files:
        return None
    # pick the newest by modified time
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    fp = files[0]
    records: list[dict] = []
    with fp.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                continue
    if not records:
        return None
    return pd.DataFrame.from_records(records)

# --- Suggestion shaping -------------------------------------------------------
def _pick(df_row: dict, *candidates: str, default: str = "") -> str:
    for c in candidates:
        if c in df_row and pd.notna(df_row[c]):
            return str(df_row[c])
    return default

def base_suggestion_fields(df: pd.DataFrame, strategy: str) -> list[dict]:
    """
    Schema-agnostic shaping: carry everything forward + minimal normalized fields
    strategy: e.g., 'vertical_bull_call'
    """
    items: list[dict] = []
    for _, row in df.iterrows():
        r = dict(row)  # carry all original columns through
        # minimal normalized fields used by web/alerts
        symbol = _pick(r, "Symbol", "Underlying Symbol", "UnderlyingSymbol", "Underlying", default="")
        expiry = _pick(r, "Expiration Date", "Expiration", "Expiry", default="—")
        r_norm = {
            "symbol": symbol,
            "strategy": strategy,
            "expiry": expiry,
            "score": 0.0,  # keep simple; rankers can post-process if needed
        }
        # Place normalized keys first, then the full record under 'raw'
        items.append({**r_norm, "raw": r})
    return items
