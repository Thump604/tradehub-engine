#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rank/Base helpers for schema-on-read (L1 JSON) pipeline.
This file is intentionally small and stable.
"""

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Tuple
import json
import math
import datetime as dt

import pandas as pd

# Paths
ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
L1 = DATA / "l1"
ARCHIVE = DATA / "archive"     # kept for backward compat (some scripts import)
BRONZE = DATA / "processed"    # kept for backward compat (some scripts import)
OUTPUTS = ROOT / "outputs"
WEB_FEED = OUTPUTS / "web_feed"

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def write_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def read_json_items(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with open(path, "r") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and "items" in data and isinstance(data["items"], list):
        return data["items"]
    return []

def read_l1(kind: str) -> Tuple[pd.DataFrame | None, Dict[str, Any]]:
    """
    Load most recent L1 JSON file for a given kind (folder under data/l1).
    Returns (DataFrame or None, meta).
    """
    folder = L1 / kind
    files = sorted(folder.glob("*.json"))
    if not files:
        return None, {"kind": kind, "source": None, "ts": None}
    latest = files[-1]
    try:
        df = pd.read_json(latest, orient="records", dtype=False)
    except ValueError:
        # Fall back for newline-delimited JSON
        df = pd.read_json(latest, lines=True, dtype=False)
    meta = {"kind": kind, "source": latest.name, "ts": None}
    return df, meta

# ---- Backward-compat shim for older rankers expecting CSV loader ----
def load_barchart_csv_any(*args, **kwargs) -> pd.DataFrame:
    """
    Legacy helper expected by older rankers. We no longer use CSV; to avoid import crashes
    we return an empty DataFrame. New rankers should use read_l1(kind).
    """
    return pd.DataFrame()

def read_items_forgiving(path: Path | str) -> tuple[list[dict[str, any]], dict[str, any]]:
    """Read a suggestions-like JSON file. Returns (items, meta).
    - If root is a list -> (list, {"source": ...})
    - If root is dict with "items" -> (items, rest_of_fields_as_meta)
    - On errors -> ([], {"source": ..., "error": ...})
    """
    import json as _json
    pth = Path(path)
    try:
        if not pth.exists():
            return [], {"source": str(pth), "error": "missing"}
        with open(pth, "r") as _f:
            data = _json.load(_f)
        if isinstance(data, list):
            return data, {"source": str(pth)}
        if isinstance(data, dict):
            items = data.get("items", [])
            meta = {k: v for k, v in data.items() if k != "items"}
            meta.setdefault("source", str(pth))
            if not isinstance(items, list):
                items = []
            return items, meta
        return [], {"source": str(pth), "error": "unexpected_root_type"}
    except Exception as _e:
        return [], {"source": str(pth), "error": repr(_e)}

def read_l1_latest(kind: str) -> tuple["pd.DataFrame | None", dict[str, any]]:
    """Alias for read_l1(kind)."""
    return read_l1(kind)

def base_suggestion_fields(
    *,
    symbol: str | None,
    strategy: str,
    expiry: str | None = None,
    score: float | int = 0,
) -> dict[str, any]:
    """Normalize suggestion fields for legacy rankers."""
    return {
        "symbol": (symbol or "UNKNOWN").upper(),
        "strategy": strategy,
        "expiry": expiry or "—",
        "score": float(score),
    }


def base_suggestion_fields_kw(
    *,
    symbol: str | None,
    strategy: str,
    expiry: str | None = None,
    score: float | int = 0,
) -> dict[str, float | str]:
    """Modern helper: returns a single normalized suggestion dict."""
    return {
        "symbol": (symbol or "UNKNOWN").upper(),
        "strategy": strategy,
        "expiry": expiry or "—",
        "score": float(score),
    }

def base_suggestion_fields(*args, **kwargs):
    """Compatibility wrapper.

    Legacy positional form:
        base_suggestion_fields(df: pd.DataFrame, strategy: str) -> list[dict]

    Modern keyword form:
        base_suggestion_fields(symbol=..., strategy=..., expiry=..., score=...) -> dict
    """
    # Legacy: DataFrame in positional args
    if args and hasattr(args[0], "iterrows"):
        import math
        df = args[0]
        strategy = args[1] if len(args) > 1 else kwargs.get("strategy", "unknown")
        items = []
        # try common column names
        sym_cols = ("symbol", "Symbol", "ticker", "Ticker")
        exp_cols = ("expiry", "Expiration", "expiration", "Exp", "exp")
        score_cols = ("score", "Score", "rank", "Rank")

        for _, row in df.iterrows():
            # symbol
            sym = None
            for c in sym_cols:
                if c in row and row[c]:
                    sym = str(row[c])
                    break
            # expiry
            exp = None
            for c in exp_cols:
                if c in row and row[c]:
                    exp = str(row[c])
                    break
            # score
            sc = 0.0
            for c in score_cols:
                if c in row and row[c] is not None and not (isinstance(row[c], float) and math.isnan(row[c])):
                    try:
                        sc = float(row[c])
                    except Exception:
                        sc = 0.0
                    break

            items.append({
                "symbol": (sym or "UNKNOWN").upper(),
                "strategy": strategy,
                "expiry": exp or "—",
                "score": float(sc),
            })
        return items

    # Modern: delegate to keyword-only version
    return base_suggestion_fields_kw(**kwargs)
