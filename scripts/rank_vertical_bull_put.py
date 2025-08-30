#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ranker: Vertical Bull Put (credit put spreads)
Reads L1 JSON and emits simple suggestions for web feed.
Output: outputs/web_feed/vertical_bull_put_suggestions.json
"""

from __future__ import annotations
from typing import Any, Dict, List
import math
from pathlib import Path
import pandas as pd

from .rank_base import WEB_FEED, read_l1, write_json

OUT = WEB_FEED / "vertical_bull_put_suggestions.json"

def _coalesce(row: Dict[str, Any], *names: str, default: Any = None) -> Any:
    for n in names:
        if n in row and pd.notna(row[n]):
            return row[n]
    return default

def _to_float(val: Any, default: float = math.nan) -> float:
    try:
        if isinstance(val, str):
            v = val.replace("%", "").replace(",", "").strip()
            return float(v)
        return float(val)
    except Exception:
        return default

def _score_row(r: Dict[str, Any]) -> float:
    # Prefer probability-of-profit; fall back to annualized return
    pop = _to_float(_coalesce(
        r, "Probability of Profit", "Chance of Profit", "Win %", "Win%", "Prob of Profit"
    ))
    if not math.isnan(pop) and pop > 0:
        return max(0.0, min(10.0, pop / 10.0))  # e.g., 80% -> 8.0

    ann = _to_float(_coalesce(
        r, "Annualized Return", "Annualized ROI", "Annualized Yield", "Return %"
    ))
    if not math.isnan(ann) and ann > 0:
        return max(0.0, min(10.0, ann / 10.0))

    return 1.0

def main() -> None:
    df, _ = read_l1("vertical_bull_put")
    items: List[Dict[str, Any]] = []

    if df is None or df.empty:
        write_json(OUT, [])
        print(f"[saved] {OUT} (items=0)")
        return

    for row in df.to_dict(orient="records"):
        symbol = _coalesce(row, "Underlying Symbol", "Symbol", "Underlying", "Ticker", default="UNKNOWN")
        expiry = _coalesce(row, "Expiration Date", "Expiration", "Expiry", default="—")
        items.append({
            "symbol": str(symbol).upper() if symbol else "UNKNOWN",
            "strategy": "vertical_bull_put",
            "expiry": str(expiry) if expiry else "—",
            "score": round(_score_row(row), 2),
        })

    # Keep best per (symbol, expiry)
    best: Dict[tuple, Dict[str, Any]] = {}
    for it in items:
        k = (it["symbol"], it["expiry"])
        if k not in best or it["score"] > best[k]["score"]:
            best[k] = it

    final = sorted(best.values(), key=lambda x: (-x["score"], x["symbol"]))
    write_json(OUT, final)
    print(f"[saved] {OUT} (items={len(final)})")

if __name__ == "__main__":
    main()
