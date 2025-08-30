# scripts/rank_vertical_bull_put.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from .rank_base import BRONZE, WEB_FEED, write_json, read_json_items, clamp

SCREENER = "bull-put-spread-option-screener"
OUT_FILE = WEB_FEED / "vertical_bull_put_suggestions.json"


def _latest_day_dir() -> Path | None:
    base = BRONZE / SCREENER
    if not base.exists():
        return None
    days = sorted([p for p in base.iterdir() if p.is_dir()])
    return days[-1] if days else None


def _score(item: Dict[str, Any]) -> float:
    # For credit spreads, favor higher credit vs width (credit/width)
    width = None
    credit = None
    for k in ("spread_width", "width"):
        if k in item:
            try:
                width = float(item[k])
                break
            except Exception:
                pass
    for k in ("net_credit", "credit", "price"):
        if k in item:
            try:
                credit = float(item[k])
                break
            except Exception:
                pass
    if width and credit is not None and width > 0:
        eff = max(0.0, min(1.0, credit / width))
        return clamp(100.0 * eff)
    return 0.0


def main() -> None:
    day_dir = _latest_day_dir()
    if not day_dir:
        write_json(OUT_FILE, [])
        print(f"[saved] {OUT_FILE} (rows=0)")
        return

    items: List[Dict[str, Any]] = []
    for jsonl in sorted(day_dir.glob("*.jsonl")):
        rows = read_json_items(jsonl)
        for r in rows:
            sym = str(r.get("Underlying Symbol", r.get("symbol", ""))).upper()
            expiry = r.get("Expiration Date", r.get("expiry", "—"))
            item = {
                "symbol": sym,
                "strategy": "vertical_bull_put",
                "expiry": expiry,
                "score": _score(r),
                "__timestamp__": r.get("__timestamp__"),
                "__screener__": r.get("__screener__"),
                "__source_file__": r.get("__source_file__"),
            }
            items.append(item)

    write_json(OUT_FILE, items)
    print(f"[saved] {OUT_FILE} (rows={len(items)})")


if __name__ == "__main__":
    main()
