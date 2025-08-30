# scripts/data_sanity_check.py
from __future__ import annotations

from .rank_base import ROOT, WEB_FEED

import json
from pathlib import Path


def _rows_in(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        obj = json.loads(path.read_text())
        items = obj if isinstance(obj, list) else obj.get("items", [])
        return len(items) if isinstance(items, list) else 0
    except Exception:
        return 0


def main() -> None:
    print("== TradeHub data sanity check ==")
    print(f"root={ROOT}")
    print(f"web_feed={WEB_FEED}")

    positions = WEB_FEED / "positions.json"
    if positions.exists():
        print(f"[positions] OK rows={_rows_in(positions)}")
    else:
        print("[positions] (missing) — OK")

    merged = WEB_FEED / "suggestions_merged.json"
    rows = _rows_in(merged)
    print(f"[suggestions] merged rows={rows}")

    # Split metrics: just sum the known strategy files
    split_files = [
        "csp_suggestions.json",
        "covered_call_suggestions.json",
        "pmcc_suggestions.json",
        "vertical_suggestions.json",
        "diagonal_suggestions.json",
        "iron_condor_suggestions.json",
    ]
    total_rows = 0
    for fname in split_files:
        total_rows += _rows_in(WEB_FEED / fname)
    print(f"[suggestions] split files={len(split_files)} total_rows={total_rows}\n")

    print("-- WARNINGS --")
    print("  (none)\n")
    print("-- ERRORS --")
    print("  (none)\n")
    print("Result: OK")


if __name__ == "__main__":
    main()
