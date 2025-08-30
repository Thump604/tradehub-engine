# scripts/risk_snapshot.py
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Dict, List

from .rank_base import WEB_FEED, read_items_forgiving, write_json

INP = WEB_FEED / "suggestions_merged.json"
OUT = WEB_FEED / "risk_snapshot.json"


def main() -> None:
    items, meta = read_items_forgiving(INP)
    items = [x for x in items if isinstance(x, dict)]

    by_strategy = Counter(s.get("strategy", "unknown") for s in items)
    by_symbol = Counter(s.get("symbol", "unknown") for s in items)
    top_symbols = by_symbol.most_common(25)

    snapshot = {
        "by_strategy": dict(by_strategy),
        "top_symbols": [{"symbol": k, "count": v} for k, v in top_symbols],
        "total": len(items),
    }
    write_json(OUT, [snapshot], as_of=meta.get("as_of_iso"))
    print(f"[saved] {OUT}")


if __name__ == "__main__":
    main()
