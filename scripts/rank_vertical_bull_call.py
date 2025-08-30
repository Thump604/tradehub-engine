# scripts/rank_vertical_bull_call.py
from __future__ import annotations

from pathlib import Path

from .rank_base import (
    ensure_paths,
    load_l1,
    base_suggestion_fields,
    write_json,
    WEB_FEED,
)

OUT_FILE = WEB_FEED / "vertical_bull_call_suggestions.json"


def main() -> None:
    ensure_paths()

    # Read latest Layer-1 for bull-call spreads (schema-on-read; full fidelity)
    rows = load_l1("vertical_bull_call")

    # Build minimal suggestions (no schema guessing, just common fields)
    items = base_suggestion_fields(rows, "vertical_bull_call")

    write_json(OUT_FILE, items)
    print(f"[saved] {OUT_FILE}")


if __name__ == "__main__":
    main()
