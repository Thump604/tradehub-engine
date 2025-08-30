from __future__ import annotations

from pathlib import Path

from .rank_base import (
    WEB_FEED,
    read_l1_latest,
    base_suggestion_fields,
    write_json,
    utc_now_iso,
)


OUT = WEB_FEED / "vertical_bull_call_suggestions.json"

def main() -> None:
    df = read_l1_latest("vertical_bull_call")
    items = base_suggestion_fields(df, "vertical_bull_call") if df is not None else []
    payload = {
        "generated_at": utc_now_iso(),
        "items": items,
    }
    write_json(OUT, payload)
    print(f"[saved] {OUT} (items={len(items)})")

if __name__ == "__main__":
    main()
