# scripts/rank_csp.py
from __future__ import annotations

from .rank_base import (
    load_barchart_csv_any,
    base_suggestion_fields,
    write_json,
    WEB_FEED,
)

OUT = WEB_FEED / "csp_suggestions.json"


def main() -> None:
    df = load_barchart_csv_any("csp")
    items = base_suggestion_fields(df, "csp") if df is not None else []
    write_json(OUT, items, as_of=df.attrs.get("as_of_iso") if df is not None else None)


if __name__ == "__main__":
    main()
