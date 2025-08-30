# scripts/rank_iron_condor.py
from __future__ import annotations

from .rank_base import (
    load_barchart_csv_any,
    base_suggestion_fields,
    write_json,
    WEB_FEED,
)

OUT = WEB_FEED / "iron_condor_suggestions.json"


def main() -> None:
    df = load_barchart_csv_any("iron_condor")
    items = base_suggestion_fields(df, "iron_condor") if df is not None else []
    write_json(OUT, items, as_of=df.attrs.get("as_of_iso") if df is not None else None)


if __name__ == "__main__":
    main()
