# scripts/bronze_from_csv.py
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Any
import pandas as pd

from .rank_base import ARCHIVE, BRONZE, utc_now_iso, ensure_dir

# Screener tokens we recognize (simple substring match on filename)
TOKENS = {
    "naked-put-option-screener": ["naked-put-option-screener"],
    "covered-call-option-screener": ["covered-call-option-screener"],
    "long-call-options-screener": ["long-call-options-screener"],
    "long-call-diagonal-option-screener": ["long-call-diagonal-option-screener"],
    "bull-call-spread-option-screener": ["bull-call-spread-option-screener"],
    "bull-put-spread-option-screener": ["bull-put-spread-option-screener"],
    "short-iron-condor-option-screener": ["short-iron-condor-option-screener"],
    "market-indices": ["market-indices"],
}

FOOTER_RE = re.compile(
    r"^DOWNLOADED FROM BARCHART\.COM AS OF\s+(?P<ts>.+)$", re.IGNORECASE
)


def detect_screener(filename: str) -> str | None:
    lname = filename.lower()
    for screener, keys in TOKENS.items():
        if any(k in lname for k in keys):
            return screener
    return None


def parse_footer_timestamp(last_line: str) -> str | None:
    m = FOOTER_RE.match(last_line.strip())
    return m.group("ts").strip() if m else None


def csv_to_bronze_jsonl(csv_path: Path) -> int:
    screener = detect_screener(csv_path.name)
    if not screener:
        return 0  # skip unknown csv

    # Read all but last line → dataframe
    df = pd.read_csv(csv_path, skipfooter=1, engine="python")

    # Read last line for footer timestamp
    last_line = ""
    with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
        for last_line in f:
            pass
    ts_footer = parse_footer_timestamp(last_line) or ""
    ingested_at = utc_now_iso()

    # Decide bronze output folder by ts footer (fallback to file mtime date)
    # We keep it simple: folder by YYYY-MM-DD substring if present
    out_date = None
    # try to find an MM-DD-YYYY or similar; otherwise just today's date part of utc
    m_date = re.search(r"(\d{2}-\d{2}-\d{4})", ts_footer)
    if m_date:
        mm, dd, yyyy = m_date.group(1).split("-")
        out_date = f"{yyyy}-{mm}-{dd}"
    else:
        out_date = utc_now_iso()[:10]

    out_dir = BRONZE / screener / out_date
    out_file = out_dir / (csv_path.stem + ".jsonl")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Write JSONL rows with metadata (no column picking)
    n = 0
    with out_file.open("w", encoding="utf-8") as out:
        for _, row in df.iterrows():
            rec: Dict[str, Any] = row.to_dict()
            rec["__screener__"] = screener
            rec["__timestamp__"] = ts_footer
            rec["__source_file__"] = csv_path.name
            rec["__ingested_at__"] = ingested_at
            out.write(pd.io.json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
    return n


def main() -> None:
    total = 0
    converted: List[str] = []
    skipped: List[str] = []

    print("== Bronze from CSV ==")
    print(f"archive: {ARCHIVE}")
    for csv_path in sorted(ARCHIVE.rglob("*.csv")):
        screener = detect_screener(csv_path.name)
        if not screener:
            skipped.append(csv_path.name)
            continue
        n = csv_to_bronze_jsonl(csv_path)
        total += n
        if n:
            converted.append(f"{csv_path.name} -> {n} rows")

    print(f"[bronze] files={len(converted)} rows={total}")
    for line in converted[:25]:
        print("  -", line)
    if skipped:
        print(f"[bronze] skipped (unknown pattern): {len(skipped)}")


if __name__ == "__main__":
    main()
