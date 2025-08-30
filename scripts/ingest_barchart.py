# scripts/ingest_barchart.py
from __future__ import annotations

import json
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd

# ----- Paths (no imports from rank_base to avoid circulars)
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
INCOMING = DATA_DIR / "incoming"
ARCHIVE = DATA_DIR / "archive"
L1_DIR = DATA_DIR / "l1"

KIND_TOKENS = {
    # canonical screener kinds mapped by filename tokens (OR logic)
    "covered_call": ["covered-call-option-screener"],
    "csp": ["naked-put-option-screener"],
    "pmcc": ["long-call-options-screener", "long-call-leap"],
    "diagonal": ["long-call-diagonal-option-screener"],
    "iron_condor": ["short-iron-condor-option-screener"],
    "vertical_bull_call": ["bull-call-spread-option-screener"],
    "vertical_bull_put": ["bull-put-spread-option-screener"],
    "indices": ["market-indices"],
}

FOOTER_RE = re.compile(
    r"AS OF\s+(\d{2})-(\d{2})-(\d{4})\s+(\d{1,2}):(\d{2})(AM|PM)\s+([A-Z]{2,5})",
    re.IGNORECASE,
)


def _ensure_dirs() -> None:
    for p in [INCOMING, ARCHIVE, L1_DIR, ROOT / "outputs" / "web_feed"]:
        p.mkdir(parents=True, exist_ok=True)
    # per-kind L1 subdirs
    for k in KIND_TOKENS:
        (L1_DIR / k).mkdir(parents=True, exist_ok=True)
    (L1_DIR / "misc").mkdir(parents=True, exist_ok=True)


def _guess_kind(basename: str) -> str:
    lower = basename.lower()
    for kind, tokens in KIND_TOKENS.items():
        for t in tokens:
            if t in lower:
                return kind
    return "misc"


def _parse_footer_timestamp(footer: str) -> str:
    """
    Try to produce ISO-8601 (local-naive). If not matched, return the raw footer.
    Example footer: 'DOWNLOADED FROM BARCHART.COM AS OF 08-28-2025 09:52AM CDT'
    """
    m = FOOTER_RE.search(footer)
    if not m:
        return footer.strip()
    mm, dd, yyyy, hh, mi, ap, tz = m.groups()
    hour = int(hh) % 12
    if ap.upper() == "PM":
        hour += 12
    try:
        dt = datetime(int(yyyy), int(mm), int(dd), hour, int(mi))
        return dt.strftime("%Y-%m-%dT%H:%M:00")  # keep it simple
    except Exception:
        return footer.strip()


def _read_csv_with_footer(path: Path) -> Tuple[pd.DataFrame, str]:
    """
    Read all rows except the last line (footer). Return (df, footer_line).
    """
    # Read CSV minus footer
    df = pd.read_csv(path, engine="python", skipfooter=1)
    # Grab footer
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()
    footer = lines[-1].strip() if lines else ""
    return df, footer


def _df_to_l1_records(
    df: pd.DataFrame,
    timestamp: str,
    source_file: str,
    kind: str,
) -> List[dict]:
    records = df.to_dict(orient="records")
    for r in records:
        r["timestamp"] = timestamp
        r["__source_file__"] = source_file
        r["__kind__"] = kind
    return records


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def main() -> None:
    _ensure_dirs()
    csvs = sorted(p for p in INCOMING.glob("*.csv"))
    moved = 0
    written = 0
    l1_files = []

    for csv_path in csvs:
        basename = csv_path.name
        kind = _guess_kind(basename)

        # Read and parse footer for timestamp
        df, footer = _read_csv_with_footer(csv_path)
        ts = _parse_footer_timestamp(footer)

        # Layer-1 JSON output (1:1 rows, plus timestamp + provenance)
        # Keep filename in the L1 name for easy traceability.
        l1_out = L1_DIR / kind / (basename.replace(".csv", ".json"))
        records = _df_to_l1_records(df, ts, basename, kind)
        _write_json(l1_out, records)
        l1_files.append(str(l1_out))
        written += 1

        # Archive the CSV
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        arch_dir = ARCHIVE / stamp
        arch_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(csv_path), str(arch_dir / basename))
        moved += 1

    # Summary
    print("\n" + "─" * 60)
    print("INGEST SUMMARY")
    print(f"incoming: {INCOMING}")
    if not csvs:
        print("  (no files in incoming)")
    else:
        for lf in l1_files:
            print(f"  [L1] {lf}")
    print("─" * 60 + "\n")


if __name__ == "__main__":
    main()
