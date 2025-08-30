#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


# --- Paths -------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
INCOMING = DATA / "incoming"
ARCHIVE = DATA / "archive"
L1 = DATA / "l1"

L1.mkdir(parents=True, exist_ok=True)

# --- File → kind routing (schema-on-read) ------------------------------------
# Keep it dead simple: infer kind from filename tokens.
ROUTES: list[tuple[re.Pattern, str]] = [
    (re.compile(r"covered-call-option-screener", re.I),        "covered_call"),
    (re.compile(r"naked-put-option-screener", re.I),            "csp"),
    (re.compile(r"long-call-diagonal-option-screener", re.I),   "diagonal"),
    (re.compile(r"short-iron-condor-option-screener", re.I),    "iron_condor"),
    (re.compile(r"long-call-options-screener", re.I),           "pmcc"),  # used by PMCC/LEAP readers
    (re.compile(r"bull-call-spread-option-screener", re.I),     "vertical_bull_call"),
    (re.compile(r"bull-put-spread-option-screener", re.I),      "vertical_bull_put"),
    (re.compile(r"market-indices", re.I),                       "indices"),
    # misc catch-alls (we still ingest to L1/misc for future use):
    (re.compile(r"bear-call-spread-option-screener", re.I),     "misc"),
    (re.compile(r"bear-put-spread-option-screener", re.I),      "misc"),
]

FOOTER_TS_RE = re.compile(
    r"(?P<ts>(?:Downloaded|DOWNLOADED).*?\b(\d{2}-\d{2}-\d{4})\s+(\d{1,2}:\d{2}\s*[AP]M)\s*(\w{3})?)",
    re.I,
)

@dataclass(frozen=True)
class IngestResult:
    src: Path
    kind: str
    out: Path
    rows: int
    footer: str
    timestamp: str

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _infer_kind(p: Path) -> Optional[str]:
    name = p.name
    for pat, kind in ROUTES:
        if pat.search(name):
            return kind
    return None

def _read_footer_line(p: Path) -> str:
    # Read only the last line, but avoid loading huge files into memory
    with p.open("rb") as f:
        f.seek(0, 2)
        size = f.tell()
        back = min(size, 4096)
        f.seek(size - back)
        tail = f.read().decode(errors="ignore")
    last_line = tail.splitlines()[-1].strip() if tail.splitlines() else ""
    return last_line

def _parse_footer_timestamp(footer: str) -> str:
    m = FOOTER_TS_RE.search(footer or "")
    return m.group("ts").strip() if m else (footer or "")

def _csv_to_l1_json(src: Path, kind: str) -> IngestResult:
    footer_line = _read_footer_line(src)
    footer_ts = _parse_footer_timestamp(footer_line)

    # Read all rows except the final footer line
    df = pd.read_csv(src, engine="python", skipfooter=1)

    # Per-row metadata (schema-on-read; do NOT drop any columns)
    df["__timestamp__"] = footer_ts
    df["__ingested_at__"] = _utc_now_iso()
    df["__source_file__"] = str(src.name)
    df["__kind__"] = kind

    # Write as newline-delimited JSON for easy downstream reading
    out_dir = L1 / kind
    out_dir.mkdir(parents=True, exist_ok=True)
    # Make an output name that mirrors the CSV name
    out_path = out_dir / (src.stem + ".json")
    records = df.to_dict(orient="records")
    with out_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    return IngestResult(
        src=src,
        kind=kind,
        out=out_path,
        rows=len(df),
        footer=footer_line,
        timestamp=footer_ts,
    )

def _iter_csvs() -> list[Path]:
    csvs: list[Path] = []
    # direct files in INCOMING and ARCHIVE root
    csvs.extend(sorted(INCOMING.glob("*.csv")))
    csvs.extend(sorted(ARCHIVE.glob("*.csv")))
    # deeper dated folders under ARCHIVE
    csvs.extend(sorted(ARCHIVE.glob("*/*.csv")))
    return csvs

def _archive_incoming_file(p: Path) -> None:
    if p.parent == INCOMING:
        # put into a dated folder; keep filename unchanged
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        dest_dir = ARCHIVE / stamp
        dest_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(p), str(dest_dir / p.name))

def main() -> None:
    csvs = _iter_csvs()
    if not csvs:
        print("\n────────────────────────────────────────────────────────────")
        print("INGEST SUMMARY")
        print(f"incoming: {INCOMING}")
        print("  (no files in incoming or archive roots)")
        print("────────────────────────────────────────────────────────────\n")
        return

    results: list[IngestResult] = []
    for src in csvs:
        kind = _infer_kind(src)
        if not kind:
            # skip unknown CSVs silently
            continue
        try:
            res = _csv_to_l1_json(src, kind)
            results.append(res)
            # move out of incoming once processed
            _archive_incoming_file(src)
        except Exception as e:
            print(f"[ERR] {src.name}: {e}")

    # Pretty summary
    print("\n────────────────────────────────────────────────────────────")
    print("INGEST SUMMARY")
    print(f"incoming: {INCOMING}")
    for r in results:
        print(f"  [L1/{r.kind}] {r.out}  (rows={r.rows}, ts='{r.timestamp}')")
    print("────────────────────────────────────────────────────────────\n")

if __name__ == "__main__":
    main()
