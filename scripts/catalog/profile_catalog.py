from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

FOOTER_PREFIX = "Downloaded from Barchart.com as of"

def is_footer_row(row: Dict[str, Any]) -> bool:
    vals = [str(v).strip() for v in row.values() if v is not None]
    if not vals:
        return False
    nonempty = [v for v in vals if v != ""]
    return len(nonempty) == 1 and nonempty[0].startswith(FOOTER_PREFIX)

def parse_footer_timestamp(s: str) -> Optional[str]:
    m = re.search(r"as of\s+(.+)$", s)
    return m.group(1).strip() if m else None

def coerce_type(sample_values: List[str]) -> str:
    def is_int(x: str) -> bool:
        try:
            int(x.replace(",", ""))
            return True
        except Exception:
            return False

    def is_float(x: str) -> bool:
        try:
            float(x.replace(",", ""))
            return True
        except Exception:
            return False

    def is_bool(x: str) -> bool:
        return x.lower() in {"true", "false", "yes", "no"}

    vals = [v for v in sample_values if v not in ("", "NA", "N/A", "null", "None")]
    if not vals:
        return "string"
    if all(is_int(v) for v in vals):
        return "integer"
    if all(is_float(v) for v in vals):
        return "number"
    if all(is_bool(v) for v in vals):
        return "boolean"
    if all(re.search(r"\d{1,4}[-/]\d{1,2}[-/]\d{1,4}", v) for v in vals if re.search(r"\d", v)):
        return "date_like"
    return "string"

def profile_csv(in_csv: Path) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    with in_csv.open("r", newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        rows = list(rdr)
    footer_ts: Optional[str] = None
    if rows:
        last = rows[-1]
        if is_footer_row(last):
            only_val = next((str(v).strip() for v in last.values() if v and str(v).strip()), "")
            footer_ts = parse_footer_timestamp(only_val)
            rows = rows[:-1]
    return rows, footer_ts

def infer_schema(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"columns": [], "row_count": 0}
    headers = list(rows[0].keys())
    total = len(rows)
    columns: List[Dict[str, Any]] = []
    for col in headers:
        values = [row.get(col, "") for row in rows]
        strvals = ["" if v is None else str(v).strip() for v in values]
        non_empty = [v for v in strvals if v not in ("", "NA", "N/A", "null", "None")]
        uniques = set(non_empty)
        inferred = coerce_type(non_empty[:500])
        columns.append({
            "name": col,
            "inferred_type": inferred,
            "nulls": total - len(non_empty),
            "nonnull": len(non_empty),
            "unique": len(uniques),
            "example": next((v for v in non_empty if v != ""), None)
        })
    return {"columns": columns, "row_count": total}

def main() -> None:
    ap = argparse.ArgumentParser(description="Profile a Barchart screener CSV and emit a JSON schema spec.")
    ap.add_argument("--in", dest="in_csv", required=True, help="Path to a single CSV file")
    ap.add_argument("--screener", required=True, help="Logical screener key, e.g. vertical_bull_put")
    ap.add_argument("--outdir", default="catalog/specs", help="Output directory for spec JSON")
    args = ap.parse_args()

    in_path = Path(args.in_csv)
    if not in_path.exists():
        raise SystemExit(f"File not found: {in_path}")
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    rows, footer_ts = profile_csv(in_path)
    schema = infer_schema(rows)

    spec = {
        "screener": args.screener,
        "source_file": str(in_path),
        "footer_timestamp": footer_ts,
        "row_count_including_footer": schema["row_count"] + (1 if footer_ts else 0),
        "row_count_data_only": schema["row_count"],
        "columns": schema["columns"],
    }

    out_path = outdir / f"{args.screener}.schema.json"
    out_path.write_text(json.dumps(spec, indent=2), encoding="utf-8")
    print(f"[spec] wrote {out_path}")

if __name__ == "__main__":
    main()
