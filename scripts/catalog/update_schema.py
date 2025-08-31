from __future__ import annotations
import argparse, csv, re, sys
from pathlib import Path
from pprint import pformat

FOOTER_RE = re.compile(r"^Downloaded from Barchart\.com as of (.+)$")

def read_header_sample_footer(csv_path: Path):
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rdr = csv.reader(f)
        header = None
        sample_row = None
        footer_ts = None
        for row in rdr:
            if not row or all(not (c or "").strip() for c in row):
                continue
            if header is None:
                header = row
                continue
            if len(row) == 1:
                m = FOOTER_RE.match((row[0] or "").strip())
                if m:
                    footer_ts = m.group(1)
                    continue
            if sample_row is None and len(row) == len(header):
                sample_row = row
        if header is None:
            raise SystemExit(f"No header found in CSV: {csv_path}")
        return header, sample_row, footer_ts

def load_existing_schemas(py_path: Path) -> dict:
    if not py_path.exists():
        return {}
    ns = {}
    src = py_path.read_text(encoding="utf-8")
    exec(compile(src, str(py_path), "exec"), ns, ns)
    return dict(ns.get("SCHEMAS", {}))

def write_schemas(py_path: Path, schemas: dict) -> None:
    py_path.parent.mkdir(parents=True, exist_ok=True)
    with py_path.open("w", encoding="utf-8") as f:
        f.write("SCHEMAS = ")
        f.write(pformat(schemas, sort_dicts=False))
        f.write("\n")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_file", required=True)
    ap.add_argument("--screener", required=True)
    ap.add_argument("--schemas", default="catalog/schemas.py")
    args = ap.parse_args()

    csv_path = Path(args.in_file)
    if not csv_path.exists():
        raise SystemExit(f"File not found: {csv_path}")

    columns, sample_row, footer_ts = read_header_sample_footer(csv_path)
    entry = {
        "source_example": str(csv_path),
        "footer_timestamp": footer_ts,
        "columns": columns,          # preserve exact order
        "sample_row": sample_row,    # aligned to columns
    }

    out_path = Path(args.schemas)
    all_schemas = load_existing_schemas(out_path)
    all_schemas[args.screener] = entry
    write_schemas(out_path, all_schemas)

    import json
    print(json.dumps({args.screener: entry}, indent=2))

if __name__ == "__main__":
    main()
