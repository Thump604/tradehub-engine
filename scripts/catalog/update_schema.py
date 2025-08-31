from __future__ import annotations
import argparse, csv, re
from pathlib import Path
from pprint import pformat
FOOTER_RE = re.compile(r"^Downloaded from Barchart\.com as of (.+)$")
def read_header_rows_footer(csv_path: Path):
    text = csv_path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    footer_ts = None
    if lines and FOOTER_RE.match(lines[-1]):
        footer_ts = FOOTER_RE.match(lines[-1]).group(1)
        lines = lines[:-1]
    reader = csv.DictReader(lines)
    columns = list(reader.fieldnames or [])
    row_count = sum(1 for _ in reader)
    return columns, row_count, footer_ts
def load_existing_schemas(py_path: Path) -> dict:
    if not py_path.exists(): return {}
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
    if not csv_path.exists(): raise SystemExit(f"File not found: {csv_path}")
    columns, row_count, footer_ts = read_header_rows_footer(csv_path)
    entry = {"source_example": str(csv_path), "footer_timestamp": footer_ts, "row_count": row_count, "columns": columns}
    out_path = Path(args.schemas)
    all_schemas = load_existing_schemas(out_path)
    all_schemas[args.screener] = entry
    write_schemas(out_path, all_schemas)
    print(f"[schemas] updated {out_path} -> {args.screener} (cols={len(columns)}, rows={row_count}, footer={'yes' if footer_ts else 'no'})")
    import json
    print(json.dumps({args.screener: entry}, indent=2))
if __name__ == "__main__": main()
