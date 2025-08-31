from __future__ import annotations
import csv, subprocess, sys
from pathlib import Path

def header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        return next(r)

def classify(cols: list[str]) -> str:
    return "csp_custom" if ("%Chg~" in cols or "Volume~" in cols or "Total OI" in cols) else "csp_main"

def run_update(csv_path: Path, key: str) -> None:
    cmd = [sys.executable, "-m", "scripts.catalog.update_schema", "--in", str(csv_path), "--screener", key]
    subprocess.check_call(cmd)

def main():
    if len(sys.argv) < 3:
        print("usage: apply_csp_refs.py FILE1 FILE2", file=sys.stderr)
        sys.exit(2)
    for p in map(Path, sys.argv[1:]):
        cols = header(p)
        key = classify(cols)
        run_update(p, key)

if __name__ == "__main__":
    main()
