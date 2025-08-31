from __future__ import annotations
import csv, sys, subprocess, shlex
from pathlib import Path

def read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        try:
            return next(r)
        except StopIteration:
            return []

MAIN_MARKERS = {"Return", "Ann Rtn", "Ptnl Rtn", "BE (Bid)", "%BE (Bid)", "Profit Prob"}
CUSTOM_MARKERS = {"%Chg~","Volume~","Ask","Mid","Theta","BE (Ask)","%BE (Ask)","ITM Prob","TP Ask","%TP Ask(a)","Static Ann Rtn","Ann Yield to Strike%","Exp B4 Earnings"}

def classify(header: list[str]) -> str:
    hs = set(header)
    c_hits = len(hs & CUSTOM_MARKERS)
    m_hits = len(hs & MAIN_MARKERS)
    if c_hits > 0 and m_hits == 0:
        return "covered_call_custom"
    if m_hits > 0 and c_hits == 0:
        return "covered_call_main"
    if c_hits > m_hits:
        return "covered_call_custom"
    if m_hits > c_hits:
        return "covered_call_main"
    raise SystemExit(f"could not classify covered call CSV (ambiguous header): {header}")

def run_update(path: Path, screener: str) -> None:
    cmd = f'python -m scripts.catalog.update_schema --in "{path}" --screener {screener}'
    print(cmd, flush=True)
    subprocess.check_call(shlex.split(cmd))

def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: apply_covered_call_refs.py <csv1> [<csv2> ...]")
    for p in map(Path, sys.argv[1:]):
        if not p.exists():
            raise SystemExit(f"file not found: {p}")
        header = read_header(p)
        key = classify(header)
        run_update(p, key)

if __name__ == "__main__":
    main()
