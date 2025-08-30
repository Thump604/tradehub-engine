# scripts/rebuild_all.py
from __future__ import annotations

import subprocess
import sys
from typing import List, Tuple
from .rank_base import ROOT

CMDS: List[Tuple[str, List[str]]] = [
    ("scripts.ingest_barchart", [sys.executable, "-m", "scripts.ingest_barchart"]),
    ("scripts.bronze_from_csv", [sys.executable, "-m", "scripts.bronze_from_csv"]),
    # Strategy ranks
    ("scripts.rank_csp", [sys.executable, "-m", "scripts.rank_csp"]),
    ("scripts.rank_covered_call", [sys.executable, "-m", "scripts.rank_covered_call"]),
    ("scripts.rank_pmcc", [sys.executable, "-m", "scripts.rank_pmcc"]),
    (
        "scripts.rank_vertical_bull_call",
        [sys.executable, "-m", "scripts.rank_vertical_bull_call"],
    ),
    (
        "scripts.rank_vertical_bull_put",
        [sys.executable, "-m", "scripts.rank_vertical_bull_put"],
    ),
    ("scripts.rank_diagonal", [sys.executable, "-m", "scripts.rank_diagonal"]),
    ("scripts.rank_iron_condor", [sys.executable, "-m", "scripts.rank_iron_condor"]),
    # Merge + Gold
    ("scripts.suggestions_merge", [sys.executable, "-m", "scripts.suggestions_merge"]),
    ("scripts.build_trade_cards", [sys.executable, "-m", "scripts.build_trade_cards"]),
    ("scripts.alerts_scan", [sys.executable, "-m", "scripts.alerts_scan"]),
    ("scripts.risk_snapshot", [sys.executable, "-m", "scripts.risk_snapshot"]),
    ("scripts.data_sanity_check", [sys.executable, "-m", "scripts.data_sanity_check"]),
]


def run_cmd(name: str, cmd: List[str]) -> int:
    print(f"\n$ {' '.join(cmd)}")
    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), text=True, check=False)
        return proc.returncode
    except Exception as e:
        print(f"[ERROR] {name}: {e}")
        return 1


def main() -> None:
    print("=== TradeHub: rebuild all ===")
    failures: List[str] = []
    for name, cmd in CMDS:
        rc = run_cmd(name, cmd)
        if rc != 0:
            failures.append(f"{name} -> exit {rc}")
    if failures:
        print("\n=== DONE (with failures) ===")
        for f in failures:
            print("  -", f)
        sys.exit(1)
    else:
        print("\n=== done ===")
        sys.exit(0)


if __name__ == "__main__":
    main()