from __future__ import annotations
import subprocess, sys

STEPS = [
    "scripts.ingest_barchart",
    "scripts.rank_csp",
    "scripts.rank_covered_call",
    "scripts.rank_pmcc",
    "scripts.rank_vertical",
    "scripts.rank_diagonal",
    "scripts.rank_iron_condor",
    "scripts.suggestions_merge",
    "scripts.build_trade_cards",
    "scripts.data_sanity_check",
]


def run(mod: str) -> int:
    print(f"$ {sys.executable} -m {mod}")
    return subprocess.call([sys.executable, "-m", mod])


def main() -> int:
    print("=== TradeHub: rebuild all ===\n")
    rc = 0
    for m in STEPS:
        rc = run(m)
        if rc != 0:
            print(f"[ERROR] step failed: {m} (rc={rc})")
            break
        print()
    print("\n=== done ===")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
