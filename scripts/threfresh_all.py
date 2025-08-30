#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TradeHub: one-shot, robust site refresh.

Run:
  python -m scripts.threfresh_all
  # or with flags:
  python -m scripts.threfresh_all --no-ingest --verbose

What it does (in order):
  1) (optional) Ingest CSVs from data/incoming/ -> data/*-latest.csv
  2) Rank per strategy if its latest CSV exists (skip if missing)
  3) Write empty feeds for any strategy that still doesn't have an output
  4) Merge suggestions -> outputs/web_feed/suggestions_merged.json
  5) Build positions dashboard feed
  6) Print a human-friendly summary

This script NEVER crashes due to optional/missing inputs.
"""

from __future__ import annotations
import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUT = ROOT / "outputs"
WEB_FEED = OUT / "web_feed"
ARCHIVE = DATA / "archive"

# Ensure folders exist
for p in (DATA, OUT, WEB_FEED, ARCHIVE):
    p.mkdir(parents=True, exist_ok=True)

# Canonical latest inputs we look for
LATEST_BY_KIND: Dict[str, Path] = {
    "csp": DATA / "csp-latest.csv",
    "covered_call": DATA / "covered_call-latest.csv",
    "vertical_bull_call": DATA / "vertical_bull_call-latest.csv",
    "vertical_bull_put": DATA / "vertical_bull_put-latest.csv",
    "diagonal_long_call": DATA / "diagonal_long_call-latest.csv",
    "indices": DATA / "market-indices-latest.csv",
    # If you add more, declare them here.
}

# Rankers: module -> output JSON it should (normally) produce
RANKERS: Dict[str, str] = {
    "scripts.rank_csp": "csp_suggestions.json",
    "scripts.rank_covered_call": "covered_call_suggestions.json",
    "scripts.rank_vertical": "vertical_suggestions.json",
    "scripts.rank_diagonal": "diagonal_suggestions.json",
    "scripts.rank_iron_condor": "iron_condor_suggestions.json",
    "scripts.rank_long_call": "long_call_suggestions.json",
    "scripts.rank_pmcc": "pmcc_suggestions.json",
    # include your short PMCC if/when you wire it:
    # "scripts.rank_pmcc_short": "pmcc_short_suggestions.json",
}

# For deciding whether to run a given ranker, map ranker -> required inputs
REQUIRES: Dict[str, List[Path]] = {
    "scripts.rank_csp": [LATEST_BY_KIND["csp"]],
    "scripts.rank_covered_call": [LATEST_BY_KIND["covered_call"]],
    "scripts.rank_vertical": [
        LATEST_BY_KIND["vertical_bull_call"],
        LATEST_BY_KIND["vertical_bull_put"],
    ],
    "scripts.rank_diagonal": [LATEST_BY_KIND["diagonal_long_call"]],
    "scripts.rank_iron_condor": [LATEST_BY_KIND["indices"]],
    "scripts.rank_long_call": [],  # if this ranker needs a feed later, add it here
    "scripts.rank_pmcc": [],  # same note as above
    # "scripts.rank_pmcc_short": [],
}

# Strategy output files we insist exist for the site (we’ll backfill empties)
REQUIRED_FEEDS: List[str] = [
    "csp_suggestions.json",
    "covered_call_suggestions.json",
    "vertical_suggestions.json",
    "diagonal_suggestions.json",
    "iron_condor_suggestions.json",
    "long_call_suggestions.json",
    "pmcc_suggestions.json",
    # "pmcc_short_suggestions.json",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run(cmd: List[str], *, verbose: bool) -> Tuple[int, str, str]:
    """Run a subprocess and capture output; never raises."""
    try:
        p = subprocess.run(
            cmd,
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if verbose:
            print(f"$ {' '.join(cmd)}")
            if p.stdout.strip():
                print(p.stdout.strip())
            if p.stderr.strip():
                print(p.stderr.strip(), file=sys.stderr)
        return p.returncode, p.stdout, p.stderr
    except Exception as e:
        return 999, "", str(e)


def file_exists(p: Path) -> bool:
    return p.exists() and p.is_file() and p.stat().st_size > 0


def write_empty_feed(name: str, reason: str) -> None:
    """Create an empty suggestions feed that the site can safely read."""
    WEB_FEED.mkdir(parents=True, exist_ok=True)
    payload = {
        "ok": True,
        "ts": utc_now_iso(),
        "strategy": name.replace("_suggestions.json", ""),
        "rows": [],
        "count": 0,
        "reason": reason,
    }
    out_json = WEB_FEED / name
    out_yml = WEB_FEED / (name.replace(".json", ".yml"))

    out_json.write_text(json.dumps(payload, indent=2))
    # minimal YAML twin (just so your tools don’t choke if they expect it)
    yml = [
        f"ok: true",
        f"ts: {payload['ts']}",
        f"strategy: {payload['strategy']}",
        f"count: 0",
        f"reason: {reason!s}".replace("\n", " "),
        f"rows: []",
        "",
    ]
    out_yml.write_text("\n".join(yml))


def maybe_ingest(verbose: bool) -> None:
    code, _, _ = run(
        [sys.executable, "-m", "scripts.ingest_barchart", "--all"], verbose=verbose
    )
    if code != 0:
        # Ingest is optional; do not fail the run
        print(
            "[ingest] WARN: ingest step did not complete cleanly; continuing.",
            file=sys.stderr,
        )


def rank_everything(verbose: bool) -> Dict[str, str]:
    """Return summary dict: ranker -> 'ran'|'skipped(missing …)'|'failed(code …)'."""
    results: Dict[str, str] = {}
    for mod, out_name in RANKERS.items():
        needed = REQUIRES.get(mod, [])
        missing = [str(p.name) for p in needed if not file_exists(p)]
        if missing:
            results[mod] = f"skipped (missing inputs: {', '.join(missing)})"
            continue

        code, so, se = run([sys.executable, "-m", mod], verbose=verbose)
        if code == 0:
            results[mod] = "ran"
        else:
            results[mod] = f"failed (code {code})"
            # If it failed, ensure the site still has a feed to read:
            write_empty_feed(out_name, reason=f"{mod} failed")
    return results


def backfill_missing_outputs(verbose: bool) -> None:
    """Ensure every required feed exists; if not, write an empty one."""
    for name in REQUIRED_FEEDS:
        dest = WEB_FEED / name
        if not file_exists(dest):
            write_empty_feed(name, reason="no input / not produced this run")
            if verbose:
                print(f"[backfill] wrote empty {name}")


def merge_and_positions(verbose: bool) -> Tuple[int, int]:
    # Merge suggestions
    run([sys.executable, "-m", "scripts.suggestions_merge"], verbose=verbose)

    # Build positions dashboard
    run([sys.executable, "-m", "scripts.positions_build_dashboard"], verbose=verbose)

    # Summaries
    merged = WEB_FEED / "suggestions_merged.json"
    merged_rows = 0
    if file_exists(merged):
        try:
            data = json.loads(merged.read_text())
            merged_rows = int(len(data.get("rows") or data.get("items") or []))
        except Exception:
            pass

    pos = WEB_FEED / "positions.json"
    pos_rows = 0
    if file_exists(pos):
        try:
            pdata = json.loads(pos.read_text())
            pos_rows = int(len(pdata.get("rows") or pdata.get("items") or []))
        except Exception:
            pass

    return merged_rows, pos_rows


def summarize(rank_results: Dict[str, str], merged_rows: int, pos_rows: int) -> None:
    print("══════════════════════════════════════════════════════════════════════")
    print("TRADE HUB — Refresh Summary")
    print("══════════════════════════════════════════════════════════════════════")
    print(f"Time (UTC): {utc_now_iso()}")
    print()
    print("Inputs present:")
    for kind, p in LATEST_BY_KIND.items():
        print(f"  - {kind:<18} {'OK' if file_exists(p) else '—'} {p.name}")
    print()
    print("Rankers:")
    for mod, status in rank_results.items():
        print(f"  - {mod:<24} {status}")
    print()
    print(f"Merged suggestions rows: {merged_rows}")
    print(f"Positions rows:          {pos_rows}")
    print()
    print(f"Web feed dir: {WEB_FEED}")
    print("Done.")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="All-in-one robust refresh for TradeHub site."
    )
    ap.add_argument("--no-ingest", action="store_true", help="Skip the ingest step")
    ap.add_argument("--verbose", action="store_true", help="Show subprocess logs")
    args = ap.parse_args()

    if not args.no_ingest:
        maybe_ingest(verbose=args.verbose)

    rank_results = rank_everything(verbose=args.verbose)
    backfill_missing_outputs(verbose=args.verbose)
    merged_rows, pos_rows = merge_and_positions(verbose=args.verbose)
    summarize(rank_results, merged_rows, pos_rows)


if __name__ == "__main__":
    main()
