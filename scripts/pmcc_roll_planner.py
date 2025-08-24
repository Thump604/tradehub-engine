#!/usr/bin/env python3
# pmcc_roll_planner.py — read leap_renewals.json and suggest replacement LEAPs
# for a set of symbols you provide. No external deps.
#
# Usage examples:
#   python3 scripts/pmcc_roll_planner.py --symbols AAPL,AMZN,NVDA
#   python3 scripts/pmcc_roll_planner.py --symbols AAPL --top 5 --dte-min 300 --delta-min 0.75
#
# Inputs:
#   outputs/suggestions/leap_renewals.json   (written by leap_lifecycle.py)
# Optional knobs (CLI):
#   --top N         number of candidates per symbol (default 3)
#   --dte-min N     hard floor for DTE (default 180)
#   --delta-min X   minimum delta (default 0.70)
#   --delta-max X   maximum delta (default 0.95)
#
# This script deliberately does NOT modify pmcc_monitor.py. It’s a clean “planner”
# you can run right after leap_lifecycle to decide what LEAP(s) you want to roll into.

import os, sys, json, argparse
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SUGG_JSON = os.path.join(ROOT, "outputs", "suggestions", "leap_renewals.json")

def load_suggestions(path):
    if not os.path.exists(path):
        print(f"[ERROR] Missing {path}. Run scripts/leap_lifecycle.py first.", file=sys.stderr)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    top = data.get("top", [])
    return top, data.get("generated_at", "")

def parse_args():
    ap = argparse.ArgumentParser(description="PMCC Roll Planner from LEAP lifecycle suggestions")
    ap.add_argument("--symbols", required=True, help="Comma-separated symbols, e.g. AAPL,AMZN,NVDA")
    ap.add_argument("--top", type=int, default=3, help="candidates per symbol (default 3)")
    ap.add_argument("--dte-min", type=int, default=180, help="min DTE for replacement LEAPs (default 180)")
    ap.add_argument("--delta-min", type=float, default=0.70, help="min delta (default 0.70)")
    ap.add_argument("--delta-max", type=float, default=0.95, help="max delta (default 0.95)")
    return ap.parse_args()

def banner():
    print("\n" + "─"*70)
    print("PMCC ROLL PLANNER — Replacement LEAP short-list".center(70))
    print("─"*70 + "\n")

def main():
    args = parse_args()
    want = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not want:
        print("[ERROR] No symbols provided.", file=sys.stderr)
        sys.exit(1)

    rows, gen_at = load_suggestions(SUGG_JSON)
    banner()
    print(f"Using: outputs/suggestions/leap_renewals.json (generated {gen_at})\n")
    print(f"Filters → DTE ≥ {args.dte_min}, {args.delta_min} ≤ Δ ≤ {args.delta_max}")
    print()

    # group by symbol
    bysym = {}
    for r in rows:
        sym = r.get("symbol","").upper()
        bysym.setdefault(sym, []).append(r)

    any_hit = False
    for sym in want:
        cand = [r for r in bysym.get(sym, [])
                if r.get("dte", 0) >= args.dte_min
                and r.get("delta") is not None
                and args.delta_min <= r["delta"] <= args.delta_max]
        cand.sort(key=lambda x: (-x.get("score",0), -x.get("dte",0), -x.get("delta",0)))
        print("──────────────────────────────────────────────────────────────")
        print(f"{sym}  |  Replacement LEAP candidates")
        print("──────────────────────────────────────────────────────────────")
        if not cand:
            print("(no candidates passed filters)\n")
            continue
        any_hit = True
        print(" Exp Date     DTE   Δ      ITM%   Ask     IVR   Score")
        print(" ----------   ---   -----  -----  ------  ----  ------")
        for r in cand[:args.top]:
            exp = r.get("exp","")
            dte = f"{r.get('dte',0):>4}"
            delt= f"{r.get('delta',0):.3f}".rjust(5)
            mny = r.get("moneyness_pct", None)
            mny = f"{(mny if mny is not None else 0):+.1f}%".rjust(6)
            ask = f"{r.get('ask',0.0):.2f}".rjust(6)
            ivr = r.get("iv_rank", None)
            ivr = f"{(ivr if ivr is not None else 0):.1f}".rjust(5)
            sc  = f"{r.get('score',0.0):.3f}".rjust(6)
            print(f" {exp}  {dte}  {delt}  {mny}  {ask}  {ivr}  {sc}")
        print()

    if not any_hit:
        print("No matches for the requested symbols under the current filters.\n")

if __name__ == "__main__":
    main()