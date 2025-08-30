#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trade_cards:
- Render rich cards from outputs/*_suggestions.json (already with metrics + card)
- Optional: --strategy <name> and --limit N
"""
from __future__ import annotations
import argparse, json
from pathlib import Path

OUT_DIR = Path("outputs")


def _read_json(p: Path):
    try:
        return json.load(p.open())
    except:
        return None


def _assess(item):
    strat = item.get("strategy")
    m = item.get("metrics", {})
    flag = item.get("flag")
    hints = []
    if strat == "covered_call":
        ivr = m.get("ivr")
        itm = m.get("itm_prob")
        if isinstance(ivr, (int, float)) and ivr >= 30:
            hints.append("Premium attractive.")
        if isinstance(itm, (int, float)) and itm > 50:
            hints.append("Higher ITM risk; mind rolls.")
    elif strat == "csp":
        ay = m.get("ann_yield_to_strike_pct") or m.get("static_ann_return")
        if isinstance(ay, (int, float)) and ay > 40:
            hints.append("Very strong annualized yield.")
        if m.get("delta") and m["delta"] < -0.35:
            hints.append("Delta conservative.")
    elif strat == "long_call":
        if m.get("delta") and m["delta"] >= 0.7:
            hints.append("Conviction delta.")
        ivr = m.get("ivr")
        if isinstance(ivr, (int, float)) and ivr < 15:
            hints.append("IVR low; debit favorable.")
    if not hints:
        hints.append("Monitor key greeks and liquidity.")
    return f"Assessment: {flag}. " + " ".join(hints)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--strategy",
        choices=[
            "covered_call",
            "csp",
            "pmcc",
            "vertical",
            "diagonal",
            "iron_condor",
            "long_call",
        ],
        default=None,
    )
    ap.add_argument("--limit", type=int, default=10)
    args = ap.parse_args()

    files = sorted(OUT_DIR.glob("*_suggestions.json"))
    if args.strategy:
        files = [OUT_DIR / f"{args.strategy}_suggestions.json"]
    shown = 0
    print("=" * 72)
    print("TRADE CARDS")
    print("=" * 72)
    for p in files:
        data = _read_json(p) or {}
        for it in data.get("items", []):
            print("\n" + "=" * 72)
            print(f"{it.get('symbol','?')} [{it.get('strategy')}]")
            print("=" * 72)
            print(it.get("card", "(no card)"))
            print(_assess(it))
            shown += 1
            if shown >= args.limit:
                print("\n(…truncated…)\n")
                return


if __name__ == "__main__":
    main()
