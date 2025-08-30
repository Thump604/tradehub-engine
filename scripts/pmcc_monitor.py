#!/usr/bin/env python3
"""
pmcc_monitor.py — environment + pairing snapshot for PMCC

What it does
------------
1) Loads LEAP (long call) and Covered Call (short call) CSVs via the unified loader.
2) Strips any trailing footers.
3) Checks headers against the authoritative "custom view" schemas (non-fatal).
4) Prints dataset counts and overlap symbols.
5) For each overlap symbol, attempts to form a PMCC *candidate pairing*:
   - Long leg: a Call from LEAP sheet, prioritize delta in [0.60, 0.85], largest DTE
   - Short leg: a Call from Covered Call sheet, DTE in [30, 60], delta in [0.20, 0.40]
6) Computes a few helpful fields: long extrinsic, coverage ratio, and emits a compact table.
7) If a symbol cannot produce a pair, prints a reason.

Usage
-----
    python -m scripts.pmcc_monitor
"""

from __future__ import annotations
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

# Internal modules (unified loader & yaml)
from scripts.utils.data_loader import (
    get_dataset_path,
    load_barchart_csv,
    strip_footer_if_present,
)
from scripts.utils.yaml_utils import read_yaml_safe

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RUNTIME_CATALOG = DATA / "data_catalog_runtime.yml"

# Authoritative "Custom View" schemas (you said these are locked)
LEAP_EXPECTED = [
    "Symbol", "Price~", "Exp Date", "DTE", "Strike", "Type",
    "Moneyness", "Ask", "%TP Ask", "BE (Ask)", "%BE (Ask)",
    "Net Debit", "Volume", "Open Int", "IV Rank", "Delta", "Profit Prob"
]

COVERED_CALL_EXPECTED = [
    "Symbol", "Price~", "Exp Date", "DTE", "Strike", "Type",
    "Moneyness", "Bid", "BE (Bid)", "%BE (Bid)", "Volume",
    "Open Int", "IV Rank", "Delta", "Return", "Ann Rtn",
    "Ptnl Rtn", "Profit Prob"
]

# Pairing heuristics (these don’t block data; they just drive the monitor output)
LONG_DELTA_MIN = 0.60
LONG_DELTA_MAX = 0.85
SHORT_DTE_MIN  = 30
SHORT_DTE_MAX  = 60
SHORT_DELTA_MIN = 0.20
SHORT_DELTA_MAX = 0.40
TOP_PER_SYMBOL = 2          # how many short candidates to show per long pick
TOP_SYMBOLS    = 12         # overall symbols to display in the pairing section


# --------------------------
# Helpers for safe extraction
# --------------------------
def f(x: Any) -> Optional[float]:
    """Convert a clean-format numeric string (maybe with %, ~, commas) to float; None if blank."""
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "na":
        return None
    # Remove common adornments without changing numeric meaning
    for ch in [",", "%"]:
        s = s.replace(ch, "")
    # Some sheets mark prices with a trailing "~"
    s = s.replace("~", "")
    try:
        return float(s)
    except ValueError:
        return None


def i(x: Any) -> Optional[int]:
    """To int via f()."""
    val = f(x)
    return int(val) if val is not None else None


def pct_str(val: Optional[float]) -> str:
    return f"{val:.2f}%" if isinstance(val, (int, float)) else ""


def money(val: Optional[float]) -> str:
    return f"{val:.2f}" if isinstance(val, (int, float)) else ""


def pick_best_long(leaps: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], str]:
    """Choose a long call candidate from the LEAP rows (for one symbol).
    Preference: Call, delta in [LONG_DELTA_MIN, LONG_DELTA_MAX], then max DTE; fallback otherwise."""
    calls = [r for r in leaps if str(r.get("Type","")).lower() == "call"]
    if not calls:
        return None, "no long calls in LEAP sheet"

    # First, try preferred delta band
    scored = []
    for r in calls:
        d = f(r.get("Delta"))
        dte = i(r.get("DTE")) or -1
        if d is None:
            continue
        if LONG_DELTA_MIN <= d <= LONG_DELTA_MAX:
            scored.append((dte, d, r))
    if scored:
        scored.sort(key=lambda t: (t[0], t[1]), reverse=True)  # prefer bigger DTE, then bigger delta
        return scored[0][2], ""

    # Fallback: just take highest DTE call
    calls_with_dte = [(i(r.get("DTE")) or -1, r) for r in calls]
    calls_with_dte.sort(key=lambda t: t[0], reverse=True)
    return calls_with_dte[0][1], "fallback long (delta outside preferred band)"


def short_candidates(shorts: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], str]:
    """Filter short calls to our preferred trade window and deltas."""
    calls = [r for r in shorts if str(r.get("Type","")).lower() == "call"]
    if not calls:
        return [], "no short calls in Covered Call sheet"

    filtered = []
    why = ""
    for r in calls:
        dte = i(r.get("DTE"))
        d = f(r.get("Delta"))
        if dte is None or d is None:
            continue
        if SHORT_DTE_MIN <= dte <= SHORT_DTE_MAX and SHORT_DELTA_MIN <= d <= SHORT_DELTA_MAX:
            filtered.append(r)

    if not filtered:
        why = "no short calls inside (DTE, delta) window"
    # Rank shorts by bid desc, then DTE desc (more premium preferred)
    filtered.sort(key=lambda r: (f(r.get("Bid")) or 0.0, i(r.get("DTE")) or 0), reverse=True)
    return filtered, why


def long_extrinsic(long_row: Dict[str, Any]) -> Optional[float]:
    """For a call: extrinsic = Ask - max(0, Underlying - Strike)."""
    ask = f(long_row.get("Ask"))
    und = f(long_row.get("Price~"))
    strike = f(long_row.get("Strike"))
    if ask is None or und is None or strike is None:
        return None
    intrinsic = max(0.0, und - strike)
    return ask - intrinsic


def coverage_ratio(long_row: Dict[str, Any], short_row: Dict[str, Any]) -> Optional[float]:
    """Short premium / long extrinsic. Larger is better for PMCC roll cadence."""
    ex = long_extrinsic(long_row)
    bid = f(short_row.get("Bid"))
    if ex is None or ex <= 0 or bid is None:
        return None
    return bid / ex


def reason_no_pair(symbol: str, leaps: List[Dict[str, Any]], shorts: List[Dict[str, Any]]) -> str:
    """Explain why we couldn't assemble a pair for a symbol."""
    if not leaps:
        return "no LEAP rows for symbol"
    if not shorts:
        return "no Covered Call rows for symbol"
    # Check for calls at all
    has_long_call = any(str(r.get("Type","")).lower()=="call" for r in leaps)
    has_short_call = any(str(r.get("Type","")).lower()=="call" for r in shorts)
    if not has_long_call:
        return "LEAP: no call entries"
    if not has_short_call:
        return "Covered Call: no call entries"

    # Window/delta checks
    _, why_s = short_candidates(shorts)
    if why_s:
        return why_s

    # If we get here, shorts exist within window; maybe long is missing delta or price fields
    cand, why_l = pick_best_long(leaps)
    if cand is None:
        return "LEAP: failed to pick a long"
    ex = long_extrinsic(cand)
    if ex is None or ex <= 0:
        return "LEAP: missing ask/underlying/strike (or extrinsic <= 0)"

    return "unknown pairing failure"


def print_header_check(rows: List[Dict[str, Any]], expected: List[str], label: str) -> None:
    print(f"{label:<22}", end="")
    if rows:
        header = list(rows[0].keys())
        print(f"header={header}")
        if header != expected:
            print(f"[WARN] {label.strip()} differs from expected schema.")
    else:
        print("rows = 0")


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def main() -> int:
    print("\n══════════════════════════════════════════════")
    print("PMCC MONITOR — environment + pairing snapshot")
    print("══════════════════════════════════════════════")

    runtime = read_yaml_safe(RUNTIME_CATALOG) if RUNTIME_CATALOG.exists() else {}
    if not runtime:
        print(f"[WARN] Runtime catalog missing or empty: {RUNTIME_CATALOG}")

    leap_path = get_dataset_path("leap", runtime=runtime, fallback=DATA / "leap-latest.csv")
    cc_path   = get_dataset_path("covered_call", runtime=runtime, fallback=DATA / "covered_call-latest.csv")

    print("\n──────────────── FILES ─────────────────")
    print(f"LEAP file         → {leap_path}")
    print(f"Covered Call file → {cc_path}")

    try:
        leap_rows = strip_footer_if_present(load_barchart_csv(leap_path))
    except FileNotFoundError:
        leap_rows = []
        print("[ERROR] LEAP CSV not found.")

    try:
        cc_rows = strip_footer_if_present(load_barchart_csv(cc_path))
    except FileNotFoundError:
        cc_rows = []
        print("[ERROR] Covered Call CSV not found.")

    print("\n──────────────── HEADERS ────────────────")
    print_header_check(leap_rows, LEAP_EXPECTED, "LEAP:")
    print_header_check(cc_rows,   COVERED_CALL_EXPECTED, "Covered Call:")

    print("\n──────────────── COUNTS ─────────────────")
    print(f"LEAP rows         : {len(leap_rows)}")
    print(f"Covered Call rows : {len(cc_rows)}")

    # -----------------------------
    # Overlap + simple pair building
    # -----------------------------
    # index by symbol
    from collections import defaultdict
    leaps_by_sym: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    shorts_by_sym: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for r in leap_rows:
        sym = str(r.get("Symbol","")).strip()
        if sym:
            leaps_by_sym[sym].append(r)

    for r in cc_rows:
        sym = str(r.get("Symbol","")).strip()
        if sym:
            shorts_by_sym[sym].append(r)

    overlap = sorted(set(leaps_by_sym.keys()) & set(shorts_by_sym.keys()))

    print("\n──────────────── OVERLAP ────────────────")
    print(f"Symbols in both sheets: {len(overlap)}")
    if overlap:
        print("Example symbols:", ", ".join(overlap[:20]))

    # Try to build pairings
    pair_rows: List[Dict[str, Any]] = []
    missed: List[Tuple[str, str]] = []

    for sym in overlap:
        leaps = leaps_by_sym[sym]
        shorts = shorts_by_sym[sym]

        long_pick, why_long = pick_best_long(leaps)
        if not long_pick:
            missed.append((sym, why_long or "failed to pick long"))
            continue

        short_list, why_short = short_candidates(shorts)
        if not short_list:
            missed.append((sym, why_short or "no short within window"))
            continue

        # Compute long extrinsic once
        ex = long_extrinsic(long_pick)
        if ex is None or ex <= 0:
            missed.append((sym, "LEAP extrinsic missing/<=0"))
            continue

        # keep top N short for this long
        for sr in short_list[:TOP_PER_SYMBOL]:
            cov = coverage_ratio(long_pick, sr)
            pair_rows.append({
                "symbol": sym,
                "long_exp": str(long_pick.get("Exp Date","")),
                "long_dte": i(long_pick.get("DTE")),
                "long_strike": f(long_pick.get("Strike")),
                "long_delta": f(long_pick.get("Delta")),
                "long_extr": ex,
                "short_exp": str(sr.get("Exp Date","")),
                "short_dte": i(sr.get("DTE")),
                "short_strike": f(sr.get("Strike")),
                "short_delta": f(sr.get("Delta")),
                "short_bid": f(sr.get("Bid")),
                "coverage": cov,
            })

    # Sort pairs by coverage desc, then short_bid desc
    pair_rows.sort(key=lambda r: (r.get("coverage") or 0.0, r.get("short_bid") or 0.0), reverse=True)

    print("\n──────────────── CANDIDATE PAIRS ────────")
    if not pair_rows:
        print("(no candidate pairs formed with current window)")
    else:
        # Trim output for readability
        show = pair_rows[:TOP_SYMBOLS]
        header = (
            "Sym   L.Exp       L.DTE  L.Strk  LΔ     L.Extr  "
            "S.Exp       S.DTE  S.Strk  SΔ     S.Bid  Cov"
        )
        print(header)
        print("-"*len(header))
        for r in show:
            print(
                f"{r['symbol']:<5} "
                f"{(r['long_exp'] or ''):<11} "
                f"{(r['long_dte'] if r['long_dte'] is not None else ''):>5}  "
                f"{money(r['long_strike']):>6}  "
                f"{(f'{r['long_delta']:.3f}' if r['long_delta'] is not None else ''):>5}  "
                f"{money(r['long_extr']):>6}  "
                f"{(r['short_exp'] or ''):<11} "
                f"{(r['short_dte'] if r['short_dte'] is not None else ''):>5}  "
                f"{money(r['short_strike']):>6}  "
                f"{(f'{r['short_delta']:.3f}' if r['short_delta'] is not None else ''):>5}  "
                f"{money(r['short_bid']):>5}  "
                f"{(f'{r['coverage']:.2f}' if r['coverage'] is not None else ''):>4}"
            )

    # Print misses with reasons (first few)
    if missed:
        print("\n──────────────── WHY NO PAIR (sample) ───")
        for sym, why in missed[:24]:
            print(f"{sym:<6} → {why}")

    # Friendly summary if still nothing
    if not pair_rows:
        print("\nTips:")
        print(f"  • Adjust SHORT window via constants: DTE [{SHORT_DTE_MIN},{SHORT_DTE_MAX}], Δ [{SHORT_DELTA_MIN},{SHORT_DELTA_MAX}]")
        print(f"  • Long pick aims for Δ [{LONG_DELTA_MIN},{LONG_DELTA_MAX}] with max DTE; edit in pmcc_monitor.py")

    print("\nGenerated:", now_utc())
    return 0


if __name__ == "__main__":
    sys.exit(main())