#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
verticals_monitor.py

Paste-in monitor for vertical spreads using PMCC-like leg rows.
- No external dependencies (std lib only)
- Accepts 2 rows per spread (same Symbol, Exp Date, DTE, Type)
- Auto-detects strategy:
    * Type == "Put"  => bull_put (credit spread)
    * Type == "Call" => bull_call (debit spread)
- Infers which leg is short vs long:
    * Prefer Bid (short) vs Ask (long) from each row
    * Fallback to strike ordering if Bid/Ask unavailable
- Computes width, credit/debit, max P/L, breakeven
- Flags threat vs short strike and 21 DTE management

INPUT COLUMNS (case-sensitive, per leg):
    Symbol, Price~, Exp Date, DTE, Strike, Type, Bid, Ask, Moneyness (Ask/Bid optional but preferred)

USAGE:
    python3 verticals_monitor.py
    [paste your rows, then Ctrl-D to end input]
"""

import sys
import csv
from io import StringIO
from collections import defaultdict

# ---- Configurable basics ----
DTE_MANAGE = 21  # tasty-style "manage/roll" threshold
MIN_COLUMNS = {"Symbol", "Price~", "Exp Date", "DTE", "Strike", "Type"}  # per leg

def read_pasted_text() -> str:
    data = sys.stdin.read()
    if not data.strip():
        sys.exit("[ERROR] No input detected on STDIN. Paste your table including header, then press Ctrl-D.")
    return data

def detect_dialect_and_parse(text: str):
    # Try csv.Sniffer; fallback to comma
    try:
        dialect = csv.Sniffer().sniff(text, delimiters=",\t|;")
    except Exception:
        class Dialect(csv.excel):
            delimiter = ","
        dialect = Dialect
    reader = csv.reader(StringIO(text), dialect)
    rows = [r for r in reader if any(cell.strip() for cell in r)]
    if not rows:
        sys.exit("[ERROR] Parsed zero rows.")
    header = [h.strip() for h in rows[0]]
    data_rows = rows[1:]
    return header, data_rows

def header_index_map(header):
    idx = {name: i for i, name in enumerate(header)}
    missing = [c for c in MIN_COLUMNS if c not in idx]
    if missing:
        sys.exit(f"[ERROR] Missing required columns: {missing}\nHeader found: {header}")
    return idx

def get_float(row, idx_map, key, default=None):
    if key not in idx_map:
        return default
    try:
        raw = row[idx_map[key]].strip().replace("%","").replace(",","")
        if raw == "" or raw.upper() == "N/A":
            return default
        return float(raw)
    except Exception:
        return default

def get_str(row, idx_map, key, default=""):
    if key not in idx_map:
        return default
    return row[idx_map[key]].strip()

def identify_legs(legs, opt_type):
    """
    legs: list of two dicts with keys: Strike, Bid, Ask
    Returns (short_leg, long_leg)
    Rule of thumb:
        - Prefer Bid => short; Ask => long
        - Fallback to strike order:
            * Calls: long = lower strike, short = higher strike (debit)
            * Puts:  short = higher strike, long = lower strike  (credit)
    """
    a, b = legs[0], legs[1]

    # If clear Bid/Ask labeling exists
    a_has_bid = a.get("Bid") is not None
    b_has_bid = b.get("Bid") is not None
    a_has_ask = a.get("Ask") is not None
    b_has_ask = b.get("Ask") is not None

    if a_has_bid and b_has_ask:
        return a, b
    if b_has_bid and a_has_ask:
        return b, a

    # Fallback: strike logic
    sa, sb = a.get("Strike"), b.get("Strike")
    if sa is None or sb is None:
        # Last resort: treat first as short, second as long
        return a, b

    if opt_type.lower() == "call":
        # bull call debit: long lower, short higher
        if sa <= sb:
            return {"Strike": sb, "Bid": a.get("Bid"), "Ask": a.get("Ask"), **{k:v for k,v in a.items() if k not in ("Strike","Bid","Ask")}}, \
                   {"Strike": sa, "Bid": b.get("Bid"), "Ask": b.get("Ask"), **{k:v for k,v in b.items() if k not in ("Strike","Bid","Ask")}}
        else:
            return {"Strike": sa, "Bid": b.get("Bid"), "Ask": b.get("Ask"), **{k:v for k,v in b.items() if k not in ("Strike","Bid","Ask")}}, \
                   {"Strike": sb, "Bid": a.get("Bid"), "Ask": a.get("Ask"), **{k:v for k,v in a.items() if k not in ("Strike","Bid","Ask")}}
    else:
        # put credit: short higher, long lower
        if sa >= sb:
            return a, b
        else:
            return b, a

def compute_vertical(symbol, exp, dte, opt_type, under, short_leg, long_leg):
    """
    Returns dict with computed economics and flags.
    opt_type: "Put" => bull_put (credit), "Call" => bull_call (debit)
    """
    sK = short_leg.get("Strike")
    lK = long_leg.get("Strike")
    if sK is None or lK is None:
        return {"error": "Missing strike on a leg."}

    width = abs(sK - lK)
    if width <= 0:
        return {"error": "Invalid width (strikes equal or missing)."}

    # Prices
    short_bid = short_leg.get("Bid")
    long_ask  = long_leg.get("Ask")

    result = {
        "Symbol": symbol,
        "Exp Date": exp,
        "DTE": dte,
        "Type": opt_type,
        "Underlying": under,
        "Short Strike": sK,
        "Long Strike": lK,
        "Width": round(width, 2),
        "Strategy": "",
        "Credit": None,
        "Debit": None,
        "MaxProfit": None,
        "MaxLoss": None,
        "Breakeven": None,
        "Threat": "",
        "Action": ""
    }

    if opt_type.lower() == "put":
        # bull put (credit)
        result["Strategy"] = "bull_put"
        # Prefer Bid(short) - Ask(long); fallback to 0 if missing
        credit = None
        if short_bid is not None and long_ask is not None:
            credit = max(0.0, short_bid - long_ask)
        result["Credit"] = None if credit is None else round(credit, 2)
        if credit is not None:
            max_profit = credit
            max_loss   = max(0.0, width - credit)
            breakeven  = sK - credit
            result["MaxProfit"] = round(max_profit, 2)
            result["MaxLoss"]   = round(max_loss, 2)
            result["Breakeven"] = round(breakeven, 2)

        # Threat: underlying at/below short put
        if under is not None and sK is not None:
            result["Threat"] = "Threatened (≤ short strike)" if under <= sK else "Safe"

    else:
        # bull call (debit)
        result["Strategy"] = "bull_call"
        # Prefer Ask(long) - Bid(short); fallback to 0 if missing
        debit = None
        if long_ask is not None and short_bid is not None:
            debit = max(0.0, long_ask - short_bid)
        result["Debit"] = None if debit is None else round(debit, 2)
        if debit is not None:
            max_profit = max(0.0, width - debit)
            max_loss   = debit
            breakeven  = long_leg["Strike"] + debit  # lower strike is long in ideal case
            result["MaxProfit"] = round(max_profit, 2)
            result["MaxLoss"]   = round(max_loss, 2)
            result["Breakeven"] = round(breakeven, 2)

        # Threat: underlying at/above short call
        if under is not None and sK is not None:
            result["Threat"] = "Threatened (≥ short strike)" if under >= sK else "Safe"

    # Action logic (basic without live marks)
    if isinstance(dte, int) and dte <= DTE_MANAGE:
        result["Action"] = f"Manage at {DTE_MANAGE} DTE (roll/close)"
    elif result["Threat"].startswith("Threatened"):
        result["Action"] = "Threatened: consider roll or close"
    else:
        result["Action"] = "Hold / Monitor"

    return result

def main():
    text = read_pasted_text()
    header, data_rows = detect_dialect_and_parse(text)
    idx = header_index_map(header)

    # Parse legs into buckets keyed by (Symbol, Exp Date, DTE, Type)
    buckets = defaultdict(list)
    for r in data_rows:
        leg = {}
        for k in ("Symbol","Exp Date","Type"):
            leg[k] = get_str(r, idx, k, "")
        leg["DTE"] = None
        try:
            dte_raw = get_str(r, idx, "DTE", "")
            leg["DTE"] = int(float(dte_raw)) if dte_raw != "" else None
        except Exception:
            leg["DTE"] = None

        leg["Strike"] = get_float(r, idx, "Strike", None)
        leg["Underlying"] = get_float(r, idx, "Price~", None)
        leg["Bid"] = get_float(r, idx, "Bid", None)
        leg["Ask"] = get_float(r, idx, "Ask", None)
        leg["Moneyness"] = get_str(r, idx, "Moneyness", "")

        key = (leg["Symbol"], leg["Exp Date"], leg["DTE"], leg["Type"])
        buckets[key].append(leg)

    results = []
    errors = []

    for key, legs in buckets.items():
        symbol, exp, dte, opt_type = key
        if len(legs) != 2:
            errors.append(f"[WARN] Skipping group {key}: expected 2 legs, found {len(legs)}")
            continue

        # Extract shared underlying (use first leg's if both present)
        under = legs[0].get("Underlying") if legs[0].get("Underlying") is not None else legs[1].get("Underlying")

        short_leg, long_leg = identify_legs(legs, opt_type)
        res = compute_vertical(symbol, exp, dte, opt_type, under, short_leg, long_leg)
        if "error" in res:
            errors.append(f"[WARN] {key}: {res['error']}")
            continue
        results.append(res)

    # Pretty print compact table
    if not results and errors:
        for e in errors:
            print(e, file=sys.stderr)
        sys.exit(1)

    cols = ["Symbol","Strategy","Type","Exp Date","DTE","Underlying",
            "Short Strike","Long Strike","Width","Credit","Debit",
            "MaxProfit","MaxLoss","Breakeven","Threat","Action"]
    # Column widths
    widths = {c:max(len(c), 12) for c in cols}
    for row in results:
        for c in cols:
            val = row.get(c, "")
            s = f"{val}"
            if isinstance(val, float):
                s = f"{val:.2f}"
            widths[c] = max(widths[c], len(s))

    # Header
    line = " ".join(c.ljust(widths[c]) for c in cols)
    print(line)
    print("-" * len(line))

    # Rows
    for row in results:
        vals = []
        for c in cols:
            v = row.get(c, "")
            if isinstance(v, float):
                v = f"{v:.2f}"
            vals.append(f"{v}".ljust(widths[c]))
        print(" ".join(vals))

    # Any warnings
    for e in errors:
        print(e, file=sys.stderr)

if __name__ == "__main__":
    main()