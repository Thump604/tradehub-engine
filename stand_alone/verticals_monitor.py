#!/usr/bin/env python3
# verticals_monitor.py — Monitor vertical option spreads from pasted broker rows (no headers).
# - Accepts 1 or more verticals; each vertical must have TWO legs with the same Symbol, Exp, and Type.
# - Legs do NOT need to be contiguous in the paste.
# - Uses only values present in your current position screen (NO Bid/Ask requirement).
# - Prints trade-card style output with fixed-width KPI columns and tasty-style checks.
#
# Environment:
#   DEBUG_VERT=1 to see parser traces.
#
# Notes:
#   - We infer spread type and direction from leg types/strikes/qty.
#   - "Spread (mid)" uses the individual leg "mark" values parsed from the first $ on each leg row.
#   - For debit spreads, remain→max $ = width - current spread value.
#   - For credit spreads, remain→max $ = width - credit (so it's the distance from width).
#     (This mirrors the previous behavior you validated in examples.)

import sys, re, os
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite

DEBUG = bool(os.environ.get("DEBUG_VERT"))

# ───────── ANSI ─────────
class C:
    R="\033[31m"; G="\033[32m"; Y="\033[33m"; B="\033[34m"; M="\033[35m"; C="\033[36m"
    DIM="\033[2m"; RESET="\033[0m"; BOLD="\033[1m"
def color(s, k): return f"{k}{s}{C.RESET}"
def green(s): return color(s, C.G)
def yellow(s): return color(s, C.Y)
def red(s): return color(s, C.R)
def bold(s): return color(s, C.BOLD)
def dim(s): return color(s, C.DIM)

# ───────── Fixed-width KPI helpers (no jank) ─────────
def _s(x): return "N/A" if x is None else str(x)

def _fmt_num(x, p=4):
    if x is None: return "N/A"
    try: return f"{float(x):.{p}f}"
    except: return "N/A"

def _fmt_int(x):
    if x is None: return "N/A"
    try:
        xi = int(round(float(x)))
        return f"{xi}"
    except:
        return "N/A"

def _fmt_money(x, p=2):
    if x is None: return "N/A"
    try: return f"{float(x):,.{p}f}"
    except: return "N/A"

def _fmt_pct(x, p=1):
    if x is None: return "N/A"
    try: return f"{float(x):.{p}f}%"
    except: return "N/A"

def _fmt_strikes(a, b):
    if a is None or b is None: return "N/A"
    return f"{_fmt_num(a,1)}/{_fmt_num(b,1)}"

def _print_pair(label_l, val_l, label_r, val_r, *, lw=22, vw=12):
    ll = (label_l + ":") if label_l else ""
    lr = (label_r + ":") if label_r else ""
    print(f"{ll:<{lw}} {_s(val_l):>{vw}}   {lr:<{lw}} {_s(val_r):>{vw}}")

# ───────── utils ─────────
def to_float(s):
    if s is None: return None
    s = s.replace(',', '')
    try: return float(s)
    except: return None

def to_int(s):
    if s is None: return None
    s = s.replace(',', '')
    try: return int(s)
    except: return None

# ───────── models ─────────
@dataclass
class Underlying:
    symbol: str
    last: float = None

@dataclass
class OptionRow:
    symbol: str
    exp: str                 # MM/DD/YYYY
    strike: float
    cp: str                  # 'C' or 'P'
    dte: int = None
    delta: float = None
    oi: int = None
    qty: int = None          # -1 short, +1 long
    mark: float = None
    itm_flag: str = None     # 'ITM'/'OTM'
    raw: str = ""

# ───────── regex ─────────
DATE_RE   = re.compile(r'(\d{2}/\d{2}/\d{4})')
HEADER_RE = re.compile(r'^([A-Z][A-Z0-9\.]{0,6})\s+(\d{2}/\d{2}/\d{4})\s+(\d+(?:\.\d+)?)\s+(C|P)\b')
MONEY_RE  = re.compile(r'\$(-?\d+(?:\.\d+)?)')
ITM_RE    = re.compile(r'\b(ITM|OTM)\b')

def _norm(s: str) -> str: return s.replace('\t', ' ').strip()

# ───────── input ─────────
def read_lines():
    print("Paste 1 or more verticals. Each vertical must have TWO legs (same Symbol, same Exp, same Type). "
          "Legs do NOT need to be contiguous. Press Ctrl-D when done.")
    raw = sys.stdin.read()
    lines = [_norm(l) for l in raw.splitlines() if _norm(l)]
    if DEBUG:
        for l in lines: print(dim(f"[LINE] {l}"))
    return lines

# ───────── parse underlying last ─────────
def detect_underlyings(lines):
    under = {}
    sym = None
    for line in lines:
        if re.fullmatch(r'[A-Z][A-Z0-9\.]{0,6}', line):
            sym = line
            continue
        if sym and line.startswith('$'):
            m = MONEY_RE.match(line)
            if m:
                last = to_float(m.group(1))
                under[sym] = Underlying(sym, last)
                if DEBUG: print(dim(f"[DEBUG] Underlying {sym}: last={last} via: {line}"))
            sym = None
    return under

# ───────── token helpers ─────────
def token_split(row: str):
    row = row.replace('+', ' +').replace('-', ' -')
    toks = [t for t in row.split() if t]
    return toks

def parse_after_itm_block(row: str):
    """
    From the row that contains '... (ITM|OTM) DTE Delta OI Qty ...'
    extract itm_flag, dte, delta, oi, qty by relative position.
    """
    m = ITM_RE.search(row)
    if not m: return None, None, None, None, None
    itm = m.group(1)
    toks = token_split(row)
    idx = None
    for i, t in enumerate(toks):
        if t == itm:
            idx = i; break
    if idx is None: return itm, None, None, None, None

    def get(i):
        if i < 0 or i >= len(toks): return None
        return toks[i].replace(',', '')

    dte_s   = get(idx+1)
    delta_s = get(idx+2)
    oi_s    = get(idx+3)
    qty_s   = get(idx+4)

    dte   = to_int(dte_s)
    delta = to_float(delta_s)
    oi    = to_int(oi_s)
    qty   = to_int(qty_s)
    if qty is not None and qty not in (-1, 1):
        alt = get(idx+5)
        q2 = to_int(alt)
        if q2 in (-1, 1): qty = q2

    return itm, dte, delta, oi, qty

# ───────── parse option legs ─────────
def parse_options(lines):
    opts = []
    n = len(lines); i = 0
    while i < n:
        h = HEADER_RE.match(lines[i])
        if not h:
            i += 1; continue
        sym, date, strike_s, cp = h.group(1), h.group(2), h.group(3), h.group(4)
        strike = float(strike_s)
        data_row = None
        j = i + 1
        while j < n and j <= i + 8:
            row = lines[j]
            if ' EXP ' in f" {row} " or row.upper().startswith('CALL ') or row.upper().startswith('PUT '):
                j += 1; continue
            if row.startswith('$'):
                data_row = row; break
            j += 1
        if not data_row:
            if DEBUG: print(dim(f"[DEBUG] No data row after header: {lines[i]}"))
            i += 1; continue

        m = MONEY_RE.search(data_row)
        mark = to_float(m.group(1)) if m else None
        itm_flag, dte, delta, oi, qty = parse_after_itm_block(data_row)
        opt = OptionRow(sym, date, strike, cp, dte, delta, oi, qty, mark, itm_flag, data_row)
        opts.append(opt)
        if DEBUG:
            print(dim(f"[DEBUG] Parsed {sym} {date} {strike} {cp} | mark={mark} ITM={itm_flag} DTE={dte} Δ={delta} OI={oi} qty={qty}"))
        i = j + 1
    return opts

# ───────── group into verticals ─────────
def group_verticals(opts):
    """
    Groups legs into spreads keyed by (symbol, exp, cp).
    We select exactly two legs where qty signs differ (one long, one short).
    If more than two legs match, pick the best one long + one short by |delta| (closer to ATM for the short).
    """
    from collections import defaultdict
    buckets = defaultdict(list)
    for o in opts:
        buckets[(o.symbol, o.exp, o.cp)].append(o)

    pairs = []
    for key, rows in buckets.items():
        longs  = [r for r in rows if r.qty and r.qty > 0]
        shorts = [r for r in rows if r.qty and r.qty < 0]
        # try to find exactly one of each
        if not longs or not shorts:
            continue
        # choose long: prefer larger |delta| (more intrinsic for debit) then lower strike for calls (higher for puts)
        def rank_long(r):
            if r.cp == 'C': return (-abs(r.delta or 0), r.strike)
            else:           return (-abs(r.delta or 0), -r.strike)
        long_pick = sorted(longs, key=rank_long)[0]

        # choose short: prefer |delta| ~ 0.35
        def rank_short(r):
            target = 0.35
            return abs(abs(r.delta or target) - target)
        short_pick = sorted(shorts, key=rank_short)[0]

        pairs.append((key, long_pick, short_pick))
    return pairs

# ───────── spread analytics ─────────
def classify_and_calc(und_last, long: OptionRow, short: OptionRow):
    """
    Determine spread type/direction and compute metrics.
    Returns dict with:
      strategy, type, width, debit, credit, spread_mid, remain_to_max, remain_pct, breakeven, short_delta,
      threatened_status, action_hint
    """
    cp = long.cp  # both same
    # normalize so we always refer explicitly to long_leg and short_leg passed in
    long_k, short_k = long.strike, short.strike
    width = abs(short_k - long_k)

    # current "spread (mid)" and credit/debit from leg marks
    long_m  = long.mark
    short_m = short.mark
    spread_mid = None
    debit = credit = None

    # Determine structure & economics
    if cp == 'C':
        if long_k < short_k:
            strategy = "bull_call"
            spread_mid = (long_m - short_m) if (long_m is not None and short_m is not None) else None
            debit = spread_mid if (spread_mid is not None and spread_mid >= 0) else None
            credit = None
            breakeven = (long_k + debit) if debit is not None else None
        else:
            strategy = "bear_call"
            spread_mid = (short_m - long_m) if (long_m is not None and short_m is not None) else None
            credit = spread_mid if (spread_mid is not None and spread_mid >= 0) else None
            debit = None
            breakeven = (short_k + credit) if credit is not None else None
    else:  # Puts
        if long_k > short_k:
            strategy = "bear_put"
            spread_mid = (long_m - short_m) if (long_m is not None and short_m is not None) else None
            debit = spread_mid if (spread_mid is not None and spread_mid >= 0) else None
            credit = None
            breakeven = (long_k - debit) if debit is not None else None
        else:
            strategy = "bull_put"
            spread_mid = (short_m - long_m) if (long_m is not None and short_m is not None) else None
            credit = spread_mid if (spread_mid is not None and spread_mid >= 0) else None
            debit = None
            breakeven = (short_k - credit) if credit is not None else None

    # Remain to max (as previously validated in examples)
    remain_to_max = None
    remain_pct = None
    if width is not None:
        if debit is not None:
            if spread_mid is not None:
                remain_to_max = max(0.0, width - spread_mid)
                remain_pct = (remain_to_max / width) * 100.0
        elif credit is not None:
            # distance to width (mirrors your prior outputs)
            remain_to_max = max(0.0, width - credit)
            remain_pct = (remain_to_max / width) * 100.0

    # Threat status vs short strike
    threatened = None
    if und_last is not None:
        if cp == 'C':
            threatened = (und_last >= short_k)
        else:
            threatened = (und_last <= short_k)

    # Short delta shown as absolute 3dp (positive)
    short_delta = abs(short.delta) if short.delta is not None else None

    # Action hint
    if threatened is True:
        action_hint = "Threatened: consider roll or close"
        status = "≥ short (max zone)" if cp == 'C' else "≤ short (risk zone)"
    elif und_last is not None and ((cp == 'C' and long_k <= und_last < short_k) or (cp == 'P' and short_k < und_last <= long_k)):
        action_hint = "Between strikes: monitor"
        status = "Between strikes"
    else:
        action_hint = "Hold / Monitor"
        status = "Safe"

    # Pricing ratio
    credit_over_width = (credit / width) if (credit is not None and width) else None
    debit_over_width  = (debit / width)  if (debit  is not None and width) else None

    return {
        "strategy": strategy,
        "type": "Call" if cp == 'C' else "Put",
        "width": width,
        "debit": debit,
        "credit": credit,
        "spread_mid": spread_mid,
        "remain_to_max": remain_to_max,
        "remain_pct": remain_pct,
        "breakeven": breakeven,
        "short_delta": short_delta,
        "threatened": threatened,
        "status": status,
        "action_hint": action_hint,
        "credit_over_width": credit_over_width,
        "debit_over_width": debit_over_width,
    }

# ───────── tasty-style checks ─────────
def tasty_checks(und_last, long: OptionRow, short: OptionRow, calc):
    """Return dict of boolean checks."""
    dte_ok = (short.dte is not None and short.dte >= 21)
    # Not-tested short strike
    if short.cp == 'C' and und_last is not None:
        not_tested = und_last < short.strike
    elif short.cp == 'P' and und_last is not None:
        not_tested = und_last > short.strike
    else:
        not_tested = None

    # Short delta band
    sd = calc["short_delta"]
    short_delta_band = (sd is not None and 0.30 <= sd <= 0.50)

    # Pricing ratio targets
    if calc["credit"] is not None and calc["credit_over_width"] is not None:
        pricing_ok = (calc["credit_over_width"] >= 1/3.0)
    elif calc["debit"] is not None and calc["debit_over_width"] is not None:
        pricing_ok = (calc["debit_over_width"] <= 0.40)
    else:
        pricing_ok = None

    # Harvestable (<25% to max) — more meaningful for debit near max-value
    harvestable = None
    if calc["debit"] is not None and calc["remain_pct"] is not None:
        harvestable = (calc["remain_pct"] <= 25.0)

    return {
        "dte_ok": dte_ok,
        "not_tested": not_tested,
        "short_delta_band": short_delta_band,
        "pricing_ok": pricing_ok,
        "harvestable": harvestable,
    }

# ───────── rendering ─────────
def banner(symbol, strat, cp, exp, dte):
    title = f"{symbol}  |  {strat.replace('_', ' ').upper()}  |  {cp}"
    when = ""
    try:
        # convert MM/DD/YYYY -> YYYY-MM-DD
        m,d,y = exp.split('/')
        exp_iso = f"{y}-{m}-{d}"
    except:
        exp_iso = exp
    when = f"Exp {exp_iso} • DTE {dte if dte is not None else 'N/A'}"
    print("─"*60)
    print(title)
    print(when)
    print("─"*60)

def check_label(p): return green("PASS") if p is True else red("FAIL") if p is False else yellow("N/A")

def render_card(und_last, sym, long: OptionRow, short: OptionRow, calc, checks):
    strat = calc["strategy"]
    cp = calc["type"]
    banner(sym, strat, cp, short.exp, short.dte)

    # Status chips
    status_chip = f"[ {calc['status']} ]"
    action_chip = f"[ {calc['action_hint']} ]"
    print(status_chip, " ", action_chip, "\n")

    # Fixed-width KPI block
    _print_pair("Underlying",        _fmt_num(und_last, 2),
                "Spread (mid)",      _fmt_num(calc["spread_mid"], 4))

    _print_pair("Strikes (L/S)",     _fmt_strikes(long.strike, short.strike),
                "Remain→Max $",      _fmt_num(calc["remain_to_max"], 4))

    _print_pair("Width",             _fmt_num(calc["width"], 0),
                "Remain→Max %",      _fmt_pct(calc["remain_pct"], 1))

    price_label = "Credit" if calc["credit"] is not None else "Debit"
    price_value = _fmt_num(calc["credit"] if calc["credit"] is not None else calc["debit"], 4)
    ratio_label = "Credit/Width" if calc["credit"] is not None else "Debit/Width"
    ratio_value = _fmt_num(calc["credit_over_width"] if calc["credit"] is not None else calc["debit_over_width"], 2)

    _print_pair(price_label,         price_value,
                ratio_label,         ratio_value)

    _print_pair("Breakeven",         _fmt_num(calc["breakeven"], 4),
                "Short Δ",           _fmt_num(calc["short_delta"], 3))

    # Checklist
    print("\nChecklist")
    print(f"  DTE ≥ 21: {check_label(checks['dte_ok'])}")
    print(f"  Short strike not tested: {check_label(checks['not_tested'])}")
    print(f"  Short Δ 0.30–0.50: {check_label(checks['short_delta_band'])}")
    print(f"  Pricing ratio target: {check_label(checks['pricing_ok'])}")
    print(f"  Harvestable (<25% to max): {check_label(checks['harvestable'])}")

    # Recommendations
    recs = []
    if calc["threatened"]:
        recs.append("- Short strike tested → consider roll up/out or close.")
    if calc["debit_over_width"] is not None and calc["debit_over_width"] > 0.70:
        recs.append("- Debit > ~70% width → consider cheaper spread or roll.")
    if calc["credit_over_width"] is not None and calc["credit_over_width"] < (1/3.0):
        recs.append("- Credit < ~1/3 width → widen or choose different strikes.")
    if checks["harvestable"]:
        recs.append("- Near max value → consider taking profits.")

    if not recs:
        recs.append("- Hold / monitor; reassess on vol shifts or Δ moves.")

    print("\nRecommendations")
    for r in recs: print(f"  {r}")
    print("─"*60 + "\n")

# ───────── main ─────────
def main():
    lines = read_lines()
    und_map = detect_underlyings(lines)
    opts = parse_options(lines)

    pairs = group_verticals(opts)
    if not pairs:
        print(red("[ERROR] No completed vertical spreads found. "
                  "Each vertical needs TWO legs with same Symbol, Exp, and Type (one long, one short)."))
        return

    # Sort for stable output: by symbol then expiration date string
    def exp_key(e):  # MM/DD/YYYY -> YYYYMMDD
        try:
            m,d,y = e.split('/')
            return f"{y}{m}{d}"
        except:
            return e
    pairs.sort(key=lambda kls: (kls[0][0], exp_key(kls[0][1]), kls[0][2]))

    print(f"[OK] Found {len(pairs)} completed vertical spread(s). Rendering Trade Card(s):\n")
    for (sym, exp, cp), long_leg, short_leg in pairs:
        und = und_map.get(sym, Underlying(sym, None))
        calc = classify_and_calc(und.last, long_leg, short_leg)
        checks = tasty_checks(und.last, long_leg, short_leg, calc)
        render_card(und.last, sym, long_leg, short_leg, calc, checks)

if __name__ == "__main__":
    main()