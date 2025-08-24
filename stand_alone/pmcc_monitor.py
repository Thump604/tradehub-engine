#!/usr/bin/env python3
# pmcc_monitor.py — Pure PMCC monitor from pasted broker rows (no headers).
# Finds: long ITM LEAP call + short near-term OTM call on same symbol.
# Prints: coverage check, cycles, 50% GTC target, and actions, in a fixed-width “trade ticket” format.
# No external deps. DEBUG_PMCC=1 shows parsing traces.

import sys, re, os
from dataclasses import dataclass
from datetime import datetime, timezone
from math import floor

DEBUG = bool(os.environ.get("DEBUG_PMCC"))

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

# ───────── Fixed-width KPI helpers (shared look with verticals) ─────────
def _s(x): return "N/A" if x is None else str(x)

def _fmt_num(x, p=4):
    if x is None: return "N/A"
    try: return f"{float(x):.{p}f}"
    except: return "N/A"

def _fmt_money(x, p=2):
    if x is None: return "N/A"
    try: return f"${float(x):,.{p}f}"
    except: return "N/A"

def _print_pair(label_l, val_l, label_r, val_r, *, lw=24, vw=12):
    ll = (label_l + ":") if label_l else ""
    lr = (label_r + ":") if label_r else ""
    print(f"{ll:<{lw}} {_s(val_l):>{vw}}   {lr:<{lw}} {_s(val_r):>{vw}}")

def _fmt_delta(x, p=3):
    if x is None: return "N/A"
    try: return f"{float(x):.{p}f}"
    except: return "N/A"

# ───────── fmt helpers ─────────
def fmt(x): return "N/A" if x is None else f"${x:,.2f}"
def fmt_num(x, p=4): return "N/A" if x is None else f"{x:.{p}f}"

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
    exp: str
    strike: float
    cp: str              # 'C'/'P'
    dte: int = None
    delta: float = None
    oi: int = None
    qty: int = None      # -1 short, +1 long
    mark: float = None
    itm_flag: str = None # 'ITM'/'OTM'
    raw: str = ""

# ───────── regex ─────────
DATE_RE   = re.compile(r'(\d{2}/\d{2}/\d{4})')                  # MM/DD/YYYY
HEADER_RE = re.compile(r'^([A-Z][A-Z0-9\.]{0,6})\s+(\d{2}/\d{2}/\d{4})\s+(\d+(?:\.\d+)?)\s+(C|P)\b')
MONEY_RE  = re.compile(r'\$(-?\d+(?:\.\d+)?)')
ITM_RE    = re.compile(r'\b(ITM|OTM)\b')

def _norm(s: str) -> str: return s.replace('\t', ' ').strip()

# ───────── input ─────────
def read_lines():
    print("Paste your full broker rows (no headers needed). Include the underlying line, the option header line "
          "(e.g., 'QQQ 09/19/2025 560.00 C'), and the first price/data line that follows. Then press Ctrl-D.")
    raw = sys.stdin.read()
    lines = [_norm(l) for l in raw.splitlines() if _norm(l)]
    if DEBUG:
        for l in lines: print(dim(f"[LINE] {l}"))
    return lines

# ───────── underlying parse ─────────
def detect_underlyings(lines):
    under = {}
    sym = None
    for line in lines:
        # bare symbol line
        if re.fullmatch(r'[A-Z][A-Z0-9\.]{0,6}', line):
            sym = line
            continue
        # price line
        if sym and line.startswith('$'):
            m = MONEY_RE.match(line)
            if m:
                last = to_float(m.group(1))
                under[sym] = Underlying(sym, last)
                if DEBUG: print(dim(f"[DEBUG] Underlying {sym}: last={last} via: {line}"))
            sym = None
    return under

# ───────── option parse helpers ─────────
def token_split(row: str):
    row = row.replace('+', ' +').replace('-', ' -')
    toks = [t for t in row.split() if t]
    return toks

def parse_after_itm_block(row: str):
    """
    Given a pricing row that contains '... (ITM|OTM) DTE Δ OI Qty ...',
    extract DTE, Delta, OI, Qty strictly by relative position after ITM/OTM.
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

# ───────── option parse ─────────
def parse_options(lines):
    """
    Find header like: 'AAPL 09/19/2025 235.00 C'
    Then skip label rows ('CALL ... EXP ...') and pick the first row starting with '$' (the price data row).
    From that row, parse mark from first $ token; then anchor at ITM/OTM and read DTE, Δ, OI, qty.
    """
    opts = []
    n = len(lines); i = 0
    while i < n:
        h = HEADER_RE.match(lines[i])
        if not h:
            i += 1; continue
        sym, date, strike_s, cp = h.group(1), h.group(2), h.group(3), h.group(4)
        strike = float(strike_s)

        # scan forward for data row: must start with '$'; skip label rows containing ' EXP '
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

        # mark price = first $ token
        m = MONEY_RE.search(data_row)
        mark = to_float(m.group(1)) if m else None

        # anchored parse after ITM/OTM
        itm_flag, dte, delta, oi, qty = parse_after_itm_block(data_row)

        opt = OptionRow(sym, date, strike, cp, dte, delta, oi, qty, mark, itm_flag, data_row)
        opts.append(opt)
        if DEBUG:
            print(dim(f"[DEBUG] Parsed {sym} {date} {strike} {cp} | mark={mark} ITM={itm_flag} DTE={dte} Δ={delta} OI={oi} qty={qty} | {data_row}"))
        i = j + 1
    return opts

# ───────── PMCC logic ─────────
def is_long_leap_call(o: OptionRow, last: float) -> bool:
    if o.cp != 'C' or o.qty is None or o.qty <= 0 or o.dte is None: return False
    itm_ok = (o.itm_flag == 'ITM') or (last is not None and last > o.strike)
    return itm_ok and o.dte >= 90 and (o.delta is None or o.delta >= 0.65)

def is_short_near_call(o: OptionRow, last: float) -> bool:
    if o.cp != 'C' or o.qty is None or o.qty >= 0 or o.dte is None: return False
    otm_ok = (o.itm_flag == 'OTM') or (last is not None and last < o.strike)
    return otm_ok and 7 <= o.dte <= 60 and (o.delta is None or 0.15 <= abs(o.delta) <= 0.55)

def cycles_left(dte_long: int) -> int:
    if dte_long is None: return 0
    return max(0, floor((dte_long - 21) / 30))

def long_extrinsic(long: OptionRow, last: float) -> float:
    if long.mark is None or last is None: return None
    intrinsic = max(0.0, last - long.strike)
    return max(0.0, long.mark - intrinsic)

def coverage_ok(extr: float, dte_long: int, short_mark: float):
    if extr is None or dte_long is None: return (None, None)
    cyc = cycles_left(dte_long)
    if cyc == 0: return (True, 0.0)
    req = extr / cyc
    ok = (short_mark or 0.0) >= 0.8 * req  # 80% heuristic
    return ok, req

def gtc_target(short_mark: float, take_pct: float = 0.5):
    if short_mark is None: return None
    return max(0.05, round(short_mark * (1 - take_pct), 2))

# ───────── rendering ─────────
def banner(sym):
    print("─"*70)
    print(f"{sym}  |  PMCC TRADE TICKET")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(now)
    print("─"*70)

def first_check_lines(und: Underlying, long: OptionRow, short: OptionRow):
    # Binary gate results with PASS/FAIL coloring
    def lab(b): return green("PASS") if b else red("FAIL")
    # LEAP ITM
    leap_itm = (long.itm_flag == 'ITM') or (und.last is not None and und.last > long.strike)
    # LEAP DTE
    leap_dte = (long.dte or 0) >= 90
    # LEAP Δ
    leap_delta = (long.delta is None) or (long.delta >= 0.65)
    # Short OTM
    short_otm = (short.itm_flag == 'OTM') or (und.last is not None and und.last < short.strike)
    # Short DTE band
    short_dte_band = (short.dte is not None and 7 <= short.dte <= 60)
    # Short Δ band
    sd = abs(short.delta) if short.delta is not None else None
    short_delta_band = (sd is not None and 0.15 <= sd <= 0.55)

    print("\nFirst Check")
    print(f"  LEAP ITM: {lab(leap_itm)}")
    print(f"  LEAP DTE ≥ 90: {lab(leap_dte)}")
    print(f"  LEAP Δ ≥ 0.65: {lab(leap_delta)}")
    print(f"  Short OTM: {lab(short_otm)}")
    print(f"  Short DTE in [7,60]: {lab(short_dte_band)}")
    print(f"  Short Δ in [0.15,0.55]: {lab(short_delta_band)}")

def deep_analysis(und: Underlying, long: OptionRow, short: OptionRow):
    extr = long_extrinsic(long, und.last)
    cyc  = cycles_left(long.dte)
    ok_cov, req30 = coverage_ok(extr, long.dte, short.mark)
    tgt = gtc_target(short.mark, 0.5)
    net_delta = (long.delta or 0.0) + (short.delta or 0.0)

    # Fixed-width KPI block
    print("\nDeep Analysis")
    _print_pair("LEAP extrinsic",        _fmt_money(extr, 2),
                "Cycles left (≈30D)",    (str(cyc) if cyc is not None else "N/A"))
    _print_pair("Required / 30D",        _fmt_money(req30, 2),
                "Short credit (mark)",   _fmt_money(short.mark, 2))
    cov_txt = "OK" if ok_cov is True else ("MARG/INSUF" if ok_cov is False else "N/A")
    _print_pair("Coverage status",       cov_txt,
                "Short 50% GTC",         _fmt_money(tgt, 2))
    _print_pair("Net Δ (long+short)",    _fmt_delta(net_delta, 3),
                "", "")

    # Recommendations
    recs = []
    if short.dte is not None and short.dte <= 21:
        recs.append("• Short DTE ≤ 21 → set roll/close.")
    if short.delta is not None and abs(short.delta) >= 0.55:
        recs.append("• Short Δ ≥ 0.55 → consider roll up/out.")
    if ok_cov is False and req30 is not None:
        recs.append(f"• Roll short for ≥ ~{_fmt_money(0.8*req30,2)} credit per 30D to cover LEAP extrinsic cadence.")
    if extr is not None and extr <= 0.50 and (long.dte or 0) >= 90:
        recs.append("• LEAP extrinsic ~spent → evaluate harvesting (close or roll LEAP up/out).")
    if not recs:
        recs.append("• Coverage OK vs. required/30D — maintain cadence.")

    print("\nRecommendations")
    for r in recs: print(f"  {r}")

def report(sym, und: Underlying, long: OptionRow, short: OptionRow):
    banner(sym)

    # Status chips
    short_band = "Δ in band" if (short.delta is not None and 0.15 <= abs(short.delta) <= 0.55) else "Δ out of band"
    short_moneyness = "Short OTM" if ((short.itm_flag == 'OTM') or (und.last is not None and und.last < short.strike)) else "Short tested"
    print(f"[ {short_moneyness} ]  [ {short_band} ]\n")

    # Header block (non-jiggly)
    print(f"Underlying: {fmt(und.last)}")
    print(f"LEAP (long): {long.strike:.2f} C  • Exp {long.exp}  • DTE {long.dte if long.dte is not None else 'N/A'}"
          f"  • Δ {_fmt_delta(long.delta,3)}  • Mark {fmt(long.mark)}")
    print(f"Short (covered): {short.strike:.2f} C  • Exp {short.exp}  • DTE {short.dte if short.dte is not None else 'N/A'}"
          f"  • Δ {_fmt_delta(short.delta,3)}  • Mark {fmt(short.mark)}")

    first_check_lines(und, long, short)
    deep_analysis(und, long, short)

    print("─"*70 + "\n")

def main():
    lines = read_lines()
    und_map = detect_underlyings(lines)
    opts = parse_options(lines)

    by = {}
    for o in opts: by.setdefault(o.symbol, []).append(o)

    found = 0
    for sym, rows in by.items():
        und = und_map.get(sym, Underlying(sym, None))
        longs  = [r for r in rows if is_long_leap_call(r, und.last)]
        shorts = [r for r in rows if is_short_near_call(r, und.last)]
        if not longs or not shorts: continue

        # best long: max DTE, then highest Δ
        long_pick = sorted(longs, key=lambda r: (r.dte or 0, r.delta or 0), reverse=True)[0]
        # best short: DTE closest to 35, then Δ near 0.35
        def key_short(r):
            return (abs((r.dte or 0) - 35), abs((abs(r.delta or 0.35)) - 0.35))
        short_pick = sorted(shorts, key=key_short)[0]

        report(sym, und, long_pick, short_pick)
        found += 1

    if found == 0:
        print(red("No PMCC pairs detected (need a long-dated ITM call and a shorter-dated short OTM call on the same symbol)."))

if __name__ == "__main__":
    main()