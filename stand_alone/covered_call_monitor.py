#!/usr/bin/env python3
# covered_call_monitor.py — Covered Call monitor from pasted broker rows (no headers).
# Paste the underlying line (with $last), the option header (e.g., "PLTR 10/18/2025 25.00 C"),
# and the first price/data line that follows. Then Ctrl-D.
#
# Scope:
#  • Detect short call legs (Qty = -1, Type = C) as covered calls (assumes you own 100 sh/sh-leg elsewhere).
#  • Tasty-style checks: DTE 21–60, Δ ~0.25–0.40, short strike not tested (under ≤ strike).
#  • KPIs: credit, breakeven on stock (last − credit), POP proxy (=1−Δ), ROI on option (credit/underlying),
#          Annualized ROI, “tested” chips, manage window chips, 50% GTC target.
#  • If underlying > strike, shows intrinsic at-risk (call is ITM) and suggests roll/close lanes.
#
# No external deps. DEBUG_CC=1 to trace parsing.

import sys, re, os
from dataclasses import dataclass
from datetime import datetime, timezone

DEBUG = bool(os.environ.get("DEBUG_CC"))

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

# ───────── fixed-width KPI formatting ─────────
def _s(x): return "N/A" if x is None else str(x)
def _fmt_num(x, p=3):
    if x is None: return "N/A"
    try: return f"{float(x):.{p}f}"
    except: return "N/A"
def _fmt_money(x, p=2):
    if x is None: return "N/A"
    try: return f"${float(x):,.{p}f}"
    except: return "N/A"
def _fmt_pct(x, p=2):
    if x is None: return "N/A"
    try: return f"{float(x)*100:.{p}f}%"
    except: return "N/A"
def _print_pair(label_l, val_l, label_r, val_r, *, lw=24, vw=12):
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
HEADER_RE = re.compile(r'^([A-Z][A-Z0-9\.]{0,6})\s+(\d{2}/\d{2}/\d{4})\s+(\d+(?:\.\d+)?)\s+(C|P)\b')
MONEY_RE  = re.compile(r'\$(-?\d+(?:\.\d+)?)')
ITM_RE    = re.compile(r'\b(ITM|OTM)\b')

def _norm(s: str) -> str: return s.replace('\t', ' ').strip()

# ───────── input ─────────
def read_lines():
    print("Paste your position rows (no headers). Include the underlying line, the option header "
          "(e.g., 'PLTR 10/18/2025 25.00 C'), and the first price/data line after labels. Then press Ctrl-D.")
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
        if re.fullmatch(r'[A-Z][A-Z0-9\.]{0,6}', line):
            sym = line
            continue
        if sym and line.startswith('$'):
            m = MONEY_RE.match(line)
            if m:
                under[sym] = Underlying(sym, to_float(m.group(1)))
                if DEBUG: print(dim(f"[DEBUG] Underlying {sym}: last={under[sym].last} via: {line}"))
            sym = None
    return under

# ───────── option parse helpers ─────────
def token_split(row: str):
    row = row.replace('+', ' +').replace('-', ' -')
    return [t for t in row.split() if t]

def parse_after_itm_block(row: str):
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
        if 0 <= i < len(toks):
            return toks[i].replace(',', '')
        return None

    dte   = to_int(get(idx+1))
    delta = to_float(get(idx+2))
    oi    = to_int(get(idx+3))
    qty   = to_int(get(idx+4))
    if qty is not None and qty not in (-1, 1):
        q2 = to_int(get(idx+5))
        if q2 in (-1, 1): qty = q2
    return itm, dte, delta, oi, qty

# ───────── option parse ─────────
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

        opts.append(OptionRow(sym, date, strike, cp, dte, delta, oi, qty, mark, itm_flag, data_row))
        if DEBUG:
            print(dim(f"[DEBUG] Parsed {sym} {date} {strike} {cp} | mark={mark} ITM={itm_flag} "
                      f"DTE={dte} Δ={delta} OI={oi} qty={qty} | {data_row}"))
        i = j + 1
    return opts

# ───────── Covered Call logic ─────────
def is_short_call(o: OptionRow) -> bool:
    return (o.cp == 'C') and (o.qty == -1)

def breakeven_stock(last: float, credit: float):
    if last is None or credit is None: return None
    return last - credit

def pop_proxy_from_delta(delta: float):
    if delta is None: return None
    # For covered calls, delta typically positive; long stock synthetically adds +1 delta,
    # but POP on short call leg alone ~ 1 - Δ.
    return max(0.0, min(1.0, 1.0 - max(0.0, float(delta))))

def roi_option(credit: float, last: float):
    if credit in (None, 0) or last in (None, 0): return None
    return credit / last

def ann_roi(roi: float, dte: int):
    if roi is None or not dte or dte <= 0: return None
    return roi * (365.0 / float(dte))

def tested_chip(last: float, strike: float):
    if last is None or strike is None: return "[ Strike test: N/A ]"
    if last > strike: return "[ Tested (price > strike) ]"
    if abs(last - strike) / strike <= 0.01: return "[ Near test (≤1%) ]"
    return "[ Not tested ]"

def dte_chip(dte: int):
    if dte is None: return "[ DTE: N/A ]"
    if dte <= 7:    return "[ DTE ≤7: Manage/Close ]"
    if dte <= 21:   return "[ DTE ≤21: Manage Window ]"
    if 22 <= dte <= 29: return "[ DTE 22–29: Prep 21D Roll ]"
    if 30 <= dte <= 45: return "[ DTE 30–45: Ideal Entry/Theta ]"
    if dte <= 60:   return "[ DTE 46–60: Acceptable ]"
    return "[ DTE >60: Far-dated (consider closer cycle) ]"

def delta_chip(delta: float):
    if delta is None: return "[ Δ: N/A ]"
    a = abs(delta)
    if 0.25 <= a <= 0.40: return "[ Δ in 0.25–0.40 band ]"
    if a < 0.25:          return "[ Δ < 0.25: Lower credit ]"
    return "[ Δ > 0.40: Higher call-away risk ]"

# ───────── rendering ─────────
def banner(sym):
    print("─"*70)
    print(f"{sym}  |  COVERED CALL TRADE TICKET")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(now)
    print("─"*70)

def checks_and_actions(und_last, o: OptionRow, credit: float):
    checks = []
    actions = []

    dte_ok   = (o.dte is not None and 21 <= o.dte <= 60)
    delta_ok = (o.delta is not None and 0.25 <= abs(o.delta) <= 0.40)
    not_test = (und_last is not None and o.strike is not None and und_last <= o.strike)

    checks.append(f"  DTE in [21,60]: {'PASS' if dte_ok else 'FAIL'}")
    checks.append(f"  Δ in [0.25,0.40]: {'PASS' if delta_ok else 'FAIL'}")
    checks.append(f"  Short strike not tested: {'PASS' if not_test else 'FAIL'}")
    if o.oi is not None:
        checks.append(f"  OI ≥ 100: {'PASS' if o.oi >= 100 else 'FAIL'}")

    # Guidance
    if o.dte is not None and o.dte <= 21:
        actions.append("• In manage window (≤21 DTE): close or roll out to 30–45 DTE for a net credit, keep Δ ~0.30.")
    if und_last is not None and o.strike is not None and und_last > o.strike:
        actions.append("• Tested/ITM: roll up/out for credit to extend duration & raise strike, or allow call-away.")
    elif und_last is not None and o.strike is not None and abs(und_last - o.strike)/o.strike <= 0.01:
        actions.append("• Near test (≤1%): consider proactive roll for small credit to keep shares.")
    if o.dte is not None and 30 <= o.dte <= 45 and delta_ok and not_test:
        actions.append("• In ideal window: maintain; set 50% profit GTC.")
    if credit is not None:
        actions.append(f"• Profit target: close near 50% — GTC ≈ {_fmt_money(credit*0.5)}.")
    if not actions:
        actions.append("• Hold / monitor; reassess on vol shifts or Δ moves.")

    return checks, actions

def render_card(und: Underlying, o: OptionRow):
    banner(o.symbol)
    chips = "  ".join([
        tested_chip(und.last, o.strike),
        delta_chip(o.delta),
        dte_chip(o.dte),
    ])
    print(f"{chips}\n")

    credit = o.mark
    be     = breakeven_stock(und.last, credit)
    pop_p  = pop_proxy_from_delta(o.delta)
    roi    = roi_option(credit, und.last)
    aroi   = ann_roi(roi, o.dte)

    # intrinsic if tested ITM
    intr = None
    if und.last is not None and o.strike is not None and und.last > o.strike:
        intr = und.last - o.strike

    print(f"Underlying: {_fmt_money(und.last)}")
    print(f"Short Call: {o.strike:.2f} C  • Exp {o.exp}  • DTE {o.dte if o.dte is not None else 'N/A'}"
          f"  • Δ {_fmt_num(o.delta,3)}  • Mark {_fmt_money(credit)}")

    print("\nKey Metrics")
    _print_pair("Breakeven on stock", _fmt_money(be),
                "POP (proxy)",        _fmt_pct(pop_p,1))
    _print_pair("Option ROI",         _fmt_pct(roi,2),
                "Annualized ROI",     _fmt_pct(aroi,2))
    _print_pair("Open interest",      (f"{o.oi:,}" if o.oi is not None else "N/A"),
                "ITM intrinsic",      (_fmt_money(intr) if intr is not None else "0"))

    checks, actions = checks_and_actions(und.last, o, credit)

    print("\nChecklist")
    for c in checks:
        tag = c.split(':')[-1].strip()
        colored = c.replace(tag, green(tag) if tag=="PASS" else red(tag))
        print(colored)

    print("\nRecommendations")
    for a in actions:
        print(f"  {a}")

    print("─"*70 + "\n")

# ───────── main ─────────
def main():
    # reuse your established parser UX
    raw = read_lines()
    und_map = detect_underlyings(raw)
    opts = parse_options(raw)

    found = 0
    for o in opts:
        if not is_short_call(o):
            if DEBUG: print(dim(f"[SKIP] Not a short call: {o.symbol} {o.exp} {o.strike}{o.cp} qty={o.qty}"))
            continue
        und = und_map.get(o.symbol, Underlying(o.symbol, None))
        render_card(und, o)
        found += 1

    if found == 0:
        print(red("No covered call candidates detected (need a short call leg: Qty = -1, Type = C)."))

if __name__ == "__main__":
    main()