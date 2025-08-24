#!/usr/bin/env python3
# csp_monitor.py — Cash-Secured Put monitor from pasted broker rows (no headers).
# Purpose: Paste your position rows and get a tasty-style “trade ticket” with fixed KPIs,
#          checks, and actionable guidance. No external dependencies.
#
# Expected paste (same UX as your PMCC/vertical tools):
#   • Underlying line with last price (symbol on its own line, then a line starting with $last)
#   • Option header:  "SYM MM/DD/YYYY STRIKE P"
#   • First data line after labels, starting with "$" and containing "... ITM/OTM DTE Δ OI Qty ..."
# Then press Ctrl-D.
#
# Notes:
#   • We treat Qty = -1 and Type = P as CSP candidates.
#   • Credit uses the parsed mark (current). Breakeven = strike − mark.
#   • Collateral (gross) = strike*100. Net collateral ≈ (strike − credit)*100.
#   • ROC = credit*100 / net_collateral. Ann. ROC = ROC * 365/DTE (if DTE available).
#   • POP proxy = 1 − |Δ|.
#
# DEBUG: set DEBUG_CSP=1 to see parse traces.

import sys, re, os
from dataclasses import dataclass
from datetime import datetime, timezone

DEBUG = bool(os.environ.get("DEBUG_CSP"))

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

def _fmt_num(x, p=4):
    if x is None: return "N/A"
    try: return f"{float(x):.{p}f}"
    except: return "N/A"

def _fmt_money(x, p=2, allow_na=True):
    if x is None: return "N/A" if allow_na else "$0.00"
    try: return f"${float(x):,.{p}f}"
    except: return "N/A" if allow_na else "$0.00"

def _fmt_pct(x, p=1):
    if x is None: return "N/A"
    try: return f"{float(x)*100:.{p}f}%"
    except: return "N/A"

def _print_pair(label_l, val_l, label_r, val_r, *, lw=24, vw=12):
    ll = (label_l + ":") if label_l else ""
    lr = (label_r + ":") if label_r else ""
    print(f"{ll:<{lw}} {_s(val_l):>{vw}}   {lr:<{lw}} {_s(val_r):>{vw}}")

# ───────── simple fmt ─────────
def fmt_money(x): return "N/A" if x is None else f"${x:,.2f}"

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
          "(e.g., 'PLTR 10/18/2025 25.00 P'), and the first price/data line after labels. Then press Ctrl-D.")
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
    Expect '... (ITM|OTM) DTE Delta OI Qty ...' after the pricing section.
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

        opt = OptionRow(sym, date, strike, cp, dte, delta, oi, qty, mark, itm_flag, data_row)
        opts.append(opt)
        if DEBUG:
            print(dim(f"[DEBUG] Parsed {sym} {date} {strike} {cp} | mark={mark} ITM={itm_flag} "
                      f"DTE={dte} Δ={delta} OI={oi} qty={qty} | {data_row}"))
        i = j + 1
    return opts

# ───────── CSP logic ─────────
def is_short_put(o: OptionRow) -> bool:
    return (o.cp == 'P') and (o.qty == -1)

def breakeven(strike: float, credit: float):
    if strike is None or credit is None: return None
    return strike - credit

def collateral_gross(strike: float):
    if strike is None: return None
    return strike * 100.0

def collateral_net(strike: float, credit: float):
    if strike is None or credit is None: return None
    return max(0.0, (strike - credit) * 100.0)

def roc(credit: float, net_collateral: float):
    if credit is None or net_collateral in (None, 0): return None
    return (credit * 100.0) / net_collateral

def ann_roc(roc_val: float, dte: int):
    if roc_val is None or not dte or dte <= 0: return None
    return roc_val * (365.0 / float(dte))

def pop_proxy(delta: float):
    if delta is None: return None
    return max(0.0, min(1.0, 1.0 - abs(delta)))

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
    if 0.15 <= a <= 0.35: return "[ Δ in 0.15–0.35 band ]"
    if a < 0.15:          return "[ Δ < 0.15: Low credit/POP high ]"
    return "[ Δ > 0.35: Higher assignment risk ]"

def tested_chip(under_last: float, strike: float):
    if under_last is None or strike is None: return "[ Strike test: N/A ]"
    if under_last < strike: return "[ Tested (under strike) ]"
    # near-tested within ~1% of strike
    if abs(under_last - strike) / strike <= 0.01: return "[ Near test (≤1%) ]"
    return "[ Not tested ]"

# ───────── rendering ─────────
def banner(sym):
    print("─"*70)
    print(f"{sym}  |  CSP TRADE TICKET")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(now)
    print("─"*70)

def checks_and_actions(und: Underlying, o: OptionRow, credit: float, be: float):
    checks = []
    actions = []

    # Checks (tasty-style)
    dte_ok   = (o.dte is not None and 21 <= o.dte <= 60)
    delta_ok = (o.delta is not None and 0.15 <= abs(o.delta) <= 0.35)
    not_test = (und.last is not None and o.strike is not None and und.last >= o.strike)

    checks.append(f"  DTE in [21,60]: {'PASS' if dte_ok else 'FAIL'}")
    checks.append(f"  Δ in [0.15,0.35]: {'PASS' if delta_ok else 'FAIL'}")
    checks.append(f"  Short strike not tested: {'PASS' if not_test else 'FAIL'}")

    # OI sanity if present
    if o.oi is not None:
        oi_ok = o.oi >= 100
        checks.append(f"  OI ≥ 100: {'PASS' if oi_ok else 'FAIL'}")

    # Guidance (priority by risk / DTE)
    if o.dte is not None and o.dte <= 21:
        actions.append("• In manage window (≤21 DTE): close or roll out to 30–45 DTE for a net credit; keep Δ ≈ current.")
    if und.last is not None and o.strike is not None and und.last <= o.strike:
        actions.append("• Tested: roll down/OUT (further DTE) for credit; or accept assignment if wheel fits plan.")
    elif und.last is not None and o.strike is not None and abs(und.last - o.strike)/o.strike <= 0.01:
        actions.append("• Near test (≤1%): consider proactive roll for small credit to push breakeven lower.")
    if o.dte is not None and 30 <= o.dte <= 45 and delta_ok and not_test:
        actions.append("• In ideal window: maintain; set 50% profit GTC.")
    if credit is not None and o.dte:
        actions.append(f"• Profit target: close near 50% max — if entering now, GTC ≈ {_fmt_money(credit*0.5)}.")
    # ROC efficiency tag
    net_coll = collateral_net(o.strike, credit)
    r = roc(credit, net_coll) if net_coll is not None else None
    if r is not None and o.dte:
        aroc = ann_roc(r, o.dte)
        if aroc is not None and aroc < 0.10:
            actions.append("• Low annualized ROC (<10%): consider different strike/underlying to improve efficiency.")

    if not actions:
        actions.append("• Hold / monitor; reassess on vol shifts or Δ moves.")

    return checks, actions

def render_card(und: Underlying, o: OptionRow):
    banner(o.symbol)

    # Status chips
    chips = "  ".join([
        tested_chip(und.last, o.strike),
        delta_chip(o.delta),
        dte_chip(o.dte),
    ])
    print(f"{chips}\n")

    # Core metrics
    credit   = o.mark
    be       = breakeven(o.strike, credit)
    cg       = collateral_gross(o.strike)
    cn       = collateral_net(o.strike, credit)
    r        = roc(credit, cn) if cn is not None else None
    aroc     = ann_roc(r, o.dte) if r is not None else None
    pop_p    = pop_proxy(o.delta)

    # Header lines
    print(f"Underlying: {fmt_money(und.last)}")
    print(f"Short Put:  {o.strike:.2f} P  • Exp {o.exp}  • DTE {o.dte if o.dte is not None else 'N/A'}"
          f"  • Δ {_fmt_num(o.delta,3)}  • Mark {fmt_money(credit)}")

    # Fixed-width KPI pairs
    print("\nKey Metrics")
    _print_pair("Breakeven",            _fmt_money(be, 2),
                "POP (proxy)",          _fmt_pct(pop_p, 1))
    _print_pair("Collateral (gross)",   _fmt_money(cg, 0),
                "Net collateral",       _fmt_money(cn, 0))
    _print_pair("ROC",                  _fmt_pct(r, 2),
                "Annualized ROC",       _fmt_pct(aroc, 2))
    _print_pair("Open interest",        (f"{o.oi:,}" if o.oi is not None else "N/A"),
                "",                     "")

    # Checks + Actions
    checks, actions = checks_and_actions(und, o, credit, be)

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
    lines = read_lines()
    und_map = detect_underlyings(lines)
    opts = parse_options(lines)

    # group by symbol for underlying mapping; render a card per CSP leg found
    found = 0
    for o in opts:
        if not is_short_put(o):
            if DEBUG: print(dim(f"[SKIP] Not a short put: {o.symbol} {o.exp} {o.strike}{o.cp} qty={o.qty}"))
            continue
        und = und_map.get(o.symbol, Underlying(o.symbol, None))
        render_card(und, o)
        found += 1

    if found == 0:
        print(red("No CSP candidates detected (need a short put leg: Qty = -1, Type = P)."))

if __name__ == "__main__":
    main()