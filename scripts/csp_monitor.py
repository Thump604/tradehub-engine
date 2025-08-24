#!/usr/bin/env python3
# csp_monitor.py — Covered Short Put monitor from pasted broker rows (no headers).
# CLI:
#   --fill SYMBOL=PRICE (repeatable)  → entry credit for short put
#   --gtc "50,75"                     → tiered GTC %s (always used)
#
# Tiering rules:
#   - if any --fill present and no --gtc given → tiers = [50, 75]
#   - if no --fill and no --gtc → tiers = [50] (estimate off current mark)
#
# Project assumption: broker rows are clean & stable.

import sys, re, argparse, os
from dataclasses import dataclass
from datetime import datetime, timezone
from math import floor

# ---------- ANSI ----------
class C:
    R="\033[31m"; G="\033[32m"; Y="\033[33m"; B="\033[34m"; M="\033[35m"; Cc="\033[36m"
    DIM="\033[2m"; RESET="\033[0m"; BOLD="\033[1m"
def color(s, k): return f"{k}{s}{C.RESET}"
def green(s): return color(s, C.G)
def yellow(s): return color(s, C.Y)
def red(s): return color(s, C.R)
def bold(s): return color(s, C.BOLD)
def dim(s): return color(s, C.DIM)

# ---------- fmt ----------
def fmt_money(x, none="N/A"): return none if x is None else f"${x:,.2f}"
def fmt_num(x, none="N/A"): return none if x is None else f"{x:.3f}"
def fmt_int(x, none="N/A"): return none if x is None else f"{int(x)}"
def fmt_pct(x, none="N/A"): return none if x is None else f"{x*100:.2f}%"
def fmt_pct_raw(p, none="N/A"): return none if p is None else f"{p:.2f}%"

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

# ---------- models ----------
@dataclass
class Underlying:
    symbol: str
    last: float = None

@dataclass
class OptionRow:
    symbol: str
    exp: str
    strike: float
    cp: str            # 'C'/'P'
    dte: int = None
    delta: float = None
    oi: int = None
    qty: int = None    # -1 short, +1 long
    mark: float = None
    itm_flag: str = None  # 'ITM'/'OTM'
    raw: str = ""

# ---------- regex ----------
DATE_RE   = re.compile(r'(\d{2}/\d{2}/\d{4})')  # MM/DD/YYYY
HEADER_RE = re.compile(r'^([A-Z][A-Z0-9\.]{0,6})\s+(\d{2}/\d{2}/\d{4})\s+(\d+(?:\.\d+)?)\s+(C|P)\b')
MONEY_RE  = re.compile(r'\$(-?\d+(?:\.\d+)?)')
ITM_RE    = re.compile(r'\b(ITM|OTM)\b')

def _norm(s: str) -> str: return s.replace('\t', ' ').strip()

# ---------- market context ----------
def load_market_state(path):
    if not path or not os.path.exists(path): return None
    try:
        import yaml
        with open(path, 'r') as f:
            y = yaml.safe_load(f) or {}
        return y
    except Exception:
        return None

def print_market_banner(state):
    if not state:
        print("REGIME N/A | TREND N/A | VOL N/A")
        print("─"*74)
        print(bold("MARKET CONTEXT"))
        print("─"*74)
        return
    regime = state.get("overall_regime", "N/A")
    trend  = state.get("trend_bias", "N/A")
    vol    = state.get("volatility", "N/A")
    line = f"REGIME {regime} | TREND {trend} | VOL {vol}"
    print(line)
    print("─"*74)
    print(bold("MARKET CONTEXT"))
    print("─"*74)

# ---------- input ----------
def read_lines():
    print("───────── CSP MONITOR — Paste Covered Short Put (CSP) rows. ──────────")
    print("Include: underlying line, short put header, and first data line. Ctrl-D to end.")
    raw = sys.stdin.read()
    return [_norm(l) for l in raw.splitlines() if _norm(l)]

# ---------- parse underlyings ----------
def detect_underlyings(lines):
    under = {}
    sym = None
    for line in lines:
        if re.fullmatch(r'[A-Z][A-Z0-9\.]{0,6}', line):
            sym = line; continue
        if sym and line.startswith('$'):
            m = MONEY_RE.match(line)
            if m:
                last = to_float(m.group(1))
                under[sym] = Underlying(sym, last)
            sym = None
    return under

# ---------- option parse helpers ----------
def token_split(row: str):
    row = row.replace('+', ' +').replace('-', ' -')
    return [t for t in row.split() if t]

def parse_after_itm_block(row: str):
    m = ITM_RE.search(row)
    if not m: return None, None, None, None, None
    itm = m.group(1)
    toks = token_split(row)
    idx = None
    for i,t in enumerate(toks):
        if t == itm: idx = i; break
    if idx is None: return itm, None, None, None, None
    def get(i): return toks[i].replace(',', '') if 0 <= i < len(toks) else None
    dte = to_int(get(idx+1))
    delta = to_float(get(idx+2))
    oi = to_int(get(idx+3))
    qty = to_int(get(idx+4))
    if qty not in (-1,1):
        q2 = to_int(get(idx+5))
        if q2 in (-1,1): qty = q2
    return itm, dte, delta, oi, qty

def parse_options(lines):
    opts = []
    n = len(lines); i = 0
    while i < n:
        h = HEADER_RE.match(lines[i])
        if not h: i += 1; continue
        sym, date, strike_s, cp = h.group(1), h.group(2), h.group(3), h.group(4)
        strike = float(strike_s)
        data_row = None
        j = i+1
        while j < n and j <= i+8:
            row = lines[j]
            if ' EXP ' in f" {row} " or row.upper().startswith('CALL ') or row.upper().startswith('PUT '):
                j += 1; continue
            if row.startswith('$'):
                data_row = row; break
            j += 1
        if not data_row:
            i += 1; continue
        m = MONEY_RE.search(data_row)
        mark = to_float(m.group(1)) if m else None
        itm_flag, dte, delta, oi, qty = parse_after_itm_block(data_row)
        opt = OptionRow(sym, date, strike, cp, dte, delta, oi, qty, mark, itm_flag, data_row)
        opts.append(opt)
        i = j+1
    return opts

# ---------- CSP logic ----------
def is_short_put(o: OptionRow, last: float) -> bool:
    if o.cp != 'P' or o.qty is None or o.qty >= 0 or o.dte is None: return False
    return True

def breakeven_from_ask(strike: float, ask: float):
    if strike is None or ask is None: return None
    return strike - ask

def roc_annual(credit: float, strike: float, dte: int):
    if credit is None or strike is None or dte is None or strike <= 0 or dte <= 0: return None
    roc = credit / strike
    return roc * (365.0 / dte)

def pop_proxy_from_delta(delta_abs: float):
    if delta_abs is None: return None
    return (1.0 - delta_abs) * 100.0

def policy_pass(o: OptionRow, last: float):
    checks = {
        "DTE in [21,60]": (o.dte is not None and 21 <= o.dte <= 60),
        "Δ in [0.15,0.35]": (o.delta is not None and 0.15 <= abs(o.delta) <= 0.35),
        "Short strike not tested": (last is not None and o.strike is not None and last < o.strike),
        "OI ≥ 100": (o.oi is not None and o.oi >= 100),
    }
    return checks

def pad_pair(l_label, l_val, r_label, r_val, w=30, vw=20):
    lv = l_val if isinstance(l_val, str) else str(l_val)
    rv = r_val if isinstance(r_val, str) else str(r_val)
    print(f"{l_label:<{w}} {lv:>{vw}}   {r_label:<{w}} {rv:>{vw}}")

def bullet_line(s, tone=""):
    if tone == "ok": return "• " + green(s)
    if tone == "warn": return "• " + yellow(s)
    if tone == "risk": return "• " + red(s)
    return "• " + s

def gtc_targets(credit_basis: float, tiers):
    if credit_basis is None: return None
    out = []
    for p in tiers:
        tgt = round(max(0.05, credit_basis * (1 - p/100.0)), 2)
        out.append((p, tgt))
    return out

def report(sym, und: Underlying, short: OptionRow, fills, tiers, market=None):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"{sym}  |  CSP TRADE TICKET")
    print(ts)
    print("─"*34 + "  " + "─"*34)
    # Badges
    tested = und.last is not None and und.last < (short.strike or 0)
    band = short.delta is not None and 0.15 <= abs(short.delta) <= 0.35
    dte_ok = short.dte is not None and 21 <= short.dte <= 60
    badges = []
    badges.append("[ Tested (under strike) ]" if tested else "[ Not tested ]")
    badges.append("[ Δ in 0.15–0.35 band ]" if band else "[ Δ out of band ]")
    badges.append("[ DTE in band ]" if dte_ok else "[ DTE out of band ]")
    print(" ".join(badges)); print()

    # Header
    print(f"Underlying: {fmt_money(und.last)}")
    print(f"Short Put:  {short.strike:>7.3f} P  • Exp {short.exp}  • DTE {short.dte}  • Δ {fmt_num(short.delta)}  • Mark {fmt_money(short.mark)}")
    print()

    # Policy
    print("Policy")
    print("Policy → Δ band 0.15–0.35 • DTE 21–60 • TP 50% • roll@21d")
    print()

    # Key metrics
    be = breakeven_from_ask(short.strike, short.mark)
    pop = pop_proxy_from_delta(abs(short.delta) if short.delta is not None else None)
    collateral = (short.strike or 0)*100.0
    net_coll = (be or 0)*100.0 if be is not None else None
    roc = (short.mark or 0)/(short.strike or 1) if (short.mark and short.strike) else None
    roc_ann = roc_annual(short.mark, short.strike, short.dte)

    pad_pair("Breakeven:", fmt_money(be), "POP (proxy):", fmt_pct_raw(pop))
    pad_pair("Collateral (gross):", fmt_money(collateral), "Net collateral:", fmt_money(net_coll))
    pad_pair("ROC:", fmt_pct(roc), "Annualized ROC:", fmt_pct(roc_ann))
    pad_pair("Open interest:", fmt_int(short.oi), "Market regime:",
             f"{market.get('overall_regime','N/A')} | {market.get('trend_bias','N/A')} | vol {market.get('volatility','N/A')}" if market else "N/A")
    print()

    # Checklist
    print("Checklist")
    checks = policy_pass(short, und.last)
    for label, ok in checks.items():
        print(f"  {label}: {'PASS' if ok else 'FAIL'}")
    print()

    # Playbook
    print("Playbook — What, Why, When, How")
    if tested: print(bullet_line("Tested/ITM → assignment risk increased; consider roll down/out.", "risk"))
    else: print(bullet_line("OTM short → theta decay works; assignment risk lower.", "ok"))
    if band: print(bullet_line("Δ 0.15–0.35 → balanced POP vs. credit.", "ok"))
    else: print(bullet_line("Δ outside band → POP/credit balance off; adjust strikes or DTE.", "warn"))
    if dte_ok: print(bullet_line("DTE 21–60 → efficient theta window.", "ok"))
    print()

    # GTC targets
    fill = fills.get(sym) if fills else None
    basis = fill if fill is not None else short.mark
    label = "GTC targets (from fill)" if fill is not None else "GTC targets (est., no fill)"
    targets = gtc_targets(basis, tiers)
    if targets:
        parts = [f"{int(p)}%→{fmt_money(t)}" for p,t in targets]
        print(label + ": " + ", ".join(parts))
    print("─"*70)

# ---------- args ----------
def parse_args():
    ap = argparse.ArgumentParser(description="CSP monitor")
    ap.add_argument("--state", help="outputs/market_state.yml for banner", default=None)
    ap.add_argument("--fill", action="append", help="Override short fill credit, SYMBOL=PRICE (repeatable)", default=[])
    ap.add_argument("--gtc", default=None, help="Comma list of GTC tiers, e.g., '50,75'")
    return ap.parse_args()

def main():
    args = parse_args()
    market = load_market_state(args.state)
    print_market_banner(market)
    lines = read_lines()
    under = detect_underlyings(lines)
    opts = parse_options(lines)

    # group shorts
    by = {}
    for o in opts:
        if is_short_put(o, under.get(o.symbol, Underlying(o.symbol)).last):
            by.setdefault(o.symbol, []).append(o)
    if not by:
        print("No CSP rows detected (need a short put header and its first price/data line on the same symbol).")
        return

    # fills
    fills = {}
    for item in args.fill:
        if '=' in item:
            sym, val = item.split('=',1)
            fills[sym.strip().upper()] = to_float(val.strip())

    # tiers
    tiers = []
    if args.gtc:
        for t in args.gtc.split(','):
            t = t.strip()
            if t:
                try: tiers.append(float(t))
                except: pass
    else:
        tiers = [50.0, 75.0] if fills else [50.0]

    # choose one per symbol & render
    for sym, rows in by.items():
        und = under.get(sym, Underlying(sym, None))
        pick = sorted(rows, key=lambda r: (abs((r.dte or 0)-35), abs((abs(r.delta or 0.25))-0.25)))[0]
        report(sym, und, pick, fills, tiers, market=market)

if __name__ == "__main__":
    main()