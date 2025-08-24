#!/usr/bin/env python3
# pmcc_monitor.py — PMCC monitor (LEAP long call + short near-dated call).
# CLI:
#   --fill SYMBOL=PRICE (repeatable)  → entry credit for the SHORT CALL
#   --gtc "50,75"                     → tiered GTC %s (always used)
#
# Tiering rules:
#   - if any --fill present and no --gtc given → tiers = [50, 75]
#   - if no --fill and no --gtc → tiers = [50] (estimate off current mark)

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
    itm_flag: str = None
    raw: str = ""

# ---------- regex ----------
DATE_RE   = re.compile(r'(\d{2}/\d{2}/\d{4})')
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
    print("────────────── PMCC MONITOR — Paste PMCC position rows. ──────────────")
    print("Include: (1) underlying line, (2) LEAP call header (e.g., 'AAPL 01/16/2026 185.00 C'),")
    print("         (3) first price/data line; and (4) short call header & its first price/data line.")
    print("Then press Ctrl-D (Linux/Mac) or Ctrl-Z + Enter (Windows).")
    raw = sys.stdin.read()
    return [_norm(l) for l in raw.splitlines() if _norm(l)]

# ---------- underlyings ----------
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

# ---------- parse helpers ----------
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

# ---------- PMCC logic ----------
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

def gtc_targets(credit_basis: float, tiers):
    if credit_basis is None: return None
    out = []
    for p in tiers:
        tgt = round(max(0.05, credit_basis * (1 - p/100.0)), 2)
        out.append((p, tgt))
    return out

def bullet_line(s, tone=""):
    if tone == "ok": return "• " + green(s)
    if tone == "warn": return "• " + yellow(s)
    if tone == "risk": return "• " + red(s)
    return "• " + s

def pad_pair(l_label, l_val, r_label, r_val, w=30, vw=18):
    lv = l_val if isinstance(l_val, str) else str(l_val)
    rv = r_val if isinstance(r_val, str) else str(r_val)
    print(f"{l_label:<{w}} {lv:>{vw}}    {r_label:<{w}} {rv:>{vw}}")

def report(sym, und: Underlying, long: OptionRow, short: OptionRow, fills, tiers, market=None):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"{sym}  |  PMCC TRADE TICKET")
    print(ts)
    print("─"*34 + "  " + "─"*34)

    short_otm = und.last is not None and und.last < (short.strike or 0)
    short_band = short.delta is not None and 0.15 <= abs(short.delta) <= 0.55
    dte_sweet = short.dte is not None and 28 <= short.dte <= 45
    badges = []
    badges.append("[ Short OTM ]" if short_otm else "[ Short ITM/Tested ]")
    badges.append("[ Δ in band ]" if short_band else "[ Δ out of band ]")
    badges.append("[ DTE sweet spot ]" if dte_sweet else "[ DTE outside sweet spot ]")
    print(" ".join(badges)); print()

    # Header
    print(f"Underlying: {fmt_money(und.last)}")
    print(f"LEAP (long): {long.strike:.3f} C  • Exp {long.exp}  • DTE {long.dte}  • Δ {fmt_num(long.delta)}  • Mark {fmt_money(long.mark)}")
    print(f"Short (covered): {short.strike:.3f} C  • Exp {short.exp}  • DTE {short.dte}  • Δ {fmt_num(short.delta)}  • Mark {fmt_money(short.mark)}")
    print()

    # Policy
    print("Policy")
    print("Policy → Δ band 0.15–0.55 • short DTE 28–45 • TP 50% • roll@21d • roll if Δ>0.55")
    print()

    # First Check
    print("First Check")
    print(f"  LEAP ITM:                  {'PASS' if und.last and long.strike and und.last>long.strike else 'FAIL'}")
    print(f"  LEAP DTE ≥ 90:             {'PASS' if (long.dte or 0) >= 90 else 'FAIL'}")
    print(f"  LEAP Δ ≥ 0.65:             {'PASS' if (long.delta or 0) >= 0.65 else 'FAIL'}")
    print(f"  Short OTM:                 {'PASS' if short_otm else 'FAIL'}")
    print(f"  Short DTE in [7,60]:       {'PASS' if (short.dte or 0) >=7 and (short.dte or 0) <=60 else 'FAIL'}")
    print(f"  Short Δ in 0.15–0.55:      {'PASS' if short_band else 'FAIL'}")
    print()

    # Deep analysis
    extr = long_extrinsic(long, und.last)
    cyc  = cycles_left(long.dte)
    required = extr / cyc if (extr is not None and cyc>0) else None
    coverage = None
    if required is not None and short.mark is not None:
        coverage = "OK" if short.mark >= 0.8*required else "MARGINAL"
    pad_pair("LEAP extrinsic:", fmt_money(extr), "Short credit (mark):", fmt_money(short.mark))
    pad_pair("Cycles left (≈30D):", str(cyc), "Short 50% GTC:", fmt_money(round((short.mark or 0)*0.5,2)))
    pad_pair("Required / 30D:", fmt_money(required), "Coverage status:", coverage or "N/A")
    reg = f"REGIME {market.get('overall_regime','N/A')} | TREND {market.get('trend_bias','N/A')} | VOL {market.get('volatility','N/A')}" if market else "N/A"
    pad_pair("Net Δ (long+short):", fmt_num((long.delta or 0)+(short.delta or 0)), "Market regime:", reg)
    print()

    # Playbook
    print("Playbook — What, Why, When, How")
    if short_otm:  print(bullet_line("Short is OTM → time decay works for you; assignment risk lower.", "ok"))
    else:          print(bullet_line("Short is tested/ITM → roll up/out to reduce risk or manage assignment.", "risk"))
    if short_band: print(bullet_line("Short Δ in band → balance between POP and credit is healthy.", "ok"))
    else:          print(bullet_line("Short Δ out of band → rethink strike or DTE for better probabilities.", "warn"))
    if dte_sweet:  print(bullet_line("Short DTE ~28–45 → sweet spot for theta vs. gamma risk.", "ok"))
    print()

    # GTC targets (short call)
    fill = fills.get(sym) if fills else None
    basis = fill if fill is not None else short.mark
    label = "GTC targets (from fill)" if fill is not None else "GTC targets (est., no fill)"
    targets = gtc_targets(basis, tiers)
    if targets:
        parts = [f"{int(p)}%→{fmt_money(t)}" for p,t in targets]
        print(label + ": " + ", ".join(parts))
    print()

    print("Recommendations")
    if not short_otm:
        print("• Short tested: consider roll up/out for credit; watch Δ>0.55.")
    else:
        print("• Hold. Keep tiered GTC working on short.")
    print("─"*70)

# ---------- args ----------
def parse_args():
    ap = argparse.ArgumentParser(description="PMCC monitor")
    ap.add_argument("--state", help="outputs/market_state.yml for banner", default=None)
    ap.add_argument("--fill", action="append", help="Override SHORT CALL fill credit, SYMBOL=PRICE (repeatable)", default=[])
    ap.add_argument("--gtc", default=None, help="Comma list of GTC tiers, e.g., '50,75'")
    return ap.parse_args()

def main():
    args = parse_args()
    market = load_market_state(args.state)
    print_market_banner(market)
    lines = read_lines()
    under = detect_underlyings(lines)
    opts = parse_options(lines)

    # group by symbol
    by = {}
    for o in opts:
        by.setdefault(o.symbol, []).append(o)

    # fills dict
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

    found = 0
    for sym, rows in by.items():
        und = under.get(sym, Underlying(sym, None))
        longs  = [r for r in rows if is_long_leap_call(r, und.last)]
        shorts = [r for r in rows if is_short_near_call(r, und.last)]
        if not longs or not shorts: continue

        long_pick = sorted(longs, key=lambda r: (r.dte or 0, r.delta or 0), reverse=True)[0]
        def key_short(r): return (abs((r.dte or 0)-35), abs((abs(r.delta or 0.35))-0.35))
        short_pick = sorted(shorts, key=key_short)[0]

        report(sym, und, long_pick, short_pick, fills, tiers, market=market)
        found += 1

    if found == 0:
        print("No PMCC pairs detected (need a long-dated ITM call and a nearer-dated short OTM call on the same symbol).")

if __name__ == "__main__":
    main()