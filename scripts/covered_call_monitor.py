#!/usr/bin/env python3
# covered_call_monitor.py — Single-leg short call (covered) monitor with tiered GTC guidance.
#
# Paste your broker rows:
#   1) underlying line (e.g., "AAPL")
#   2) short call header (e.g., "AAPL 09/20/2025 235.00 C")
#   3) the first price/data row after the labels
#
# CLI:
#   --state outputs/market_state.yml
#   --fill SYMBOL=CREDIT        (repeatable) e.g., --fill AAPL=1.25
#   --gtc  "50,75"              (optional)   % buyback tiers (50% = fill * (1-0.50))
#
# Tiering defaults:
#   - if any --fill and no --gtc → [50, 75]
#   - if no --fill and no --gtc  → [50]
#
# Policy: tasty-style
#   Δ band ≈ 0.15–0.35 • DTE 28–45 sweet spot (allow 7–60 for mgmt) • TP at 50% +
#   Manage/risk: roll@21D; if tested/ITM or Δ>0.55 consider roll/close.

import sys, os, re, argparse
from dataclasses import dataclass
from datetime import datetime, timezone

# ===== ANSI
class C:
    R="\033[31m"; G="\033[32m"; Y="\033[33m"; B="\033[34m"; M="\033[35m"; CY="\033[36m"
    DIM="\033[2m"; RESET="\033[0m"; BOLD="\033[1m"
def color(s,k): return f"{k}{s}{C.RESET}"
def bold(s): return color(s, C.BOLD)
def green(s): return color(s, C.G)
def yellow(s): return color(s, C.Y)
def red(s): return color(s, C.R)

# ===== fmt
def fmt_money(x, none="N/A"):
    if x is None: return none
    return f"${x:,.2f}"
def fmt_num(x, none="N/A"): return none if x is None else f"{x:.3f}"
def fmt_int(x, none="N/A"): return none if x is None else f"{int(x)}"

def to_float(s):
    if s is None: return None
    s=s.replace(',','')
    try: return float(s)
    except: return None

def to_int(s):
    if s is None: return None
    s=s.replace(',','')
    try: return int(s)
    except: return None

# ===== models
@dataclass
class Underlying:
    symbol: str
    last: float=None

@dataclass
class OptRow:
    symbol: str
    exp: str
    strike: float
    cp: str            # 'C' or 'P'
    dte: int=None
    delta: float=None
    oi: int=None
    qty: int=None      # -1 short, +1 long
    mark: float=None
    itm_flag: str=None
    raw: str=""

# ===== parsing
HEADER_RE = re.compile(r'^([A-Z][A-Z0-9\.]{0,6})\s+(\d{2}/\d{2}/\d{4})\s+(\d+(?:\.\d+)?)\s+(C|P)\b')
MONEY_RE  = re.compile(r'\$(-?\d+(?:\.\d+)?)')
ITM_RE    = re.compile(r'\b(ITM|OTM)\b')

def _norm(s:str)->str: return s.replace('\t',' ').strip()

def token_split(row:str):
    row = row.replace('+',' +').replace('-',' -')
    return [t for t in row.split() if t]

def parse_after_itm_block(row:str):
    # ... ITM/OTM  DTE  Δ  OI  QTY  ...
    m = ITM_RE.search(row)
    if not m: return None, None, None, None, None
    flag = m.group(1)
    toks = token_split(row)
    idx=None
    for i,t in enumerate(toks):
        if t==flag: idx=i; break
    if idx is None: return flag, None, None, None, None
    def get(i): return toks[i].replace(',','') if 0<=i<len(toks) else None
    dte   = to_int(get(idx+1))
    delta = to_float(get(idx+2))
    oi    = to_int(get(idx+3))
    qty   = to_int(get(idx+4))
    if qty not in (-1,1):
        q2 = to_int(get(idx+5))
        if q2 in (-1,1): qty = q2
    return flag, dte, delta, oi, qty

def detect_underlyings(lines):
    under={}
    sym=None
    for line in lines:
        if re.fullmatch(r'[A-Z][A-Z0-9\.]{0,6}', line):
            sym=line; continue
        if sym and line.startswith('$'):
            m=MONEY_RE.match(line)
            if m: under[sym]=Underlying(sym, to_float(m.group(1)))
            sym=None
    return under

def parse_options(lines):
    out=[]
    n=len(lines); i=0
    while i<n:
        h=HEADER_RE.match(lines[i])
        if not h: i+=1; continue
        sym, date, strike_s, cp = h.group(1), h.group(2), h.group(3), h.group(4)
        strike=float(strike_s)
        data_row=None
        j=i+1
        while j<n and j<=i+8:
            row=lines[j]
            if ' EXP ' in f" {row} " or row.upper().startswith('CALL ') or row.upper().startswith('PUT '):
                j+=1; continue
            if row.startswith('$'):
                data_row=row; break
            j+=1
        if not data_row: i+=1; continue
        # mark from first $ amount
        m=MONEY_RE.search(data_row)
        mark = to_float(m.group(1)) if m else None
        flag,dte,delta,oi,qty = parse_after_itm_block(data_row)
        out.append(OptRow(sym,date,strike,cp,dte,delta,oi,qty,mark,flag,data_row))
        i=j+1
    return out

# ===== market banner
def load_market_state(path):
    if not path or not os.path.exists(path): return None
    try:
        import yaml
        with open(path,'r') as f: return (yaml.safe_load(f) or {})
    except Exception:
        return None

def print_market_banner(state):
    if not state:
        print("REGIME N/A | TREND N/A | VOL N/A")
        print("─"*74); print(bold("MARKET CONTEXT")); print("─"*74); return
    line = f"REGIME {state.get('overall_regime','N/A')} | TREND {state.get('trend_bias','N/A')} | VOL {state.get('volatility','N/A')}"
    print(line); print("─"*74); print(bold("MARKET CONTEXT")); print("─"*74)

# ===== UI helpers
def bullet(s, tone=""):
    if tone=="ok": return "• "+green(s)
    if tone=="warn": return "• "+yellow(s)
    if tone=="risk": return "• "+red(s)
    return "• "+s

def pad_pair(lab, val, rlab, rval, w=28, vw=16):
    lv = val if isinstance(val,str) else str(val)
    rv = rval if isinstance(rval,str) else str(rval)
    print(f"{lab:<{w}} {lv:>{vw}}    {rlab:<{w}} {rv:>{vw}}")

def gtc_buyback_targets(basis_credit: float, tiers):
    # For short calls, we buy back at reduced price: fill * (1 - pct)
    if basis_credit is None: return None
    out=[]
    for p in tiers:
        target = round(max(0.01, basis_credit * (1 - p/100.0)), 2)
        out.append((p, target))
    return out

# ===== classify
def is_short_call(o:OptRow):  # covered call leg
    return o.cp=='C' and (o.qty is not None and o.qty < 0)

# ===== main
def main():
    ap = argparse.ArgumentParser(description="Covered Call monitor with tiered GTC.")
    ap.add_argument("--state", default=None, help="outputs/market_state.yml")
    ap.add_argument("--fill", action="append", default=[], help="SYMBOL=CREDIT  (repeatable)")
    ap.add_argument("--gtc", default=None, help="Comma list, e.g., '50,75'")
    args = ap.parse_args()

    market = load_market_state(args.state)
    print_market_banner(market)

    print("──────── COVERED CALL MONITOR — Paste short-call rows. ────────")
    print("Include: underlying line, short call header, and first data row. Ctrl-D to end.")
    raw = sys.stdin.read()
    lines = [_norm(l) for l in raw.splitlines() if _norm(l)]

    under = detect_underlyings(lines)
    opts  = parse_options(lines)
    shorts=[o for o in opts if is_short_call(o)]
    if not shorts:
        print("No covered-call rows detected (need a short call header and its first data line).")
        return

    # fills
    fills={}
    for item in args.fill:
        if '=' in item:
            sym,val = item.split('=',1)
            fills[sym.strip().upper()] = to_float(val.strip())

    # tiers
    tiers=[]
    if args.gtc:
        for t in args.gtc.split(','):
            t=t.strip()
            if t:
                try: tiers.append(float(t))
                except: pass
    else:
        tiers = [50.0, 75.0] if fills else [50.0]

    # render one card per symbol (pick best by DTE and Δ band)
    by={}
    for o in shorts:
        by.setdefault(o.symbol, []).append(o)

    for sym, rows in by.items():
        und = under.get(sym, Underlying(sym, None))
        # prefer DTE 28–45 and Δ near 0.30
        pick = sorted(rows, key=lambda r: (
            0 if (r.dte or 0) >= 28 and (r.dte or 0) <= 45 else 1,
            abs((abs(r.delta) if r.delta is not None else 0.30) - 0.30),
            - (r.oi or 0)
        ))[0]

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        print(f"{sym}  |  COVERED CALL TRADE TICKET")
        print(ts)
        print("─"*34 + "  " + "─"*34)

        # badges
        short_otm = (und.last is not None and pick.strike is not None and und.last < pick.strike)
        dte_band  = (pick.dte or 0) >= 28 and (pick.dte or 0) <= 45
        delta_band= (pick.delta is not None and 0.15 <= abs(pick.delta) <= 0.35)
        badges=[]
        badges.append("[ Short OTM ]" if short_otm else "[ Short ITM/Tested ]")
        badges.append("[ Δ in band ]" if delta_band else "[ Δ out of band ]")
        badges.append("[ DTE sweet spot ]" if dte_band else "[ DTE outside sweet spot ]")
        print(" ".join(badges)); print()

        # header
        print(f"Underlying: {fmt_money(und.last)}")
        print(f"Short (covered): {pick.strike:>.3f} C  • Exp {pick.exp}  • DTE {pick.dte}  • Δ {fmt_num(pick.delta)}  • Mark {fmt_money(pick.mark)}")
        print()

        # policy
        print("Policy")
        print("Policy → Δ band 0.15–0.35 • short DTE 28–45 • TP 50% • roll@21d • roll if Δ>0.55")
        print()

        # metrics
        pad_pair("Breakeven (stock - credit):", "N/A", "Short Δ:", fmt_num(pick.delta))
        pad_pair("Open interest:", fmt_int(pick.oi), "DTE:", str(pick.dte))
        reg = f"REGIME {market.get('overall_regime','N/A')} | TREND {market.get('trend_bias','N/A')} | VOL {market.get('volatility','N/A')}" if market else "N/A"
        pad_pair("Market regime:", reg, "", "")
        print()

        # checklist
        print("Checklist")
        print(f"  Short OTM: {'PASS' if short_otm else 'FAIL'}")
        print(f"  Δ in [0.15,0.35]: {'PASS' if delta_band else 'FAIL'}")
        print(f"  DTE in [28,45]: {'PASS' if dte_band else 'FAIL'}")
        print(f"  OI ≥ 100: {'PASS' if (pick.oi or 0) >= 100 else 'FAIL'}")
        print()

        # playbook
        print("Playbook — What, Why, When, How")
        print(bullet("OTM short → theta decay works for you; assignment risk lower.", "ok"))
        if delta_band:
            print(bullet("Δ 0.15–0.35 → balanced POP vs. credit.", "ok"))
        else:
            print(bullet("Δ out of band → either low credit (too small) or high assignment risk.", "warn"))
        if dte_band:
            print(bullet("DTE ~28–45 → sweet spot for decay and manageable gamma.", "ok"))
        else:
            print(bullet("Outside sweet spot → expect either slow decay or rising gamma risk.", "warn"))
        print()

        # GTC targets
        basis = fills.get(sym, pick.mark)
        label = "GTC buyback tiers (from fill)" if sym in fills else "GTC buyback tiers (est., no fill)"
        tgts = gtc_buyback_targets(basis, tiers)
        if tgts:
            parts = [f"{int(p)}%→{fmt_money(v)}" for p,v in tgts]
            print(label + ": " + ", ".join(parts))
        print("─"*70)

if __name__ == "__main__":
    main()