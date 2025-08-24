#!/usr/bin/env python3
# long_call_monitor.py — Monitor a **single-leg long call** from pasted broker rows.
# Adds tiered GTC target guidance (off fill if given, else off mark).
#
# CLI:
#   --state outputs/market_state.yml
#   --fill SYMBOL=DEBIT         (repeatable)  e.g., --fill AAPL=4.85
#   --gtc "50,75"               (optional)    percent gain tiers on premium
#
# Tiering:
#   - any --fill and no --gtc → [50, 75]
#   - no --fill and no --gtc  → [50]

import sys, re, os, argparse
from dataclasses import dataclass
from datetime import datetime, timezone

# ===== ANSI
class C: R="\033[31m"; G="\033[32m"; Y="\033[33m"; B="\033[34m"; M="\033[35m"; CY="\033[36m"; DIM="\033[2m"; RESET="\033[0m"; BOLD="\033[1m"
def color(s,k): return f"{k}{s}{C.RESET}"
def bold(s): return color(s, C.BOLD)
def green(s): return color(s, C.G)
def yellow(s): return color(s, C.Y)
def red(s): return color(s, C.R)

# ===== fmt
def fmt_money(x, none="N/A"): return none if x is None else f"${x:,.2f}"
def fmt_num(x, none="N/A"): return none if x is None else f"{x:.3f}"
def fmt_pctf(x, none="N/A"): return none if x is None else f"{x*100:.2f}%"
def to_float(s):
    if s is None: return None
    s = s.replace(',','')
    try: return float(s)
    except: return None
def to_int(s):
    if s is None: return None
    s = s.replace(',','')
    try: return int(s)
    except: return None

# ===== models
@dataclass
class Underlying:
    symbol: str
    last: float=None

@dataclass
class OptionRow:
    symbol: str
    exp: str
    strike: float
    cp: str           # 'C'/'P'
    dte: int=None
    delta: float=None
    oi: int=None
    qty: int=None     # +1 long, -1 short
    mark: float=None
    itm_flag: str=None
    raw: str=""

# ===== regex & helpers
HEADER_RE = re.compile(r'^([A-Z][A-Z0-9\.]{0,6})\s+(\d{2}/\d{2}/\d{4})\s+(\d+(?:\.\d+)?)\s+(C|P)\b')
MONEY_RE  = re.compile(r'\$(-?\d+(?:\.\d+)?)')
ITM_RE    = re.compile(r'\b(ITM|OTM)\b')

def _norm(s:str)->str: return s.replace('\t',' ').strip()

def read_lines():
    print(bold("──────────── LONG CALL MONITOR — paste rows, then Ctrl-D ────────────"))
    print("Include: underlying line, long call header (e.g., 'AAPL 01/16/2026 190.00 C'), then its first data row.")
    raw = sys.stdin.read()
    return [_norm(l) for l in raw.splitlines() if _norm(l)]

def detect_underlyings(lines):
    under = {}
    sym = None
    for line in lines:
        if re.fullmatch(r'[A-Z][A-Z0-9\.]{0,6}', line):
            sym = line; continue
        if sym and line.startswith('$'):
            m = MONEY_RE.match(line)
            if m:
                under[sym] = Underlying(sym, to_float(m.group(1)))
            sym = None
    return under

def token_split(row:str):
    row = row.replace('+',' +').replace('-',' -')
    return [t for t in row.split() if t]

def parse_after_itm_block(row:str):
    m = ITM_RE.search(row)
    if not m: return None, None, None, None, None
    flag = m.group(1)
    toks = token_split(row)
    idx=None
    for i,t in enumerate(toks):
        if t==flag: idx=i;break
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

def parse_options(lines):
    opts=[]
    n=len(lines); i=0
    while i<n:
        h = HEADER_RE.match(lines[i])
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
        m=MONEY_RE.search(data_row)
        mark = to_float(m.group(1)) if m else None
        flag,dte,delta,oi,qty = parse_after_itm_block(data_row)
        opts.append(OptionRow(sym, date, strike, cp, dte, delta, oi, qty, mark, flag, data_row))
        i=j+1
    return opts

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

# ===== utility
def bullet(s, tone=""):
    if tone=="ok": return "• "+green(s)
    if tone=="warn": return "• "+yellow(s)
    if tone=="risk": return "• "+red(s)
    return "• "+s

def pad_pair(lab, val, rlab, rval, w=30, vw=16):
    lv = val if isinstance(val,str) else str(val)
    rv = rval if isinstance(rval,str) else str(rval)
    print(f"{lab:<{w}} {lv:>{vw}}    {rlab:<{w}} {rv:>{vw}}")

def gtc_targets_long(basis_debit: float, tiers, max_cap=None):
    # For long calls, tiers are **profit** tiers: target premium = basis * (1 + pct)
    if basis_debit is None: return None
    out=[]
    for p in tiers:
        t = round(basis_debit * (1 + p/100.0), 2)
        if max_cap is not None: t = min(max_cap, t)
        out.append((p, t))
    return out

# ===== core
def is_long_call(o:OptionRow, last:float)->bool:
    if o.cp != 'C' or o.qty is None or o.qty <= 0 or o.dte is None: return False
    return True

def main():
    ap = argparse.ArgumentParser(description="Long Call monitor with tiered GTC.")
    ap.add_argument("--state", default=None, help="outputs/market_state.yml")
    ap.add_argument("--fill", action="append", default=[], help="SYMBOL=DEBIT (repeatable)")
    ap.add_argument("--gtc", default=None, help="Comma list, e.g., '50,75'")
    args = ap.parse_args()

    market = load_market_state(args.state)
    print_market_banner(market)
    lines = read_lines()
    under = detect_underlyings(lines)
    opts = parse_options(lines)

    # group longs per symbol
    by={}
    for o in opts:
        if is_long_call(o, under.get(o.symbol, Underlying(o.symbol)).last):
            by.setdefault(o.symbol, []).append(o)

    if not by:
        print("No long call rows detected (need a long call header and its first data line).")
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

    # render one per symbol (closest to tasty style: Δ ~0.70–0.85, DTE ≥ 180 for LEAPs, else prefer >45)
    for sym, rows in by.items():
        und = under.get(sym, Underlying(sym, None))
        pick = sorted(rows, key=lambda r: (
            0 if (r.dte or 0)>=180 else 1,            # prefer LEAP-y
            abs((r.delta or 0.8)-0.8),                # prefer Δ around 0.8
            - (r.dte or 0)                            # more time
        ))[0]

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        print(f"{sym}  |  LONG CALL TRADE TICKET")
        print(ts)
        print("─"*34 + "  " + "─"*34)

        itm = (und.last is not None and pick.strike is not None and und.last>pick.strike)
        near_leap = (pick.dte or 0) >= 180

        badges=[]
        badges.append("[ ITM ]" if itm else "[ OTM ]")
        badges.append("[ LEAP (≥180 DTE) ]" if near_leap else "[ <180 DTE ]")
        print(" ".join(badges)); print()

        # header
        print(f"Underlying: {fmt_money(und.last)}")
        print(f"Long Call:  {pick.strike:.3f} C  • Exp {pick.exp}  • DTE {pick.dte}  • Δ {fmt_num(pick.delta)}  • Mark {fmt_money(pick.mark)}")
        print()

        # policy
        print("Policy")
        print("Policy → Prefer Δ 0.70–0.85 • DTE ≥ 180 for LEAPs • Take profit in tiers • Manage if Δ>0.90 or IV crush risk")
        print()

        # metrics
        intrinsic = None
        extrinsic = None
        if und.last is not None and pick.mark is not None and pick.strike is not None:
            intrinsic = max(0.0, und.last - pick.strike)
            extrinsic = max(0.0, pick.mark - intrinsic)
        pad_pair("Intrinsic value:", fmt_money(intrinsic), "Extrinsic value:", fmt_money(extrinsic))
        pad_pair("Delta:", fmt_num(pick.delta), "Open interest:", str(pick.oi) if pick.oi is not None else "N/A")
        reg = f"REGIME {market.get('overall_regime','N/A')} | TREND {market.get('trend_bias','N/A')} | VOL {market.get('volatility','N/A')}" if market else "N/A"
        pad_pair("DTE:", str(pick.dte), "Market regime:", reg)
        print()

        # playbook
        print("Playbook — What, Why, When, How")
        print(bullet("Aim for Δ ~0.70–0.85: responsive to upside while not ultra-gamma-sensitive.", "ok"))
        if near_leap:
            print(bullet("LEAP time buffer reduces roll frequency; decays slower.", "ok"))
        else:
            print(bullet("Shorter DTE: faster theta decay against you — be nimble.", "warn"))
        if extrinsic is not None and extrinsic/ (pick.mark or 1) < 0.15:
            print(bullet("Low extrinsic → more intrinsic; moves track stock closely.", "ok"))
        print()

        # GTC targets (profit tiers on premium)
        basis = fills.get(sym) if sym in fills else pick.mark
        label = "GTC profit tiers (from fill)" if sym in fills else "GTC profit tiers (est., no fill)"
        targets = gtc_targets_long(basis, tiers)
        if targets:
            parts = [f"{int(p)}%→{fmt_money(v)}" for p,v in targets]
            print(label + ": " + ", ".join(parts))
        print("─"*70)

if __name__ == "__main__":
    main()