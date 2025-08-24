#!/usr/bin/env python3
# verticals_monitor.py — Monitor Bull Call (debit) & Bull Put (credit) spreads.
# Paste broker rows (no headers). Detects legs and builds spread cards.
#
# CLI (consistent):
#   --state outputs/market_state.yml
#   --fill SYMBOL=PRICE   (repeatable)   → NET fill (debit for bull call, credit for bull put)
#   --gtc "50,75"                      → Profit/Buyback tiers:
#       * Bull PUT (credit): close at price ≈ fill * (1 - tier%)
#       * Bull CALL (debit): take profits at price ≈ fill * (1 + tier%), capped by width
#
# Tiering if omitted:
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
def fmt_money(x, none="N/A"): return none if x is None else f"${x:,.4f}" if abs(x)<10 else f"${x:,.2f}"
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
class Opt:
    symbol: str
    exp: str
    strike: float
    cp: str
    dte: int=None
    delta: float=None
    oi: int=None
    qty: int=None   # +1 long / -1 short (per leg)
    mark: float=None
    itm_flag: str=None
    raw: str=""

# ===== parse helpers
HEADER_RE = re.compile(r'^([A-Z][A-Z0-9\.]{0,6})\s+(\d{2}/\d{2}/\d{4})\s+(\d+(?:\.\d+)?)\s+(C|P)\b')
MONEY_RE  = re.compile(r'\$(-?\d+(?:\.\d+)?)')
ITM_RE    = re.compile(r'\b(ITM|OTM)\b')

def _norm(s:str)->str: return s.replace('\t',' ').strip()

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
        if t==flag: idx=i; break
    if idx is None: return flag, None, None, None, None
    def get(i): return toks[i].replace(',','') if 0<=i<len(toks) else None
    dte   = to_int(get(idx+1))
    delta = to_float(get(idx+2))
    oi    = to_int(get(idx+3))
    qty   = to_int(get(idx+4))
    if qty not in (-1,1):
        q2 = to_int(get(idx+5))
        if q2 in (-1,1): qty=q2
    return flag, dte, delta, oi, qty

def parse_options(lines):
    opts=[]
    n=len(lines); i=0
    while i<n:
        h=HEADER_RE.match(lines[i])
        if not h: i+=1; continue
        sym,date,strike_s,cp = h.group(1), h.group(2), h.group(3), h.group(4)
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
        opts.append(Opt(sym, date, strike, cp, dte, delta, oi, qty, mark, flag, data_row))
        i=j+1
    return opts

def detect_underlyings(lines):
    under = {}
    sym=None
    for line in lines:
        if re.fullmatch(r'[A-Z][A-Z0-9\.]{0,6}', line):
            sym=line; continue
        if sym and line.startswith('$'):
            m=MONEY_RE.match(line)
            if m: under[sym]=Underlying(sym, to_float(m.group(1)))
            sym=None
    return under

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

# ===== helpers
def bullet(s,tone=""):
    if tone=="ok": return "• "+green(s)
    if tone=="warn": return "• "+yellow(s)
    if tone=="risk": return "• "+red(s)
    return "• "+s

def pad_pair(lab, val, rlab, rval, w=26, vw=14):
    lv = val if isinstance(val,str) else str(val)
    rv = rval if isinstance(rval,str) else str(rval)
    print(f"{lab:<{w}} {lv:>{vw}}   {rlab:<{w}} {rv:>{vw}}")

def gtc_credit_targets(basis_credit: float, tiers):
    if basis_credit is None: return None
    out=[]
    for p in tiers:
        t = round(max(0.05, basis_credit * (1 - p/100.0)), 2)  # buy to close
        out.append((p,t))
    return out

def gtc_debit_targets(basis_debit: float, tiers, max_cap=None):
    if basis_debit is None: return None
    out=[]
    for p in tiers:
        t = round(basis_debit * (1 + p/100.0), 2)              # sell to close
        if max_cap is not None: t = min(max_cap, t)
        out.append((p,t))
    return out

# ===== spread typing
def classify_vertical(a:Opt,b:Opt):
    # must match symbol / exp / type
    if not (a.symbol==b.symbol and a.exp==b.exp and a.cp==b.cp): return None
    # ensure one long (+1) and one short (-1)
    if a.qty is None or b.qty is None or a.qty*b.qty>=0: return None
    # normalize so long is "L", short is "S"
    long_leg = a if a.qty>0 else b
    short_leg= b if a.qty>0 else a
    width = abs(short_leg.strike - long_leg.strike)
    if a.cp=='P':
        # Bull Put: long lower (protect), short higher strike (credit)
        kind = "bull_put" if long_leg.strike < short_leg.strike else None
        net = (short_leg.mark or 0) - (long_leg.mark or 0)  # credit
    else:
        # Bull Call: long lower, short higher (debit)
        kind = "bull_call" if long_leg.strike < short_leg.strike else None
        net = (long_leg.mark or 0) - (short_leg.mark or 0)  # debit
    if kind is None: return None
    return {
        "kind": kind,
        "L": long_leg,
        "S": short_leg,
        "width": width,
        "net": net
    }

# ===== main render
def render_spread(sym, und, sp, fills, tiers, market):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    title = "BULL CALL TRADE TICKET" if sp["kind"]=="bull_call" else "BULL PUT TRADE TICKET"
    print(f"{sym}  |  {title}")
    print(ts)
    print("─"*34 + "  " + "─"*34)

    L,S = sp["L"], sp["S"]
    width = sp["width"]; net = sp["net"]

    # badges
    tested = None
    if sp["kind"]=="bull_put":
        tested = (und.last is not None and und.last < (S.strike or 0)) is False  # not tested = pass
        tested_badge = "[ Not tested ]" if tested else "[ Tested ]"
        band = S.delta is not None and 0.30 <= abs(S.delta) <= 0.50
        band_badge = "[ Δ in 0.30–0.50 band ]" if band else "[ Δ out of band ]"
        print(" ".join([tested_badge, band_badge, "[ DTE ≥21 ]" if (S.dte or 0)>=21 else "[ DTE <21 ]"]))
    else:
        # bull call
        at_or_over_short = (und.last is not None and S.strike is not None and und.last >= S.strike)
        ratio_ok = (net is not None and width>0 and (net/width) <= 0.70)
        band = S.delta is not None and 0.30 <= abs(S.delta) <= 0.50
        badges = []
        badges.append("[ ≥ short (max zone) ]" if at_or_over_short else "[ < short ]")
        badges.append("[ Debit ≤70%/width target ]" if ratio_ok else "[ Debit >70%/width ]")
        badges.append("[ Δ in band ]" if band else "[ Δ out of band ]")
        print(" ".join(badges))
    print()

    # header
    print(f"Underlying: {fmt_money(und.last)}")
    legfmt = lambda leg: f"{leg.strike:>7.3f} {leg.cp}  • Exp {leg.exp}  • DTE {leg.dte}  • Δ {fmt_num(leg.delta)}  • Mark {fmt_money(leg.mark)}"
    print(f"Long:  {legfmt(L)}")
    print(f"Short: {legfmt(S)}")
    print()

    # policy
    print("Policy")
    if sp["kind"]=="bull_put":
        print("Policy → Δ band 0.30–0.50 • DTE ≥21 • credit ≥33% width • roll@21d • TP 50–75%")
    else:
        print("Policy → Δ band 0.30–0.50 • DTE ≥21 • debit ≤70% width • TP near 75–100% of max when ≥ short")
    print()

    # metrics
    if sp["kind"]=="bull_put":
        credit = net
        remain_max = (width - credit)
        pad_pair("Spread (mid):", fmt_money(credit), "Width:", f"{width:.1f}")
        pad_pair("Remain→Max $:", fmt_money(width - credit), "Remain→Max %:", f"{(remain_max/width*100.0):.1f}%")
        pad_pair("Credit:", fmt_money(credit), "Credit/Width:", f"{(credit/width):.2f}" if width else "N/A")
        pad_pair("Breakeven:", fmt_money(S.strike - credit), "Short Δ:", fmt_num(S.delta))
    else:
        debit = net
        remain_max = (width - debit)
        pad_pair("Spread (mid):", fmt_money(debit), "Width:", f"{width:.1f}")
        pad_pair("Remain→Max $:", fmt_money(remain_max), "Remain→Max %:", f"{(remain_max/width*100.0):.1f}%")
        pad_pair("Debit:", fmt_money(debit), "Debit/Width:", f"{(debit/width):.2f}" if width else "N/A")
        pad_pair("Breakeven:", fmt_money(L.strike + debit), "Short Δ:", fmt_num(S.delta))
    pad_pair("Open interest (S):", fmt_int(S.oi), "Open interest (L):", fmt_int(L.oi))
    reg = f"REGIME {market.get('overall_regime','N/A')} | TREND {market.get('trend_bias','N/A')} | VOL {market.get('volatility','N/A')}" if market else "N/A"
    pad_pair("Market regime:", reg, "", "")
    print()

    # checklist
    print("Checklist")
    if sp["kind"]=="bull_put":
        print(f"  DTE ≥ 21: {'PASS' if (S.dte or 0)>=21 else 'FAIL'}")
        print(f"  Short strike not tested: {'PASS' if und.last is not None and und.last > (S.strike or 0) else 'FAIL'}")
        print(f"  Short Δ 0.30–0.50: {'PASS' if S.delta is not None and 0.30 <= abs(S.delta) <= 0.50 else 'FAIL'}")
        print(f"  Pricing ratio target: {'PASS' if width and net/width >= 0.33 else 'FAIL'}")
        print(f"  Harvestable (<25% to max): {'PASS' if (remain_max/width) < 0.25 else 'FAIL'}")
    else:
        print(f"  DTE ≥ 21: {'PASS' if (S.dte or 0)>=21 else 'FAIL'}")
        print(f"  Short strike not tested: {'FAIL' if und.last is not None and und.last >= (S.strike or 0) else 'PASS'}")
        print(f"  Short Δ 0.30–0.50: {'PASS' if S.delta is not None and 0.30 <= abs(S.delta) <= 0.50 else 'FAIL'}")
        print(f"  Pricing ratio target: {'PASS' if width and net/width <= 0.70 else 'FAIL'}")
        print(f"  Harvestable (<25% to max): {'PASS' if (remain_max/width) < 0.25 else 'FAIL'}")
    print()

    # playbook
    print("Playbook — What, Why, When, How")
    if sp["kind"]=="bull_put":
        print(bullet("OTM short → theta decay on your side; assignment risk lower.", "ok"))
        print(bullet("Δ 0.30–0.50 → balanced POP/credit for spreads.", "ok"))
        print(bullet("Target ≥1/3 width credit; roll at ~21 DTE if needed.", "ok"))
    else:
        print(bullet("Price ≥ short strike → in max-gain zone; consider harvesting.", "ok"))
        print(bullet("Keep debit ≤ ~70% of width to keep R/R reasonable.", "ok"))
        print(bullet("Δ 0.30–0.50 on short leg stabilizes gamma risk.", "ok"))
    print()

    # GTC targets
    sym = L.symbol
    basis = fills.get(sym)
    if sp["kind"]=="bull_put":
        # credit spread: reduce to buyback price
        if basis is None: basis = net
        label = "GTC buyback tiers (from fill)" if sym in fills else "GTC buyback tiers (est., no fill)"
        tgts = gtc_credit_targets(basis, tiers)
        if tgts:
            parts = [f"{int(p)}%→{fmt_money(v)}" for p,v in tgts]
            print(label + ": " + ", ".join(parts))
    else:
        # debit spread: profit tiers on spread value, cap at width
        if basis is None: basis = net
        label = "GTC profit tiers (from fill)" if sym in fills else "GTC profit tiers (est., no fill)"
        tgts = gtc_debit_targets(basis, tiers, max_cap=width)
        if tgts:
            parts = [f"{int(p)}%→{fmt_money(v)}" for p,v in tgts]
            print(label + ": " + ", ".join(parts))
    print("─"*70)

def main():
    ap = argparse.ArgumentParser(description="Verticals monitor (bull call / bull put)")
    ap.add_argument("--state", default=None, help="outputs/market_state.yml")
    ap.add_argument("--fill", action="append", default=[], help="SYMBOL=NET (debit for bull call, credit for bull put)")
    ap.add_argument("--gtc", default=None, help="Comma list, e.g., '50,75'")
    args = ap.parse_args()

    market = load_market_state(args.state)
    print_market_banner(market)
    print("Paste 1 or more verticals. Each vertical must have TWO legs (same Symbol, same Exp, same Type).")
    print("Legs do NOT need to be contiguous. Press Ctrl-D (Linux/Mac) or Ctrl-Z + Enter (Windows) when done.")
    raw = sys.stdin.read()
    lines = [_norm(l) for l in raw.splitlines() if _norm(l)]

    under = detect_underlyings(lines)
    legs = parse_options(lines)
    if not legs:
        print("No option legs detected.")
        return

    # assemble spreads per (symbol, exp, cp)
    buckets={}
    for leg in legs:
        key=(leg.symbol, leg.exp, leg.cp)
        buckets.setdefault(key, []).append(leg)

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

    found=0
    for (sym, exp, cp), legs in buckets.items():
        # need one long (+) and one short (-)
        if len(legs) < 2: continue
        # pick the best pair that forms a proper vertical
        best=None
        for i in range(len(legs)):
            for j in range(i+1,len(legs)):
                sp = classify_vertical(legs[i], legs[j])
                if sp:
                    # prefer Δ short ~0.40 and DTE ≥21, minimize debit>70%/width for calls or credit<33%/width for puts
                    S=sp["S"]; width=sp["width"]; net=sp["net"]
                    score = 0.0
                    if S.dte and S.dte>=21: score += 1.0
                    if S.delta is not None: score += 1.0 - min(1.0, abs(abs(S.delta)-0.40))
                    if sp["kind"]=="bull_call" and width>0 and net/width<=0.70: score += 0.5
                    if sp["kind"]=="bull_put"  and width>0 and net/width>=0.33: score += 0.5
                    if not best or score > best[0]: best=(score, sp)
        if best:
            render_spread(sym, under.get(sym, Underlying(sym,None)), best[1], fills, tiers, market)
            found += 1

    if found==0:
        print("No valid bull call / bull put verticals detected.")

if __name__ == "__main__":
    main()