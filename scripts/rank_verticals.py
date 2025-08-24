#!/usr/bin/env python3
# rank_verticals.py — rank bull call & bull put spreads; account-aware sizing
# Inputs: vertical_bull_call-latest.csv, vertical_bull_put-latest.csv, data/account_state.yml

import argparse, csv, json, os, sys, math
from datetime import datetime, timezone
from typing import List, Dict, Any

# --------- tiny YAML ----------
def load_simple_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path): return {}
    out={}; stack=[out]; ind=[0]
    for raw in open(path,"r",encoding="utf-8"):
        line=raw.rstrip("\n")
        if not line.strip() or line.strip().startswith("#"): continue
        i=len(line)-len(line.lstrip(" "))
        while i<ind[-1]: stack.pop(); ind.pop()
        if ":" in line:
            k,v=line.lstrip().split(":",1)
            k=k.strip(); v=v.strip()
            if v=="":
                d={}; stack[-1][k]=d; stack.append(d); ind.append(i+2)
            else:
                try:
                    sv=float(v) if any(c in v for c in ".0123456789") and v.replace(".","",1).replace("-","",1).isdigit() else v.strip("'\"")
                except:
                    sv=v.strip("'\"")
                stack[-1][k]=sv
    return out

# --------- print ----------
def bold(s): return f"\033[1m{s}\033[0m"
def green(s): return f"\033[32m{s}\033[0m"
def yellow(s): return f"\033[33m{s}\033[0m"
def red(s): return f"\033[31m{s}\033[0m"
def dim(s): return f"\033[2m{s}\033[0m"

def banner(t): print("\n"+"─"*70+"\n"+t.center(70)+"\n"+"─"*70+"\n")

def to_float(x):
    if x is None: return None
    x=str(x).strip().replace(",","").replace("%","")
    if x in ("","N/A"): return None
    try: return float(x)
    except: return None

def latest_path(strategy:str)->str:
    cat=load_simple_yaml(os.path.join("data","data_catalog_runtime.yml"))
    return ((cat.get("datasets") or {}).get(strategy) or {}).get("file","")

def load_account_state(path:str, cash_override:float=None)->Dict[str,float]:
    st=load_simple_yaml(path) or {}
    total=float(st.get("total_value",0))
    sleeve=float(st.get("alloc_pct_to_options",0.0))
    cash=float(st.get("cash_available",0.0))
    per_trade=float(st.get("per_trade_cap_pct",0.02))
    if cash_override is not None: cash=float(cash_override)
    return {
        "total_value": total,
        "sleeve_pct": sleeve,
        "cash_available": cash,
        "per_trade_cap_pct": per_trade,
        "sleeve_cap": total*sleeve,
        "per_trade_cap": total*per_trade
    }

# --------- read CSVs (Barchart vertical screens vary; use core fields) ----------
def read_bull_call(path:str)->List[Dict[str,Any]]:
    out=[]
    if not path or not os.path.exists(path): return out
    with open(path,"r",encoding="utf-8") as f:
        rdr=csv.DictReader(f)
        for r in rdr:
            if (r.get("Type") or "").title()!="Call": continue
            # Try to extract both legs metrics provided by screen (some screens provide net debit/width)
            sym=r.get("Symbol")
            dte=int(to_float(r.get("DTE")) or 0)
            short_delta=abs(to_float(r.get("Delta")) or 0.0)
            debit=to_float(r.get("Mid") or r.get("Debit") or r.get("Midpoint") or r.get("Price") or r.get("Spread Price"))
            width=to_float(r.get("Width") or r.get("Spread Width"))
            if not sym or debit is None or width is None: continue
            out.append({
                "symbol": sym, "exp": r.get("Exp Date"), "dte": dte,
                "debit": debit, "width": width,
                "short_delta": short_delta
            })
    return out

def read_bull_put(path:str)->List[Dict[str,Any]]:
    out=[]
    if not path or not os.path.exists(path): return out
    with open(path,"r",encoding="utf-8") as f:
        rdr=csv.DictReader(f)
        for r in rdr:
            if (r.get("Type") or "").title()!="Put": continue
            sym=r.get("Symbol")
            dte=int(to_float(r.get("DTE")) or 0)
            credit=to_float(r.get("Mid") or r.get("Credit") or r.get("Midpoint") or r.get("Price") or r.get("Spread Price"))
            width=to_float(r.get("Width") or r.get("Spread Width"))
            short_delta=abs(to_float(r.get("Delta")) or 0.0)
            if not sym or credit is None or width is None: continue
            out.append({
                "symbol": sym, "exp": r.get("Exp Date"), "dte": dte,
                "credit": credit, "width": width,
                "short_delta": short_delta
            })
    return out

# --------- policy ----------
BCALL = {"dte_min":21, "short_delta_min":0.30, "short_delta_max":0.50, "debit_max_width":0.70}
BPUT  = {"dte_min":21, "short_delta_min":0.30, "short_delta_max":0.50, "credit_min_width":0.33}

def score_bull_call(x):
    # maximize remaining to max, prefer delta band center (~0.40)
    rem = max(0.0, (x["width"] - x["debit"]) / x["width"])  # %
    delta_score = 1.0 - abs(x["short_delta"] - 0.40) / 0.40
    return 0.6*rem + 0.4*max(0.0,delta_score)

def score_bull_put(x):
    # prefer higher credit/width and delta near 0.40
    crw = (x["credit"]/x["width"]) if x["width"] else 0.0
    delta_score = 1.0 - abs(x["short_delta"] - 0.40) / 0.40
    return 0.6*crw + 0.4*max(0.0,delta_score)

# --------- sizing ----------
def size_bull_call(x, acct):
    per = (x["debit"] or 0.0) * 100.0
    hard_cap = min(acct["cash_available"], acct["per_trade_cap"])
    if per<=0 or hard_cap<=0: return {"contracts":0,"status":"blocked","reason":"no cash","required_cash":0.0}
    n = math.floor(hard_cap/per)
    if n<=0: return {"contracts":0,"status":"blocked","reason":"not enough for 1 spread","required_cash":per}
    req = n*per; remain = acct["cash_available"] - req
    status="ok"; reason="fits within cash/per-trade cap"
    if n==1 and remain < per*0.25: status="tight"; reason="low remaining cash after 1 spread"
    return {"contracts":n,"required_cash":req,"status":status,"reason":reason}

def size_bull_put(x, acct):
    # max loss per spread = (width - credit) * 100
    per = max(0.0,(x["width"]-x["credit"])) * 100.0
    hard_cap = min(acct["cash_available"], acct["per_trade_cap"])
    if per<=0 or hard_cap<=0: return {"contracts":0,"status":"blocked","reason":"no cash","required_cash":0.0}
    n = math.floor(hard_cap/per)
    if n<=0: return {"contracts":0,"status":"blocked","reason":"not enough for 1 spread","required_cash":per}
    req = n*per; remain = acct["cash_available"] - req
    status="ok"; reason="fits within cash/per-trade cap"
    if n==1 and remain < per*0.25: status="tight"; reason="low remaining cash after 1 spread"
    return {"contracts":n,"required_cash":req,"status":status,"reason":reason}

# --------- outputs ----------
def ensure_dir(p): os.makedirs(p, exist_ok=True)

def write_outputs(bc_items, bp_items):
    ensure_dir(os.path.join("outputs","suggestions"))
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    payload={"generated_at": now_iso,
             "policy":{"bull_call":BCALL,"bull_put":BPUT},
             "bull_call": bc_items, "bull_put": bp_items}
    y=os.path.join("outputs","suggestions","vertical_suggestions.yml")
    j=os.path.join("outputs","suggestions","vertical_suggestions.json")
    with open(j,"w",encoding="utf-8") as f: json.dump(payload,f,indent=2)
    with open(y,"w",encoding="utf-8") as f: f.write(json.dumps(payload,indent=2))
    print(f"\n[OK] Wrote {y}\n[OK] Wrote {j}")

# --------- main ----------
def main():
    ap=argparse.ArgumentParser(description="Rank bull call & bull put spreads with account-aware sizing.")
    ap.add_argument("--cash", type=float, default=None, help="override cash_available")
    ap.add_argument("--top", type=int, default=10)
    args=ap.parse_args()

    acct=load_account_state(os.path.join("data","account_state.yml"), cash_override=args.cash)
    p_bcall=latest_path("vertical_bull_call")
    p_bput =latest_path("vertical_bull_put")
    bc=read_bull_call(p_bcall); bp=read_bull_put(p_bput)

    # policy filters
    bc=[x for x in bc if x["dte"]>=BCALL["dte_min"] and x["short_delta"] is not None and BCALL["short_delta_min"]<=x["short_delta"]<=BCALL["short_delta_max"] and (x["debit"]/x["width"]<=BCALL["debit_max_width"] if x["width"] else False)]
    bp=[x for x in bp if x["dte"]>=BPUT["dte_min"] and x["short_delta"] is not None and BPUT["short_delta_min"]<=x["short_delta"]<=BPUT["short_delta_max"] and (x["credit"]/x["width"]>=BPUT["credit_min_width"] if x["width"] else False)]

    for x in bc: x["score"]=round(score_bull_call(x),3)
    for x in bp: x["score"]=round(score_bull_put(x),3)
    bc.sort(key=lambda z:z["score"], reverse=True)
    bp.sort(key=lambda z:z["score"], reverse=True)

    banner("VERTICAL RANKER — Bull Call / Bull Put (policy filters + sizing)")
    print(bold("Top Bull Call candidates"))
    bc_items=[]
    if not bc:
        print(dim("(no candidates passed policy filters)"))
    else:
        print(bold(f"{' #':>3} {'Sym':<6} {'Exp':<10} {'DTE':>4} {'Δs':>6} {'Debit':>7} {'Width':>7} {'Rem%':>6}  {'Size':>4} {'CashReq':>9}  Status"))
        for i,x in enumerate(bc[:max(args.top,10)], start=1):
            rem = (x["width"]-x["debit"])/x["width"] if x["width"] else 0.0
            s = size_bull_call(x, acct)
            badge = green("🟢") if s["status"]=="ok" else yellow("🟡") if s["status"]=="tight" else red("🔴")
            print(f"{i:>3} {x['symbol']:<6} {x['exp']:<10} {x['dte']:>4} {x['short_delta']:>6.3f} {x['debit']:>7.2f} {x['width']:>7.2f} {rem*100:>6.1f}%  {s.get('contracts',0):>4} ${s.get('required_cash',0):>8.0f}  {badge} {s.get('reason','')}")
            bc_items.append({
                "symbol":x["symbol"],"exp":x["exp"],"dte":x["dte"],"short_delta":x["short_delta"],
                "debit":x["debit"],"width":x["width"],"remain_to_max_pct":round(rem*100,2),
                "suggested_contracts":s.get("contracts",0),"required_cash":s.get("required_cash",0),
                "sizing_status":s.get("status",""),"sizing_reason":s.get("reason","")
            })

    print("\n"+bold("Top Bull Put candidates"))
    bp_items=[]
    if not bp:
        print(dim("(no candidates passed policy filters)"))
    else:
        print(bold(f"{' #':>3} {'Sym':<6} {'Exp':<10} {'DTE':>4} {'Δs':>6} {'Credit':>7} {'Width':>7} {'Cr/W':>6}  {'Size':>4} {'CashReq':>9}  Status"))
        for i,x in enumerate(bp[:max(args.top,10)], start=1):
            crw = (x["credit"]/x["width"]) if x["width"] else 0.0
            s = size_bull_put(x, acct)
            badge = green("🟢") if s["status"]=="ok" else yellow("🟡") if s["status"]=="tight" else red("🔴")
            print(f"{i:>3} {x['symbol']:<6} {x['exp']:<10} {x['dte']:>4} {x['short_delta']:>6.3f} {x['credit']:>7.2f} {x['width']:>7.2f} {crw*100:>6.1f}%  {s.get('contracts',0):>4} ${s.get('required_cash',0):>8.0f}  {badge} {s.get('reason','')}")
            bp_items.append({
                "symbol":x["symbol"],"exp":x["exp"],"dte":x["dte"],"short_delta":x["short_delta"],
                "credit":x["credit"],"width":x["width"],"credit_to_width_pct":round(crw*100,2),
                "suggested_contracts":s.get("contracts",0),"required_cash":s.get("required_cash",0),
                "sizing_status":s.get("status",""),"sizing_reason":s.get("reason","")
            })

    write_outputs(bc_items, bp_items)

if __name__ == "__main__":
    main()