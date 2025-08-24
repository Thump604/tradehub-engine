#!/usr/bin/env python3
# rank_pmcc.py — pairs LEAP (long call) with short call; account-aware LEAP sizing (debit).
# Inputs: data/leap-latest.csv, data/covered_call-latest.csv, data/account_state.yml
# Output: prints ranked pairs + suggested contracts; writes outputs/suggestions/pmcc_suggestions.*

import argparse, csv, json, os, sys, math
from datetime import datetime, timezone
from typing import List, Dict, Any

# ---------- tiny YAML ----------
def load_simple_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    out = {}
    stack = [out]; indent_stack = [0]
    for raw in open(path,"r",encoding="utf-8"):
        line = raw.rstrip("\n")
        if not line.strip() or line.strip().startswith("#"): continue
        indent = len(line) - len(line.lstrip(" "))
        while indent < indent_stack[-1]:
            stack.pop(); indent_stack.pop()
        if ":" in line:
            k,v = line.lstrip().split(":",1)
            k=k.strip(); v=v.strip()
            if v=="":
                d={}
                stack[-1][k]=d
                stack.append(d); indent_stack.append(indent+2)
            else:
                if v.lower() in ("true","false"):
                    sv = v.lower()=="true"
                else:
                    try:
                        sv = float(v) if any(c in v for c in ".0123456789") and v.replace(".","",1).replace("-","",1).isdigit() else v.strip("'\"")
                    except:
                        sv = v.strip("'\"")
                stack[-1][k]=sv
    return out

# ---------- print helpers ----------
def bold(s): return f"\033[1m{s}\033[0m"
def green(s): return f"\033[32m{s}\033[0m"
def yellow(s): return f"\033[33m{s}\033[0m"
def red(s): return f"\033[31m{s}\033[0m"
def dim(s): return f"\033[2m{s}\033[0m"

def banner(title: str):
    print("\n" + "─"*70)
    print(title.center(70))
    print("─"*70 + "\n")

def to_float(x):
    if x is None: return None
    x = str(x).strip().replace(",","").replace("%","")
    if x in ("","N/A"): return None
    try: return float(x)
    except: return None

def latest_path(strategy: str) -> str:
    cat = load_simple_yaml(os.path.join("data","data_catalog_runtime.yml"))
    ds = (cat.get("datasets") or {}).get(strategy) or {}
    return ds.get("file","")

def load_account_state(path: str, cash_override: float=None) -> Dict[str,float]:
    st = load_simple_yaml(path) or {}
    total = float(st.get("total_value",0))
    sleeve = float(st.get("alloc_pct_to_options",0.0))
    cash = float(st.get("cash_available",0.0))
    per_trade = float(st.get("per_trade_cap_pct",0.02))
    if cash_override is not None:
        cash = float(cash_override)
    return {
        "total_value": total,
        "sleeve_pct": sleeve,
        "cash_available": cash,
        "per_trade_cap_pct": per_trade,
        "sleeve_cap": total * sleeve,
        "per_trade_cap": total * per_trade
    }

# ---------- load CSVs ----------
def read_leaps(path: str) -> List[Dict[str,Any]]:
    out=[]
    if not path or not os.path.exists(path): return out
    with open(path,"r",encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            if (r.get("Type") or "").title()!="Call": continue
            out.append({
                "symbol": r.get("Symbol"),
                "exp": r.get("Exp Date"),
                "dte": int(to_float(r.get("DTE")) or 0),
                "strike": to_float(r.get("Strike")),
                "delta": to_float(r.get("Delta")),
                "ask": to_float(r.get("Ask") or r.get("Option Ask Price") or r.get("Ask Price")),
                "last": to_float(r.get("Last") or r.get("Option Last Price")),
                "ivr": to_float(r.get("IV Rank")),
                "moneyness": r.get("Moneyness")
            })
    return [x for x in out if x["symbol"]]

def read_short_calls(path: str) -> List[Dict[str,Any]]:
    out=[]
    if not path or not os.path.exists(path): return out
    with open(path,"r",encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            if (r.get("Type") or "").title()!="Call": continue
            out.append({
                "symbol": r.get("Symbol"),
                "exp": r.get("Exp Date"),
                "dte": int(to_float(r.get("DTE")) or 0),
                "strike": to_float(r.get("Strike")),
                "delta_abs": abs(to_float(r.get("Delta")) or 0.0),
                "bid": to_float(r.get("Bid") or r.get("Option Bid Price")),
                "ask": to_float(r.get("Ask") or r.get("Option Ask Price")),
                "oi": int(to_float(r.get("Open Int") or r.get("Option Open Interest") or 0) or 0),
                "moneyness": r.get("Moneyness"),
                "ivr": to_float(r.get("IV Rank"))
            })
    return [x for x in out if x["symbol"]]

# ---------- policy ----------
POL = {
    "leap_dte_min": 180, "leap_delta_min": 0.65,
    "short_dte_min": 28, "short_dte_max": 45,
    "short_delta_min": 0.15, "short_delta_max": 0.55,
    "short_oi_min": 100
}

def pair_candidates(leaps, shorts):
    pairs=[]
    bysym = {}
    for s in shorts:
        bysym.setdefault(s["symbol"], []).append(s)
    for L in leaps:
        if L["dte"] < POL["leap_dte_min"] or (L["delta"] or 0) < POL["leap_delta_min"]:
            continue
        for S in bysym.get(L["symbol"], []):
            if not (POL["short_dte_min"] <= S["dte"] <= POL["short_dte_max"]): continue
            if not (POL["short_delta_min"] <= S["delta_abs"] <= POL["short_delta_max"]): continue
            if S["oi"] < POL["short_oi_min"]: continue
            # simple score: prefer short Δ close to 0.35 and DTE ~35, and bigger LEAP Δ
            score = (1.0 - abs(S["delta_abs"]-0.35)) * 0.6 + (1.0 - abs(S["dte"]-35)/35.0)*0.3 + (min(1.0,(L["delta"] or 0.0)))*0.1
            pairs.append((score, L, S))
    pairs.sort(key=lambda x: x[0], reverse=True)
    return pairs

# ---------- sizing (LEAP debit-based) ----------
def size_pmcc(L: Dict[str,Any], acct: Dict[str,float]) -> Dict[str,Any]:
    per_contract_debit = (L["ask"] or 0.0) * 100.0
    hard_cap = min(acct["cash_available"], acct["per_trade_cap"])
    if per_contract_debit <= 0 or hard_cap <= 0:
        return {"contracts": 0, "status":"blocked", "reason":"no cash", "required_cash": 0.0}
    max_by_cash = math.floor(hard_cap / per_contract_debit)
    if max_by_cash <= 0:
        return {"contracts": 0, "status":"blocked", "reason":"not enough for 1 contract", "required_cash": per_contract_debit}
    contracts = max_by_cash
    req = contracts * per_contract_debit
    remain = acct["cash_available"] - req
    status = "ok"; reason = "fits within cash/per-trade cap"
    if contracts==1 and remain < per_contract_debit*0.25:
        status="tight"; reason="low remaining cash after 1 contract"
    return {"contracts": contracts, "status": status, "reason": reason, "required_cash": req, "cash_left": remain}

# ---------- outputs ----------
def ensure_dir(p): os.makedirs(p, exist_ok=True)

def write_outputs(items: List[Dict[str,Any]]):
    ensure_dir(os.path.join("outputs","suggestions"))
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    payload = {"generated_at": now_iso, "policy":{
        "leap":{"dte_min": POL["leap_dte_min"], "delta_min": POL["leap_delta_min"]},
        "short":{"dte_range":[POL["short_dte_min"], POL["short_dte_max"]],"delta_abs_range":[POL["short_delta_min"], POL["short_delta_max"]],"oi_min": POL["short_oi_min"]},
        "targets":{"short_delta":0.35,"short_dte":35}
    }, "count": len(items), "top": items}
    y = os.path.join("outputs","suggestions","pmcc_suggestions.yml")
    j = os.path.join("outputs","suggestions","pmcc_suggestions.json")
    with open(j,"w",encoding="utf-8") as f: json.dump(payload,f,indent=2)
    with open(y,"w",encoding="utf-8") as f:
        f.write(json.dumps(payload, indent=2))  # simple
    print(f"\n[OK] Wrote {y}\n[OK] Wrote {j}")

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Rank PMCC pairs with account-aware LEAP sizing.")
    ap.add_argument("--cash", type=float, default=None, help="override cash_available")
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()

    acct = load_account_state(os.path.join("data","account_state.yml"), cash_override=args.cash)
    leaps = read_leaps(latest_path("leap"))
    shorts = read_short_calls(latest_path("covered_call"))

    pairs = pair_candidates(leaps, shorts)
    banner("PMCC RANKER — Top pairings (sized by LEAP debit & per-trade cap)")
    if not pairs:
        print(dim("(no candidates passed policy filters)"))
        write_outputs([])
        return

    print(bold(f"{' #':>3} {'Sym':<6}   {'LEAP':<28} | {'SHORT':<34}   {'Size':>4} {'Debit':>10}  Status"))
    items=[]
    for i,(score,L,S) in enumerate(pairs[:max(args.top,10)], start=1):
        sizing = size_pmcc(L, acct)
        badge = green("🟢") if sizing["status"]=="ok" else yellow("🟡") if sizing["status"]=="tight" else red("🔴")
        leap_txt = f"{L['strike']:.2f}C exp {L['exp']} DTE {L['dte']} Δ {L['delta'] or 0:.3f}"
        short_txt= f"{S['strike']:.2f}C exp {S['exp']} DTE {S['dte']} Δ {S['delta_abs']:.3f} OI {S['oi']}"
        print(f"{i:>3} {L['symbol']:<6}   {leap_txt:<28} | {short_txt:<34}   {sizing.get('contracts',0):>4} ${sizing.get('required_cash',0):>9.0f}  {badge} {sizing.get('reason','')}")
        items.append({
            "symbol": L["symbol"],
            "leap": {k:L[k] for k in ("exp","dte","strike","delta","ask","last","ivr","moneyness")},
            "short": {k:S[k] for k in ("exp","dte","strike","delta_abs","bid","ask","oi","ivr","moneyness")},
            "suggested_contracts": sizing.get("contracts",0),
            "required_cash": sizing.get("required_cash",0),
            "sizing_status": sizing.get("status",""),
            "sizing_reason": sizing.get("reason","")
        })
    write_outputs(items)

if __name__ == "__main__":
    main()