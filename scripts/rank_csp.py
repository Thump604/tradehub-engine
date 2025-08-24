#!/usr/bin/env python3
# rank_csp.py — rank Covered Short Put candidates with account-aware sizing.
# Inputs:
#   - data/data_catalog_runtime.yml (from ingest_latest.py)
#   - data/csp-latest.csv
#   - data/account_state.yml (optional; --cash overrides)
# Output:
#   - prints ranked table with suggested contracts + risk notes
#   - writes outputs/suggestions/csp_suggestions.{yml,json}

import argparse, csv, json, os, sys, math
from datetime import datetime, timezone
from typing import List, Dict, Any

# ---------- tiny YAML reader (no deps) ----------
def load_simple_yaml(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    out = {}
    stack = [out]
    indent_stack = [0]
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line.strip() or line.strip().startswith("#"):
                continue
            indent = len(line) - len(line.lstrip(" "))
            while indent < indent_stack[-1]:
                stack.pop(); indent_stack.pop()
            if ":" in line:
                k, v = line.lstrip().split(":", 1)
                k = k.strip()
                v = v.strip()
                if v == "":
                    # start new dict level
                    d = {}
                    stack[-1][k] = d
                    stack.append(d); indent_stack.append(indent+2)
                else:
                    # scalar
                    if v.lower() in ("true","false"):
                        sv = v.lower()=="true"
                    else:
                        try:
                            if v.endswith("%"):
                                sv = float(v[:-1])
                            else:
                                sv = float(v) if "." in v or v.isdigit() else v.strip("'\"")
                        except:
                            sv = v.strip("'\"")
                    stack[-1][k] = sv
    return out

# ---------- printing ----------
def C(s): return s
def bold(s): return f"\033[1m{s}\033[0m"
def green(s): return f"\033[32m{s}\033[0m"
def yellow(s): return f"\033[33m{s}\033[0m"
def red(s): return f"\033[31m{s}\033[0m"
def dim(s): return f"\033[2m{s}\033[0m"

def banner(title: str):
    print("\n" + "─"*70)
    print(title.center(70))
    print("─"*70 + "\n")

# ---------- money/nums ----------
def f2(x): return f"{x:.2f}"
def pct(x): return f"{x:.2f}%"

# ---------- account state ----------
def load_account_state(path: str, cash_override: float=None) -> Dict[str, float]:
    st = load_simple_yaml(path) or {}
    total = float(st.get("total_value", 0))
    sleeve = float(st.get("alloc_pct_to_options", 0.0))
    cash = float(st.get("cash_available", 0.0))
    per_trade = float(st.get("per_trade_cap_pct", 0.02))
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

# ---------- data catalog ----------
def latest_path(strategy: str) -> str:
    cat = load_simple_yaml(os.path.join("data","data_catalog_runtime.yml"))
    ds = (cat.get("datasets") or {}).get(strategy) or {}
    return ds.get("file", "")

# ---------- csv helpers ----------
def to_float(x):
    if x is None: return None
    x = str(x).strip().replace(",","").replace("%","")
    if x in ("", "N/A"): return None
    try: return float(x)
    except: return None

def read_csp_rows(path: str) -> List[Dict[str,Any]]:
    rows = []
    if not path or not os.path.exists(path):
        return rows
    with open(path, "r", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            # file headers (Barchart CSP custom view)
            sym = r.get("Symbol")
            exp = r.get("Exp Date") or r.get("Expiration Date")
            dte = to_float(r.get("DTE"))
            strike = to_float(r.get("Strike"))
            typ = (r.get("Type") or "").strip().title()
            delta = to_float(r.get("Delta"))
            oi = to_float(r.get("Total OI") or r.get("Open Int") or r.get("Option Open Interest"))
            ask_opt = to_float(r.get("Ask") or r.get("Option Ask Price"))
            last_opt = to_float(r.get("Last") or r.get("Option Last Price"))
            be_ask = to_float(r.get("BE (Ask)") or r.get("Break Even (Ask)"))
            pop = to_float(r.get("OTM Prob")) or (100 - (to_float(r.get("ITM Prob")) or 0))
            mny = r.get("Moneyness")
            if not sym or typ != "Put": 
                continue
            rows.append({
                "symbol": sym, "exp": exp, "dte": int(dte) if dte is not None else None,
                "strike": strike, "ask": ask_opt, "last": last_opt,
                "delta_abs": abs(delta) if delta is not None else None,
                "oi": int(oi) if oi is not None else None,
                "breakeven_ask": be_ask, "pop": pop, "moneyness": mny
            })
    return rows

# ---------- policy filters ----------
POLICY = {
    "dte_min": 21, "dte_max": 60,
    "delta_min": 0.15, "delta_max": 0.35,
    "oi_min": 100
}

def pass_policy(r: Dict[str,Any]) -> bool:
    if r["dte"] is None or r["delta_abs"] is None or r["oi"] is None:
        return False
    if not (POLICY["dte_min"] <= r["dte"] <= POLICY["dte_max"]): return False
    if not (POLICY["delta_min"] <= r["delta_abs"] <= POLICY["delta_max"]): return False
    if r["oi"] < POLICY["oi_min"]: return False
    return True

# ---------- scoring ----------
def ann_roc(ask: float, strike: float, dte: int) -> float:
    if not ask or not strike or not dte or dte <= 0: return 0.0
    yield_pct = ask / strike
    return yield_pct * (365.0 / dte) * 100.0

def score_row(r: Dict[str,Any]) -> float:
    # basic blend: ROC and POP proxy
    roc = ann_roc(r["ask"], r["strike"], r["dte"])
    pop = r["pop"] or 0.0
    return 0.6*roc + 0.4*pop

# ---------- sizing ----------
def size_csp(r: Dict[str,Any], acct: Dict[str,float]) -> Dict[str,Any]:
    # requirement per contract = strike * 100 (cash-secured)
    per_contract_cash = (r["strike"] or 0) * 100.0
    sleeve_cap = acct["sleeve_cap"]
    # sleeve already abstract; assume we're independent per candidate — cap by per-trade & available cash:
    hard_cap = min(acct["cash_available"], acct["per_trade_cap"])
    if hard_cap <= 0 or per_contract_cash <= 0:
        return {"contracts": 0, "status": "blocked", "reason": "no cash", "required_cash": 0.0}
    max_by_cash = math.floor(hard_cap / per_contract_cash)
    if max_by_cash <= 0:
        return {"contracts": 0, "status": "blocked", "reason": "not enough for 1 contract", "required_cash": per_contract_cash}
    # start with 1, up to max_by_cash
    contracts = max_by_cash
    required_cash = contracts * per_contract_cash
    remain = acct["cash_available"] - required_cash
    status = "ok"
    reason = "fits within cash/per-trade cap"
    if contracts == 1 and remain < per_contract_cash*0.25:
        status = "tight"; reason = "low remaining cash after 1 contract"
    return {"contracts": contracts, "status": status, "reason": reason, "required_cash": required_cash, "cash_left": remain}

# ---------- io ----------
def ensure_dir(p): os.makedirs(p, exist_ok=True)

def write_outputs(items: List[Dict[str,Any]]):
    ensure_dir(os.path.join("outputs","suggestions"))
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    payload = {
        "generated_at": now_iso,
        "policy": {
            "dte_range": [POLICY["dte_min"], POLICY["dte_max"]],
            "delta_abs_range": [POLICY["delta_min"], POLICY["delta_max"]],
            "oi_min": POLICY["oi_min"]
        },
        "count": len(items),
        "top": items
    }
    ypath = os.path.join("outputs","suggestions","csp_suggestions.yml")
    jpath = os.path.join("outputs","suggestions","csp_suggestions.json")
    # naive YAML
    with open(ypath,"w",encoding="utf-8") as f:
        f.write(f"generated_at: '{now_iso}'\npolicy:\n")
        f.write(f"  dte_range:\n  - {POLICY['dte_min']}\n  - {POLICY['dte_max']}\n")
        f.write(f"  delta_abs_range:\n  - {POLICY['delta_min']}\n  - {POLICY['delta_max']}\n")
        f.write(f"  oi_min: {POLICY['oi_min']}\n")
        f.write(f"count: {len(items)}\n")
        f.write("top:\n")
        for it in items:
            f.write("- " + json.dumps(it).replace("{","").replace("}","").replace('"',"") + "\n")
    with open(jpath,"w",encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"\n[OK] Wrote {ypath}")
    print(f"[OK] Wrote {jpath}")

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Rank CSP candidates with account-aware sizing.")
    ap.add_argument("--cash", type=float, default=None, help="override cash_available for this run")
    ap.add_argument("--top", type=int, default=10, help="how many to print")
    args = ap.parse_args()

    acct = load_account_state(os.path.join("data","account_state.yml"), cash_override=args.cash)
    csp_path = latest_path("csp")
    rows = read_csp_rows(csp_path)
    # filter & score
    filt = [r for r in rows if pass_policy(r)]
    for r in filt:
        r["roc_annual"] = round(ann_roc(r["ask"], r["strike"], r["dte"]), 2)
        r["score"] = round(score_row(r), 2)
    filt.sort(key=lambda x: x["score"], reverse=True)

    banner("CSP RANKER — Top candidates (sized by cash & per-trade cap)")
    if not filt:
        print(dim("(no candidates passed policy filters)"))
        write_outputs([])
        return

    out_items = []
    print(bold(f"{' #':>3} {'Sym':<6} {'Exp':<10} {'DTE':>4} {'Δ':>6} {'OI':>7} {'Strike':>8} {'Ask':>6} "
               f"{'ROC(ann)':>9} {'POP~':>6}   {'Size':>4}  {'Cash Req':>11}  Status/Reason"))
    i = 0
    for r in filt[:max(args.top, 10)]:
        i += 1
        sizing = size_csp(r, acct)
        badge = green("🟢") if sizing["status"]=="ok" else yellow("🟡") if sizing["status"]=="tight" else red("🔴")
        print(f"{i:>3} {r['symbol']:<6} {r['exp']:<10} {r['dte']:>4} {r['delta_abs']:>6.3f} {r['oi']:>7} "
              f"{r['strike']:>8.2f} {r['ask']:>6.2f} {r['roc_annual']:>9.2f} {r['pop'] or 0:>6.1f}%   "
              f"{sizing.get('contracts',0):>4}  ${sizing.get('required_cash',0):>10.0f}  {badge} {sizing.get('reason','')}")
        out_items.append({
            "symbol": r["symbol"], "exp": r["exp"], "dte": r["dte"], "strike": r["strike"],
            "delta": r["delta_abs"], "oi": r["oi"], "ask": r["ask"], "last": r["last"],
            "breakeven_ask": r["breakeven_ask"], "roc_annual": r["roc_annual"],
            "pop_proxy": round(r["pop"] or 0.0,2),
            "suggested_contracts": sizing.get("contracts",0),
            "required_cash": sizing.get("required_cash",0),
            "sizing_status": sizing.get("status",""),
            "sizing_reason": sizing.get("reason","")
        })
    write_outputs(out_items)

if __name__ == "__main__":
    main()