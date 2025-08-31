from __future__ import annotations
import argparse, json, sys
from pathlib import Path
import pandas as pd

def jlog(**kw): print(json.dumps(kw, ensure_ascii=False))

def to_float(x):
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip().replace(",", "")
    if " to " in s: s = s.split(" to ", 1)[0]
    try: return float(s)
    except Exception: return None

def to_pct(x):
    if x is None: return None
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip().replace(",", "")
    if s.endswith("%"): s = s[:-1]
    try: return float(s)
    except Exception: return None

def safe_col(df, *names):
    for n in names:
        if n in df.columns: return n
    return None

def align_logical(df: pd.DataFrame, screener_key: str) -> pd.DataFrame:
    from catalog.schemas import SCHEMAS
    e = SCHEMAS.get(screener_key, {})
    header_map = e.get("header_map", {}) or {}
    cols_new, seen = [], {}
    for c in df.columns:
        logical = header_map.get(c, c)
        k = logical
        if k in seen:
            seen[k] += 1
            k = f"{logical}__{seen[logical]}"
        else:
            seen[k] = 0
        cols_new.append(k)
    df = df.copy()
    df.columns = cols_new
    return df

def get_join_keys_pair() -> list[str]:
    from catalog.schemas import SCHEMAS
    for key in ("covered_call_main", "covered_call_custom"):
        e = SCHEMAS.get(key, {})
        if e.get("join_keys"): return list(e["join_keys"])
    return ["symbol", "Expiration Date", "Strike Price"]

def compute_rank(df: pd.DataFrame) -> pd.DataFrame:
    req = ["symbol", "Expiration Date", "Strike Price", "DTE"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        jlog(stage="rank", status="reject", reason=f"missing required columns: {missing}")
        return df.iloc[0:0].copy()

    dte = df["DTE"].apply(to_float)
    delta_col = safe_col(df, "Delta")
    delta = df[delta_col].apply(lambda x: abs(to_float(x)) if x is not None else None) if delta_col else pd.Series([None]*len(df))
    ivr_col = safe_col(df, "IV Rank")
    ivr = df[ivr_col].apply(to_pct) if ivr_col else pd.Series([None]*len(df))
    mny_col = safe_col(df, "Moneyness")
    mny = df[mny_col].apply(to_pct) if mny_col else pd.Series([None]*len(df))
    ay_col = safe_col(df, "Ann Rtn", "%Time Premium Ask Annual Rtn%", "Static Annual Return%", "Yield to Strike Annual Rtn%")
    annual_yield = df[ay_col].apply(to_pct) if ay_col else pd.Series([None]*len(df))
    pp_col = safe_col(df, "Profit Prob")
    profit_prob = df[pp_col].apply(to_pct) if pp_col else pd.Series([None]*len(df))

    target_dte = 30.0
    ideal_delta = 0.30
    target_mny = 2.0
    mny_band   = 5.0

    weights = {
        "annual_yield": 0.35,
        "dte":          0.15,
        "delta":        0.20,
        "iv_rank":      0.15,
        "profit_prob":  0.10,
        "moneyness":    0.05,
    }

    def clamp01(x):
        if x is None: return 0.0
        try:
            if x < 0: return 0.0
            if x > 1: return 1.0
            return float(x)
        except Exception:
            return 0.0

    comp_ay  = annual_yield.apply(lambda v: clamp01((to_float(v) or 0)/100.0))
    comp_ivr = ivr.apply(lambda v: clamp01((to_float(v) or 0)/100.0))
    comp_pp  = profit_prob.apply(lambda v: clamp01((to_float(v) or 0)/100.0))
    comp_dte = dte.apply(lambda v: clamp01(1.0 - min(abs((v or 0)-target_dte)/max(1.0,target_dte), 1.0)))
    comp_del = delta.apply(lambda v: clamp01(1.0 - min(abs((v or 0)-ideal_delta)/max(ideal_delta,1e-9), 1.0)))
    comp_mny = mny.apply(lambda v: clamp01(1.0 - min(abs((v or 0)-target_mny)/max(mny_band,1e-9), 1.0)))

    score = (
        weights["annual_yield"] * comp_ay +
        weights["dte"]          * comp_dte +
        weights["delta"]        * comp_del +
        weights["iv_rank"]      * comp_ivr +
        weights["profit_prob"]  * comp_pp +
        weights["moneyness"]    * comp_mny
    )

    out = df.copy()
    out["__score"] = score
    out["__rank"]  = out["__score"].rank(method="first", ascending=False).astype(int)
    out["__ay"]    = annual_yield
    out["__pp"]    = profit_prob
    out["__ivr"]   = ivr
    out["__dte_c"] = comp_dte
    out["__del_c"] = comp_del
    out["__mny_c"] = comp_mny
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-main",   default="data/l1/covered_call/main.parquet")
    ap.add_argument("--in-custom", default="data/l1/covered_call/custom.parquet")
    ap.add_argument("--outdir",    default="data/l2/covered_call")
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    main_pq   = Path(args.in_main)
    custom_pq = Path(args.in_custom)
    if not main_pq.exists() or not custom_pq.exists():
        jlog(stage="l2", status="error", reason="missing L1 parquet", main=str(main_pq), custom=str(custom_pq))
        sys.exit(2)

    df_m = pd.read_parquet(main_pq)
    df_c = pd.read_parquet(custom_pq)
    df_m = align_logical(df_m, "covered_call_main")
    df_c = align_logical(df_c, "covered_call_custom")

    join_keys = get_join_keys_pair()
    present = [k for k in join_keys if k in df_m.columns and k in df_c.columns]
    if not present:
        jlog(stage="join", mode="fallback_concat", reason="no join keys present in both views", join_keys=join_keys)
        df_joined = pd.concat([df_m, df_c], ignore_index=True)
    else:
        df_joined = pd.merge(df_m, df_c, on=present, how="outer", suffixes=("_m","_c"))
        jlog(stage="join", mode="merge", join_keys=present, rows=len(df_joined))

    ranked = compute_rank(df_joined)
    out_parquet = outdir / "ranked.parquet"
    ranked.sort_values("__score", ascending=False).to_parquet(out_parquet, index=False)

    head = ranked.sort_values("__score", ascending=False).head(args.top)
    preview = head[["symbol","Expiration Date","Strike Price","DTE","__score","__rank"]].to_dict(orient="records")
    jlog(stage="l2_rank", screener="covered_call", total=len(ranked), out=str(out_parquet), top=preview)

if __name__ == "__main__":
    main()
