#!/usr/bin/env python
import argparse, json
from pathlib import Path
import pandas as pd

def jlog(**kw): print(json.dumps(kw))

def to_float(x):
    if x is None: return None
    try:
        if isinstance(x,str):
            s=x.strip().replace(',','')
            if s.endswith('%'): s=s[:-1]
            if s=='': return None
            return float(s)
        return float(x)
    except Exception:
        return None

def to_pct(x):
    v=to_float(x)
    return None if v is None else (v/100.0 if abs(v)>1.5 else v)  # accept 0.12 or 12%

def load_l1_pair(base):
    p_unified = base/'unified.parquet'
    if p_unified.exists():
        df = pd.read_parquet(p_unified)
        jlog(stage="l1_norm_csp", mode="use_unified", rows=len(df))
        return df

    p_m = base/'main.parquet'
    p_c = base/'custom.parquet'
    df_m = pd.read_parquet(p_m) if p_m.exists() else pd.DataFrame()
    df_c = pd.read_parquet(p_c) if p_c.exists() else pd.DataFrame()

    join = [c for c in ["symbol","Expiration Date","Strike Price"] if c in df_m.columns and c in df_c.columns]
    if join:
        df = pd.merge(df_m, df_c, on=join, how="outer", suffixes=("_m","_c"))
        jlog(stage="l1_norm_csp", mode="merge", join_keys=join, rows=len(df))
    else:
        df = pd.concat([df_m, df_c], ignore_index=True)
        jlog(stage="l1_norm_csp", mode="concat", rows=len(df))
    return df

def coalesce(df):
    # bring canonical columns to front; drop helper *_m/_c afterward
    for col in ["DTE","Delta","IV Rank","Moneyness","Volume","Profit Prob"]:
        m,f = f"{col}_m", f"{col}_c"
        if col in df.columns: continue
        if m in df.columns or f in df.columns:
            df[col] = df[m] if m in df.columns else None
            if f in df.columns:
                df[col] = df[col].where(pd.notna(df[col]), df[f])

    # prefer existing Volume; fall back to Option Volume if needed
    if "Volume" not in df.columns and "Option Volume" in df.columns:
        df["Volume"] = df["Option Volume"]

    # type/clean
    for c in ["DTE","Delta","IV Rank","Moneyness","Volume","Profit Prob"]:
        if c in df.columns:
            if c in ("IV Rank","Moneyness","Profit Prob"):
                df[c] = df[c].map(to_pct)
            else:
                df[c] = df[c].map(to_float)
    if "Delta" in df.columns:
        df["Delta"] = df["Delta"].abs()

    # drop helper columns and dups
    helper = [c for c in df.columns if c.endswith("_m") or c.endswith("_c") or c=="Option Volume"]
    if helper: df = df.drop(columns=helper)
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-age-minutes", type=int, default=15)  # reserved; not enforced here
    ap.add_argument("--allow-stale", action="store_true")       # reserved; not enforced here
    args = ap.parse_args()

    base = Path("data/l1/csp")
    out  = base/"normalized.parquet"
    df   = load_l1_pair(base)
    df   = coalesce(df)

    # keep only rows with core keys present
    required = ["symbol","Expiration Date","Strike Price"]
    mask = pd.Series(True, index=df.index)
    for c in required:
        mask &= df.columns.isin([c]).any() and df[c].notna()
    df_norm = df.loc[mask].copy()

    df_norm.to_parquet(out, index=False)
    jlog(stage="l1_norm_csp", out=str(out), rows=len(df_norm))

if __name__=="__main__":
    main()
