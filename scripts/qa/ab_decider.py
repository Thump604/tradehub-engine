#!/usr/bin/env python
import json, argparse
from pathlib import Path
import pandas as pd

def jlog(**kw): print(json.dumps(kw))

def load_params():
    p = Path("catalog/params.json")
    if p.exists():
        try: return json.loads(p.read_text())
        except Exception: return {}
    return {}

def join_key_tuple(df):
    cols = [c for c in ["symbol","Expiration Date","Strike Price"] if c in df.columns]
    if not all(c in df.columns for c in ["symbol","Expiration Date","Strike Price"]):
        return None
    return tuple(df[c] for c in ["symbol","Expiration Date","Strike Price"])

def simple_score(df, screener):
    # Try annualized return style fields first
    for c in ["__AY","Ann Rtn","%Time Premium Ask Annual Rtn%","Static Annual Return%","Yield to Strike Annual Rtn%"]:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            return s.fillna(s.min()).rank(pct=True)
    # Fallback by IV Rank then Volume
    iv = pd.to_numeric(df.get("IV Rank"), errors="coerce")
    vol = pd.to_numeric(df.get("Volume"), errors="coerce")
    iv = (iv - iv.min()) / (iv.max()-iv.min()) if iv.notna().any() else 0
    vol = (vol - vol.min()) / (vol.max()-vol.min()) if vol.notna().any() else 0
    return 0.7*iv.fillna(0) + 0.3*vol.fillna(0)

def kendall_tau(r1, r2):
    items = list(set(r1.keys()) & set(r2.keys()))
    n = len(items)
    if n < 2: return None
    concord, discord = 0, 0
    for i in range(n):
        for j in range(i+1, n):
            a, b = items[i], items[j]
            s1 = (r1[a] - r1[b]) > 0
            s2 = (r2[a] - r2[b]) > 0
            concord += int(s1 == s2)
            discord += int(s1 != s2)
    denom = concord + discord
    if denom == 0: return 1.0
    return (concord - discord) / denom

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--screener", required=True, choices=["covered_call","csp","long_call"])
    ap.add_argument("--k", type=int, default=20)
    ap.add_argument("--prefer", choices=["main","custom"], default=None)
    args = ap.parse_args()

    l2 = Path(f"data/l2/{args.screener}/ranked.parquet")
    if not l2.exists():
        jlog(stage="qa_ab", status="no_l2_ranked"); return
    dfm = pd.read_parquet(l2)
    if "__rank" in dfm.columns:
        dfm = dfm.sort_values(["__rank","__score"], ascending=[True, False])
    else:
        dfm = dfm.sort_values("__score", ascending=False)
    topm = dfm.head(args.k).copy()
    key_cols = [c for c in ["symbol","Expiration Date","Strike Price"] if c in topm.columns]
    if len(key_cols) < 3:
        jlog(stage="qa_ab", status="missing_join_keys_in_l2"); return
    topm["__key"] = list(zip(*[topm[c] for c in ["symbol","Expiration Date","Strike Price"]]))
    merged_keys = list(topm["__key"])
    merged_rank = {k:i+1 for i,k in enumerate(merged_keys)}

    pref = args.prefer
    params = load_params()
    if not pref:
        pref = params.get("preferences", {}).get(args.screener, {}).get("single_source", "custom")
    src_path = Path(f"data/l1/{args.screener}/{pref}.parquet")
    alt_path = Path(f"data/l1/{args.screener}/{'main' if pref=='custom' else 'custom'}.parquet")
    src = src_path if src_path.exists() else (alt_path if alt_path.exists() else None)
    if src is None:
        jlog(stage="qa_ab", status="no_single_source"); return

    dfs = pd.read_parquet(src)
    need = [c for c in ["symbol","Expiration Date","Strike Price"] if c in dfs.columns]
    if len(need) < 3:
        jlog(stage="qa_ab", status="single_source_missing_keys", source=pref, path=str(src)); return
    dfs["__key"] = list(zip(*[dfs[c] for c in ["symbol","Expiration Date","Strike Price"]]))
    dfs["__score_src"] = simple_score(dfs, args.screener)
    dfs = dfs.sort_values("__score_src", ascending=False)
    src_rank = {k:i+1 for i,k in enumerate(dfs["__key"].tolist())}

    coverage = sum(1 for k in merged_keys if k in src_rank) / max(1, len(merged_keys))
    tau = kendall_tau(merged_rank, src_rank)
    rec = "merged"
    if tau is not None and coverage >= 0.9 and tau >= 0.8:
        rec = f"single:{pref}"
    jlog(stage="qa_ab", screener=args.screener, k=args.k, coverage=round(coverage,4), kendall_tau=(None if tau is None else round(tau,4)), recommendation=rec,
         l2=str(l2), source=str(src))

if __name__ == "__main__":
    main()
