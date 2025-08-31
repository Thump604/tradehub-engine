#!/usr/bin/env python
import argparse, json
from pathlib import Path
import pandas as pd

def jlog(**kw): print(json.dumps(kw))

def nz(series, fill):
    s = series.copy()
    return s.fillna(fill) if s.isna().any() else s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()

    df = pd.read_parquet(args.infile)
    jlog(stage="source", using="normalized", rows=len(df))

    # Features (robust to missing)
    dte   = nz(df.get("DTE", pd.Series([180]*len(df))), df.get("DTE", pd.Series([180]*len(df))).median() if "DTE" in df else 180)
    ivr   = nz(df.get("IV Rank", pd.Series([0.2]*len(df))), 0.2)
    mny   = nz(df.get("Moneyness", pd.Series([0.0]*len(df))), 0.0)
    pprob = nz(df.get("ITM Probability", df.get("Profit Prob", pd.Series([0.5]*len(df)))), 0.5)

    # Long Call heuristic: prefer higher ITM/pprob, lower IVR, moderate DTE
    dte_pref = 180.0
    dte_norm = (1.0 - (dte - dte_pref).abs().clip(0, dte_pref) / dte_pref)  # peak near target horizon
    score = 0.45*pprob + 0.25*(mny.clip(-0.5,0.5)+0.5) + 0.20*dte_norm + 0.10*(1.0 - ivr.clip(0,1))

    out = df.copy()
    out["__score"] = score
    out["__rank"]  = out["__score"].rank(method="first", ascending=False).astype(int)

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    out_p = outdir/"ranked.parquet"
    out.sort_values(["__score","__rank"], ascending=[False, True]).to_parquet(out_p, index=False)

    head = out.sort_values(["__score","__rank"], ascending=[False, True]).head(args.top)
    cols = [c for c in ["symbol","Expiration Date","Strike Price","DTE","__score","__rank"] if c in head.columns]
    jlog(stage="l2_rank", screener="long_call", total=len(out), out=str(out_p), top=head[cols].to_dict(orient="records"))

if __name__=="__main__":
    main()
