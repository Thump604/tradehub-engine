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
    dte  = nz(df.get("DTE", pd.Series([30]*len(df))), df.get("DTE", pd.Series([30]*len(df))).median() if "DTE" in df else 30)
    ivr  = nz(df.get("IV Rank", pd.Series([0.2]*len(df))), 0.2)
    pprob= nz(df.get("Profit Prob", pd.Series([0.5]*len(df))), 0.5)

    # CSP heuristic: prefer higher Profit Prob, higher IVR, shorter DTE
    dte_norm = (dte.clip(lower=0, upper=60) / 60.0)
    score = 0.5*pprob + 0.3*ivr + 0.2*(1.0 - dte_norm)

    out = df.copy()
    out["__score"] = score
    out["__rank"]  = out["__score"].rank(method="first", ascending=False).astype(int)

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    out_p = outdir/"ranked.parquet"
    out.sort_values(["__score","__rank"], ascending=[False, True]).to_parquet(out_p, index=False)

    head = out.sort_values(["__score","__rank"], ascending=[False, True]).head(args.top)
    cols = [c for c in ["symbol","Expiration Date","Strike Price","DTE","__score","__rank"] if c in head.columns]
    jlog(stage="l2_rank", screener="csp", total=len(out), out=str(out_p), top=head[cols].to_dict(orient="records"))

if __name__=="__main__":
    main()
