#!/usr/bin/env python
import argparse, json
from pathlib import Path
import pandas as pd

def jlog(**kw): print(json.dumps(kw))

def to_float(x):
    if x is None: return None
    if isinstance(x,(int,float)): return float(x)
    s=str(x).strip()
    if s in ("","nan","None","NaN"): return None
    s=s.replace(",","")
    try: return float(s)
    except: return None

def to_pct(x):
    if x is None: return None
    if isinstance(x,(int,float)): return float(x)
    s=str(x).strip()
    if s in ("","nan","None","NaN"): return None
    neg = s.startswith("-")
    s = s.replace("%","").replace(",","").replace("+","").replace("-","")
    try:
        v = float(s)/100.0
        return -v if neg else v
    except:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-age-minutes", type=int, default=15)
    ap.add_argument("--allow-stale", action="store_true", default=False)
    args = ap.parse_args()

    # prefer unified if present
    uni_p = Path("data/l1/covered_call/unified.parquet")
    if not uni_p.exists():
        jlog(stage="l1_norm", error="missing_unified", path=str(uni_p))
        raise SystemExit(1)

    df = pd.read_parquet(uni_p)
    jlog(stage="l1_norm", mode="use_unified", rows=int(len(df)))

    # ---- coalesce core fields if *_m/*_c exist
    def coalesce(col):
        l, r = f"{col}_m", f"{col}_c"
        if l in df.columns and r in df.columns:
            df[col] = df[l].where(pd.notna(df[l]), df[r])
        elif l in df.columns:
            df[col] = df[l]
        elif r in df.columns:
            df[col] = df[r]
        # else keep as-is if already exists

    for col in ["DTE","Delta","IV Rank","Moneyness","Volume"]:
        if col not in df.columns or f"{col}_m" in df.columns or f"{col}_c" in df.columns:
            coalesce(col)

    # Collapse Option Volume -> Volume if Volume missing
    if "Volume" not in df.columns and "Option Volume" in df.columns:
        df["Volume"] = df["Option Volume"]

    # Type/clean
    if "DTE" in df:        df["DTE"]       = df["DTE"].map(to_float)
    if "Delta" in df:      df["Delta"]     = df["Delta"].map(to_float).abs()
    if "IV Rank" in df:    df["IV Rank"]   = df["IV Rank"].map(to_pct)
    if "Moneyness" in df:  df["Moneyness"] = df["Moneyness"].map(to_pct)
    if "Volume" in df:     df["Volume"]    = df["Volume"].map(to_float)

    # Annualized yield (__AY) from any available columns
    ay_candidates = [
        "Ann Rtn",
        "%Time Premium Ask Annual Rtn%",
        "Static Annual Return%",
        "Yield to Strike Annual Rtn%",
    ]
    df["__AY_src"] = None
    df["__AY"]     = None
    for c in ay_candidates:
        if c in df.columns:
            # only fill where still NaN
            val = df[c].map(to_pct)
            df["__AY"] = df["__AY"].where(pd.notna(df["__AY"]), val)
            df["__AY_src"] = df["__AY_src"].where(pd.notna(df["__AY_src"]), c)

    # Drop helper merge columns *_m/*_c if any
    helper = [c for c in df.columns if c.endswith("_m") or c.endswith("_c")]
    if helper:
        df = df.drop(columns=helper)

    # Deduplicate headers (keep first)
    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()].copy()

    # Keep rows that have the true must-haves for ranking
    required = ["symbol","Expiration Date","Strike Price","DTE","Delta","IV Rank","Moneyness","__AY"]
    mask = pd.Series(True, index=df.index)
    for c in required:
        mask &= (c in df.columns) and df[c].notna()
    out = df.loc[mask].copy()

    jlog(stage="l1_norm", screener="covered_call",
         in_rows=int(len(df)), out_rows=int(len(out)),
         dropped_rows=int(len(df)-len(out)),
         ay_source_counts=(out["__AY_src"].value_counts(dropna=False).to_dict() if "__AY_src" in out.columns else {}),
         out="data/l1/covered_call/normalized.parquet")

    Path("data/l1/covered_call").mkdir(parents=True, exist_ok=True)
    out.to_parquet("data/l1/covered_call/normalized.parquet", index=False)

if __name__ == "__main__":
    main()
