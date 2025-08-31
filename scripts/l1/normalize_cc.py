from __future__ import annotations
import json, sys
from pathlib import Path
import pandas as pd

def jlog(**kw): print(json.dumps(kw, ensure_ascii=False))

def to_float(x):
    if x is None: return None
    if isinstance(x, (int,float)): return float(x)
    s = str(x).strip().replace(",", "")
    if " to " in s: s = s.split(" to ", 1)[0]
    try: return float(s)
    except: return None

def to_pct(x):
    if x is None: return None
    if isinstance(x, (int,float)): return float(x)
    s = str(x).strip().replace(",", "")
    if s.endswith("%"): s = s[:-1]
    try: return float(s)
    except: return None

p_in  = Path("data/l1/covered_call/unified.parquet")
p_out = Path("data/l1/covered_call/normalized.parquet")
if not p_in.exists():
    jlog(stage="l1_norm", status="error", reason="missing unified", path=str(p_in))
    sys.exit(2)

df = pd.read_parquet(p_in)

need_any_of_ay = [
    "Ann Rtn",
    "%Time Premium Ask Annual Rtn%",
    "Static Annual Return%",
    "Yield to Strike Annual Rtn%",
]
# Required for ranking
need_all = ["symbol","Expiration Date","Strike Price","DTE","Delta","IV Rank","Moneyness","Volume"]

df = df.copy()
df["DTE"]        = df["DTE"].map(to_float)
df["Delta"]      = df["Delta"].map(to_float).abs()
df["IV Rank"]    = df["IV Rank"].map(to_pct)
df["Moneyness"]  = df["Moneyness"].map(to_pct)
df["Volume"]     = df["Volume"].map(to_float)

df["__AY_src"] = None
for c in need_any_of_ay:
    if c in df.columns:
        df["__AY_src"] = df["__AY_src"].where(df["__AY_src"].notna(), c)

df["__AY"] = None
for c in need_any_of_ay:
    if c in df.columns:
        df["__AY"] = df["__AY"].where(df["__AY"].notna(), df[c].map(to_pct))

mask_all = (
    df["symbol"].notna()
    & df["Expiration Date"].notna()
    & df["Strike Price"].notna()
    & df["DTE"].notna()
    & df["Delta"].notna()
    & df["IV Rank"].notna()
    & df["Moneyness"].notna()
    & df["Volume"].notna()
    & df["__AY"].notna()
)

removed = int((~mask_all).sum())
df_norm = df.loc[mask_all].copy()
df_norm.to_parquet(p_out, index=False)

jlog(stage="l1_norm",
     screener="covered_call",
     in_rows=len(df),
     out_rows=len(df_norm),
     dropped_rows=removed,
     ay_source_counts=df_norm["__AY_src"].value_counts(dropna=False).to_dict(),
     out=str(p_out))
