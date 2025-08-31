from __future__ import annotations
import json, sys
from pathlib import Path
import pandas as pd
from catalog.schemas import SCHEMAS

def jlog(**kw): print(json.dumps(kw, ensure_ascii=False))

def to_float(x):
    if x is None: return None
    if isinstance(x,(int,float)): return float(x)
    s = str(x).strip().replace(",","")
    # strip percent if present
    if s.endswith("%"): s = s[:-1]
    # handle "a to b" style strings
    if " to " in s: s = s.split(" to ",1)[0]
    try: return float(s)
    except: return None

def to_pct(x): return to_float(x)

def align_logical(df: pd.DataFrame, schema_key: str) -> pd.DataFrame:
    e = SCHEMAS.get(schema_key, {})
    hm = e.get("header_map", {})
    return df.rename(columns={c: hm.get(c, c) for c in df.columns})

# inputs
main_p   = Path("data/l1/covered_call/main.parquet")
custom_p = Path("data/l1/covered_call/custom.parquet")
out_p    = Path("data/l1/covered_call/normalized.parquet")

if not main_p.exists() or not custom_p.exists():
    jlog(stage="l1_norm", status="error", reason="missing L1 main/custom", main=str(main_p), custom=str(custom_p))
    sys.exit(2)

df_m = pd.read_parquet(main_p)
df_c = pd.read_parquet(custom_p)

# map to logical names per catalog
df_m = align_logical(df_m, "covered_call_main")
df_c = align_logical(df_c, "covered_call_custom")

# join keys from catalog
join_keys = []
for key in ("covered_call_main","covered_call_custom"):
    e = SCHEMAS.get(key)
    if e and e.get("join_keys"):
        join_keys = list(e["join_keys"]); break
if not join_keys: join_keys = ["symbol","Expiration Date","Strike Price"]

present = [k for k in join_keys if k in df_m.columns and k in df_c.columns]
if present:
    df = pd.merge(df_m, df_c, on=present, how="outer", suffixes=("_m","_c"))
    jlog(stage="l1_norm", mode="merge", join_keys=present, rows=len(df))
else:
    # fallback: union rows; we’ll coalesce columns by suffix
    df = pd.concat([df_m, df_c], ignore_index=True, sort=False)
    # add suffixes so coalesce logic still works when duplicates exist
    dup_cols = set(df_m.columns).intersection(df_c.columns)
    # (if concat path, we don't have *_m/*_c duplicates; we just keep single column)
    jlog(stage="l1_norm", mode="concat", rows=len(df), note="no shared join keys")

# coalesce core/AY columns from *_m/*_c to canonical names
def coalesce(col: str):
    a, b = f"{col}_m", f"{col}_c"
    if a in df.columns and b in df.columns:
        df[col] = df[a].where(df[a].notna(), df[b])
    elif a in df.columns:
        df[col] = df[a]
    elif b in df.columns:
        df[col] = df[b]
    # else leave as-is if already present

for col in ["symbol","Expiration Date","Strike Price","DTE","Delta","IV Rank","Moneyness","Volume",
            "Ann Rtn","%Time Premium Ask Annual Rtn%","Static Annual Return%","Yield to Strike Annual Rtn%"]:
    coalesce(col)

# Type/clean
df["DTE"]        = df.get("DTE").map(to_float)
df["Delta"]      = df.get("Delta").map(to_float).abs()
df["IV Rank"]    = df.get("IV Rank").map(to_pct)
df["Moneyness"]  = df.get("Moneyness").map(to_pct)
df["Volume"]     = df.get("Volume").map(to_float)

# Annual yield proxy with source tracking
ay_cols = ["Ann Rtn","%Time Premium Ask Annual Rtn%","Static Annual Return%","Yield to Strike Annual Rtn%"]
df["__AY_src"] = None
df["__AY"]     = None
for c in ay_cols:
    if c in df.columns:
        df["__AY_src"] = df["__AY_src"].where(df["__AY_src"].notna(), c)
        df["__AY"]     = df["__AY"].where(df["__AY"].notna(), df[c].map(to_pct))


# ---- Drop helper merge columns and normalize headers before writing ----
# Collapse Option Volume -> Volume (prefer existing 'Volume')
if "Volume" not in df.columns and "Option Volume" in df.columns:
    df["Volume"] = df["Option Volume"]
if "Option Volume" in df.columns:
    df = df.drop(columns=["Option Volume"])

# Drop *_m/*_c helper columns from the merge
helper_cols = [c for c in df.columns if c.endswith("_m") or c.endswith("_c")]
if helper_cols:
    df = df.drop(columns=helper_cols)
    jlog(stage="l1_norm", note="dropped_helper_cols", count=len(helper_cols))

# Final guard: remove duplicated column names, keep first occurrence
if df.columns.duplicated().any():
    df = df.loc[:, ~df.columns.duplicated()].copy()
    jlog(stage="l1_norm", note="dedup_headers")

# keep only fully-typed rows
required = ["symbol","Expiration Date","Strike Price","DTE","Delta","IV Rank","Moneyness","Volume","__AY"]
mask = pd.Series(True, index=df.index)
for c in required:
    if c in df.columns:
        mask &= df[c].notna()
    else:
        mask &= False

dropped = int((~mask).sum())
df_norm = df.loc[mask].copy()
df_norm.to_parquet(out_p, index=False)

jlog(stage="l1_norm",
     screener="covered_call",
     mode="finalize",
     in_rows=int(len(df)),
     out_rows=int(len(df_norm)),
     dropped_rows=dropped,
     ay_source_counts=(df_norm["__AY_src"].value_counts(dropna=False).to_dict() if "__AY_src" in df_norm.columns else {}),
     out=str(out_p))
