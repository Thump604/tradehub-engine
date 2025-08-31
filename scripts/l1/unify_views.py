#!/usr/bin/env python
from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd

ns = {}
exec(compile(Path("catalog/schemas.py").read_text(encoding="utf-8"), "catalog/schemas.py", "exec"), ns, ns)
SCHEMAS = dict(ns.get("SCHEMAS", {}))

JOIN_KEYS = {
    "covered_call": ["symbol","Expiration Date","Strike Price"],
    "csp":          ["symbol","Expiration Date","Strike Price"],
    "long_call":    ["symbol","Expiration Date","Strike Price"],
}

def jlog(**kw): print(json.dumps(kw))

def require_incoming(path: str) -> None:
    rp = Path(path).resolve()
    inc = Path("incoming").resolve()
    if inc != rp and inc not in rp.parents:
        raise SystemExit(f"ERROR: refusing to read outside incoming/: {rp}")

def dedup_columns(cols):
    seen, out = {}, []
    for c in cols:
        if c not in seen:
            seen[c] = 0; out.append(c)
        else:
            seen[c] += 1; out.append(f"{c}.{seen[c]}")
    return out

def apply_header_map(df: pd.DataFrame, screener: str, view: str) -> pd.DataFrame:
    key = f"{screener}_{view}"
    meta = SCHEMAS.get(key, {})
    hmap = meta.get("header_map", {})
    if hmap:
        df = df.rename(columns=hmap)
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip()
    if "Expiration Date" in df.columns:
        df["Expiration Date"] = pd.to_datetime(df["Expiration Date"], errors="coerce").dt.date.astype("string")
    if "Strike Price" in df.columns:
        df["Strike Price"] = pd.to_numeric(df["Strike Price"].astype(str).str.replace(",","", regex=False), errors="coerce")
    return df

def load_and_align(path: str, screener: str, view: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    df.columns = dedup_columns(list(df.columns))
    df = apply_header_map(df, screener, view)
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--screener", required=True, choices=["covered_call","csp","long_call"])
    ap.add_argument("--outdir", required=True)
    ap.add_argument("main_csv")
    ap.add_argument("custom_csv")
    args = ap.parse_args()

    require_incoming(args.main_csv)
    require_incoming(args.custom_csv)

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    df_main   = load_and_align(args.main_csv, args.screener, "main")
    df_custom = load_and_align(args.custom_csv, args.screener, "custom")

    desired = JOIN_KEYS.get(args.screener, [])
    key_cols = [k for k in desired if (k in df_main.columns and k in df_custom.columns)]
    common_cols = sorted(list(set(df_main.columns) & set(df_custom.columns)))

    jlog(stage="input_info", screener=args.screener,
         main_file=args.main_csv, custom_file=args.custom_csv,
         rows_main=len(df_main), cols_main=len(df_main.columns),
         rows_custom=len(df_custom), cols_custom=len(df_custom.columns))

    def uniq(df, keys):
        if not keys: return None
        base = df.dropna(subset=keys)
        return int(base.drop_duplicates(subset=keys).shape[0])

    jlog(stage="alignment", screener=args.screener,
         join_keys_candidate=desired, join_keys_used=key_cols,
         common_cols=common_cols,
         key_cardinality_main=uniq(df_main, key_cols),
         key_cardinality_custom=uniq(df_custom, key_cols))

    lhs = df_main[common_cols].reset_index(drop=True)
    rhs = df_custom[common_cols].reset_index(drop=True)

    if key_cols:
        df_unified = pd.concat([lhs, rhs], ignore_index=True).drop_duplicates(subset=key_cols)
    else:
        df_unified = pd.concat([lhs, rhs], ignore_index=True).drop_duplicates()

    df_main.to_parquet(outdir / "main.parquet", index=False)
    df_custom.to_parquet(outdir / "custom.parquet", index=False)
    df_unified.to_parquet(outdir / "unified.parquet", index=False)

    jlog(stage="output", screener=args.screener,
         rows_main=len(df_main), rows_custom=len(df_custom), rows_unified=len(df_unified),
         artifacts=[str(outdir / "main.parquet"), str(outdir / "custom.parquet"), str(outdir / "unified.parquet")])

if __name__ == "__main__":
    main()
