#!/usr/bin/env python3
import argparse, json
from pathlib import Path
import pandas as pd
from catalog.schemas import SCHEMAS

def dedup_columns(cols):
    """Ensure column names are unique by appending suffixes .1, .2, etc."""
    seen = {}
    out = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}.{seen[c]}")
    return out

def load_and_align(path, screener, view):
    df = pd.read_csv(path)
    schema_key = f"{screener}_{view}"
    schema = SCHEMAS.get(schema_key)
    if not schema:
        raise ValueError(f"schema not found: {schema_key}")
    header_map = schema.get("header_map", {})
    df = df.rename(columns=header_map)

    # ✅ enforce unique names safely
    df.columns = dedup_columns(df.columns)

    keep = schema.get("logical_columns", schema["columns"])
    df = df[[c for c in keep if c in df.columns]]
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--screener", required=True)
    ap.add_argument("--outdir", required=True, type=Path)
    ap.add_argument("main_csv")
    ap.add_argument("custom_csv")
    args = ap.parse_args()

    outdir = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)

    df_main   = load_and_align(args.main_csv, args.screener, "main")
    df_custom = load_and_align(args.custom_csv, args.screener, "custom")

    common_cols = list(set(df_main.columns) & set(df_custom.columns))
    key_cols = [c for c in ["symbol","Expiration Date","DTE","Strike Price"] if c in common_cols]

    df_unified = pd.concat([df_main, df_custom], ignore_index=True).drop_duplicates(subset=key_cols)

    df_main.to_parquet(outdir / "main.parquet", index=False)
    df_custom.to_parquet(outdir / "custom.parquet", index=False)
    df_unified.to_parquet(outdir / "unified.parquet", index=False)

    print(json.dumps({
        "outdir": str(outdir),
        "rows_main": len(df_main),
        "rows_custom": len(df_custom),
        "rows_unified": len(df_unified),
        "artifacts": [str(outdir / "main.parquet"),
                      str(outdir / "custom.parquet"),
                      str(outdir / "unified.parquet")]
    }, indent=2))

if __name__ == "__main__":
    main()
