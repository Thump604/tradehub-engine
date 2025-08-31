#!/usr/bin/env python
from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd

from catalog.schemas import SCHEMAS

def jlog(**payload):
    print(json.dumps(payload, separators=(",", ":")))

def dedup_columns(cols):
    seen = {}
    out = []
    for c in cols:
        n = seen.get(c, 0)
        if n == 0:
            out.append(c)
        else:
            out.append(f"{c}.{n+1}")
        seen[c] = n + 1
    return out

def schema_key(screener: str, view: str) -> str:
    return f"{screener}_{view}"

def normalize_headers(df: pd.DataFrame, screener: str, view: str) -> pd.DataFrame:
    key = schema_key(screener, view)
    entry = SCHEMAS.get(key, {})
    hmap = entry.get("header_map", {})
    # De-dup physical names first (customs can have "Ask" twice etc.)
    df = df.copy()
    df.columns = dedup_columns(list(df.columns))
    # Map to logical names where provided
    mapped = [hmap.get(c, c) for c in df.columns]
    df.columns = mapped
    return df

def load_and_align(path: str, screener: str, view: str) -> pd.DataFrame:
    df = pd.read_csv(path, engine="python")
    # Drop a trailing Barchart footer row if present (single text cell)
    if df.shape[0] > 0 and df.shape[1] >= 1:
        last = df.iloc[-1]
        if last.isna().sum() >= (df.shape[1] - 1):
            try:
                s = str(last.iloc[0])
                if s.startswith("Downloaded from Barchart.com"):
                    df = df.iloc[:-1]
            except Exception:
                pass
    df = normalize_headers(df, screener, view)
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--screener", required=True, choices=["covered_call","csp","long_call"])
    ap.add_argument("--outdir", required=True)
    ap.add_argument("main_csv")
    ap.add_argument("custom_csv")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df_main   = load_and_align(args.main_csv, args.screener, "main")
    df_custom = load_and_align(args.custom_csv, args.screener, "custom")

    jlog(stage="input_info",
         screener=args.screener,
         main_file=args.main_csv,
         custom_file=args.custom_csv,
         rows_main=len(df_main),
         cols_main=len(df_main.columns),
         rows_custom=len(df_custom),
         cols_custom=len(df_custom.columns))

    # Intersection on logical names only
    common_cols = sorted(set(df_main.columns) & set(df_custom.columns))
    key_cols = [c for c in ["symbol","Expiration Date","DTE","Strike Price"] if c in common_cols]

    jlog(stage="alignment",
         screener=args.screener,
         common_cols=common_cols,
         key_cols=key_cols)

    df_unified = (
        pd.concat(
            [df_main[common_cols].reset_index(drop=True),
             df_custom[common_cols].reset_index(drop=True)],
            ignore_index=True
        ).drop_duplicates(subset=key_cols)
    )

    df_main.to_parquet(outdir / "main.parquet", index=False)
    df_custom.to_parquet(outdir / "custom.parquet", index=False)
    df_unified.to_parquet(outdir / "unified.parquet", index=False)

    jlog(stage="output",
         screener=args.screener,
         rows_main=len(df_main),
         rows_custom=len(df_custom),
         rows_unified=len(df_unified),
         artifacts=[str(outdir / "main.parquet"),
                    str(outdir / "custom.parquet"),
                    str(outdir / "unified.parquet")])

if __name__ == "__main__":
    main()
