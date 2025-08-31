#!/usr/bin/env python3
import argparse, pathlib, pandas as pd, sys, json
from catalog import schemas

def load_csv(path, header_map=None):
    df = pd.read_csv(path)
    if header_map:
        # map physical -> logical names
        rename_map = {k: v for k, v in header_map.items() if k in df.columns}
        df = df.rename(columns=rename_map)
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--screener", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("main_file")
    ap.add_argument("custom_file")
    args = ap.parse_args()

    screener_main = f"{args.screener}_main"
    screener_custom = f"{args.screener}_custom"

    if screener_main not in schemas.SCHEMAS or screener_custom not in schemas.SCHEMAS:
        sys.exit(f"schema not found for {args.screener}")

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # load
    main_map   = schemas.SCHEMAS[screener_main].get("header_map")
    custom_map = schemas.SCHEMAS[screener_custom].get("header_map")

    df_main   = load_csv(args.main_file,   main_map)
    df_custom = load_csv(args.custom_file, custom_map)

    # align on logical columns intersection
    common_cols = [c for c in df_main.columns if c in df_custom.columns]
    df_main   = df_main[common_cols]
    df_custom = df_custom[common_cols]

    # composite key for joining
    key_cols = [c for c in ["symbol","Expiration Date","DTE","Strike Price"] if c in common_cols]

    df_unified = pd.concat([df_main, df_custom], ignore_index=True).drop_duplicates(subset=key_cols)

    # write parquet
    (outdir / "main.parquet").write_bytes(df_main.to_parquet(index=False))
    (outdir / "custom.parquet").write_bytes(df_custom.to_parquet(index=False))
    (outdir / "unified.parquet").write_bytes(df_unified.to_parquet(index=False))

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
