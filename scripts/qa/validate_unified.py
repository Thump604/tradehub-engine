#!/usr/bin/env python
from __future__ import annotations
import argparse, json, sys
from pathlib import Path
import pandas as pd

from catalog.schemas import SCHEMAS

def jlog(**k):
    print(json.dumps(k, separators=(",",":")))

def get_join_keys(screener: str) -> list[str]:
    # prefer main, then custom
    for key in (f"{screener}_main", f"{screener}_custom"):
        e = SCHEMAS.get(key)
        if e and e.get("join_keys"):
            return list(e["join_keys"])
    return []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--screener", required=True)
    ap.add_argument("--unified", required=True, help="path to unified.parquet")
    ap.add_argument("--main",     required=False, help="optional path to main.parquet")
    ap.add_argument("--custom",   required=False, help="optional path to custom.parquet")
    args = ap.parse_args()

    uk_path = Path(args.unified)
    if not uk_path.exists():
        jlog(stage="qa", screener=args.screener, error=f"file not found: {uk_path}")
        sys.exit(2)

    join_keys = get_join_keys(args.screener)
    df_u = pd.read_parquet(uk_path)

    missing_cols = [c for c in join_keys if c not in df_u.columns]
    if missing_cols:
        jlog(stage="qa", screener=args.screener, status="fail", reason="missing_join_cols", join_keys=join_keys, missing=missing_cols)
        sys.exit(1)

    # basic facts
    n = len(df_u)
    null_stats = {c: int(df_u[c].isna().sum()) for c in join_keys}
    # uniqueness
    dup_mask = df_u[join_keys].duplicated(keep=False)
    dup_count = int(dup_mask.sum())

    # coverage vs inputs (optional)
    cov = {}
    for label, p in (("main", args.main), ("custom", args.custom)):
        if p and Path(p).exists():
            df_i = pd.read_parquet(p)
            miss_cols_i = [c for c in join_keys if c not in df_i.columns]
            if miss_cols_i:
                cov[label] = {"rows": len(df_i), "missing_cols": miss_cols_i}
            else:
                # how many input keys are represented in unified
                u_keys = set(map(tuple, df_u[join_keys].astype(str).values.tolist()))
                i_keys = set(map(tuple, df_i[join_keys].astype(str).values.tolist()))
                cov[label] = {
                    "rows": len(df_i),
                    "keys_in_unified": len(i_keys & u_keys),
                    "keys_total": len(i_keys),
                    "pct_in_unified": round(100.0 * (len(i_keys & u_keys) / max(1, len(i_keys))), 2)
                }

    status = "pass" if dup_count == 0 else "warn"
    jlog(stage="qa", screener=args.screener, status=status, rows_unified=n,
         join_keys=join_keys, nulls=null_stats, duplicate_keys=dup_count, coverage=cov)

if __name__ == "__main__":
    main()
