#!/usr/bin/env python3
import json
from pathlib import Path
import pandas as pd

def jlog(**kw): print(json.dumps(kw))

inputs = [
    Path("incoming/indices.csv"),
    Path("incoming/market_indices.csv"),
    Path("data/l1/indices/unified.parquet"),
]
src = next((p for p in inputs if p.exists()), None)

if src is None:
    jlog(stage="l1_norm_idx", status="skip_no_input")
    raise SystemExit(0)

if src.suffix == ".parquet":
    df = pd.read_parquet(src)
else:
    df = pd.read_csv(src)

outdir = Path("data/l1/indices"); outdir.mkdir(parents=True, exist_ok=True)
out = outdir / "normalized.parquet"
df.to_parquet(out, index=False)
jlog(stage="l1_norm_idx", status="ok", rows=len(df), out=str(out))
