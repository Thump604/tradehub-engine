#!/usr/bin/env bash
set -euo pipefail
python scripts/l1/normalize_cc.py
python scripts/l2/covered_call_rank.py \
  --in-main data/l1/covered_call/main.parquet \
  --in-custom data/l1/covered_call/custom.parquet \
  --outdir data/l2/covered_call \
  --top 10
