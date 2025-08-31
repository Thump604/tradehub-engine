#!/usr/bin/env bash
set -euo pipefail

# 1) Normalize Covered Call L1 into typed canonical fields (15-min freshness)
python scripts/l1/normalize_cc.py --max-age-minutes 15

# 2) Rank Covered Call (prefers normalized if present, flags required for fallback)
python scripts/l2/covered_call_rank.py \
  --in-main   data/l1/covered_call/main.parquet \
  --in-custom data/l1/covered_call/custom.parquet \
  --outdir    data/l2/covered_call \
  --top 10
