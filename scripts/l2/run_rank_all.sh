#!/usr/bin/env bash
set -euo pipefail

# Covered Call (uses normalized if present; flags are for fallback path)
python scripts/l2/covered_call_rank.py \
  --in-main   data/l1/covered_call/main.parquet \
  --in-custom data/l1/covered_call/custom.parquet \
  --outdir    data/l2/covered_call \
  --top 10

# CSP (from normalized L1)
python scripts/l2/csp_rank.py \
  --infile data/l1/csp/normalized.parquet \
  --outdir data/l2/csp \
  --top 10 || true

# Long Call (from normalized L1)
python scripts/l2/long_call_rank.py \
  --infile data/l1/long_call/normalized.parquet \
  --outdir data/l2/long_call \
  --top 10 || true
