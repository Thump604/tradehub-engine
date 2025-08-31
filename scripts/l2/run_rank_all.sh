#!/usr/bin/env bash
set -euo pipefail

# Example run (freshness enforced at 15 minutes)
# Assumes L1 parquet exists (run scripts/l1/run_unify_all.sh first if needed).

# 1) Normalize Covered Call L1 into typed canonical fields
python scripts/l1/normalize_cc.py --max-age-minutes 15

# 2) Rank Covered Call using normalized L1 (preferred). Falls back to main/custom if normalized is missing.
python scripts/l2/covered_call_rank.py --max-age-minutes 15 --top 10

# Optional: Backfill/testing with a looser freshness window (manually run these lines if needed)
# python scripts/l1/normalize_cc.py --max-age-minutes 120 --allow-stale
# python scripts/l2/covered_call_rank.py --max-age-minutes 120 --allow-stale --top 10
