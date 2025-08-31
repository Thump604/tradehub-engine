#!/usr/bin/env bash
set -euo pipefail

python scripts/l1/unify_views.py \
  --screener covered_call \
  --outdir data/l1/covered_call \
  catalog/references/covered-call-option-screener-call-short-stock-08-30-2025.csv \
  "catalog/references/covered-call-option-screener-call-short-stock-08-30-2025 (1).csv"

python scripts/l1/unify_views.py \
  --screener csp \
  --outdir data/l1/csp \
  catalog/references/naked-put-option-screener-csp-stock-etf-08-30-2025.csv \
  "catalog/references/naked-put-option-screener-csp-stock-etf-08-30-2025 (1).csv"

python scripts/l1/unify_views.py \
  --screener long_call \
  --outdir data/l1/long_call \
  catalog/references/long-call-options-screener-long-call-leap-stock-etf-08-30-2025.csv \
  "catalog/references/long-call-options-screener-long-call-leap-stock-etf-08-30-2025 (1).csv"
