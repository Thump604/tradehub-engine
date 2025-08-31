#!/usr/bin/env bash
set -euo pipefail
python scripts/qa/validate_unified.py --screener covered_call --unified data/l1/covered_call/unified.parquet --main data/l1/covered_call/main.parquet --custom data/l1/covered_call/custom.parquet
python scripts/qa/validate_unified.py --screener csp          --unified data/l1/csp/unified.parquet          --main data/l1/csp/main.parquet          --custom data/l1/csp/custom.parquet
python scripts/qa/validate_unified.py --screener long_call    --unified data/l1/long_call/unified.parquet    --main data/l1/long_call/main.parquet    --custom data/l1/long_call/custom.parquet
