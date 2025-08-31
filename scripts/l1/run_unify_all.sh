#!/usr/bin/env bash
set -euo pipefail
python scripts/l1/unify_views.py --screener covered_call --outdir data/l1/covered_call incoming/covered_call_main.csv incoming/covered_call_custom.csv
python scripts/l1/unify_views.py --screener csp          --outdir data/l1/csp          incoming/csp_main.csv          incoming/csp_custom.csv
python scripts/l1/unify_views.py --screener long_call    --outdir data/l1/long_call    incoming/long_call_main.csv    incoming/long_call_custom.csv
