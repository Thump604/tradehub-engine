#!/usr/bin/env bash
set -euo pipefail
python scripts/l1/normalize_cc.py --max-age-minutes 15
python scripts/l1/normalize_csp.py --max-age-minutes 15
python scripts/l1/normalize_long_call.py --max-age-minutes 15
