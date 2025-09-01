#!/usr/bin/env bash
set -euo pipefail
python scripts/l1/normalize_cc.py
python scripts/l1/normalize_csp.py
python scripts/l1/normalize_long_call.py
python scripts/l1/normalize_indices.py || true
