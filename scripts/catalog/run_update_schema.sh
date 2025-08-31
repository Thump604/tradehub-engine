#!/usr/bin/env bash
set -euo pipefail
python -m scripts.catalog.update_schema --in "$1" --screener "$2"
