#!/usr/bin/env bash
set -euo pipefail
python -m scripts.catalog.profile_catalog --in "$1" --screener "$2" --outdir catalog/specs
