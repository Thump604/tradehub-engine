#!/usr/bin/env bash
set -euo pipefail

# Root of the project (edit if you run from elsewhere)
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Activate venv if present
if [[ -f ".venv/bin/activate" ]]; then
  source .venv/bin/activate
fi

echo "[$(date -u +%FT%TZ)] Ingesting latest CSVs (market + screeners)…"
python -m scripts.ingest_latest

echo "[$(date -u +%FT%TZ)] Running rankers (saving suggestions)…"
SAVE_SUGGESTIONS=1 python -m scripts.rank_csp
SAVE_SUGGESTIONS=1 python -m scripts.rank_covered_call
SAVE_SUGGESTIONS=1 python -m scripts.rank_pmcc
SAVE_SUGGESTIONS=1 python -m scripts.rank_verticals
SAVE_SUGGESTIONS=1 python -m scripts.rank_diagonal
SAVE_SUGGESTIONS=1 python -m scripts.rank_iron_condor

echo "[$(date -u +%FT%TZ)] Launching the Hub…"
python -m scripts.trade_hub_menu