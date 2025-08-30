#!/usr/bin/env bash
# repair_suggestions.sh — shell wrapper that calls the Python repairer and
# then shows a quick summary for sanity.

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python -m scripts.repair_suggestions --dirs "$ROOT/outputs" "$ROOT/outputs/suggestions"

echo
echo "[summary] counts and ages the Hub cares about:"
jq -r '.strategy, .generated_at, .count' "$ROOT"/outputs/*_suggestions.json 2>/dev/null || true