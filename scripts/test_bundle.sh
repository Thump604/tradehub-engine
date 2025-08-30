#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

banner() {
  printf "\n\033[1;36m%s\033[0m\n" "══════════════════════════════════════════════════════════════════════"
  printf "\033[1;36m%s\033[0m\n" " $1"
  printf "\033[1;36m%s\033[0m\n\n" "══════════════════════════════════════════════════════════════════════"
}

need_venv() {
  if [[ -z "${VIRTUAL_ENV:-}" ]]; then
    echo "[-] You're not in a virtualenv. Run:  source .venv/bin/activate"
    exit 1
  fi
}

check_pyyaml() {
  python - <<'PY' >/dev/null 2>&1 || { echo "[ERROR] PyYAML missing. Run: python -m pip install PyYAML"; exit 1; }
try:
    import yaml  # noqa: F401
except Exception:
    raise SystemExit(1)
PY
}

# Optional knobs via env vars
: "${IC_IVR_MIN:=0}"      # iron condor ivr floor
: "${IC_DTE_MIN:=30}"
: "${IC_DTE_MAX:=60}"
: "${TOPN:=12}"

need_venv
check_pyyaml

banner "INGEST LATEST CSVs"
python -m scripts.ingest_latest

banner "RANK: CSP"
python -m scripts.rank_csp --top "$TOPN" || true

banner "RANK: Covered Calls"
python -m scripts.rank_covered_call --top "$TOPN" || true

banner "RANK: PMCC (pairs)"
python -m scripts.rank_pmcc --top "$TOPN" || true

banner "RANK: Verticals (Bull Call & Bull Put)"
python -m scripts.rank_verticals --top "$TOPN" || true

banner "RANK: Long Call Diagonals"
python -m scripts.rank_diagonal --top "$TOPN" || true

banner "RANK: Short Iron Condors"
python -m scripts.rank_iron_condor --ivr-min "$IC_IVR_MIN" --dte-min "$IC_DTE_MIN" --dte-max "$IC_DTE_MAX" --top "$TOPN" || true

banner "BACKFILL: Suggestion IDs"
python -m scripts.suggestion_ids || true

banner "TRADE HUB SNAPSHOT"
python -m scripts.trade_hub || true

banner "ENGINE DOCTOR"
python -m scripts.doctor || true

echo
echo "[OK] test_bundle completed."
echo "     Tweak with env vars, e.g.: IC_IVR_MIN=10 TOPN=20 bash scripts/test_bundle.sh"