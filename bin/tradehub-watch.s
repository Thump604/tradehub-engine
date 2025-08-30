#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/David/Documents/Chatgpt/engine"
PY="$ROOT/.venv/bin/python"

# === TUNE HERE ===
FAST_SECS=30      # market hours (08:00–17:00 CT)
SLOW_SECS=300     # off hours (after 17:00 CT and before 08:00 CT)
TZNAME="America/Chicago"
# =================

cd "$ROOT"

export PYTHONPATH="$ROOT:${PYTHONPATH:-}"
export SUGGESTION_GLOBS="outputs/web_feed/*_suggestions.json"

# simple loop that picks an interval by local Central time
while true; do
  NOW_HOUR=$(TZ="$TZNAME" date +%H)
  NOW_MIN=$(TZ="$TZNAME" date +%M)

  # market window: 08:00–16:59 inclusive
  if [[ "$NOW_HOUR" -ge 8 && "$NOW_HOUR" -lt 17 ]]; then
    POLL="$FAST_SECS"
  else
    POLL="$SLOW_SECS"
  fi

  echo "[watch] $(TZ="$TZNAME" date '+%Y-%m-%d %H:%M:%S %Z') • poll=${POLL}s"
  # Launch one iteration with the chosen poll; the script itself loops and sleeps POLL seconds.
  "$PY" -m scripts.watch_incoming --poll-secs "$POLL" || true

  # If the watcher exits unexpectedly, wait a moment and re-evaluate the window.
  sleep 2
done