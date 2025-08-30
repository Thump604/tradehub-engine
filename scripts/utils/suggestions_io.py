# scripts/utils/suggestions_io.py
from __future__ import annotations
import os, json, yaml
from datetime import datetime, timezone
from typing import List, Dict, Any

OUTDIR = os.environ.get("SUGGESTIONS_DIR", "outputs")

def _ensure_outdir():
    os.makedirs(OUTDIR, exist_ok=True)

def _stamp() -> str:
    # Hub expects a space-style UTC stamp like 2025-08-25 15:28:01Z
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")

def write_suggestions(strategy_key: str, rows: List[Dict[str, Any]]) -> None:
    """
    Write suggestions for Trade Hub in both JSON and YAML with metadata the Hub expects.
    Schema:
      {
        "strategy": "<strategy_key>",
        "generated_at": "YYYY-MM-DD HH:MM:SSZ",
        "count": <int>,
        "top": [ { ... suggestion rows ... } ]
      }
    """
    _ensure_outdir()

    payload = {
        "strategy": strategy_key,
        "generated_at": _stamp(),
        "count": len(rows or []),
        "top": rows or [],
    }

    base = os.path.join(OUTDIR, f"{strategy_key}_suggestions")
    with open(base + ".json", "w") as f:
        json.dump(payload, f, indent=2)
    with open(base + ".yml", "w") as f:
        yaml.safe_dump(payload, f, sort_keys=False)

    print(f"[saved] {base}.json")
    print(f"[saved] {base}.yml")