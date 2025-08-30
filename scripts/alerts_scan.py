# scripts/alerts_scan.py
from __future__ import annotations
import json
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
WEB_FEED = ROOT / "outputs" / "web_feed"
STATE = WEB_FEED / "_state_last_run.json"

# Tunables
HIGH_SCORE = 85.0  # raise if you want fewer high-score alerts
SPIKE_MIN_DELTA = 10.0  # score improvement threshold vs last run
CLUSTER_MIN_STRATS = 3  # number of distinct strategies per symbol to alert


def _load_items():
    src = WEB_FEED / "suggestions_merged.json"
    if not src.exists():
        return []
    data = json.loads(src.read_text())
    items = (
        data["items"]
        if isinstance(data, dict) and "items" in data
        else (data if isinstance(data, list) else [])
    )
    return [it for it in items if isinstance(it, dict)]


def _key(it: dict) -> tuple[str, str, str]:
    return (
        str(it.get("symbol", "")).upper(),
        str(it.get("strategy", "")),
        str(it.get("expiry", "—")),
    )


def _load_state() -> dict:
    if STATE.exists():
        try:
            return json.loads(STATE.read_text())
        except Exception:
            return {}
    return {}


def _save_state(current_scores: dict):
    STATE.write_text(json.dumps({"scores": current_scores}, indent=2))


def main():
    items = _load_items()
    alerts = []

    # Build current score index and symbol→strategies set
    current_scores = {}
    sym_strats = defaultdict(set)

    for it in items:
        sym, strat, exp = _key(it)
        score = float(it.get("score", 0.0))
        current_scores["|".join([sym, strat, exp])] = score
        sym_strats[sym].add(strat)

        if score >= HIGH_SCORE:
            alerts.append(
                {
                    "kind": "high_score",
                    "symbol": sym,
                    "strategy": strat,
                    "expiry": exp,
                    "score": score,
                }
            )

    # Spike alerts
    prev = _load_state().get("scores", {})
    for k, score in current_scores.items():
        delta = score - float(prev.get(k, 0.0))
        if delta >= SPIKE_MIN_DELTA:
            sym, strat, exp = k.split("|", 2)
            alerts.append(
                {
                    "kind": "spike",
                    "symbol": sym,
                    "strategy": strat,
                    "expiry": exp,
                    "delta": round(delta, 3),
                    "score": score,
                }
            )

    # Cluster alerts (multiple distinct strategies on same symbol)
    for sym, strategies in sym_strats.items():
        if len(strategies) >= CLUSTER_MIN_STRATS:
            alerts.append(
                {
                    "kind": "cluster",
                    "symbol": sym,
                    "strategies": sorted(list(strategies)),
                    "count": len(strategies),
                }
            )

    out = WEB_FEED / "alerts.json"
    out.write_text(json.dumps(alerts, indent=2))
    print(f"[saved] {out} (alerts={len(alerts)})")

    # persist state for next run
    _save_state(current_scores)


if __name__ == "__main__":
    main()
