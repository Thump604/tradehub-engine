#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_web_view.py
- Read existing suggestion files from outputs/
- Normalize into a strict schema for the web (without modifying originals)
- Write to outputs/web_normalized/<strategy>_suggestions.json
"""

from __future__ import annotations
import json, sys, os, glob, hashlib
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import datetime, timezone

try:
    import yaml  # PyYAML (already in your venv per earlier work)

    HAVE_YAML = True
except Exception:
    HAVE_YAML = False

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs"
SRC_GLOB = str(OUT_DIR / "*_suggestions.*")
DEST_DIR = OUT_DIR / "web_normalized"
DEST_DIR.mkdir(parents=True, exist_ok=True)

STRATEGIES = {
    "covered_call",
    "csp",
    "pmcc",
    "vertical",
    "diagonal",
    "iron_condor",
    "long_call",
}


def iso_utc(ts: float | None = None) -> str:
    dt = (
        datetime.fromtimestamp(ts, tz=timezone.utc)
        if ts
        else datetime.now(timezone.utc)
    )
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def newest_per_strategy(files: List[Path]) -> Dict[str, Path]:
    keep: Dict[str, Tuple[float, Path]] = {}
    for p in files:
        name = p.name.lower()
        # extract strategy from file stem prefix
        if "_suggestions." not in name:
            continue
        strat = name.split("_suggestions.")[0]
        # allow long_call too
        if strat not in STRATEGIES:
            # tolerate verticals naming variations
            if strat.startswith("vertical"):
                strat = "vertical"
            elif strat.startswith("iron"):
                strat = "iron_condor"
            elif strat.startswith("covered"):
                strat = "covered_call"
            elif strat.startswith("long_call"):
                strat = "long_call"
        if strat not in STRATEGIES:
            continue
        mtime = p.stat().st_mtime
        curr = keep.get(strat)
        if curr is None or mtime > curr[0]:
            keep[strat] = (mtime, p)
    return {k: v[1] for k, v in keep.items()}


def load_any(path: Path) -> Any:
    txt = path.read_text(encoding="utf-8").strip()
    if path.suffix.lower() in (".yml", ".yaml"):
        if not HAVE_YAML:
            raise RuntimeError(f"PyYAML not available to read {path}")
        return yaml.safe_load(txt)
    # JSON fallback
    return json.loads(txt) if txt else {}


def coerce_str(x: Any, fallback: str) -> str:
    if x is None:
        return fallback
    try:
        s = str(x).strip()
        return s if s else fallback
    except Exception:
        return fallback


def coerce_float(x: Any, fallback: float) -> float:
    try:
        return float(x)
    except Exception:
        return fallback


def coerce_bool(x: Any, fallback: bool) -> bool:
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return bool(x)
    if isinstance(x, str):
        t = x.strip().lower()
        if t in ("true", "yes", "y", "1"):
            return True
        if t in ("false", "no", "n", "0"):
            return False
    return fallback


def make_id(strategy: str, symbol: str, exp: str, strike: str, score: float) -> str:
    raw = f"{strategy}|{symbol}|{exp}|{strike}|{score:.6f}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    return f"{strategy}:{symbol}:{exp}:{strike}:{h}"


def normalize_one_item(strategy: str, item: Dict[str, Any]) -> Dict[str, Any]:
    symbol = coerce_str(item.get("symbol"), "?").upper()
    exp = coerce_str(item.get("exp"), "?")
    strike_val = item.get("strike", "-")
    # strike as string for uniformity (may be '-' if NA)
    try:
        if isinstance(strike_val, (int, float)):
            strike = f"{float(strike_val):.2f}"
        else:
            s = str(strike_val).strip()
            # if numeric-ish, format it
            try:
                strike = f"{float(s):.2f}"
            except Exception:
                strike = s if s else "-"
    except Exception:
        strike = "-"
    score = coerce_float(item.get("score", item.get("Score")), 0.0)
    flag = coerce_str(item.get("flag", item.get("Flag")), "YELLOW").upper()
    if flag not in ("GREEN", "YELLOW", "RED"):
        flag = "YELLOW"
    taken = coerce_bool(item.get("taken"), False)

    # carry through useful optional metrics if present
    carry_keys = [
        "delta",
        "theta",
        "vega",
        "gamma",
        "bid",
        "ask",
        "mid",
        "credit",
        "debit",
        "pop",
        "p50",
        "p90",
        "iv",
        "ivr",
        "iv30",
        "iv_rank",
        "otm_pct",
        "breakeven",
        "oi",
        "volume",
        "earnings_date",
    ]
    extra = {k: item.get(k) for k in carry_keys if k in item}

    _id = coerce_str(item.get("id"), "")
    if not _id or _id == "?":
        _id = make_id(strategy, symbol, exp, strike, score)

    norm = {
        "id": _id,
        "strategy": strategy,
        "symbol": symbol,
        "exp": exp if exp else "?",
        "strike": strike if strike else "-",
        "score": score,
        "flag": flag,
        "taken": taken,
    }
    norm.update(extra)
    return norm


def normalize_payload(strategy: str, payload: Any, generated_at: str) -> Dict[str, Any]:
    """
    Accepts a dict with 'top': [...] or a plain list of suggestions.
    Produces: {"strategy": ..., "generated_at": ..., "count": N, "top": [...]}
    """
    if (
        isinstance(payload, dict)
        and "top" in payload
        and isinstance(payload["top"], list)
    ):
        items_src = payload["top"]
    elif isinstance(payload, list):
        items_src = payload
    else:
        # Some rankers might nest under other keys; try to find a list
        items_src = None
        if isinstance(payload, dict):
            for v in payload.values():
                if isinstance(v, list):
                    items_src = v
                    break
        if items_src is None:
            items_src = []

    normalized: List[Dict[str, Any]] = []
    for it in items_src:
        if not isinstance(it, dict):
            continue
        normalized.append(normalize_one_item(strategy, it))

    return {
        "strategy": strategy,
        "generated_at": generated_at,
        "count": len(normalized),
        "top": normalized,
    }


def main() -> int:
    files = [Path(p) for p in glob.glob(SRC_GLOB) if "web_normalized" not in p]
    latest = newest_per_strategy(files)
    if not latest:
        print(
            "[make_web_view] No suggestion sources found under outputs/",
            file=sys.stderr,
        )
        return 2

    wrote = 0
    for strat, path in sorted(latest.items()):
        try:
            payload = load_any(path)
            gen = None
            if isinstance(payload, dict):
                gen = payload.get("generated_at")
            if not gen:
                gen = iso_utc(path.stat().st_mtime)

            view = normalize_payload(strat, payload, gen)
            dest = DEST_DIR / f"{strat}_suggestions.json"
            dest.write_text(json.dumps(view, indent=2), encoding="utf-8")
            wrote += 1
            print(f"[make_web_view] Wrote {dest} (count={view['count']})")
        except Exception as e:
            print(f"[make_web_view] ERROR on {path.name}: {e}", file=sys.stderr)
            return 3

    print(f"[make_web_view] Done. Files written: {wrote}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
