#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bridge writer: reads outputs/market_state.json (rich format) and emits a
legacy/simple market.json the web UI already knows how to display.

Writes:
  - outputs/market.json
  - outputs/web_feed/market.json (extra, in case the web looks there)
"""
from __future__ import annotations
import json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs"
WEB_FEED = OUT / "web_feed"
STATE = OUT / "market_state.json"
LEGACY1 = OUT / "market.json"
LEGACY2 = WEB_FEED / "market.json"


def load_state():
    if not STATE.exists():
        raise SystemExit(f"[legacy_market] missing {STATE}")
    with STATE.open() as f:
        return json.load(f)


def mk_guidance(regime: str, trend: str, vol: str) -> str:
    # Plain but helpful one-liner that mirrors old UI language
    parts = []
    parts.append(
        f"{regime} regime — balance directional and defined-risk."
        if regime
        else "Regime unknown."
    )
    parts.append(
        f"Trend: {trend} — price direction bias." if trend else "Trend unknown."
    )
    parts.append(
        f"Vol: {vol} — IV/VIX context for defined-risk vs premium harvest."
        if vol
        else "Vol unknown."
    )
    return " ".join(parts)


def main():
    d = load_state()
    regime = d.get("regime") or "Unknown"
    trend = d.get("trend") or "Unknown"
    vol = d.get("vol") or "Unknown"
    vol_detail = d.get("vol_detail") or ""
    headline = d.get("headline") or ""

    legacy = {
        "generated_at": d.get("generated_at"),
        "regime": regime,
        "trend": trend,
        "vol": vol,
        "vol_detail": vol_detail,
        "headline": headline,
        "guidance": mk_guidance(regime, trend, vol),
    }

    OUT.mkdir(parents=True, exist_ok=True)
    WEB_FEED.mkdir(parents=True, exist_ok=True)

    for p in (LEGACY1, LEGACY2):
        with p.open("w") as f:
            json.dump(legacy, f, indent=2)
        print(f"[legacy_market] wrote {p}")


if __name__ == "__main__":
    main()
