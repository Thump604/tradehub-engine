#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from datetime import datetime
from .rank_base import (
    load_barchart_csv,
    build_metrics,
    compute_dte,
    score_long_call,
    color_flag,
    write_json,
    write_yaml,
    card_text,
    CT,
)

DATA = Path("data/leap-latest.csv")
OUT_JSON = Path("outputs/long_call_suggestions.json")
OUT_YAML = Path("outputs/long_call_suggestions.yml")


def main():
    rows, footer_dt = load_barchart_csv(DATA)
    ref = footer_dt or datetime.now(CT)
    generated_at = ref.isoformat(timespec="seconds")
    items = []
    for r in rows:
        symbol = r.get("symbol") or r.get("sym") or r.get("ticker")
        exp = r.get("exp") or r.get("expiry") or r.get("exp_date")
        strike = r.get("strike")
        try:
            strike_f = float(strike) if strike not in (None, "", "-") else None
        except:
            strike_f = None
        dte = compute_dte(exp, ref) if exp else r.get("dte")
        dte = dte if isinstance(dte, int) else None
        metrics = build_metrics(r)
        metrics["dte"] = dte
        score = score_long_call(metrics, dte)
        flag = color_flag(score, green=1.5, yellow=1.1)
        items.append(
            {
                "strategy": "long_call",
                "symbol": symbol,
                "exp": exp,
                "strike": strike_f,
                "dte": dte,
                "score": round(float(score), 3),
                "flag": flag,
                "generated_at": generated_at,
                "source_file": str(DATA),
                "metrics": metrics,
                "card": card_text("long_call", symbol, exp, strike_f, dte, metrics),
            }
        )
    out = {"count": len(items), "generated_at": generated_at, "items": items}
    write_json(OUT_JSON, out)
    write_yaml(OUT_YAML, out)
    print(f"[saved] {OUT_JSON} ({len(items)} items)")
    print(f"[saved] {OUT_YAML} ({len(items)} items)")


if __name__ == "__main__":
    main()
