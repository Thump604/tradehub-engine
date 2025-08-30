#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified feed writer (V2 schema) for TradeHub — Web.

Reads:
  - outputs/{strategy}_suggestions.json  (ranker outputs; minimal shape)
  - data/*-latest.csv                    (raw Barchart downloads; rich columns)
  - outputs/market_state.json            (headline + breadth + vol)

Writes (web-consumed):
  - outputs/web_feed/{strategy}_suggestions.json  (V2 cards, enriched)
  - outputs/web_feed/market_state.json            (copy of market_state.json)
  - outputs/market.json                           (legacy small banner)
"""
from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

ENGINE = Path(__file__).resolve().parents[1]
DATA = ENGINE / "data"
OUT = ENGINE / "outputs"
WEB_FEED = OUT / "web_feed"
WEB_FEED.mkdir(parents=True, exist_ok=True)

STRATEGIES = [
    "covered_call",
    "csp",
    "pmcc",
    "vertical",
    "diagonal",
    "iron_condor",
    "long_call",
]

CSV_PATHS = {
    "covered_call": DATA / "covered_call-latest.csv",
    "csp": DATA / "csp-latest.csv",
    "pmcc": DATA / "short_call-latest.csv",  # short leg ideas
    "vertical": DATA / "vertical_bull_call-latest.csv",  # (best-effort: bull call feed)
    "diagonal": DATA / "long_call_diagonal-latest.csv",
    "iron_condor": DATA / "iron_condor-latest.csv",
    "long_call": DATA / "long_call-latest.csv",
}

RANKER_JSON = {s: OUT / f"{s}_suggestions.json" for s in STRATEGIES}

CT = timezone(
    timedelta(hours=-5)
)  # Central Time (no DST awareness needed for file stamps)


def now_ct_iso() -> str:
    return datetime.now(tz=CT).replace(microsecond=0).isoformat()


# -----------------------------
# Parsing helpers
# -----------------------------
NON_ALNUM = re.compile(r"[^A-Za-z0-9]+")


def norm(s: str) -> str:
    return NON_ALNUM.sub("_", s.strip()).strip("_").lower()


def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    # remove commas, percent, tildes
    s = s.replace(",", "")
    s = s.replace("%", "")
    s = s.replace("~", "")
    # handle "N/A" etc
    if s in {"NA", "N/A", "nan", "-", "—"}:
        return None
    try:
        return float(s)
    except Exception:
        # handle cases like '09/19/25 (23)' — not a float
        return None


def to_int(x: Any) -> Optional[int]:
    f = to_float(x)
    return int(round(f)) if f is not None else None


def parse_moneyness(val: Any) -> Optional[float]:
    # e.g. "-0.21%" → -0.21
    f = to_float(val)
    return f if f is not None else None


def parse_date_yy(x: str) -> Optional[str]:
    """Accepts either '09/19/25 (23)' or '2025-09-19' or '09/19/2025'. Returns YYYY-MM-DD."""
    s = str(x).strip()
    if not s:
        return None
    # strip trailing "(23)"
    s = s.split()[0]
    # 2025-09-19
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    # 09/19/25 or 09/19/2025
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", s)
    if m:
        mm, dd, yy = m.groups()
        mm = int(mm)
        dd = int(dd)
        yy = int(yy)
        if yy < 100:
            yy += 2000
        return f"{yy:04d}-{mm:02d}-{dd:02d}"
    return None


def dte_from_exp(exp_yyyy_mm_dd: str, ref: Optional[datetime] = None) -> Optional[int]:
    try:
        if ref is None:
            ref = datetime.now(tz=CT)
        exp = datetime.strptime(exp_yyyy_mm_dd, "%Y-%m-%d").replace(
            tzinfo=CT, hour=15, minute=0, second=0
        )
        return max(0, (exp - ref).days)
    except Exception:
        return None


def spread_quality(
    bid: Optional[float], ask: Optional[float]
) -> Tuple[str, Optional[float]]:
    if bid is None or ask is None or ask <= 0:
        return ("Unknown", None)
    width = ask - bid
    mid = (ask + bid) / 2.0
    pct = (width / mid * 100.0) if mid > 0 else None
    if pct is None:
        return ("Unknown", None)
    if pct <= 2.0:
        return ("Great", pct)
    if pct <= 5.0:
        return ("Good", pct)
    if pct <= 10.0:
        return ("OK", pct)
    return ("Wide", pct)


def liquidity_label(oi: Optional[int], vol: Optional[int], spread_q: str) -> str:
    oi = oi or 0
    vol = vol or 0
    level = 0
    if oi >= 5000 or vol >= 2000:
        level += 2
    elif oi >= 1000 or vol >= 500:
        level += 1
    if spread_q in {"Great", "Good"}:
        level += 1
    if level >= 3:
        return "Excellent"
    if level == 2:
        return "Good"
    if level == 1:
        return "Fair"
    return "Thin"


def risk_label_from_put(delta: Optional[float], be_pct: Optional[float]) -> str:
    # delta is negative for puts from Barchart
    d = abs(delta) if isinstance(delta, (int, float)) else None
    cushion = -(be_pct or 0.0)  # be_pct negative = cushion below spot
    if d is not None and cushion is not None:
        if d <= 0.25 and cushion >= 5:
            return "Low assignment risk"
        if d <= 0.35 and cushion >= 3:
            return "Balanced"
        if d <= 0.45 and cushion >= 1:
            return "Elevated"
        return "High assignment risk"
    return "Balanced"


def pop_from_itm(itm_prob: Optional[float], strategy: str) -> Optional[float]:
    # POP proxy ≈ 100 - ITM Prob for short premium strategies.
    if itm_prob is None:
        return None
    if strategy in {"csp", "covered_call"}:
        return max(0.0, min(100.0, 100.0 - itm_prob))
    return None


# -----------------------------
# CSV ingestion → index
# -----------------------------
def load_csv_index(
    csv_path: Path, strategy: str
) -> Dict[Tuple[str, str, float, str], Dict[str, Any]]:
    """
    Build an index keyed by (symbol, exp_yyyy_mm_dd, strike, type).
    """
    ix: Dict[Tuple[str, str, float, str], Dict[str, Any]] = {}
    if not csv_path.exists():
        return ix

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        headers = [norm(h) for h in rdr.fieldnames or []]
        rows = []
        for raw in rdr:
            row = {norm(k): v for k, v in raw.items()}
            rows.append(row)

    # Try to detect columns
    # Expected keys (normalized):
    # symbol, exp_date, strike, type, moneyness, bid, ask, mid,
    # volume (option), open_int, iv_rank, delta, theta, be_ask, pct_be_ask, itm_prob,
    # price (underlying last) or last
    for row in rows:
        sym = (row.get("symbol") or "").strip().upper()
        typ = (row.get("type") or "").strip().upper()
        exp = parse_date_yy(row.get("exp_date") or row.get("exp") or "")
        strike = to_float(row.get("strike"))
        if not (sym and typ and exp and isinstance(strike, float)):
            continue

        record = {
            "symbol": sym,
            "type": typ,  # "CALL" or "PUT"
            "exp": exp,
            "strike": strike,
            "moneyness": parse_moneyness(row.get("moneyness")),
            "bid": to_float(row.get("bid")),
            "ask": to_float(row.get("ask")),
            "mid": to_float(row.get("mid")),
            "volume": to_int(row.get("volume")),
            "open_interest": to_int(row.get("open_int") or row.get("open_interest")),
            "iv_rank": to_float(row.get("iv_rank")),
            "delta": to_float(row.get("delta")),
            "theta": to_float(row.get("theta")),
            "be": to_float(row.get("be_ask") or row.get("be") or row.get("be__ask")),
            "be_pct": to_float(
                row.get("pct_be_ask") or row.get("be_pct") or row.get("percent_be_ask")
            ),
            "itm_prob": to_float(row.get("itm_prob")),
            "take_profit_price": to_float(
                row.get("tp_ask") or row.get("take_profit_price")
            ),
            "take_profit_pct": to_float(
                row.get("percent_tp_ask_a") or row.get("take_profit_pct")
            ),
            "static_ann_return": to_float(row.get("static_ann_rtn")),
            "ann_yield_to_strike_pct": to_float(row.get("ann_yield_to_strike_")),
            "underlying_last": to_float(row.get("price") or row.get("last")),
        }
        ix[(sym, exp, strike, typ)] = record

    return ix


# -----------------------------
# Enrichment
# -----------------------------
def enrich_card(
    base: Dict[str, Any], csv_ix: Dict[Tuple[str, str, float, str], Dict[str, Any]]
) -> Dict[str, Any]:
    """
    base: {"strategy","symbol","exp","strike","score","flag",...}
    returns a full V2 card dict.
    """
    strategy = base.get("strategy")
    symbol = base.get("symbol")
    exp = base.get("exp")
    strike = base.get("strike")
    opt_type = base.get("option_type")  # may be missing in legacy rankers

    # infer type by strategy when missing
    if not opt_type:
        if strategy in {"csp"}:
            opt_type = "PUT"
        elif strategy in {"covered_call", "long_call"}:
            opt_type = "CALL"
        else:
            opt_type = "CALL"  # default

    # Use CSV row (if available) to enrich
    key_variants = []
    try:
        strike_f = float(strike)
        key_variants.append((symbol, exp, strike_f, opt_type))
        # Sometimes strike header rounding differs slightly; try 2 decimals:
        key_variants.append((symbol, exp, float(f"{strike_f:.2f}"), opt_type))
    except Exception:
        strike_f = None

    row = None
    for k in key_variants:
        if k in csv_ix:
            row = csv_ix[k]
            break

    dte = dte_from_exp(exp)

    # Spread and liquidity
    bid = row.get("bid") if row else None
    ask = row.get("ask") if row else None
    spread_q, spread_pct = spread_quality(bid, ask)
    liq = liquidity_label(
        row.get("open_interest") if row else None,
        row.get("volume") if row else None,
        spread_q,
    )

    # POP proxy (short premium strats)
    pop_proxy = pop_from_itm(row.get("itm_prob") if row else None, strategy=strategy)

    # Risk label
    rlabel = (
        risk_label_from_put(
            row.get("delta") if row else None, row.get("be_pct") if row else None
        )
        if strategy == "csp"
        else None
    )

    # Assessment & rationale (lightweight, data-driven)
    assessment = None
    rationale: List[str] = []
    if strategy == "csp":
        assessment = (
            "Premium harvest with cushion"
            if rlabel in {"Low assignment risk", "Balanced"}
            else "High-yield, watch assignment risk"
        )
        if row:
            if row.get("iv_rank") is not None:
                rationale.append(f"IVR {row['iv_rank']:.0f} supports premium.")
            if row.get("open_interest") or row.get("volume"):
                rationale.append(
                    f"Liquidity {liq} (OI {row.get('open_interest') or 0}, Vol {row.get('volume') or 0})."
                )
            if row.get("delta") is not None:
                rationale.append(f"Δ {row['delta']:.2f} balances yield vs risk.")
            if row.get("be_pct") is not None:
                rationale.append(f"Cushion {row['be_pct']:.2f}% to BE.")
    elif strategy == "long_call":
        assessment = "LEAP exposure with defined risk"
        if row:
            if row.get("delta") is not None:
                rationale.append(f"Target Δ {row['delta']:.2f}.")
            if row.get("iv_rank") is not None:
                rationale.append(
                    f"IVR {row['iv_rank']:.0f} (prefer lower for long premium)."
                )
            if row.get("open_interest"):
                rationale.append(f"OI {row['open_interest']} indicates tradability.")
    else:
        assessment = base.get("assessment") or "Opportunity"
        if row and row.get("open_interest"):
            rationale.append(
                f"OI {row['open_interest']} • spread {spread_q}{f' ({spread_pct:.1f}%)' if spread_pct is not None else ''}."
            )

    # Ticket stub (simple defaults; front-end can edit)
    stub: Dict[str, Any] = {}
    if strategy == "csp":
        stub = {
            "action": "SELL",
            "legs": [{"type": "PUT", "exp": exp, "strike": strike, "qty": 1}],
            "tif": "DAY",
            "price": "MID",
        }
    elif strategy == "covered_call":
        stub = {
            "action": "SELL",
            "legs": [{"type": "CALL", "exp": exp, "strike": strike, "qty": 1}],
            "tif": "DAY",
            "price": "MID",
        }
    elif strategy == "long_call":
        stub = {
            "action": "BUY",
            "legs": [{"type": "CALL", "exp": exp, "strike": strike, "qty": 1}],
            "tif": "DAY",
            "price": "MID",
        }

    card = {
        "id": base.get("id") or f"{strategy}:{symbol}:{exp}:{strike}",
        "generated_at": now_ct_iso(),
        "strategy": strategy,
        "symbol": symbol,
        "underlying_last": (row.get("underlying_last") if row else None),
        "exp": exp,
        "dte": dte,
        "option_type": opt_type,
        "strike": strike,
        "moneyness": row.get("moneyness") if row else None,
        "bid": bid,
        "ask": ask,
        "mid": (row.get("mid") if row else None),
        "volume": (row.get("volume") if row else None),
        "open_interest": (row.get("open_interest") if row else None),
        "iv_rank": (row.get("iv_rank") if row else None),
        "delta": (row.get("delta") if row else None),
        "theta": (row.get("theta") if row else None),
        "be": (row.get("be") if row else None),
        "be_pct": (row.get("be_pct") if row else None),
        "itm_prob": (row.get("itm_prob") if row else None),
        "take_profit_price": (row.get("take_profit_price") if row else None),
        "take_profit_pct": (row.get("take_profit_pct") if row else None),
        "static_ann_return": (row.get("static_ann_return") if row else None),
        "ann_yield_to_strike_pct": (
            row.get("ann_yield_to_strike_pct") if row else None
        ),
        "pop_proxy": pop_proxy,
        "liquidity": liq,
        "spread_quality": spread_q,
        "spread_pct": spread_pct,
        "risk_label": rlabel,
        "score": base.get("score"),
        "flag": base.get("flag"),
        "assessment": assessment,
        "rationale": rationale[:3],
        "ticket_stub": stub,
        "source": base.get("source") or {},
    }
    return card


# -----------------------------
# Load ranker outputs (legacy)
# -----------------------------
def load_ranker_items(strategy: str) -> List[Dict[str, Any]]:
    p = RANKER_JSON[strategy]
    if not p.exists():
        return []
    try:
        d = json.loads(p.read_text())
    except Exception:
        return []
    items = d.get("items") or d.get("data") or []
    # normalize legacy minimal rows -> base
    normalized: List[Dict[str, Any]] = []
    for it in items:
        symbol = it.get("symbol")
        exp = it.get("exp")
        strike = it.get("strike")
        base = {
            "id": it.get("id"),
            "strategy": it.get("strategy") or strategy,
            "symbol": symbol,
            "exp": exp,
            "strike": strike,
            "option_type": it.get("option_type"),
            "score": it.get("score"),
            "flag": it.get("flag"),
            "source": it.get("source") or {},
        }
        # Skip footer garbage rows that slipped through some CSVs
        if (symbol or "").upper().startswith("DOWNLOADED FROM BARCHART"):
            continue
        normalized.append(base)
    return normalized


# -----------------------------
# Market banner
# -----------------------------
def write_market_banner():
    src = OUT / "market_state.json"
    if src.exists():
        # Copy to web_feed
        WEB_FEED.joinpath("market_state.json").write_text(src.read_text())
        try:
            ms = json.loads(src.read_text())
            banner = {
                "generated_at": ms.get("generated_at"),
                "regime": ms.get("regime"),
                "trend": ms.get("trend"),
                "vol": ms.get("vol"),
                "vol_detail": ms.get("vol_detail"),
                "headline": ms.get("headline"),
                "summary_text": ms.get("summary_text"),
            }
            (OUT / "market.json").write_text(json.dumps(banner, indent=2))
            (WEB_FEED / "market.json").write_text(json.dumps(banner, indent=2))
        except Exception:
            pass


# -----------------------------
# Main
# -----------------------------
def main():
    total = 0
    for strat in STRATEGIES:
        items = load_ranker_items(strat)
        csv_ix = load_csv_index(CSV_PATHS.get(strat, Path("_missing.csv")), strat)
        v2_cards = [enrich_card(base=it, csv_ix=csv_ix) for it in items]
        outp = WEB_FEED / f"{strat}_suggestions.json"
        outp.write_text(
            json.dumps({"count": len(v2_cards), "items": v2_cards}, indent=2)
        )
        print(f"[feed] Wrote {outp} (count={len(v2_cards)} strategy={strat})")
        total += len(v2_cards)
    write_market_banner()
    print(f"[feed] Done. Files written: {len(STRATEGIES)+1} | total rows: {total}")


if __name__ == "__main__":
    main()
