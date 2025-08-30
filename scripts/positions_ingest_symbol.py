#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
positions_ingest_symbol.py

Ingest a single symbol's position paste (one symbol, possibly multiple legs)
from STDIN and persist a time-series snapshot suitable for analytics.

Usage (example):
  pbpaste | python -m scripts.positions_ingest_symbol --strategy pmcc
  pbpaste | python -m scripts.positions_ingest_symbol --strategy covered_call
  pbpaste | python -m scripts.positions_ingest_symbol --strategy short_call
  pbpaste | python -m scripts.positions_ingest_symbol --strategy csp
  pbpaste | python -m scripts.positions_ingest_symbol --strategy vertical_bull_put
  pbpaste | python -m scripts.positions_ingest_symbol --strategy vertical_bull_call
  pbpaste | python -m scripts.positions_ingest_symbol --strategy iron_condor
  pbpaste | python -m scripts.positions_ingest_symbol --strategy diagonal

Notes:
- This script assumes the broker layout you pasted: a SYMBOL header,
  then one or more 3-line blocks per leg:
    1) "SYM MM/DD/YYYY STRIKE C|P"
    2) (a human description line we ignore)
    3) a tab-separated metrics row beginning with a dollar amount (e.g. "$1.285")
- We record every ingest (no de-dupe) so you can trend Greeks, IV, P/L, etc.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any


# ---------- IO & paths ----------

ENGINE_ROOT = Path(__file__).resolve().parents[1]  # .../engine
OUT_DIR = ENGINE_ROOT / "outputs" / "positions"
LEDGER = OUT_DIR / "ledger.jsonl"
BY_SYMBOL_DIR = OUT_DIR / "by_symbol"
OUT_DIR.mkdir(parents=True, exist_ok=True)
BY_SYMBOL_DIR.mkdir(parents=True, exist_ok=True)


# ---------- helpers: parsing ----------

MONEY_RE = re.compile(r"^\$?\s*([+-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?|[+-]?\d+(?:\.\d+)?)$")
PCT_RE = re.compile(r"^([+-]?\d+(?:\.\d+)?)\s*%$")
SYM_LINE_RE = re.compile(
    r"^(?P<sym>[A-Z][A-Z0-9\.]{0,6})\s+(?P<exp>\d{2}/\d{2}/\d{4})\s+(?P<strike>\d+(?:\.\d+)?)\s+(?P<cp>[CP])\s*$"
)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_money(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    s = s.strip()
    if s in {"-", "N/A", "NA", ""}:
        return None
    s = s.replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        m = MONEY_RE.match(s)
        if m:
            return float(m.group(1).replace(",", ""))
    return None


def parse_pct(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    s = s.strip()
    if s in {"-", "N/A", "NA", ""}:
        return None
    m = PCT_RE.match(s)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    # Allow raw like "+1.23" without %
    try:
        return float(s)
    except ValueError:
        return None


def parse_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    s = s.strip().replace(",", "")
    if s in {"-", "N/A", "NA", ""}:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def parse_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    s = s.strip().replace(",", "")
    if s in {"-", "N/A", "NA", ""}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ---------- data structures ----------


@dataclass
class PositionLeg:
    symbol: str
    type: str  # "CALL" | "PUT"
    side: str  # "LONG" | "SHORT"
    exp: str  # ISO-ish date string YYYY-MM-DD or MM/DD/YYYY from broker
    strike: float
    itm: Optional[str]
    dte: Optional[int]
    delta: Optional[float]
    open_int: Optional[int]
    qty: int
    mark: Optional[float]

    # Intraday / valuation fields
    day_change_usd: Optional[float]
    day_change_pct: Optional[float]
    day_high: Optional[float]
    day_low: Optional[float]
    price_change_usd: Optional[float]
    price_change_pct: Optional[float]
    market_value: Optional[float]
    cost_basis: Optional[float]
    cost_share: Optional[float]
    gain_loss_usd: Optional[float]
    gain_loss_pct: Optional[float]


@dataclass
class Snapshot:
    created_at: str
    symbol: str
    strategy: str
    legs: List[PositionLeg]
    paste_hash: str
    raw_lines: List[str]


# ---------- core parsing ----------


def detect_header_symbol(lines: List[str]) -> Optional[str]:
    """
    Try to detect the symbol header section like:

      AAPL
      APPLE INC
      $230.49  ...  (spot row)

    We accept the first ALLCAPS [A-Z0-9.] token line (1-7 chars) that isn't a metrics row.
    """
    for line in lines:
        s = line.strip()
        if not s:
            continue
        if "\t" in s:
            # metrics rows are tabbed; skip
            continue
        # ignore lines that look like leg lines
        if SYM_LINE_RE.match(s):
            continue
        if re.fullmatch(r"[A-Z][A-Z0-9\.]{0,6}", s):
            return s
    return None


def parse_paste(text: str) -> Dict[str, Any]:
    """
    Parse the broker paste for one symbol with 0+ legs.
    Returns {"symbol": "...", "legs": [PositionLeg, ...]}
    """
    lines = [ln.rstrip("\n") for ln in text.splitlines()]
    symbol = detect_header_symbol(lines) or "UNKNOWN"

    legs: List[PositionLeg] = []
    n = len(lines)
    i = 0
    while i < n:
        line = lines[i].strip()
        m = SYM_LINE_RE.match(line)
        if not m:
            i += 1
            continue

        sym2 = m.group("sym")
        exp = m.group("exp")
        strike = float(m.group("strike"))
        cp = m.group("cp")

        # The next significant line after one description line should be a tabbed metrics row.
        # We scan forward up to, say, 4 lines to find it.
        metrics_row = None
        j = i + 1
        scan_limit = min(n, i + 6)
        while j < scan_limit:
            lj = lines[j].strip()
            if "\t" in lj and (
                lj.startswith("$")
                or lj.startswith("-$")
                or lj.startswith("+$")
                or lj.startswith("$")
                or lj.startswith("0")
                or lj.startswith("N/A")
                or lj.startswith("-")
            ):
                metrics_row = lj
                break
            j += 1

        # Default (if metrics row missing), we still record the leg skeleton
        price = None
        day_chg_usd = None
        day_chg_pct = None
        day_hi = None
        day_lo = None
        exp_mat = exp
        strike_txt = f"{strike:.2f}"
        itm = None
        dte = None
        delta = None
        open_int = None
        qty = 0
        px_chg_usd = None
        px_chg_pct = None
        mkt_val = None
        cost_basis = None
        cost_share = None
        gl_usd = None
        gl_pct = None

        if metrics_row:
            toks = metrics_row.split("\t")
            # Broker column order per your sample:
            #  0  Price
            #  1  Day Chng $
            #  2  Day Chng %
            #  3  Day High
            #  4  Day Low
            #  5  Exp/Mat
            #  6  Strike
            #  7  ITM
            #  8  DTE
            #  9  Delta
            # 10  Open Int
            # 11  Qty
            # 12  Price Chng $
            # 13  Price Chng %
            # 14  Mkt Val
            # 15  Cost Basis
            # 16  Cost/Share
            # 17  Gain/Loss $
            # 18  Gain/Loss %
            # (then often extra broker columns we ignore)

            def safe(k: int) -> Optional[str]:
                return toks[k].strip() if k < len(toks) else None

            price = parse_money(safe(0))
            day_chg_usd = parse_money(safe(1))
            day_chg_pct = parse_pct(safe(2))
            day_hi = parse_money(safe(3))
            day_lo = parse_money(safe(4))
            exp_mat = safe(5) or exp
            strike_txt = safe(6) or f"{strike:.2f}"
            itm = safe(7) or None
            dte = parse_int(safe(8))
            delta = parse_float(safe(9))
            open_int = parse_int(safe(10))
            qty = parse_int(safe(11)) or 0
            px_chg_usd = parse_money(safe(12))
            px_chg_pct = parse_pct(safe(13))
            mkt_val = parse_money(safe(14))
            cost_basis = parse_money(safe(15))
            cost_share = parse_money(safe(16))
            gl_usd = parse_money(safe(17))
            gl_pct = parse_pct(safe(18))

            # Move index past the metrics row we consumed
            i = max(i + 1, j + 1)
        else:
            i += 1

        legs.append(
            PositionLeg(
                symbol=sym2,
                type="CALL" if cp == "C" else "PUT",
                side="SHORT" if qty < 0 else "LONG",
                exp=exp_mat if exp_mat else exp,
                strike=parse_float(strike_txt) or strike,
                itm=itm,
                dte=dte,
                delta=delta,
                open_int=open_int,
                qty=qty,
                mark=price,
                day_change_usd=day_chg_usd,
                day_change_pct=day_chg_pct,
                day_high=day_hi,
                day_low=day_lo,
                price_change_usd=px_chg_usd,
                price_change_pct=px_chg_pct,
                market_value=mkt_val,
                cost_basis=cost_basis,
                cost_share=cost_share,
                gain_loss_usd=gl_usd,
                gain_loss_pct=gl_pct,
            )
        )

    return {"symbol": symbol, "legs": legs, "raw_lines": lines}


# ---------- persistence ----------


def write_snapshot(strategy: str, parsed: Dict[str, Any]) -> Snapshot:
    created_at = now_utc_iso()
    symbol = parsed["symbol"]
    legs = [
        PositionLeg(**asdict_leg(PositionLeg(**asdict_leg(PositionLeg(**{})))))
        for _ in []
    ]  # placeholder to satisfy type checkers

    # Rehydrate legs from dicts if needed, but here we already have PositionLeg objects
    legs = parsed["legs"]  # type: ignore

    # Build paste hash for audit (content-addressable)
    raw_text = "\n".join(parsed["raw_lines"])
    paste_hash = hashlib.sha256(raw_text.encode("utf-8")).hexdigest()[:16]

    snap = Snapshot(
        created_at=created_at,
        symbol=symbol,
        strategy=strategy,
        legs=legs,  # type: ignore
        paste_hash=paste_hash,
        raw_lines=parsed["raw_lines"],
    )

    # Ensure dirs
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    BY_SYMBOL_DIR.mkdir(parents=True, exist_ok=True)

    # Append to ledger.jsonl
    with LEDGER.open("a", encoding="utf-8") as f:
        f.write(json.dumps(to_jsonable(snap)) + "\n")

    # Append to per-symbol .jsonl and update latest
    sym_file = BY_SYMBOL_DIR / f"{symbol}.jsonl"
    with sym_file.open("a", encoding="utf-8") as f:
        f.write(json.dumps(to_jsonable(snap)) + "\n")

    sym_latest = BY_SYMBOL_DIR / f"{symbol}-latest.json"
    with sym_latest.open("w", encoding="utf-8") as f:
        json.dump(to_jsonable(snap), f, indent=2)

    return snap


def asdict_leg(leg: PositionLeg) -> Dict[str, Any]:
    return asdict(leg)


def to_jsonable(snap: Snapshot) -> Dict[str, Any]:
    return {
        "created_at": snap.created_at,
        "symbol": snap.symbol,
        "strategy": snap.strategy,
        "paste_hash": snap.paste_hash,
        "legs": [asdict_leg(l) for l in snap.legs],
        "raw_lines": snap.raw_lines,
    }


# ---------- CLI ----------


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Ingest a symbol's broker paste into time-series storage."
    )
    ap.add_argument(
        "--strategy",
        required=True,
        choices=[
            "short_call",
            "covered_call",
            "long_call",
            "pmcc",
            "csp",
            "vertical_bull_put",
            "vertical_bull_call",
            "iron_condor",
            "diagonal",
        ],
        help="Strategy context (stored with the snapshot).",
    )
    args = ap.parse_args()

    # Read entire STDIN (so you can paste then press Ctrl-D)
    buf = sys.stdin.read()
    if not buf.strip():
        print(
            "No input received on STDIN. Paste your broker rows, then press Ctrl-D.",
            file=sys.stderr,
        )
        sys.exit(1)

    parsed = parse_paste(buf)
    snap = write_snapshot(args.strategy, parsed)

    print(f"[positions] {snap.symbol} • {len(snap.legs)} leg(s) saved")
    print(f"  ↳ {LEDGER}")
    print(f"  ↳ {BY_SYMBOL_DIR / (snap.symbol + '.jsonl')}")
    print(f"  ↳ {BY_SYMBOL_DIR / (snap.symbol + '-latest.json')}")


if __name__ == "__main__":
    main()
