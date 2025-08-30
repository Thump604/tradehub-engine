#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_market_state.py
- Parse the latest market-indices CSV (full file, not just VIX)
- Compute indices, sector breadth, vol context, notable themes (SOX, Dollar, Commodities)
- Generate human-readable narrative + structured JSON for the web
- Archive the source CSV if it was in data/incoming

Outputs:
  outputs/market_state.json
"""

from __future__ import annotations
import csv, json, re, shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Project helpers (you already have this file)
from rank_base import CT, to_ct, parse_barchart_footer_timestamp, now_ct_iso

ROOT = Path(__file__).resolve().parents[1]
INCOMING = ROOT / "data" / "incoming"
ARCHIVE_ROOT = ROOT / "data" / "archive"
OUT = ROOT / "outputs" / "market_state.json"
OUT.parent.mkdir(parents=True, exist_ok=True)

# canonical tickers present in your download
IDX_TICKERS = {
    "$SPX": "S&P 500",
    "$DOWI": "Dow Jones Industrial",
    "$IUXX": "Nasdaq 100",
}
SECTOR_PREFIX = "$SR"  # S&P 500 sectors
SECTOR_MAP = {
    "$SRIT": "Information Technology",
    "$SRCD": "Consumer Discretionary",
    "$SRTS": "Communication Services",
    "$SRCS": "Consumer Staples",
    "$SRHC": "Health Care",
    "$SRIN": "Industrials",
    "$SRMA": "Materials",
    "$SREN": "Energy",
    "$SRRE": "Real Estate",
    "$SRUT": "Utilities",
    "$SRFI": "Financials",
}
SPECIALS = {
    "$SOX": "Semiconductors",
    "$VIX": "VIX",
    "$VXN": "VXN",
    "$GNX": "S&P GSCI",
    "$BCI": "DJ Commodity",
    "$DXY": "US Dollar Index",
}

PCT_RE = re.compile(r"([+-]?\d+(?:\.\d+)?)%")


def pct_to_float(s: str | float | int | None) -> Optional[float]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    s = s.strip()
    if not s:
        return None
    m = PCT_RE.fullmatch(s) or PCT_RE.search(s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


def num(s: str | float | int | None) -> Optional[float]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    try:
        return float(s.replace(",", ""))
    except Exception:
        return None


@dataclass
class Row:
    symbol: str
    name: str
    last: Optional[float]
    pct: Optional[float]


def discover_latest_csv() -> Tuple[Optional[Path], Optional[str]]:
    """Prefer newest in incoming; fallback to newest archived.
    Return (path, footer_ct_iso)."""
    candidates: List[Tuple[float, Path]] = []
    for p in INCOMING.glob("market-indices-*.csv"):
        candidates.append((p.stat().st_mtime, p))
    if candidates:
        candidates.sort(reverse=True)
        p = candidates[0][1]
        ts = parse_barchart_footer_timestamp(p)  # CT ISO or None
        return p, ts

    # fallback: newest archived copy
    arch_candidates: List[Tuple[float, Path]] = []
    for d in ARCHIVE_ROOT.glob("*"):
        if not d.is_dir():
            continue
        for p in d.glob("market-indices-*.csv"):
            arch_candidates.append((p.stat().st_mtime, p))
    if arch_candidates:
        arch_candidates.sort(reverse=True)
        p = arch_candidates[0][1]
        ts = parse_barchart_footer_timestamp(p)
        return p, ts

    return None, None


def archive_if_incoming(src: Path) -> Path:
    """Move incoming file to a dated archive folder; return archived path."""
    if not src.exists():
        return src
    if str(src).startswith(str(ARCHIVE_ROOT)):  # already archived
        return src
    stamp = to_ct().strftime("%Y%m%d-%H%M%S")
    dest_dir = ARCHIVE_ROOT / stamp
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.move(str(src), str(dest))
    return dest


def load_rows(path: Path) -> List[Row]:
    rows: List[Row] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            sym = (r.get("Symbol") or "").strip()
            name = (r.get("Name") or "").strip()
            last = num(r.get("Last"))
            pct = pct_to_float(r.get("%Chg"))
            if not sym or sym.startswith("Downloaded from Barchart"):
                continue
            rows.append(Row(sym, name, last, pct))
    return rows


def label_vol(vix_last: Optional[float]) -> str:
    if vix_last is None:
        return "Unknown"
    if vix_last < 14:
        return "Low"
    if vix_last <= 22:
        return "Moderate"
    return "High"


def majority_sign(values: List[Optional[float]]) -> str:
    ups = sum(1 for v in values if v is not None and v > 0)
    downs = sum(1 for v in values if v is not None and v < 0)
    if ups and downs:
        return "Mixed"
    if ups and not downs:
        return "Up"
    if downs and not ups:
        return "Down"
    return "Mixed"


def compute_regime(spx: Optional[float], sector_changes: Dict[str, float]) -> str:
    if not sector_changes:
        return "Neutral"
    risk_on_basket = [
        "Information Technology",
        "Consumer Discretionary",
        "Communication Services",
        "Financials",
        "Industrials",
    ]
    defensives = ["Utilities", "Consumer Staples", "Health Care", "Real Estate"]
    ro = sum(1 for k in risk_on_basket if sector_changes.get(k, 0) > 0)
    df = sum(1 for k in defensives if sector_changes.get(k, 0) > 0)
    if spx is not None and spx < 0:
        if df - ro >= 2:
            return "Risk-Off"
    if ro - df >= 2:
        return "Risk-On"
    return "Neutral"


def top_n(d: Dict[str, float], n=3) -> List[Tuple[str, float]]:
    return sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]


def bottom_n(d: Dict[str, float], n=3) -> List[Tuple[str, float]]:
    return sorted(d.items(), key=lambda kv: kv[1])[:n]


def build_narrative(
    indices: Dict[str, float],
    sectors: Dict[str, float],
    vix_last: Optional[float],
    vix_pct: Optional[float],
    vxn_last: Optional[float],
    vxn_pct: Optional[float],
    sox_pct: Optional[float],
    dxy_pct: Optional[float],
    gnx_pct: Optional[float],
) -> Tuple[str, str]:
    """Return (headline, summary_text)"""
    spx = indices.get("S&P 500")
    ndx = indices.get("Nasdaq 100")
    dji = indices.get("Dow Jones Industrial")
    trend = majority_sign([spx, ndx, dji])

    # Sector breadth
    ups = [k for k, v in sectors.items() if v > 0]
    downs = [k for k, v in sectors.items() if v < 0]
    g = top_n(sectors, 2)
    l = bottom_n(sectors, 2)

    vol_hint = label_vol(vix_last)
    parts: List[str] = []

    # Headline
    headline_bits = []
    if trend == "Up":
        headline_bits.append("Indices firmer")
    elif trend == "Down":
        headline_bits.append("Indices softer")
    else:
        headline_bits.append("Mixed tape")

    if g:
        headline_bits.append(f"{g[0][0]} leads")
    if vix_pct and vix_pct > 0 and (spx or 0) > 0:
        headline_bits.append("VIX up on green")

    headline = "; ".join(headline_bits)

    # Body
    idx_bits = []
    if spx is not None:
        idx_bits.append(f"SPX {spx:+.2f}%")
    if ndx is not None:
        idx_bits.append(f"NDX {ndx:+.2f}%")
    if dji is not None:
        idx_bits.append(f"DJI {dji:+.2f}%")
    if idx_bits:
        parts.append(", ".join(idx_bits) + ".")

    if ups or downs:
        parts.append(f"Sector breadth: {len(ups)} up / {len(downs)} down.")
    if g:
        parts.append("Leaders: " + ", ".join(f"{k} ({v:+.2f}%)" for k, v in g) + ".")
    if l:
        parts.append("Laggards: " + ", ".join(f"{k} ({v:+.2f}%)" for k, v in l) + ".")

    if sox_pct is not None:
        parts.append(f"Semis (SOX) {sox_pct:+.2f}% — a proxy for AI/mega-cap risk.")
    if vix_last is not None:
        more = f" (VIX {vix_last:.2f}"
        if vix_pct is not None:
            more += f", {vix_pct:+.2f}%"
        more += ")"
        parts.append(f"Vol regime: {vol_hint}{more}.")
    if vxn_last is not None:
        parts.append(
            f"Nasdaq vol (VXN {vxn_last:.2f}"
            + (f", {vxn_pct:+.2f}%" if vxn_pct is not None else "")
            + ")."
        )
    if dxy_pct is not None:
        parts.append(f"US Dollar (DXY) {dxy_pct:+.2f}% — watch for macro crosswinds.")
    if gnx_pct is not None:
        parts.append(f"Commodities (GSCI) {gnx_pct:+.2f}%.")

    # Tension: VIX up while indices up
    if (spx or 0) > 0 and (vix_pct or 0) > 0:
        parts.append(
            "Unusual: volatility up while indices green → hedging demand into catalysts."
        )

    summary = " ".join(parts)
    return headline, summary


def main():
    path, footer_ct_iso = discover_latest_csv()
    if path is None or not path.exists():
        data = {
            "generated_at": now_ct_iso(),
            "headline": "No market file",
            "regime": "Neutral",
            "trend": "Mixed",
            "vol": "Unknown",
            "vol_detail": "No data",
            "indices": {},
            "sectors": {},
            "specials": {},
            "summary_text": "No market-indices file found.",
            "source_file": None,
        }
        OUT.write_text(json.dumps(data, indent=2))
        print(f"[market_state] wrote {OUT} (no data)")
        return

    # Move to archive if from incoming
    archived = archive_if_incoming(path)

    rows = load_rows(archived)
    idx_changes: Dict[str, float] = {}
    sectors: Dict[str, float] = {}
    specials: Dict[str, Dict[str, Optional[float]]] = {}

    for r in rows:
        if r.symbol in IDX_TICKERS and r.pct is not None:
            idx_changes[IDX_TICKERS[r.symbol]] = r.pct
        elif (
            r.symbol.startswith(SECTOR_PREFIX)
            and r.symbol in SECTOR_MAP
            and r.pct is not None
        ):
            sectors[SECTOR_MAP[r.symbol]] = r.pct
        elif r.symbol in SPECIALS:
            specials[SPECIALS[r.symbol]] = {"last": r.last, "pct": r.pct}

    spx = idx_changes.get("S&P 500")
    ndx = idx_changes.get("Nasdaq 100")
    dji = idx_changes.get("Dow Jones Industrial")

    trend = majority_sign([spx, ndx, dji])
    vix_last = specials.get("VIX", {}).get("last")
    vix_pct = specials.get("VIX", {}).get("pct")
    vxn_last = specials.get("VXN", {}).get("last")
    vxn_pct = specials.get("VXN", {}).get("pct")
    sox_pct = specials.get("Semiconductors", {}).get("pct")
    dxy_pct = specials.get("US Dollar Index", {}).get("pct")
    gnx_pct = specials.get("S&P GSCI", {}).get("pct")

    vol_label = label_vol(vix_last)
    regime = compute_regime(spx, sectors)
    headline, summary = build_narrative(
        indices=idx_changes,
        sectors=sectors,
        vix_last=vix_last,
        vix_pct=vix_pct,
        vxn_last=vxn_last,
        vxn_pct=vxn_pct,
        sox_pct=sox_pct,
        dxy_pct=dxy_pct,
        gnx_pct=gnx_pct,
    )

    generated_at = footer_ct_iso or now_ct_iso()
    vol_detail = f"VIX {vix_last:.2f}" if vix_last is not None else "Unknown"

    data = {
        "generated_at": generated_at,
        "headline": headline,
        "regime": regime,
        "trend": trend,
        "vol": vol_label,
        "vol_detail": vol_detail,
        "indices": idx_changes,  # {"S&P 500": +0.24, ...}
        "sectors": sectors,  # {"Energy": +1.15, ...}
        "specials": specials,  # {"VIX": {"last":14.85,"pct":+1.57}, ...}
        "summary_text": summary,  # readable narrative for the web tile
        "source_file": str(archived),
    }
    OUT.write_text(json.dumps(data, indent=2))
    print(f"[market_state] wrote {OUT}")


if __name__ == "__main__":
    main()
