#!/usr/bin/env python3
# market_loader.py — Load Barchart market indices CSV, derive session date, compute regime,
# and emit canonical state (JSON/YAML) for downstream strategy engines.

import sys, os, re, json, argparse, datetime as dt
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

# No heavy deps; use stdlib CSV parsing that tolerates the Barchart header format
import csv

# Try optional yaml if present; otherwise we still produce JSON (canonical)
try:
    import yaml  # type: ignore
except Exception:
    yaml = None

# ---------------------------
# Utilities
# ---------------------------

def to_float(x: str) -> Optional[float]:
    if x is None:
        return None
    s = x.strip().replace(',', '')
    # Accept forms like "+1.65%", "-13.92%", "14.29"
    if s.endswith('%'):
        s = s[:-1]
    try:
        return float(s)
    except Exception:
        return None

def parse_pct(x: str) -> Optional[float]:
    """Return percent as decimal, e.g. '+1.65%' -> 0.0165."""
    v = to_float(x)
    if v is None:
        return None
    return v / 100.0

def guess_session_date_from_name(p: Path) -> Optional[str]:
    # Accept names like: market-indices-08-22-2025.csv (or with (1))
    m = re.search(r'(\d{2})-(\d{2})-(\d{4})', p.name)
    if not m:
        return None
    mm, dd, yyyy = m.groups()
    try:
        d = dt.date(int(yyyy), int(mm), int(dd))
        return d.isoformat()
    except Exception:
        return None

def load_calendar_context(calendar_path: Path) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not calendar_path.exists():
        return out
    try:
        if yaml is None:
            return out
        data = yaml.safe_load(calendar_path.read_text(encoding='utf-8')) or {}
        # We don’t enforce any schema here; just pass-through for future use.
        return data
    except Exception:
        return out

def find_input_csv(explicit: Optional[Path]) -> Path:
    if explicit:
        if not explicit.exists():
            sys.exit(f"[ERROR] CSV not found: {explicit}")
        return explicit
    # Fallback search: prefer CWD, then ./data, then /mnt/data
    candidates = []
    for base in [Path.cwd(), Path.cwd() / "data", Path("/mnt/data")]:
        if base.exists():
            for pat in ["market-indices-*.csv", "market*indices*.csv", "*.csv"]:
                candidates.extend(base.glob(pat))
    if not candidates:
        sys.exit("[ERROR] No CSV file found. Pass --csv PATH.")
    # Choose most recent by mtime
    candidates = sorted({c.resolve() for c in candidates if c.is_file()},
                        key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]

def ensure_outdirs() -> Tuple[Path, Path]:
    out_dir = Path("outputs")
    out_dir.mkdir(parents=True, exist_ok=True)
    snap_dir = out_dir / "snapshots"
    snap_dir.mkdir(parents=True, exist_ok=True)
    return out_dir, snap_dir

# ---------------------------
# CSV Loader (Barchart format)
# ---------------------------

def load_indices(csv_path: Path) -> Dict[str, Dict[str, Any]]:
    """
    Returns dict keyed by Symbol (e.g., '$SPX', '$VIX')
    with numeric fields: last, change, pct_chg, open, high, low.
    """
    result: Dict[str, Dict[str, Any]] = {}
    with csv_path.open(newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        # Expected columns from your file:
        # ['Symbol','Name','Last','Change','%Chg','Open','High','Low','Time']
        for row in reader:
            sym = (row.get('Symbol') or '').strip()
            if not sym:
                continue
            result[sym] = {
                "name": (row.get('Name') or '').strip(),
                "last": to_float(row.get('Last') or ''),
                "change": to_float(row.get('Change') or ''),
                "pct_chg": parse_pct(row.get('%Chg') or ''),
                "open": to_float(row.get('Open') or ''),
                "high": to_float(row.get('High') or ''),
                "low": to_float(row.get('Low') or ''),
                "time": (row.get('Time') or '').strip()
            }
    if not result:
        sys.exit(f"[ERROR] CSV parsed but no rows found: {csv_path}")
    return result

# ---------------------------
# Regime Logic (deterministic, simple)
# ---------------------------

def classify_vol_regime(vix_last: Optional[float]) -> str:
    # Simple bands; adjust later if desired
    if vix_last is None:
        return "unknown"
    if vix_last < 15: return "low"
    if vix_last < 22: return "medium"
    return "high"

def classify_trend_bias(spx_pct: Optional[float],
                        mid_pct: Optional[float],
                        small_pct: Optional[float]) -> str:
    """
    Uses SPX day % and relative strength from Mid/Small vs SPX.
    """
    if spx_pct is None:
        return "unknown"
    # Relative strength
    mid_out = (mid_pct is not None and spx_pct is not None and mid_pct > spx_pct)
    sml_out = (small_pct is not None and spx_pct is not None and small_pct > spx_pct)

    if spx_pct >= 0.005:  # >= +0.5%
        if mid_out and sml_out:
            return "bullish (broad)"
        return "bullish"
    if spx_pct <= -0.005:  # <= -0.5%
        return "bearish"
    return "neutral"

def classify_overall(spx_pct: Optional[float],
                     vix_chg: Optional[float],
                     trend_bias: str,
                     vol_regime: str) -> str:
    """
    Combine into an overall market regime.
    """
    if spx_pct is None:
        return "unknown"

    # Heuristics:
    # - Bullish if SPX green and VIX falling; bearish if SPX red and VIX rising; else neutral.
    vix_down = (vix_chg is not None and vix_chg < 0)
    vix_up   = (vix_chg is not None and vix_chg > 0)

    if spx_pct > 0 and vix_down:
        if "bullish" in trend_bias:
            return "bullish"
        return "modestly_bullish"

    if spx_pct < 0 and vix_up:
        if trend_bias == "bearish":
            return "bearish"
        return "modestly_bearish"

    # Vol overlay: high vol + neutral trend → defensive neutral
    if vol_regime == "high" and trend_bias == "neutral":
        return "neutral_defensive"

    return "neutral"

# ---------------------------
# Pretty printing
# ---------------------------

def fmt_pct(x: Optional[float]) -> str:
    if x is None: return "N/A"
    return f"{x*100:.2f}%"

def fmt_num(x: Optional[float]) -> str:
    if x is None: return "N/A"
    return f"{x:.2f}"

def banner(s: str) -> str:
    line = "─" * max(72, len(s) + 4)
    return f"{line}\n{s}\n{line}"

def print_summary(session_date: str,
                  spx: Dict[str, Any],
                  vix: Dict[str, Any],
                  mid: Optional[Dict[str, Any]],
                  small: Optional[Dict[str, Any]],
                  vol_regime: str,
                  trend_bias: str,
                  overall: str) -> None:
    print(banner(f"MARKET REGIME — {session_date}"))
    print(f"SPX:  Last {fmt_num(spx.get('last'))}  Δ {fmt_num(spx.get('change'))}  {fmt_pct(spx.get('pct_chg'))}")
    print(f"VIX:  Last {fmt_num(vix.get('last'))}  Δ {fmt_num(vix.get('change'))}  {fmt_pct(vix.get('pct_chg'))}  → Vol: {vol_regime}")
    if mid:
        print(f"MID:  Last {fmt_num(mid.get('last'))}  {fmt_pct(mid.get('pct_chg'))}")
    if small:
        print(f"SML:  Last {fmt_num(small.get('last'))} {fmt_pct(small.get('pct_chg'))}")
    print("")
    print(f"Trend Bias: {trend_bias}")
    print(f"Overall Regime: {overall}")
    print("")

# ---------------------------
# Main
# ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Market indices loader → regime classifier.")
    ap.add_argument("--csv", type=str, default=None,
                    help="Path to Barchart indices CSV. If omitted, picks most recent in CWD/data/ /mnt/data.")
    ap.add_argument("--calendar", type=str, default="calendar.yaml",
                    help="Optional calendar context file (YAML). Non-blocking.")
    ap.add_argument("--date", type=str, default=None,
                    help="Override session date (YYYY-MM-DD). Otherwise derived from filename if possible; else today.")
    ap.add_argument("--outdir", type=str, default="outputs",
                    help="Directory for canonical outputs.")
    args = ap.parse_args()

    csv_path = find_input_csv(Path(args.csv) if args.csv else None)
    indices = load_indices(csv_path)

    # Map symbols of interest (present in your file):
    spx = indices.get("$SPX") or indices.get("^SPX") or indices.get("SPX")
    vix = indices.get("$VIX") or indices.get("^VIX") or indices.get("VIX")
    mid = indices.get("$IDX")  # S&P Midcap 400
    sml = indices.get("$IQY")  # S&P Smallcap 600

    if not spx:
        sys.exit("[ERROR] $SPX row not found in CSV.")
    if not vix:
        sys.exit("[ERROR] $VIX row not found in CSV.")

    # Session date
    session_date = args.date or guess_session_date_from_name(csv_path) or dt.date.today().isoformat()

    # Calendar context (optional)
    calendar_ctx = load_calendar_context(Path(args.calendar))

    # Classify
    vol_regime = classify_vol_regime(vix.get("last"))
    trend_bias = classify_trend_bias(spx.get("pct_chg"), mid.get("pct_chg") if mid else None, sml.get("pct_chg") if sml else None)
    overall    = classify_overall(spx.get("pct_chg"), vix.get("pct_chg"), trend_bias, vol_regime)

    # Output state
    state = {
        "session_date": session_date,
        "source_csv": str(csv_path),
        "calendar_context_loaded": bool(calendar_ctx),
        "indices": {
            "SPX": {
                "last": spx.get("last"), "change": spx.get("change"), "pct_chg": spx.get("pct_chg"),
                "open": spx.get("open"), "high": spx.get("high"), "low": spx.get("low"),
            },
            "VIX": {
                "last": vix.get("last"), "change": vix.get("change"), "pct_chg": vix.get("pct_chg"),
                "open": vix.get("open"), "high": vix.get("high"), "low": vix.get("low"),
            },
            "MID": ({
                "last": mid.get("last"), "pct_chg": mid.get("pct_chg"),
                "open": mid.get("open"), "high": mid.get("high"), "low": mid.get("low"),
            } if mid else None),
            "SMALL": ({
                "last": sml.get("last"), "pct_chg": sml.get("pct_chg"),
                "open": sml.get("open"), "high": sml.get("high"), "low": sml.get("low"),
            } if sml else None),
        },
        "classification": {
            "vol_regime": vol_regime,
            "trend_bias": trend_bias,
            "overall_regime": overall,
        },
        "notes": {
            "logic": "Bullish if SPX>0 and VIX<0; Bearish if SPX<0 and VIX>0; else Neutral. Trend bias uses Mid/Small relative to SPX.",
            "units": {"pct": "decimal (0.0165 == +1.65%)"}
        }
    }

    # Screen summary
    print_summary(session_date, spx, vix, mid, sml, vol_regime, trend_bias, overall)

    # Persist
    out_dir, _ = ensure_outdirs()
    json_path = out_dir / "market_state.json"
    yml_path  = out_dir / "market_state.yml"

    json_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    if yaml is not None:
        yml_path.write_text(yaml.safe_dump(state, sort_keys=False), encoding="utf-8")

    print(f"[OK] Wrote {json_path}")
    if yaml is not None:
        print(f"[OK] Wrote {yml_path}")

if __name__ == "__main__":
    main()