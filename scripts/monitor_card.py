#!/usr/bin/env python3
# scripts/monitor_card.py
#
# TradeHub — Monitor Cards (by strategy)
# - Paste 1..N positions for a single strategy; we print a trade card per symbol
# - Robust side detection: accepts -1, +1, and bare '1' anywhere on the leg line
# - Vertical "self-heal": if a 2-leg same-exp same-type vertical is L/L or S/S,
#   fix by convention (calls: lower=strike long, higher=strike short; puts reverse)
# - Loads symbol-matched suggestions from outputs/<strategy>_suggestions.json
# - Prints a portfolio summary at the end
#
# Usage:
#   python -m scripts.monitor_card --strategy pmcc
#   python -m scripts.monitor_card --strategy csp
#   python -m scripts.monitor_card --strategy vertical
#   ... (covered_call, diagonal, iron_condor, long_call)
#
# Requirements: pyyaml (optional but preferred)

from __future__ import annotations
import argparse, re, sys, json, math, datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml  # pip install pyyaml
except Exception:
    yaml = None  # fallback to minimal parser if needed

# --------------------------------------------------------------------------------------
# Paths / files
# --------------------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / "outputs"

SUG_FILES = {
    "covered_call": OUTPUTS / "covered_call_suggestions.json",
    "csp": OUTPUTS / "csp_suggestions.json",
    "pmcc": OUTPUTS / "pmcc_suggestions.json",
    "vertical": OUTPUTS / "vertical_suggestions.json",
    "diagonal": OUTPUTS / "diagonal_suggestions.json",
    "iron_condor": OUTPUTS / "iron_condor_suggestions.json",
    "long_call": OUTPUTS / "long_call_suggestions.json",  # may or may not exist
}

MARKET_YML = OUTPUTS / "market_state.yml"
MARKET_JSON = OUTPUTS / "market_state.json"

# --------------------------------------------------------------------------------------
# Regexes & parsing helpers
# --------------------------------------------------------------------------------------
STOP_TICKERS = {"CALL", "PUT", "OPTIONS", "OPTION"}
TICKER_LINE = re.compile(r"^[A-Z]{1,6}(?:\.[A-Z])?$")

# 1) "QQQ 10/17/2025 560.00 P" or "IWM 2025-09-30 230.00 C"
LEG_RE_1 = re.compile(
    r"(?:(?P<sym>[A-Z]{1,6})\s+)?(?P<exp>\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4})\s+(?P<strike>\d{1,5}(?:\.\d+)?)\s*(?P<cp>[cCpP])"
)

# 2) "CALL ... $230 EXP 09/30/25" | "PUT ... $560 EXP 10/17/25"
#    IMPORTANT: require the $ before strike and bind it to the $ closest to EXP.
LEG_RE_2 = re.compile(
    r"(?i)\b(?P<typ>CALL|PUT)\b(?:(?!\bEXP\b).)*?\$(?P<strike>\d{1,5}(?:\.\d+)?)(?:(?!\bEXP\b).)*?\bEXP\b\s+(?P<exp>\d{1,2}/\d{1,2}/\d{2,4})"
)

ITM_RE = re.compile(r"\bITM\b", re.IGNORECASE)
OTM_RE = re.compile(r"\bOTM\b", re.IGNORECASE)

# isolated quantity token: -1, +1, or bare 1 (not adjacent to digits)
QTY_RE = re.compile(r"(?<![\d])([+-]?1)(?![\d])")


def today_utc_date() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


def _norm_date(s: str) -> str:
    s = s.strip()
    if "-" in s:
        return s
    if "/" in s:
        mm, dd, yy = s.split("/")
        yy = yy if len(yy) == 4 else ("20" + yy[-2:])
        return f"{yy}-{int(mm):02d}-{int(dd):02d}"
    return s


def safe_float(x: Any, default: float = float("nan")) -> float:
    try:
        return float(x)
    except Exception:
        return default


# --------------------------------------------------------------------------------------
# Market state & suggestions
# --------------------------------------------------------------------------------------
def read_yaml_fallback(p: Path) -> Dict[str, Any]:
    if not p.exists():
        return {}
    if yaml:
        try:
            return yaml.safe_load(p.read_text()) or {}
        except Exception:
            pass
    out: Dict[str, Any] = {}
    for line in p.read_text().splitlines():
        if not line or line.strip().startswith("#"):
            continue
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = v.strip()
    return out


def load_market_state() -> Tuple[str, str, str]:
    if MARKET_YML.exists():
        d = read_yaml_fallback(MARKET_YML)
    elif MARKET_JSON.exists():
        try:
            d = json.loads(MARKET_JSON.read_text())
        except Exception:
            d = {}
    else:
        d = {}
    reg = d.get("overall_regime") or d.get("Regime") or "Unknown"
    trn = d.get("trend_bias") or d.get("Trend") or "Mixed"
    vol = d.get("volatility") or d.get("Vol") or "Unknown"
    return str(reg), str(trn), str(vol)


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def load_symbol_suggestions(strategy: str, symbol: str) -> List[Dict[str, Any]]:
    path = SUG_FILES.get(strategy)
    if not path:
        return []
    data = read_json(path)
    items = data.get("top") or []
    out = []
    symu = (symbol or "").upper()
    for r in items:
        sym = (r.get("symbol") or r.get("Sym") or r.get("ticker") or "").upper()
        if sym and sym == symu:
            out.append(r)
    out.sort(key=lambda x: float(x.get("score") or 0), reverse=True)
    return out[:10]


# --------------------------------------------------------------------------------------
# Position parsing
# --------------------------------------------------------------------------------------
def is_standalone_ticker(line: str) -> bool:
    s = line.strip().upper()
    if s in STOP_TICKERS:
        return False
    return bool(TICKER_LINE.match(s))


def split_groups_by_ticker(blob: str) -> List[List[str]]:
    lines = [ln.rstrip("\n") for ln in blob.splitlines()]
    groups: List[List[str]] = []
    cur: List[str] = []

    saw_ticker = False
    for ln in lines:
        if is_standalone_ticker(ln):
            saw_ticker = True
            if cur:
                groups.append(cur)
                cur = []
        cur.append(ln)
    if cur:
        groups.append(cur)

    if saw_ticker:
        return groups

    blob2 = blob.strip()
    if not blob2:
        return []
    raw_blocks = re.split(r"\n\s*\n", blob2)
    return [b.splitlines() for b in raw_blocks if b.strip()]


def qty_token(line: str) -> Optional[int]:
    m = QTY_RE.search(line)
    if not m:
        return None
    tok = m.group(1)
    if tok == "-1":
        return -1
    return 1  # "+1" or bare "1"


def infer_side_from_line(line: str) -> Optional[str]:
    q = qty_token(line)
    if q is None:
        return None
    return "short" if q < 0 else "long"


def infer_moneyness(line: str) -> Optional[str]:
    if ITM_RE.search(line):
        return "ITM"
    if OTM_RE.search(line):
        return "OTM"
    return None


def parse_position_group(group: List[str]) -> Dict[str, Any]:
    # symbol
    sym = "?"
    for ln in group:
        s = ln.strip().upper()
        if is_standalone_ticker(s):
            sym = s
            break
    if sym == "?":
        tokens = re.findall(r"[A-Z]{1,6}", " ".join(group))
        for t in tokens:
            if t not in STOP_TICKERS:
                sym = t
                break

    pos: Dict[str, Any] = {
        "symbol": sym,
        "shares": 0,
        "legs": [],
        "raw": "\n".join(group),
    }

    # parse legs
    seen: set = set()
    for i, ln in enumerate(group):
        # Pattern 1
        for mm in LEG_RE_1.finditer(ln):
            exp = _norm_date(mm.group("exp"))
            strike = safe_float(mm.group("strike"))
            cp = mm.group("cp").upper()
            key = (exp, strike, cp)
            if key in seen:
                continue
            side = infer_side_from_line(ln)
            mon = infer_moneyness(ln)
            pos["legs"].append(
                {
                    "type": "call" if cp == "C" else "put",
                    "side": side,
                    "strike": strike,
                    "exp": exp,
                    "moneyness": mon,
                    "_line": i,
                }
            )
            seen.add(key)

        # Pattern 2 (requires $ before strike)
        for mm in LEG_RE_2.finditer(ln):
            exp = _norm_date(mm.group("exp"))
            strike = safe_float(mm.group("strike"))
            cp = "C" if mm.group("typ").upper() == "CALL" else "P"
            key = (exp, strike, cp)
            if key in seen:
                continue
            side = infer_side_from_line(ln)
            mon = infer_moneyness(ln)
            pos["legs"].append(
                {
                    "type": "call" if cp == "C" else "put",
                    "side": side,
                    "strike": strike,
                    "exp": exp,
                    "moneyness": mon,
                    "_line": i,
                }
            )
            seen.add(key)

    # Fallback side by qty or DTE
    for leg in pos["legs"]:
        if not leg.get("side"):
            i = leg.get("_line", 0)
            for j in (i, max(0, i - 1), min(len(group) - 1, i + 1)):
                ln = group[j]
                q = qty_token(ln)
                if q is not None:
                    leg["side"] = "short" if q < 0 else "long"
                    break
            if not leg.get("side"):
                try:
                    y, m, d = map(int, leg["exp"].split("-"))
                    dte = (dt.date(y, m, d) - today_utc_date()).days
                    leg["side"] = "long" if dte >= 120 else "short"
                except Exception:
                    leg["side"] = "short"

    for leg in pos["legs"]:
        leg.pop("_line", None)

    normalize_vertical_sides(pos)
    return pos


def normalize_vertical_sides(pos: Dict[str, Any]) -> None:
    legs = pos.get("legs", [])
    if len(legs) != 2:
        return
    a, b = legs[0], legs[1]
    if a["type"] != b["type"] or a["exp"] != b["exp"]:
        return
    if a["side"] == b["side"]:
        if a["type"] == "call":
            if a["strike"] <= b["strike"]:
                a["side"], b["side"] = "long", "short"
            else:
                a["side"], b["side"] = "short", "long"
        else:
            if a["strike"] >= b["strike"]:
                a["side"], b["side"] = "long", "short"
            else:
                a["side"], b["side"] = "short", "long"


# --------------------------------------------------------------------------------------
# Assessment logic
# --------------------------------------------------------------------------------------
def min_dte(legs: List[Dict[str, Any]]) -> int:
    if not legs:
        return 999
    today = today_utc_date()
    dtes: List[int] = []
    for l in legs:
        try:
            y, m, d = map(int, l["exp"].split("-"))
            dtes.append((dt.date(y, m, d) - today).days)
        except Exception:
            pass
    return min(dtes) if dtes else 999


def any_short_leg(legs: List[Dict[str, Any]], typ: Optional[str] = None) -> bool:
    for l in legs:
        if l.get("side") == "short" and (typ is None or l.get("type") == typ):
            return True
    return False


def any_moneyness(legs: List[Dict[str, Any]], mon: str) -> bool:
    for l in legs:
        if (l.get("moneyness") or "").upper() == mon.upper():
            return True
    return False


def assess(strategy: str, pos: Dict[str, Any]) -> Tuple[str, List[str]]:
    dte = min_dte(pos.get("legs", []))
    bullets: List[str] = []
    s = strategy

    if s == "covered_call":
        if dte <= 21 and any_short_leg(pos["legs"], "call"):
            return "ROLL", [
                "Covered call near ≤21 DTE — roll out to harvest extrinsic / manage assignment."
            ]
        return "HOLD", ["Maintain covered call; watch delta & assignment risk."]

    if s == "csp":
        if any_short_leg(pos["legs"], "put") and any_moneyness(pos["legs"], "ITM"):
            return "HOLD", [
                "CSP is ITM and red — consider roll-for-credit / manage delta; monitor cushion."
            ]
        if dte <= 21 and any_short_leg(pos["legs"], "put"):
            return "ROLL", [
                "CSP near ≤21 DTE — roll out (preserve cushion, extend duration)."
            ]
        return "HOLD", ["CSP: no immediate trigger if ≥21 DTE and cushion remains."]

    if s == "pmcc":
        if dte <= 21 and any_short_leg(pos["legs"], "call"):
            return "ROLL", [
                "PMCC short leg nearing ≤21 DTE — roll to maintain extrinsic & coverage."
            ]
        return "HOLD", ["Coverage looks okay; monitor short delta & extrinsic."]

    if s == "diagonal":
        if dte <= 21 and any_short_leg(pos["legs"]):
            return "ROLL", [
                "Diagonal short near ≤21 DTE — roll per thesis & keep timeline."
            ]
        return "HOLD", ["Diagonal: maintain structure; manage short leg cadence."]

    if s == "vertical":
        if dte <= 21:
            return "CLOSE", [
                "Vertical (defined risk) near ≤21 DTE — tidy up or realize P/L."
            ]
        return "HOLD", ["Vertical (defined risk): hold."]

    if s == "iron_condor":
        if dte <= 21:
            return "CLOSE", [
                "Iron Condor near ≤21 DTE — reduce tail risk / close or take profits."
            ]
        return "HOLD", ["Iron Condor: hold; manage wings if breached."]

    if s == "long_call":
        if dte <= 21:
            return "CLOSE", [
                "Long call approaching expiry — close/roll per thesis & vol."
            ]
        return "HOLD", ["Long call: hold per thesis; reassess delta/vol."]

    return "HOLD", ["Unknown strategy; no rule fired."]


# --------------------------------------------------------------------------------------
# Printing
# --------------------------------------------------------------------------------------
def fmt_legs(legs: List[Dict[str, Any]]) -> str:
    if not legs:
        return "-"
    parts = []
    for l in legs:
        t = "C" if l.get("type") == "call" else "P"
        parts.append(
            f"{l.get('side','?')[:1].upper()} {l.get('exp','?')} {safe_float(l.get('strike')):.1f}{t}"
        )
    return "; ".join(parts)


def pad(s: str, n: int) -> str:
    s = str(s)
    return s + " " * max(0, n - len(s))


def load_market_state() -> Tuple[str, str, str]:
    if MARKET_YML.exists():
        d = read_yaml_fallback(MARKET_YML)
    elif MARKET_JSON.exists():
        try:
            d = json.loads(MARKET_JSON.read_text())
        except Exception:
            d = {}
    else:
        d = {}
    reg = d.get("overall_regime") or d.get("Regime") or "Unknown"
    trn = d.get("trend_bias") or d.get("Trend") or "Mixed"
    vol = d.get("volatility") or d.get("Vol") or "Unknown"
    return str(reg), str(trn), str(vol)


def print_card(
    strategy: str,
    market: Tuple[str, str, str],
    pos: Dict[str, Any],
    suggestions: List[Dict[str, Any]],
) -> Tuple[str, str, int]:
    reg, trn, vol = market
    sym = pos.get("symbol") or "?"
    legs = pos.get("legs", [])
    action, bullets = assess(strategy, pos)
    dte = min_dte(legs)

    print("\n" + "=" * 72)
    print(f"TRADE CARD — {sym} [{strategy}]")
    print("=" * 72)
    print(f"Market: Regime {reg} | Trend {trn} | Vol {vol}")
    print(f"Position: {sym} | legs: {fmt_legs(legs)}")

    if not legs:
        print("(parser note: no legs detected in this block — double-check paste.)")

    print("\nAssessment:")
    print(f"  → Action: {action}")
    for b in bullets:
        print(f"    - {b}")

    print("\nSuggested adjustments (from latest rankers):")
    if not suggestions:
        print(
            "  (none found for this symbol/strategy — refresh screeners/rankers if needed)"
        )
    else:
        hdr = f" {pad('#',3)} {pad('Exp',12)} {pad('Strike',8)} {pad('Score',7)} {pad('Flag',6)} {pad('Δ',10)} {pad('P(Prof)',8)} {pad('Quote',7)}"
        print(hdr)
        print(" " + "-" * (len(hdr) - 1))
        for i, r in enumerate(suggestions, 1):
            exp = r.get("exp") or r.get("Exp") or "?"
            strike = r.get("strike") or r.get("Strike") or "-"
            score = r.get("score") or 0
            flag = r.get("flag") or "YELLOW"
            delta = r.get("delta") or r.get("Δ") or "?"
            pprof = r.get("p_profit") or r.get("P(Profit)") or r.get("prof_prob") or "?"
            quote = (
                r.get("quote")
                or r.get("bid")
                or r.get("ask")
                or r.get("Bid")
                or r.get("Ask")
                or "?"
            )
            try:
                score = f"{float(score):.3f}"
            except Exception:
                score = str(score)
            print(
                f" {pad(i,3)} {pad(exp,12)} {pad(str(strike),8)} {pad(score,7)} {pad(flag,6)} {pad(str(delta),10)} {pad(str(pprof),8)} {pad(str(quote),7)}"
            )
    return sym, action, dte


def print_summary(summ: List[Tuple[str, str, int, str]]) -> None:
    print("\n" + "=" * 72)
    print("PORTFOLIO SUMMARY — Monitor Focus")
    print("=" * 72)

    short = [(sym, act, dte, strat) for (sym, act, dte, strat) in summ if dte <= 38]
    risky = [
        (sym, act, dte, strat)
        for (sym, act, dte, strat) in summ
        if act in ("ROLL", "CLOSE")
    ]
    stable = [
        (sym, act, dte, strat)
        for (sym, act, dte, strat) in summ
        if act == "HOLD" and dte > 38
    ]

    def line(sym, act, dte, strat):
        return f"  • {sym} ({strat}) — {act}.  ({dte} DTE)"

    print("🔍 Short-dated positions (≤ 38 DTE)")
    if short:
        for row in sorted(short, key=lambda x: x[2]):
            print(line(*row))
    else:
        print("  • (none)")

    print("\n⚠️ Higher-risk / manage soon")
    if risky:
        for row in sorted(risky, key=lambda x: x[2]):
            print(line(*row))
    else:
        print("  • (none)")

    print("\n✅ Stable / monitoring")
    if stable:
        for row in sorted(stable, key=lambda x: (x[2], x[0])):
            print(line(*row))
    else:
        print("  • (none)")


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="TradeHub monitor cards (by strategy)")
    parser.add_argument(
        "--strategy",
        required=True,
        choices=[
            "covered_call",
            "csp",
            "pmcc",
            "vertical",
            "diagonal",
            "iron_condor",
            "long_call",
        ],
        help="Select which monitor rules to apply to the pasted positions.",
    )
    args = parser.parse_args()
    strategy = args.strategy

    print("\n" + "=" * 72)
    print(
        "PASTE YOUR POSITION(S) — separate positions by a standalone ticker line (e.g., AAPL)"
    )
    print("Finish with a single '.' on its own line")
    print("=" * 72)

    lines: List[str] = []
    while True:
        try:
            ln = input()
        except EOFError:
            break
        if ln.strip() == ".":
            break
        lines.append(ln)

    blob = "\n".join(lines).strip()
    if not blob:
        print("No input received. Exiting.")
        return

    groups = split_groups_by_ticker(blob)
    if not groups:
        print("Could not parse any positions. Exiting.")
        return

    market = load_market_state()
    summary_rows: List[Tuple[str, str, int, str]] = []

    for g in groups:
        pos = parse_position_group(g)
        sugg = load_symbol_suggestions(strategy, pos.get("symbol") or "")
        sym, action, dte = print_card(strategy, market, pos, sugg)
        summary_rows.append((sym, action, dte, strategy))

    print_summary(summary_rows)


if __name__ == "__main__":
    main()
