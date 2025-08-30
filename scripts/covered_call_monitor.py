#!/usr/bin/env python3
# covered_call_monitor.py — Covered Call monitor from pasted broker rows (no headers).
# Scope:
#   - Detects a short CALL on a symbol you own 100 shares of (covered).
#   - Presents a trade ticket with badges, key metrics, checklist, and playbook.
#   - Supports market context via --state outputs/market_state.yml
#   - Supports tiered GTC targets via --gtc "50,75" and optional fill price via --fill SYMBOL=price
#
# Usage:
#   python3 covered_call_monitor.py --state outputs/market_state.yml --fill AAPL=1.75 --gtc "50,75"
#
# Paste the position rows:
#   (1) Underlying line(s): SYMBOL, Company line, then the $... last price line
#   (2) Short call header line: 'AAPL 09/19/2025 235.00 C'
#   (3) First price/data line after labels (starts with '$'), includes ITM/OTM, DTE, Δ, OI, qty
# End with Ctrl-D (Mac/Linux) or Ctrl-Z Enter (Windows).

import sys, re, os, argparse, json
from dataclasses import dataclass
from datetime import datetime, timezone
from math import floor

# ----------------- ANSI / style -----------------
class C:
    R="\033[31m"; G="\033[32m"; Y="\033[33m"; B="\033[34m"; M="\033[35m"; C="\033[36m"
    DIM="\033[2m"; RESET="\033[0m"; BOLD="\033[1m"

def color(s, k): return f"{k}{s}{C.RESET}"
def green(s): return color(s, C.G)
def yellow(s): return color(s, C.Y)
def red(s): return color(s, C.R)
def bold(s): return color(s, C.BOLD)
def dim(s): return color(s, C.DIM)

def heading(s): print(bold(s))
def hbar(): print("─"*70)

def badge(text, kind="info"):
    if kind=="ok":   return "["+green(text)+"]"
    if kind=="warn": return "["+yellow(text)+"]"
    if kind=="bad":  return "["+red(text)+"]"
    return "["+text+"]"

def two_col(left_label, left_value, right_label, right_value, lw=28, vw=14):
    lv = f"{left_label:<{lw}} {left_value:>{vw}}"
    rv = f"{right_label:<{lw}} {right_value:>{vw}}"
    print(lv + "   " + rv)

def fmt(x):
    if x is None: return "N/A"
    return f"${x:,.2f}"

def fmt_pct(x):
    if x is None: return "N/A"
    return f"{x*100:.2f}%"

def fmt_pct1(x):
    if x is None: return "N/A"
    return f"{x*100:.1f}%"

def fmt_num(x, nd=3):
    if x is None: return "N/A"
    return f"{x:.{nd}f}"

def to_float(s):
    if s is None: return None
    s = s.replace(',', '')
    try: return float(s)
    except: return None

def to_int(s):
    if s is None: return None
    s = s.replace(',', '')
    try: return int(s)
    except: return None

# ----------------- Models -----------------
@dataclass
class Underlying:
    symbol: str
    last: float = None

@dataclass
class OptionRow:
    symbol: str
    exp: str
    strike: float
    cp: str              # 'C'/'P'
    dte: int = None
    delta: float = None
    oi: int = None
    qty: int = None      # -1 short, +1 long
    mark: float = None
    itm_flag: str = None # 'ITM'/'OTM'
    raw: str = ""

# ----------------- Regex -----------------
DATE_RE   = re.compile(r'(\d{2}/\d{2}/\d{4})')  # MM/DD/YYYY
HEADER_RE = re.compile(r'^([A-Z][A-Z0-9\.]{0,6})\s+(\d{2}/\d{2}/\d{4})\s+(\d+(?:\.\d+)?)\s+(C|P)\b')
MONEY_RE  = re.compile(r'\$(-?\d+(?:\.\d+)?)')
ITM_RE    = re.compile(r'\b(ITM|OTM)\b')

def _norm(s: str) -> str: return s.replace('\t', ' ').strip()

# ----------------- Input -----------------
def read_lines():
    heading("──────────────────────── COVERED CALL MONITOR — paste rows, then Ctrl-D ────────────────────────")
    print("Include: underlying line(s), short CALL header (e.g., 'AAPL 09/19/2025 235.00 C'), then its first data row.")
    raw = sys.stdin.read()
    return [_norm(l) for l in raw.splitlines() if _norm(l)]

# ----------------- Market state -----------------
def load_market_state(path):
    try:
        import yaml
        with open(path, "r") as f:
            y = yaml.safe_load(f) or {}
        reg = y.get("regime", "N/A")
        trend = y.get("trend_bias", "N/A")
        vol = y.get("volatility", "N/A")
        return {"regime": reg, "trend": trend, "vol": vol}
    except Exception:
        return {"regime": "N/A", "trend": "N/A", "vol": "N/A"}

def print_market_state(ms):
    s = f"REGIME {ms['regime']} | TREND {ms['trend']} | VOL {ms['vol']}"
    print(s)
    heading("─"*27 + " MARKET CONTEXT " + "─"*27)

# ----------------- Parse helpers -----------------
def detect_underlyings(lines):
    under = {}
    sym = None
    for line in lines:
        if re.fullmatch(r'[A-Z][A-Z0-9\.]{0,6}', line):
            sym = line
            continue
        if sym and line.startswith('$'):
            m = MONEY_RE.match(line)
            if m:
                last = to_float(m.group(1))
                under[sym] = Underlying(sym, last)
            sym = None
    return under

def token_split(row: str):
    row = row.replace('+', ' +').replace('-', ' -')
    toks = [t for t in row.split() if t]
    return toks

def parse_after_itm_block(row: str):
    m = ITM_RE.search(row)
    if not m: return None, None, None, None, None
    itm = m.group(1)
    toks = token_split(row)
    idx = None
    for i, t in enumerate(toks):
        if t == itm:
            idx = i; break
    if idx is None: return itm, None, None, None, None

    def get(i):
        if i < 0 or i >= len(toks): return None
        return toks[i].replace(',', '')

    dte_s   = get(idx+1)
    delta_s = get(idx+2)
    oi_s    = get(idx+3)
    qty_s   = get(idx+4)

    dte   = to_int(dte_s) if dte_s and dte_s.lstrip('+-').isdigit() else to_int(dte_s)
    delta = to_float(delta_s)
    oi    = to_int(oi_s)
    qty   = to_int(qty_s)

    if dte is not None and dte < 0: dte = None
    if delta is not None and not (-1.0 <= delta <= 1.0): delta = None
    if qty is not None and qty not in (-1, 1):
        alt = get(idx+5)
        q2 = to_int(alt)
        if q2 in (-1, 1): qty = q2

    return itm, dte, delta, oi, qty

def parse_options(lines):
    opts = []
    n = len(lines); i = 0
    while i < n:
        h = HEADER_RE.match(lines[i])
        if not h:
            i += 1; continue
        sym, date, strike_s, cp = h.group(1), h.group(2), h.group(3), h.group(4)
        strike = float(strike_s)

        data_row = None
        j = i + 1
        while j < n and j <= i + 8:
            row = lines[j]
            if ' EXP ' in f" {row} " or row.upper().startswith('CALL ') or row.upper().startswith('PUT '):
                j += 1; continue
            if row.startswith('$'):
                data_row = row; break
            j += 1

        if not data_row:
            i += 1; continue

        m = MONEY_RE.search(data_row)
        mark = to_float(m.group(1)) if m else None
        itm_flag, dte, delta, oi, qty = parse_after_itm_block(data_row)

        opts.append(OptionRow(sym, date, strike, cp, dte, delta, oi, qty, mark, itm_flag, data_row))
        i = j + 1
    return opts

# ----------------- Covered Call policy/logic -----------------
POLICY = {
    "short_delta_min": 0.25,
    "short_delta_max": 0.40,
    "short_dte_min": 21,
    "short_dte_sweet": (28, 45),   # tasty-style sweet spot
    "manage_dte": 21
}

def is_short_call(o: OptionRow) -> bool:
    return o.cp == 'C' and (o.qty is not None and o.qty < 0) and (o.dte is not None)

def extrinsic_of_call(mark, last, strike):
    if mark is None or last is None or strike is None: return None
    intrinsic = max(0.0, last - strike)
    return max(0.0, mark - intrinsic)

def pop_proxy_from_delta(delta):
    if delta is None: return None
    # For short call, POP ≈ 1 - P(ITM) ≈ 1 - max(0, delta)
    d = abs(delta)
    return max(0.0, 1 - d)

def gtc_targets(fill, tiers):
    # For a SHORT call sold @ fill credit, buy-to-close target = fill*(1 - tier)
    if fill is None or not tiers: return None
    out = []
    for t in tiers:
        price = max(0.01, round(fill*(1 - t), 2))
        out.append((int(t*100), price))
    return out

def band_badge(val, lo, hi, label_ok="in band", label_bad="out of band"):
    if val is None: return badge("Δ N/A", "bad")
    if lo <= abs(val) <= hi: return badge(f"Δ {label_ok}", "ok")
    return badge(f"Δ {label_bad}", "bad")

# ----------------- Report -----------------
def report(sym, und: Underlying, short: OptionRow, market, fill_map, gtc_tiers):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    tested = (und.last is not None and short.strike is not None and und.last >= short.strike)
    in_band = (short.delta is not None and POLICY["short_delta_min"] <= abs(short.delta) <= POLICY["short_delta_max"])
    dte_in_band = (short.dte is not None and POLICY["short_dte_min"] <= short.dte)
    sweet_lo, sweet_hi = POLICY["short_dte_sweet"]
    sweet = (short.dte is not None and sweet_lo <= short.dte <= sweet_hi)

    b1 = badge("Short tested/ITM", "bad") if tested else badge("Not tested", "ok")
    b2 = band_badge(short.delta, POLICY["short_delta_min"], POLICY["short_delta_max"])
    b3 = badge("DTE sweet spot", "ok") if sweet else badge("DTE outside sweet", "warn" if dte_in_band else "bad")

    hbar()
    print(f"{sym}  |  COVERED CALL TRADE TICKET")
    print(now)
    hbar()
    print(f"{b1} {b2} {b3}\n")

    print(f"Underlying: {fmt(und.last)}")
    print(f"Short Call: {short.strike:>7.3f} C  • Exp {short.exp}  • DTE {short.dte or 'N/A'}  • Δ {fmt_num(short.delta)}  • Mark {fmt(short.mark)}\n")

    heading("Policy")
    print(f"Policy → Δ band {POLICY['short_delta_min']:.2f}–{POLICY['short_delta_max']:.2f} • short DTE {sweet_lo}–{sweet_hi} • TP 50% • roll@{POLICY['manage_dte']}d • roll if Δ>0.55\n")

    # Key metrics (from-here perspective)
    extr = extrinsic_of_call(short.mark, und.last, short.strike)
    pop  = pop_proxy_from_delta(short.delta)
    # "Static" return: premium / price (if stock stays ~unchanged)
    static = (short.mark / und.last) if (short.mark is not None and und.last) else None
    # "Yield to strike" (from here, if called away): (premium + max(0, strike-last)) / last
    yts = None
    if und.last is not None and short.strike is not None and short.mark is not None:
        yts = (short.mark + max(0.0, short.strike - und.last)) / und.last
    # Annualize with simple 365/dte
    static_ann = (static * 365 / short.dte) if (static is not None and short.dte) else None
    yts_ann    = (yts * 365 / short.dte) if (yts is not None and short.dte) else None

    two_col("Extrinsic (time value):", fmt(extr),
            "POP (proxy):", fmt_pct(pop))
    two_col("Static return:", fmt_pct(static),
            "Static annualized:", fmt_pct(static_ann))
    two_col("Yield to strike:", fmt_pct(yts),
            "Ann. yield to strike:", fmt_pct(yts_ann))
    two_col("Open interest:", f"{short.oi or 0}",
            "Market regime:", f"REGIME {market['regime']} | TREND {market['trend']} | VOL {market['vol']}")
    print()

    heading("Checklist")
    c1 = "PASS" if dte_in_band else "FAIL"
    c2 = "PASS" if in_band else "FAIL"
    c3 = "PASS" if not tested else "FAIL"
    c4 = "PASS" if (short.oi or 0) >= 100 else "FAIL"
    print(f"  DTE ≥ {POLICY['short_dte_min']}: {c1}")
    print(f"  Δ in [{POLICY['short_delta_min']:.2f},{POLICY['short_delta_max']:.2f}]: {c2}")
    print(f"  Short strike not tested: {c3}")
    print(f"  OI ≥ 100: {c4}\n")

    heading("Playbook — What, Why, When, How")
    if tested:
        print("• Tested/ITM → upside capped; decide: let shares be called, or roll up/out to keep stock.")
    else:
        print("• OTM short → theta decay works; assignment risk lower.")
    if in_band:
        print("• Δ in band → balanced POP vs credit.")
    else:
        print("• Δ out of band → risk/reward off; adjust strike/DTE.")
    if sweet:
        print(f"• DTE {sweet_lo}–{sweet_hi} → sweet spot for theta vs gamma.")
    else:
        print(f"• DTE outside sweet → monitor; manage at {POLICY['manage_dte']} DTE.\n")

    # Tiered GTC from fill
    fill = None
    for k,v in (fill_map or {}).items():
        if k.upper() == sym.upper():
            fill = v; break
    tiers = None
    if gtc_tiers:
        tiers = [t/100.0 for t in gtc_tiers]
    tgts = gtc_targets(fill, tiers) if tiers else None
    if tgts:
        gtcs = ", ".join([f"{pct}%→{fmt(px)}" for pct,px in tgts])
        print(f"GTC profit tiers: {gtcs}")
    elif tiers:
        # show estimate off MARK if fill not given
        est = gtc_targets(short.mark, tiers)
        if est:
            gtcs = ", ".join([f"{pct}%→{fmt(px)}" for pct,px in est])
            print(f"GTC targets (est., no fill): {gtcs}")

    print()
    heading("Recommendations")
    recs = []
    if short.dte is not None and short.dte <= POLICY["manage_dte"]:
        recs.append("• ≤21 DTE → plan to roll or close.")
    if short.delta is not None and abs(short.delta) >= 0.55:
        recs.append("• Δ ≥ 0.55 → consider roll up/out.")
    if tested:
        recs.append("• Tested: choose between assignment (let shares go) or roll up/out for credit.")
    if not recs:
        recs.append("• Hold. Keep tiered GTC working on short.")
    for r in recs: print(r)
    hbar()

# ----------------- Main -----------------
def parse_fill_map(fill_args):
    m = {}
    if not fill_args: return m
    for item in fill_args:
        if '=' in item:
            sym, price = item.split('=', 1)
            m[sym.strip().upper()] = to_float(price.strip())
    return m

def parse_gtc_list(s):
    if not s: return None
    out = []
    for part in s.split(','):
        part = part.strip().replace('%','')
        try:
            v = float(part)
            if v <= 0 or v >= 100: continue
            out.append(v)
        except:
            pass
    return out

def main():
    ap = argparse.ArgumentParser(description="Covered Call monitor")
    ap.add_argument("--state", default=None, help="Path to market_state.yml")
    ap.add_argument("--fill", nargs='*', help="One or more SYMBOL=price (credit) for short call fill")
    ap.add_argument("--gtc", default=None, help="Comma list of take-profit percents, e.g., '50,75'")
    args = ap.parse_args()

    market = {"regime":"N/A","trend":"N/A","vol":"N/A"}
    if args.state:
        market = load_market_state(args.state)
        print(f"REGIME {market['regime']} | TREND {market['trend']} | VOL {market['vol']}")
        heading("─"*27 + " MARKET CONTEXT " + "─"*27)
        print()

    fill_map = parse_fill_map(args.fill)
    gtc_list = parse_gtc_list(args.gtc)

    lines = read_lines()
    if not lines:
        print("No input detected. Paste your position rows and try again.")
        return

    und_map = detect_underlyings(lines)
    opts = parse_options(lines)
    # pick first short call per symbol
    by = {}
    for o in opts:
        if is_short_call(o):
            by.setdefault(o.symbol, []).append(o)

    if not by:
        print("No covered call row detected (need a short CALL with qty -1).")
        return

    for sym, rows in by.items():
        # choose the row closest to DTE sweet spot
        sweet_lo, sweet_hi = POLICY["short_dte_sweet"]
        def key_short(r):
            tgt = (sweet_lo + sweet_hi)/2
            return abs((r.dte or 0) - tgt), abs((abs(r.delta or 0.33)) - 0.33)
        short_pick = sorted(rows, key=key_short)[0]
        und = und_map.get(sym, Underlying(sym, None))

        report(sym, und, short_pick, market, fill_map, gtc_list)

if __name__ == "__main__":
    main()