#!/usr/bin/env python3
# pmcc_monitor.py — PMCC monitor (LEAP + short call) with regime-aware policy.
# Input: paste broker rows (underlying line + option header + first price/data row).
# Reads: outputs/market_state.json and configs/strategy_policy.(yml|json) if present.
# Writes: outputs/pmcc_last_run_policy.json(.yml if yaml present).
# No required external deps. PyYAML is optional.

import sys, re, os, json
from dataclasses import dataclass
from datetime import datetime, timezone
from math import floor
from copy import deepcopy

DEBUG = bool(os.environ.get("DEBUG_PMCC"))

# ---------- ANSI ----------
class C:
    R="\033[31m"; G="\033[32m"; Y="\033[33m"; B="\033[34m"; M="\033[35m"; C="\033[36m"
    DIM="\033[2m"; RESET="\033[0m"; BOLD="\033[1m"
def color(s, k): return f"{k}{s}{C.RESET}"
def green(s): return color(s, C.G)
def yellow(s): return color(s, C.Y)
def red(s): return color(s, C.R)
def bold(s): return color(s, C.BOLD)
def dim(s): return color(s, C.DIM)

# ---------- fmt ----------
def fmt_usd(x):
    return "N/A" if x is None else f"${x:,.2f}"
def fmt_pct(x):
    return "N/A" if x is None else f"{x*100:.1f}%"
def fmt_num(x, nd=3):
    return "N/A" if x is None else f"{x:.{nd}f}"

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

# ---------- models ----------
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

# ---------- regex ----------
DATE_RE   = re.compile(r'(\d{2}/\d{2}/\d{4})')  # MM/DD/YYYY
HEADER_RE = re.compile(r'^([A-Z][A-Z0-9\.]{0,6})\s+(\d{2}/\d{2}/\d{4})\s+(\d+(?:\.\d+)?)\s+(C|P)\b')
MONEY_RE  = re.compile(r'\$(-?\d+(?:\.\d+)?)')
ITM_RE    = re.compile(r'\b(ITM|OTM)\b')

def _norm(s: str) -> str: return s.replace('\t', ' ').strip()

# ---------- input ----------
def read_lines():
    print("Paste your full broker rows (no headers needed). Include the underlying line, the option header line (e.g., 'QQQ 09/19/2025 560.00 C'), and the first price/data line that follows. Then press Ctrl-D.")
    raw = sys.stdin.read()
    lines = [_norm(l) for l in raw.splitlines() if _norm(l)]
    if DEBUG:
        for l in lines: print(dim(f"[LINE] {l}"))
    return lines

# ---------- underlying parse ----------
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
                if DEBUG: print(dim(f"[DEBUG] Underlying {sym}: last={last} via: {line}"))
            sym = None
    return under

# ---------- option parse helpers ----------
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

    dte   = to_int(get(idx+1))
    delta = to_float(get(idx+2))
    oi    = to_int(get(idx+3))
    qty   = to_int(get(idx+4))
    if qty not in (-1,1):
        q2 = to_int(get(idx+5))
        if q2 in (-1,1): qty = q2
    return itm, dte, delta, oi, qty

# ---------- option parse ----------
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
            if DEBUG: print(dim(f"[DEBUG] No data row after header: {lines[i]}"))
            i += 1; continue

        m = MONEY_RE.search(data_row)
        mark = to_float(m.group(1)) if m else None

        itm_flag, dte, delta, oi, qty = parse_after_itm_block(data_row)

        opt = OptionRow(sym, date, strike, cp, dte, delta, oi, qty, mark, itm_flag, data_row)
        opts.append(opt)
        if DEBUG:
            print(dim(f"[DEBUG] Parsed {sym} {date} {strike} {cp} | mark={mark} ITM={itm_flag} DTE={dte} Δ={delta} OI={oi} qty={qty} | {data_row}"))
        i = j + 1
    return opts

# ---------- Policy load/merge ----------
DEFAULT_POLICY = {
    "defaults": {
        "targets": {
            "pmcc": {
                "leap_delta_min": 0.65,
                "leap_min_dte": 90,
                "short_dte_pref": [28, 45],
                "short_delta_band": [0.15, 0.55],
                "short_take_profit": 0.50,
                "roll_at_days_to_exp": 21,
                "roll_if_short_delta_gt": 0.55,
            }
        }
    },
    "regimes": {
        "bullish": {
            "vol": {
                "low": {"adjust": {"pmcc.short_delta_band": [0.20, 0.45]}},
                "medium": {"adjust": {"pmcc.short_delta_band": [0.20, 0.50]}},
                "high": {"adjust": {"pmcc.short_delta_band": [0.25, 0.55]}},
            }
        },
        "neutral": {
            "vol": {
                "low": {"adjust": {"pmcc.short_dte_pref": [21, 35]}},
                "medium": {},
                "high": {},
            }
        },
        "bearish": {
            "vol": {
                "low": {"adjust": {"pmcc.short_delta_band": [0.15, 0.35]}},
                "medium": {},
                "high": {},
            }
        },
    }
}

def load_json(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None

def try_load_yaml(path):
    # Optional dependency; if missing, return None
    try:
        import yaml  # type: ignore
    except Exception:
        return None
    try:
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return None

def deep_merge(a, b):
    if not isinstance(b, dict): return b
    out = deepcopy(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = deepcopy(v)
    return out

def set_by_dotted(d, dotted_key, value):
    parts = dotted_key.split(".")
    cur = d
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value

def load_market_state(path="outputs/market_state.json"):
    js = load_json(path)
    if not js:
        return {"regime": "neutral", "vol": {"regime": "medium"}}
    regime = js.get("regime") or "neutral"
    vol = js.get("vol", {})
    vol_regime = vol.get("regime") or "medium"
    return {"regime": regime, "vol": {"regime": vol_regime}}

def load_strategy_policy():
    # Priority: YAML -> JSON -> default
    pol = try_load_yaml("configs/strategy_policy.yml")
    if pol is None:
        pol = load_json("configs/strategy_policy.json")
    if pol is None:
        pol = deepcopy(DEFAULT_POLICY)
        pol["_policy_source"] = "default"
    else:
        pol["_policy_source"] = "file"
    return pol

def resolve_pmcc_targets(policy, market_state):
    # Start from defaults.targets.pmcc
    base = (((policy.get("defaults") or {}).get("targets") or {}).get("pmcc") or {})
    resolved = deepcopy(base)

    regime = market_state["regime"]
    vol_regime = market_state["vol"]["regime"]
    adj = (((policy.get("regimes") or {}).get(regime) or {}).get("vol") or {}).get(vol_regime) or {}
    adjust = adj.get("adjust") or {}

    # apply dotted overrides
    for k, v in adjust.items():
        if not k.startswith("pmcc."): 
            continue
        set_by_dotted(resolved, k[len("pmcc."):], v)

    # sanity defaults if missing
    resolved.setdefault("leap_delta_min", 0.65)
    resolved.setdefault("leap_min_dte", 90)
    resolved.setdefault("short_dte_pref", [28, 45])
    resolved.setdefault("short_delta_band", [0.15, 0.55])
    resolved.setdefault("short_take_profit", 0.50)
    resolved.setdefault("roll_at_days_to_exp", 21)
    resolved.setdefault("roll_if_short_delta_gt", 0.55)
    return resolved

def ensure_outputs_dir():
    os.makedirs("outputs", exist_ok=True)

def dump_last_run_policy(engine_name, resolved, market_state, policy_source):
    ensure_outputs_dir()
    payload = {
        "engine": engine_name,
        "used_policy": resolved,
        "market_state": market_state,
        "policy_source": policy_source,
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
    # JSON
    with open(f"outputs/{engine_name}_last_run_policy.json", "w") as f:
        json.dump(payload, f, indent=2)
    # YAML if available
    try:
        import yaml  # type: ignore
        with open(f"outputs/{engine_name}_last_run_policy.yml", "w") as f:
            yaml.safe_dump(payload, f, sort_keys=False)
    except Exception:
        pass

# ---------- PMCC logic ----------
def is_long_leap_call(o: OptionRow, last: float, P):
    if o.cp != 'C' or o.qty is None or o.qty <= 0 or o.dte is None: return False
    itm_ok = (o.itm_flag == 'ITM') or (last is not None and last > o.strike)
    return itm_ok and o.dte >= P["leap_min_dte"] and (o.delta is None or o.delta >= P["leap_delta_min"])

def is_short_near_call(o: OptionRow, last: float, P):
    if o.cp != 'C' or o.qty is None or o.qty >= 0 or o.dte is None: return False
    otm_ok = (o.itm_flag == 'OTM') or (last is not None and last < o.strike)
    lo, hi = P["short_delta_band"]
    return otm_ok and 7 <= o.dte <= 60 and (o.delta is None or lo <= abs(o.delta) <= hi)

def cycles_left(dte_long: int) -> int:
    if dte_long is None: return 0
    return max(0, floor((dte_long - 21) / 30))

def long_extrinsic(long: OptionRow, last: float) -> float:
    if long.mark is None or last is None: return None
    intrinsic = max(0.0, last - long.strike)
    return max(0.0, long.mark - intrinsic)

def coverage_ok(extr: float, dte_long: int, short_mark: float):
    if extr is None or dte_long is None: return (False, None)
    cyc = cycles_left(dte_long)
    if cyc == 0: return (True, 0.0)
    req = extr / cyc
    ok = (short_mark or 0.0) >= 0.8 * req  # 80% heuristic
    return ok, req

def gtc_target(short_mark: float, take_pct: float):
    if short_mark is None: return None
    tgt = short_mark * (1 - take_pct)
    # round to nearest nickel
    return max(0.05, round(round(tgt / 0.05) * 0.05, 2))

# ---------- UI helpers ----------
def pad(s, w): 
    s = str(s)
    return s + " " * max(0, w - len(s))

def line2(label_left, val_left, label_right, val_right, w_label=18, w_val=14):
    L = pad(label_left + ":", w_label) + " " + pad(val_left, w_val)
    R = pad(label_right + ":", w_label) + " " + pad(val_right, w_val)
    return f"{L}   {R}"

def header(sym):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    bar = "─"*70
    print(bar)
    print(bold(f"{sym}  |  PMCC TRADE TICKET"))
    print(now)
    print(bar)

def policy_line(P):
    band = f"{P['short_delta_band'][0]:.2f}–{P['short_delta_band'][1]:.2f}"
    dtew = f"{P['short_dte_pref'][0]}–{P['short_dte_pref'][1]}"
    tp   = f"{int(P['short_take_profit']*100)}%"
    rae  = P['roll_at_days_to_exp']
    ridg = P['roll_if_short_delta_gt']
    return f"Policy → Δ band {band} • short DTE {dtew} • TP {tp} • roll@{rae}d • roll if Δ>{ridg:.2f}"

# ---------- report ----------
def report(sym, und: Underlying, long: OptionRow, short: OptionRow, P, policy_src, market_state):
    header(sym)

    short_otm = (und.last is not None and short.strike > und.last) or (short.itm_flag == 'OTM')
    short_delta_band = P["short_delta_band"]
    in_band = (short.delta is None) or (short_delta_band[0] <= abs(short.delta) <= short_delta_band[1])

    badges = []
    badges.append("[ Short OTM ]" if short_otm else red("[ Short tested ]"))
    badges.append("[ Δ in band ]" if in_band else yellow("[ Δ out of band ]"))
    # short DTE context
    dte_msg = ""
    if short.dte is not None:
        lo, hi = P["short_dte_pref"]
        if short.dte < P["roll_at_days_to_exp"]:
            dte_msg = red(f"[ DTE < {P['roll_at_days_to_exp']}: Prep roll ]")
        elif lo <= short.dte <= hi:
            dte_msg = "[ DTE sweet spot ]"
        else:
            dte_msg = "[ DTE outside sweet spot ]"
        badges.append(dte_msg)
    print("  ".join(badges)); print()

    print(f"Underlying: {fmt_usd(und.last)}")
    print(f"LEAP (long): {long.strike:.2f} C  • Exp {long.exp}  • DTE {long.dte}  • Δ {fmt_num(long.delta)}  • Mark {fmt_usd(long.mark)}")
    print(f"Short (covered): {short.strike:.2f} C  • Exp {short.exp}  • DTE {short.dte}  • Δ {fmt_num(short.delta)}  • Mark {fmt_usd(short.mark)}")
    print()

    print(bold("Policy"))
    print(policy_line(P)); print()

    print(bold("First Check"))
    print(f"  LEAP ITM: {'PASS' if (und.last is not None and und.last > long.strike) or (long.itm_flag=='ITM') else 'FAIL'}")
    print(f"  LEAP DTE ≥ {P['leap_min_dte']}: {'PASS' if (long.dte or 0) >= P['leap_min_dte'] else 'FAIL'}")
    print(f"  LEAP Δ ≥ {P['leap_delta_min']:.2f}: {'PASS' if (long.delta or 0) >= P['leap_delta_min'] else 'FAIL'}")
    print(f"  Short OTM: {'PASS' if short_otm else 'FAIL'}")
    print(f"  Short DTE in [7,60]: {'PASS' if (short.dte or 0) >= 7 and (short.dte or 0) <= 60 else 'FAIL'}")
    print(f"  Short Δ in {P['short_delta_band'][0]:.2f}–{P['short_delta_band'][1]:.2f}: {'PASS' if in_band else 'FAIL'}")
    print()

    print(bold("Deep Analysis"))
    extr = long_extrinsic(long, und.last)
    cyc  = cycles_left(long.dte)
    ok, req = coverage_ok(extr, long.dte, short.mark)
    tgt = gtc_target(short.mark, P["short_take_profit"])
    net_delta = (long.delta or 0.0) + (short.delta or 0.0)

    print(line2("LEAP extrinsic", fmt_usd(extr), "Cycles left (≈30D)", str(cyc)))
    print(line2("Required / 30D", fmt_usd(req), "Short credit (mark)", fmt_usd(short.mark)))
    print(line2("Coverage status", ("OK" if ok else "MARGINAL") if req is not None else "N/A",
                "Short 50% GTC", fmt_usd(tgt)))
    print(line2("Net Δ (long+short)", fmt_num(net_delta), "Market regime",
                f"{market_state['regime']}/{market_state['vol']['regime']}"))
    print()

    print(bold("Recommendations"))
    recs = []
    if (short.dte or 999) <= P["roll_at_days_to_exp"]:
        recs.append(red(f"• DTE ≤ {P['roll_at_days_to_exp']}: set roll/close"))
    if short.delta is not None and abs(short.delta) > P["roll_if_short_delta_gt"]:
        recs.append(red(f"• Short Δ>{P['roll_if_short_delta_gt']:.2f}: consider roll up/out"))
    if not ok and req is not None:
        recs.append(yellow(f"• Coverage marginal: seek ≥ {fmt_usd(0.8*req)} credit per 30D cycle"))
    if extr is not None and extr <= 0.50 and (long.dte or 0) >= P["leap_min_dte"]:
        recs.append(yellow("• LEAP extrinsic ~spent → evaluate harvesting/roll up-out"))
    if not recs:
        recs.append(green("• Hold. Keep 50% GTC working on short."))

    for r in recs: print(r)

    # persist last-run policy for audit
    dump_last_run_policy("pmcc", P, market_state, policy_src)
    print("─"*70)

# ---------- main ----------
def main():
    lines = read_lines()
    und_map = detect_underlyings(lines)
    opts = parse_options(lines)

    policy = load_strategy_policy()
    market_state = load_market_state()
    P = resolve_pmcc_targets(policy, market_state)
    policy_src = policy.get("_policy_source", "default")

    by = {}
    for o in opts: by.setdefault(o.symbol, []).append(o)

    found = 0
    for sym, rows in by.items():
        und = und_map.get(sym, Underlying(sym, None))
        longs  = [r for r in rows if is_long_leap_call(r, und.last, P)]
        shorts = [r for r in rows if is_short_near_call(r, und.last, P)]
        if not longs or not shorts: continue

        long_pick = sorted(longs, key=lambda r: (r.dte or 0, r.delta or 0), reverse=True)[0]
        def key_short(r):
            lo, hi = P["short_delta_band"]
            target_dte = sum(P["short_dte_pref"])/2.0
            delta_mid = sum(P["short_delta_band"])/2.0
            return (abs((r.dte or 0) - target_dte), abs((abs(r.delta or delta_mid)) - delta_mid))
        short_pick = sorted(shorts, key=key_short)[0]

        report(sym, und, long_pick, short_pick, P, policy_src, market_state)
        found += 1

    if found == 0:
        print("No PMCC pairs detected (need a long-dated ITM call and a nearer-dated short OTM call on the same symbol).")

if __name__ == "__main__":
    main()