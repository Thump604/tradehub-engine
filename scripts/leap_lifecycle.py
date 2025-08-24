#!/usr/bin/env python3
# leap_lifecycle.py — rank replacement LEAP candidates from Barchart "Long Call Screener" CSV.
# Inputs:
#   - data/leap-latest.csv  (created by ingest_latest.py)
#   - data/strategy_policy.yml  (optional; overrides defaults below)
# Outputs:
#   - outputs/suggestions/leap_renewals.yml
#   - outputs/suggestions/leap_renewals.json
#
# No external deps; YAML written with a tiny emitter.

import csv, json, os, sys, re
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT, "data")
OUT_DIR  = os.path.join(ROOT, "outputs", "suggestions")

LEAP_CSV_DEFAULT = os.path.join(DATA_DIR, "leap-latest.csv")
POLICY_YML = os.path.join(DATA_DIR, "strategy_policy.yml")

# --------- Defaults (overridable via data/strategy_policy.yml) ---------
POLICY_DEFAULT = {
    "leap_lifecycle": {
        "target_count": 20,
        "dte_min": 300,            # target window for new LEAPs
        "dte_max": 700,
        "delta_min": 0.75,
        "delta_max": 0.92,
        "prefer_itm_moneyness_min_pct": 5.0,   # prefer slightly ITM (e.g., +10% ITM)
        "prefer_itm_moneyness_max_pct": 30.0,
        "deprioritize_high_iv_rank_above": 50.0,
        "weights": {               # scoring weights
            "delta": 0.55,
            "dte_window": 0.25,
            "moneyness_pref": 0.15,
            "iv_penalty": 0.05
        }
    }
}

# --------- Tiny YAML helpers (no external deps) ---------
def yaml_dump(obj, indent=0):
    """minimal YAML emitter for dict/list/str/int/float/bool/None"""
    sp = "  " * indent
    if isinstance(obj, dict):
        lines = []
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                lines.append(f"{sp}{k}:")
                lines.append(yaml_dump(v, indent+1))
            else:
                lines.append(f"{sp}{k}: {yaml_scalar(v)}")
        return "\n".join(lines)
    elif isinstance(obj, list):
        lines = []
        for item in obj:
            if isinstance(item, (dict, list)):
                lines.append(f"{sp}-")
                lines.append(yaml_dump(item, indent+1))
            else:
                lines.append(f"{sp}- {yaml_scalar(item)}")
        return "\n".join(lines)
    else:
        return f"{sp}{yaml_scalar(obj)}"

def yaml_scalar(v):
    if v is None: return "null"
    if isinstance(v, bool): return "true" if v else "false"
    if isinstance(v, (int, float)):
        # keep short float representation
        s = f"{v}"
        return s
    s = str(v)
    # quote if contains special chars
    if re.search(r"[:#,\[\]\{\}\n\"']", s) or s.strip() != s or " " in s:
        return '"' + s.replace('"','\\"') + '"'
    return s

# --------- Simple policy reader (very permissive, key:value only) ---------
def load_policy(path):
    policy = POLICY_DEFAULT.copy()
    if not os.path.exists(path):
        return policy
    try:
        # extremely permissive parse: only handles 'a: b' and nested keys we expect
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()
        # find leap_lifecycle.* overrides: we accept lines like
        # leap_lifecycle:
        #   dte_min: 320
        #   delta_min: 0.72
        block = re.search(r'(?ms)^\s*leap_lifecycle\s*:\s*(.+)$', raw)
        if not block:
            return policy
        # parse pairs under leap_lifecycle:
        # grab the full indented block
        lines = []
        started = False
        for line in raw.splitlines():
            if re.match(r'^\s*leap_lifecycle\s*:\s*$', line):
                started = True
                continue
            if started:
                if re.match(r'^\S', line):  # next root key
                    break
                lines.append(line)
        # Now scan for simple pairs we know
        def pick_float(key, default):
            m = re.search(rf'^\s*{re.escape(key)}\s*:\s*([0-9.]+)\s*$', "\n".join(lines), re.M)
            return float(m.group(1)) if m else default
        def pick_int(key, default):
            m = re.search(rf'^\s*{re.escape(key)}\s*:\s*([0-9]+)\s*$', "\n".join(lines), re.M)
            return int(m.group(1)) if m else default
        policy["leap_lifecycle"]["target_count"] = pick_int("target_count", policy["leap_lifecycle"]["target_count"])
        policy["leap_lifecycle"]["dte_min"] = pick_int("dte_min", policy["leap_lifecycle"]["dte_min"])
        policy["leap_lifecycle"]["dte_max"] = pick_int("dte_max", policy["leap_lifecycle"]["dte_max"])
        policy["leap_lifecycle"]["delta_min"] = pick_float("delta_min", policy["leap_lifecycle"]["delta_min"])
        policy["leap_lifecycle"]["delta_max"] = pick_float("delta_max", policy["leap_lifecycle"]["delta_max"])
        policy["leap_lifecycle"]["prefer_itm_moneyness_min_pct"] = pick_float("prefer_itm_moneyness_min_pct", policy["leap_lifecycle"]["prefer_itm_moneyness_min_pct"])
        policy["leap_lifecycle"]["prefer_itm_moneyness_max_pct"] = pick_float("prefer_itm_moneyness_max_pct", policy["leap_lifecycle"]["prefer_itm_moneyness_max_pct"])
        policy["leap_lifecycle"]["deprioritize_high_iv_rank_above"] = pick_float("deprioritize_high_iv_rank_above", policy["leap_lifecycle"]["deprioritize_high_iv_rank_above"])
    except Exception:
        # keep defaults on any parse issue
        pass
    return policy

# --------- CSV normalization ---------
def norm_header(h):
    h = h.strip().strip('"')
    h = h.replace("Exp Date", "Exp Date")
    h = h.replace("Strike Price", "Strike")  # if present
    h = h.replace("Option Type", "Type")
    h = h.replace("Option Ask Price", "Ask2")
    h = h.replace("Option Last Price", "Last")
    h = h.replace("Option Volume", "Volume2")
    h = h.replace("Option Open Interest", "Open Int")
    h = h.replace("IV Rank", "IV Rank")
    h = h.replace("Break Even (Ask)", "BE (Ask)")
    h = h.replace("% To Break Even (Ask)", "%BE (Ask)")
    h = h.replace("Short Term Opinion Signal/Percent", "Short Term")
    h = h.replace("Short Term~", "Short Term")
    h = h.replace("52-Week High", "52W High")
    h = h.replace("52W High~", "52W High")
    h = h.replace("%Chg~", "%Chg")
    h = h.replace("Volume~", "Volume")
    return h

def read_leap_csv(path):
    if not os.path.exists(path):
        print(f"[ERROR] Missing {path}. Run ingest_latest.py first.", file=sys.stderr)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        print(f"[ERROR] Empty CSV: {path}", file=sys.stderr)
        sys.exit(1)
    headers = [norm_header(h) for h in rows[0]]
    # Map first occurrence of important fields; tolerate duplicates
    def idx(name):
        try: return headers.index(name)
        except ValueError: return -1
    I = {
        "Symbol": idx("Symbol"),
        "Exp Date": idx("Exp Date"),
        "DTE": idx("DTE"),
        "Strike": idx("Strike"),
        "Type": idx("Type"),
        "Bid": idx("Bid"),
        "Ask": headers.index("Ask") if "Ask" in headers else -1,  # first Ask
        "Last": idx("Last"),
        "Moneyness": idx("Moneyness"),
        "Delta": idx("Delta"),
        "IV Rank": idx("IV Rank"),
        "BE (Ask)": idx("BE (Ask)"),
    }
    # minimal required:
    needed = ["Symbol","Exp Date","DTE","Strike","Type","Ask","Delta"]
    missing = [k for k in needed if I.get(k, -1) < 0]
    if missing:
        print(f"[ERROR] Missing required columns in LEAP CSV: {missing}", file=sys.stderr)
        sys.exit(1)

    out = []
    for r in rows[1:]:
        if not r or len(r) < len(headers): continue
        typ = (r[I["Type"]].strip() if I["Type"]>=0 else "").lower()
        if typ != "call":    # long LEAP calls only
            continue
        try:
            symbol = r[I["Symbol"]].strip()
            exp    = r[I["Exp Date"]].strip()
            dte    = int(float(r[I["DTE"]]))
            strike = float(r[I["Strike"]])
            ask    = float(str(r[I["Ask"]]).replace(',',''))
            delta  = float(str(r[I["Delta"]]))
            mny    = r[I["Moneyness"]].strip() if I["Moneyness"]>=0 else ""
            ivr    = float(str(r[I["IV Rank"]]).replace('%','')) if I["IV Rank"]>=0 and r[I["IV Rank"]] else None
            be_ask = float(str(r[I["BE (Ask)"]]).replace(',','')) if I["BE (Ask)"]>=0 and r[I["BE (Ask)"]] else None
        except Exception:
            continue

        # parse moneyness like "+16.58%" or "-3.18%"
        def parse_pct(s):
            if not s: return None
            s = s.replace('%','').replace('+','')
            try: return float(s) if s.strip() != "" else None
            except: return None
        mny_pct = parse_pct(mny)

        out.append({
            "symbol": symbol,
            "exp": exp,
            "dte": dte,
            "strike": strike,
            "ask": ask,
            "delta": delta,
            "moneyness_pct": mny_pct,     # positive => ITM for calls
            "iv_rank": ivr,
            "be_ask": be_ask
        })
    return out

# --------- Scoring ----------
def clamp01(x): 
    if x < 0: return 0.0
    if x > 1: return 1.0
    return x

def score_row(row, pol):
    p = pol["leap_lifecycle"]
    w = p["weights"]
    # delta score → 1 near midpoint of [delta_min, delta_max], roll off outside
    dmin, dmax = p["delta_min"], p["delta_max"]
    if row["delta"] is None: delta_score = 0.0
    else:
        # distance from center normalized to range
        center = 0.5*(dmin+dmax)
        half   = max(1e-6, 0.5*(dmax-dmin))
        delta_score = clamp01(1.0 - abs(row["delta"]-center)/half)

    # dte window score
    tmin, tmax = p["dte_min"], p["dte_max"]
    dte = row["dte"] or 0
    if dte < tmin:
        dte_score = max(0.0, 1.0 - (tmin - dte)/tmin)
    elif dte > tmax:
        dte_score = max(0.0, 1.0 - (dte - tmax)/tmax)
    else:
        dte_score = 1.0

    # moneyness preference: prefer within [itm_min, itm_max] (positive = ITM)
    mmin, mmax = p["prefer_itm_moneyness_min_pct"], p["prefer_itm_moneyness_max_pct"]
    mn = row["moneyness_pct"]
    if mn is None:
        mny_score = 0.5  # neutral if missing
    else:
        if mn < mmin:            # OTM or barely ITM → linearly lower
            mny_score = max(0.0, 1.0 - (mmin - mn)/max(1.0, mmin))
        elif mn > mmax:          # too deep ITM → linearly lower
            mny_score = max(0.0, 1.0 - (mn - mmax)/max(1.0, mmax))
        else:
            mny_score = 1.0

    # IV penalty (soft)
    ivr = row["iv_rank"]
    iv_pen = 0.0
    if ivr is not None and ivr > p["deprioritize_high_iv_rank_above"]:
        iv_pen = min(1.0, (ivr - p["deprioritize_high_iv_rank_above"]) / 50.0)

    total = (w["delta"]*delta_score +
             w["dte_window"]*dte_score +
             w["moneyness_pref"]*mny_score -
             w["iv_penalty"]*iv_pen)

    return max(0.0, total)

# --------- Main ----------
def main():
    # policy
    policy = load_policy(POLICY_YML)
    p = policy["leap_lifecycle"]

    # load CSV
    src = LEAP_CSV_DEFAULT
    if len(sys.argv) > 1 and sys.argv[1] not in ("-h","--help"):
        src = sys.argv[1]
    rows = read_leap_csv(src)
    if not rows:
        print("[ERROR] No call rows parsed from leap-latest.csv", file=sys.stderr)
        sys.exit(1)

    # score + filter inside delta range (hard) and dte >= 180 (soft default)
    filtered = []
    for r in rows:
        if r["delta"] is None: 
            continue
        if not (p["delta_min"] - 0.05 <= r["delta"] <= p["delta_max"] + 0.05):
            continue
        if r["dte"] < 180:   # hard floor so we don't recommend near-dated as “replacement LEAPs”
            continue
        s = score_row(r, policy)
        r2 = r.copy()
        r2["score"] = round(s, 4)
        filtered.append(r2)

    filtered.sort(key=lambda x: (-x["score"], -x["dte"], -x["delta"]))
    topN = filtered[:p["target_count"]]

    # pretty print
    print("\n" + "─"*70)
    print("LEAP LIFECYCLE — Replacement candidates".center(70))
    print("─"*70 + "\n")
    if not topN:
        print("(no candidates passed policy filters)\n")
    else:
        print(" Sym    Exp Date     DTE   Δ      ITM%   Ask     IVR   Score")
        print(" ----   ----------   ---   -----  -----  ------  ----  ------")
        for r in topN:
            sym = r["symbol"][:6].ljust(6)
            exp = r["exp"]
            dte = f"{r['dte']:>4}"
            delt= f"{r['delta']:.3f}".rjust(5)
            mny = f"{(r['moneyness_pct'] if r['moneyness_pct'] is not None else 0):+.1f}%".rjust(6)
            ask = f"{r['ask']:.2f}".rjust(6)
            ivr = f"{(r['iv_rank'] if r['iv_rank'] is not None else 0):.1f}".rjust(5)
            sc  = f"{r['score']:.3f}".rjust(6)
            print(f" {sym}  {exp}  {dte}  {delt}  {mny}  {ask}  {ivr}  {sc}")
        print()

    # ensure out dir
    os.makedirs(OUT_DIR, exist_ok=True)
    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    payload = {
        "generated_at": now_iso,
        "policy": {
            "dte_min": p["dte_min"],
            "dte_max": p["dte_max"],
            "delta_min": p["delta_min"],
            "delta_max": p["delta_max"],
            "prefer_itm_moneyness_min_pct": p["prefer_itm_moneyness_min_pct"],
            "prefer_itm_moneyness_max_pct": p["prefer_itm_moneyness_max_pct"],
            "deprioritize_high_iv_rank_above": p["deprioritize_high_iv_rank_above"],
            "target_count": p["target_count"]
        },
        "count": len(topN),
        "top": topN
    }

    # write JSON
    with open(os.path.join(OUT_DIR, "leap_renewals.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # write YAML
    with open(os.path.join(OUT_DIR, "leap_renewals.yml"), "w", encoding="utf-8") as f:
        f.write(yaml_dump(payload) + "\n")

    print(f"[OK] Wrote {os.path.join(OUT_DIR, 'leap_renewals.yml')}")
    print(f"[OK] Wrote {os.path.join(OUT_DIR, 'leap_renewals.json')}")

if __name__ == "__main__":
    main()