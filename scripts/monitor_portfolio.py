#!/usr/bin/env python3
# scripts/monitor_portfolio.py
# Single-file version (parser + rules embedded). No extra local imports.

from __future__ import annotations
import os, sys, re, csv, json, math, time, datetime as dt
from pathlib import Path
from typing import List, Dict, Any, Optional

try:
    import yaml  # pip install pyyaml
except ImportError:
    print("Please install pyyaml in your venv: pip install pyyaml", file=sys.stderr)
    sys.exit(1)

# --------------------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUTPUTS = ROOT / "outputs"
TICKETS = OUTPUTS / "tickets"
for p in (OUTPUTS, TICKETS):
    p.mkdir(parents=True, exist_ok=True)

MARKET_STATE_FILE_Y = OUTPUTS / "market_state.yml"
MARKET_STATE_FILE_J = OUTPUTS / "market_state.json"

# --------------------------------------------------------------------------------------
# Market state
# --------------------------------------------------------------------------------------
def load_market_state() -> Dict[str, Any]:
    for p in (MARKET_STATE_FILE_Y, MARKET_STATE_FILE_J):
        if p.exists():
            try:
                return yaml.safe_load(p.read_text()) if p.suffix == ".yml" else json.loads(p.read_text())
            except Exception:
                pass
    return {}

# --------------------------------------------------------------------------------------
# Load staged screeners (produced by ingest_latest.py)
# --------------------------------------------------------------------------------------
def _load_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append({(k or "").strip(): (v.strip() if isinstance(v, str) else v) for k, v in r.items()})
    return rows

def load_screeners() -> Dict[str, List[Dict[str, Any]]]:
    return {
        "covered_call": _load_csv(DATA / "covered_call-latest.csv"),
        "csp":          _load_csv(DATA / "csp-latest.csv"),
        "vertical_bc":  _load_csv(DATA / "vertical_bull_call-latest.csv"),
        "vertical_bp":  _load_csv(DATA / "vertical_bull_put-latest.csv"),
        "diagonal":     _load_csv(DATA / "long_call_diagonal-latest.csv"),
        "iron_condor":  _load_csv(DATA / "iron_condor-latest.csv"),
        "leap":         _load_csv(DATA / "leap-latest.csv"),
    }

# --------------------------------------------------------------------------------------
# Position parser (permissive; handles tasty/IBKR-ish pastes)
# --------------------------------------------------------------------------------------
LEG_RE   = re.compile(r'(?P<exp>\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{2,4})\s+(?P<strike>\d+(\.\d+)?)\s*(?P<cp>[cCpP])')
SHARE_RE = re.compile(r'(?P<qty>[+-]?\d+)\s*(shares?|sh|stk|stock)', re.I)
CTR_RE   = re.compile(r'(?P<qty>[+-]?\d+)\s*(contracts?|ctrs?|cntr?)', re.I)
PRICE_RE = re.compile(r'@?\s*(price|mark)?\s*\$?(?P<px>\d+(\.\d+)?)', re.I)
PL_RE    = re.compile(r'P/?L\s*(=|:)?\s*(?P<pl>-?\$?\d+(\.\d+)?)', re.I)

def _norm_date(s: str) -> str:
    s = s.strip()
    if "-" in s:
        return s
    if "/" in s:
        mm, dd, yy = s.split("/")
        yy = yy if len(yy) == 4 else ("20" + yy[-2:])
        return f"{yy}-{int(mm):02d}-{int(dd):02d}"
    return s

def parse_positions_text(text: str) -> List[Dict[str, Any]]:
    blocks = [b for b in re.split(r"\n\s*\n", text.strip()) if b.strip()]
    out: List[Dict[str, Any]] = []

    for b in blocks:
        lines = [ln.strip() for ln in b.splitlines() if ln.strip()]
        if not lines:
            continue
        first = lines[0]
        sym = re.split(r"[\s,;]+", first)[0].upper().strip(".$")
        pos: Dict[str, Any] = {"symbol": sym, "shares": 0, "legs": [], "strategy": "unknown", "raw": b}

        for ln in lines:
            m = SHARE_RE.search(ln)
            if m:
                pos["shares"] += int(m.group("qty"))

            # legs
            for mm in LEG_RE.finditer(ln):
                exp = _norm_date(mm.group("exp"))
                strike = float(mm.group("strike"))
                cp = mm.group("cp").upper()
                # crude side inference from +/- before match
                prefix = ln[:mm.start()]
                side = "short" if "-" in prefix else ("long" if "+" in prefix else "short")
                pos["legs"].append({"type": "call" if cp == "C" else "put", "side": side, "strike": strike, "exp": exp})

            # optional mark / p&l
            m2 = PRICE_RE.search(ln)
            if m2 and "price" not in pos:
                pos["price"] = float(m2.group("px"))
            m3 = PL_RE.search(ln)
            if m3 and "pl" not in pos:
                try:
                    pos["pl"] = float(m3.group("pl").replace("$", ""))
                except:
                    pass

        # classify strategy
        calls = [l for l in pos["legs"] if l["type"]=="call"]
        puts  = [l for l in pos["legs"] if l["type"]=="put"]
        sc    = [l for l in calls if l["side"]=="short"]
        sp    = [l for l in puts  if l["side"]=="short"]
        lc    = [l for l in calls if l["side"]=="long"]
        lp    = [l for l in puts  if l["side"]=="long"]

        if pos["shares"] > 0 and sc:
            pos["strategy"] = "covered_call"
        elif pos["shares"] == 0 and sp and not (lc or lp or sc):
            pos["strategy"] = "csp"
        elif lc and sc:
            # PMCC or diagonal (different expirations implies diagonal/pmcc)
            exps_l = {l["exp"] for l in lc}
            exps_s = {l["exp"] for l in sc}
            pos["strategy"] = "pmcc" if (exps_l and exps_s and list(exps_l)[0] != list(exps_s)[0]) else "diagonal"
        elif lc and lp and sc and sp:
            pos["strategy"] = "iron_condor"
        elif (lc and sc) or (lp and sp):
            pos["strategy"] = "vertical"
        elif lc and not (sc or lp or sp):
            pos["strategy"] = "long_call"
        else:
            pos["strategy"] = "unknown"

        out.append(pos)
    return out

def pretty_position(pos: Dict[str, Any]) -> str:
    parts = [pos["symbol"], pos.get("strategy","?")]
    if pos.get("shares"):
        parts.append(f"shares={pos['shares']}")
    legs = pos.get("legs",[])
    if legs:
        lp = "; ".join([f"{l['side'][0].upper()} {l['exp']} {l['strike']}{'C' if l['type']=='call' else 'P'}" for l in legs])
        parts.append(f"legs=[{lp}]")
    return " | ".join(parts)

# --------------------------------------------------------------------------------------
# Tastytrade-ish management rules (pragmatic, heuristic)
# --------------------------------------------------------------------------------------
def _min_dte(legs: List[Dict[str, Any]]) -> int:
    if not legs:
        return 999
    today = dt.date.today()
    dtes = []
    for l in legs:
        try:
            y,m,d = map(int, l["exp"].split("-"))
            dtes.append((dt.date(y,m,d) - today).days)
        except Exception:
            pass
    return min(dtes) if dtes else 999

def evaluate_position(pos: Dict[str, Any], market: Dict[str, Any]) -> Dict[str, Any]:
    strat = pos.get("strategy","unknown")
    dte = _min_dte(pos.get("legs",[]))
    regime = (market or {}).get("overall_regime", "Unknown")

    # Coarse bias: in Risk-Off be faster to roll/close
    tighten = (regime == "Risk-Off")

    if strat == "covered_call":
        if dte <= 21:
            return {"action":"roll","why":f"DTE={dte} ≤ 21 → roll out (maintain coverage, harvest extrinsic)."}
        sc = [l for l in pos.get("legs",[]) if l["type"]=="call" and l["side"]=="short"]
        if sc:
            return {"action":"roll","why":"Maintain short call: roll up/out if delta high or assignment risk rises."}
        return {"action":"hold","why":"Covered call: no immediate trigger."}

    if strat == "csp":
        if dte <= 21:
            return {"action":"roll","why":f"DTE={dte} ≤ 21 → roll to next monthly; preserve cushion."}
        return {"action":"hold","why":"CSP: no immediate trigger."}

    if strat == "pmcc":
        if dte <= 21:
            return {"action":"roll","why":"PMCC: roll short call before ~21 DTE to preserve extrinsic."}
        return {"action":"hold","why":"PMCC: short not near window; hold."}

    if strat == "diagonal":
        if dte <= 21:
            return {"action":"roll","why":"Diagonal: manage short leg around 21 DTE."}
        return {"action":"hold","why":"Diagonal: hold."}

    if strat == "vertical":
        if dte <= 21:
            return {"action":"close","why":"Vertical (defined risk): tidy up near 21 DTE."}
        return {"action":"hold","why":"Vertical: hold."}

    if strat == "iron_condor":
        if dte <= 21:
            return {"action":"close","why":"IC: reduce tail risk near 21 DTE."}
        return {"action":"hold","why":"IC: hold."}

    if strat == "long_call":
        if dte <= 21:
            return {"action":"close","why":"Long call: nearing expiry; close/roll per thesis."}
        return {"action":"hold","why":"Long call: hold."}

    return {"action":"hold","why":"Unknown strategy; no rule fired."}

def nearest_screener_candidates(pos: Dict[str, Any], action: str, screeners: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    sym = pos.get("symbol","").upper()
    strat = pos.get("strategy")

    def sym_match(pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        same = [r for r in pool if (r.get("Symbol") or r.get("symbol") or "").upper() == sym]
        return same or pool

    if strat == "covered_call":
        return sym_match(screeners.get("covered_call",[]))
    if strat == "csp":
        return sym_match(screeners.get("csp",[]))
    if strat == "pmcc":
        # choose short-call from covered_call list by same symbol
        return sym_match(screeners.get("covered_call",[]))
    if strat == "diagonal":
        return sym_match(screeners.get("diagonal",[]))
    if strat == "vertical":
        pool = (screeners.get("vertical_bp",[]) or []) + (screeners.get("vertical_bc",[]) or [])
        return sym_match(pool)
    if strat == "iron_condor":
        return sym_match(screeners.get("iron_condor",[]))
    return []

# --------------------------------------------------------------------------------------
# I/O helpers
# --------------------------------------------------------------------------------------
def header(title: str) -> None:
    print("\n" + "="*78)
    print(title)
    print("="*78)

def prompt_paste() -> str:
    header("PASTE YOUR POSITIONS — end with a single '.' on its own line")
    lines = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if line.strip() == ".":
            break
        lines.append(line)
    return "\n".join(lines).strip()

def write_ticket(symbol: str, action: str, details: Dict[str, Any]) -> Path:
    ts = dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    name = f"MON-{ts}-{symbol}-{action.replace(' ','_')}.yml"
    path = TICKETS / name
    with path.open("w") as f:
        yaml.safe_dump({
            "type": "monitor_action",
            "timestamp": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "symbol": symbol,
            "action": action,
            "details": details,
        }, f, sort_keys=False)
    return path

def append_summary(entries: List[str]) -> None:
    out = OUTPUTS / "monitor_summary.md"
    stamp = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    with out.open("a") as f:
        f.write(f"\n## Monitor Session {stamp}\n\n")
        for e in entries:
            f.write(f"- {e}\n")

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main():
    header("MONITOR — Portfolio evaluation & guided adjustments")
    mk = load_market_state()
    if mk:
        print(f"Market: Regime={mk.get('overall_regime','Unknown')} Trend={mk.get('trend_bias','?')} Vol={mk.get('volatility','?')}")
    else:
        print("Market: (no market_state found)")

    blob = prompt_paste()
    if not blob:
        print("No positions pasted. Exiting.")
        return

    positions = parse_positions_text(blob)
    if not positions:
        print("Could not parse any positions. Exiting.")
        return

    screeners = load_screeners()
    notes: List[str] = []

    for i, pos in enumerate(positions, 1):
        print("\n" + "-"*60)
        print(f"[{i}/{len(positions)}] {pretty_position(pos)}")
        verdict = evaluate_position(pos, mk)
        action  = verdict.get("action","hold")
        why     = verdict.get("why","")
        print(f"Action: {action.upper()} — {why}")

        chosen: Dict[str, Any] = {}
        if action != "hold":
            cands = nearest_screener_candidates(pos, action, screeners)[:5]
            if cands:
                print("\nCandidates (top 5):")
                for idx, c in enumerate(cands, 1):
                    exp  = c.get("Exp Date") or c.get("Exp") or c.get("exp") or "?"
                    strike = c.get("Strike") or c.get("Leg1 Strike") or c.get("strike") or "?"
                    side = c.get("Type") or c.get("Type1") or c.get("type") or "?"
                    quote = c.get("Bid") or c.get("Ask1") or c.get("bid") or c.get("ask") or "?"
                    print(f"  {idx}) {exp:>10}  {str(strike):>7} {side:<4}  quote={quote}")
                ans = input("Pick candidate # (ENTER to skip): ").strip()
                if ans:
                    try:
                        chosen = cands[int(ans)-1]
                    except Exception:
                        chosen = {}

            # ticket either way if action != hold
            pth = write_ticket(pos["symbol"], action, {"position": pos, "verdict": verdict, "chosen_candidate": chosen})
            print(f"[ticket] wrote {pth}")
            notes.append(f"{pos['symbol']}: {action} — {why}")
        else:
            notes.append(f"{pos['symbol']}: hold — {why or 'no trigger'}")

    append_summary(notes)
    print("\nDone. Summary → outputs/monitor_summary.md")
    print("Tickets (if any) → outputs/tickets/")

if __name__ == "__main__":
    main()