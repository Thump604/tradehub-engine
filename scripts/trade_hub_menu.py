#!/usr/bin/env python3
"""
trade_hub_menu.py

Single entry point for:
- Market snapshot (from outputs/market_state.yml|json if present)
- Monitor flow (by strategy): shows cards, lets you paste your current position for analysis,
  suggests roll/adjust actions using today's Barchart-based suggestions.
- Trade flow (by strategy): shows ranked suggestions and lets you create a ticket.
- Top 3 Overall view.
- Toggle hide-taken / change window by environment or config.
- Optional: re-run rankers (SAVE_SUGGESTIONS=1).

Usage:
  python -m scripts.trade_hub_menu
Env overrides:
  SUGGESTIONS_DIR=/abs/path
  FRESH_MIN=600
  HIDE_TAKEN=false|true
  PREFER_JSON=1|0
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    import yaml
except Exception:
    yaml = None

# ---------- Config ----------
DEFAULT_SUGG_DIR = Path("outputs")
DEFAULT_FRESH_MIN = 600
DEFAULT_HIDE_TAKEN = True
DEFAULT_PREFER_JSON = True
ENGINE_YAML = Path("engine.yaml")

STRATEGIES = ["covered_call", "csp", "pmcc", "vertical", "diagonal", "iron_condor"]
STRAT_FILEKEY = {
    "covered_call": "covered_call",
    "csp": "csp",
    "pmcc": "pmcc",
    "vertical": "vertical",
    "diagonal": "diagonal",
    "iron_condor": "iron_condor",
}

# Colors/icons
STRAT_ICON = {
    "covered_call": "🟦",
    "csp": "🟩",
    "pmcc": "🟪",
    "vertical": "🟨",
    "diagonal": "🟧",
    "iron_condor": "⬜",
}
FLAG_COLOR = {
    "GREEN": "🟢",
    "YELLOW": "🟡",
    "RED": "🔴",
}

# ---------- Datatypes ----------
@dataclass
class EngineConfig:
    suggestions_dir: Path
    freshness_min: int
    hide_taken_default: bool
    prefer_json: bool
    market_state_path: Path | None

# ---------- Utils ----------
def _bool_env(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).lower() in ("1","true","yes","y","on")

def _int_env(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default

def load_engine_config() -> EngineConfig:
    sugg_dir = Path(os.getenv("SUGGESTIONS_DIR", str(DEFAULT_SUGG_DIR)))
    fresh_min = _int_env("FRESH_MIN", DEFAULT_FRESH_MIN)
    hide_taken = _bool_env("HIDE_TAKEN", DEFAULT_HIDE_TAKEN)
    prefer_json = _bool_env("PREFER_JSON", DEFAULT_PREFER_JSON)
    market_state_path = None

    if ENGINE_YAML.exists():
        try:
            raw = yaml.safe_load(ENGINE_YAML.read_text()) if yaml else None
            if isinstance(raw, dict):
                hub = raw.get("hub", {})
                market = raw.get("market", {})
                sugg_dir = Path(hub.get("suggestions_dir", sugg_dir))
                fresh_min = int(hub.get("freshness_min", fresh_min))
                hide_taken = bool(hub.get("hide_taken_default", hide_taken))
                prefer_json = bool(hub.get("prefer_json", prefer_json))
                if "state_file" in (market or {}):
                    market_state_path = Path(market["state_file"])
        except Exception:
            pass

    return EngineConfig(
        suggestions_dir=sugg_dir,
        freshness_min=fresh_min,
        hide_taken_default=hide_taken,
        prefer_json=prefer_json,
        market_state_path=market_state_path,
    )

def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    s = s.strip().replace(" ", "T")
    if s.endswith("Z"):
        fmt = "%Y-%m-%dT%H:%M:%SZ"
    else:
        fmt = "%Y-%m-%dT%H:%M:%S"
    try:
        return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _age_minutes(dtg: datetime | None) -> int | None:
    if not dtg:
        return None
    return int((datetime.now(timezone.utc) - dtg).total_seconds() // 60)

def _load_yaml_or_json(path: Path) -> dict | None:
    try:
        if path.suffix.lower() in (".yaml",".yml") and yaml:
            return yaml.safe_load(path.read_text())
        else:
            return json.loads(path.read_text())
    except Exception:
        return None

def _prefer_file(base: Path, prefer_json: bool) -> Path | None:
    j = base.with_suffix(".json")
    y = base.with_suffix(".yml")
    if prefer_json:
        if j.exists(): return j
        if y.exists(): return y
    else:
        if y.exists(): return y
        if j.exists(): return j
    return None

def _safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _sym(x: str | None) -> str:
    return (x or "").strip().upper()

def load_suggestions(cfg: EngineConfig, hide_taken: bool | None = None, fresh_min: int | None = None)\
        -> Tuple[Dict[str, List[dict]], List[dict], Dict[str, int], Dict[str, int]]:
    """
    Returns:
      per_strategy: {strategy: [records]}
      overall:      [records]
      ages_min:     {strategy: minutes or -1}
      counts:       {strategy: count}
    """
    base = cfg.suggestions_dir
    prefer_json = cfg.prefer_json
    fresh_cut = cfg.freshness_min if fresh_min is None else fresh_min
    hide = cfg.hide_taken_default if hide_taken is None else hide_taken

    per: Dict[str, List[dict]] = {}
    ages: Dict[str, int] = {}
    counts: Dict[str, int] = {}
    overall: List[dict] = []

    for strat in STRATEGIES:
        key = STRAT_FILEKEY[strat]
        basefile = base / f"{key}_suggestions"
        fpath = _prefer_file(basefile, prefer_json)
        per[strat] = []
        ages[strat] = -1
        counts[strat] = 0
        if not fpath or not fpath.exists():
            continue

        blob = _load_yaml_or_json(fpath)
        if not isinstance(blob, dict):
            continue

        generated_at = _parse_dt(blob.get("generated_at") or blob.get("Generated") or blob.get("generated"))
        age = _age_minutes(generated_at) or 99999
        ages[strat] = age

        top = blob.get("top") or []
        if not isinstance(top, list):
            top = []

        # normalize
        norm: List[dict] = []
        for i, rec in enumerate(top):
            if not isinstance(rec, dict):
                continue
            rec = dict(rec)  # copy
            rec.setdefault("strategy", strat)
            rec["symbol"] = _sym(rec.get("symbol") or rec.get("Symbol"))
            rec["flag"] = (rec.get("flag") or rec.get("Flag") or "YELLOW").upper()
            rec["score"] = _safe_float(rec.get("score"), default=_safe_float(rec.get("Score"), 0.0))
            rec["exp"] = rec.get("exp") or rec.get("Exp") or rec.get("exp_date") or rec.get("Exp Date") or "?"
            rec["strike"] = str(rec.get("strike") or rec.get("Strike") or rec.get("call_strike") or rec.get("Call Strike") or "-")
            rec["taken"] = bool(rec.get("taken", False))
            # id (fallback)
            rec.setdefault("id", f"{strat}:{rec['symbol']}:{rec['exp']}:{rec['strike']}:{i}")
            norm.append(rec)

        # freshness/hide filters
        if age <= fresh_cut:
            if hide:
                norm = [r for r in norm if not r.get("taken", False)]
            per[strat] = norm
            counts[strat] = len(norm)

    # overall (top few from each)
    for strat in STRATEGIES:
        overall.extend(per.get(strat, []))
    overall.sort(key=lambda r: float(r.get("score") or 0.0), reverse=True)

    return per, overall, ages, counts

def load_market_state(cfg: EngineConfig) -> dict:
    # try engine.yaml market.state_file
    if cfg.market_state_path and cfg.market_state_path.exists():
        return _load_yaml_or_json(cfg.market_state_path) or {}
    # fallbacks in outputs/
    yml = Path("outputs/market_state.yml")
    jsn = Path("outputs/market_state.json")
    if yml.exists():
        return _load_yaml_or_json(yml) or {}
    if jsn.exists():
        return _load_yaml_or_json(jsn) or {}
    return {}

def color_for_day(state: dict) -> tuple[str, str]:
    reg = (state.get("overall_regime") or "").lower()
    trn = (state.get("trend_bias") or "").lower()
    vol = (state.get("volatility") or "").lower()
    # simple rule set
    if "risk-off" in reg or "down" in trn or "high" in vol:
        return "RED", "Risk-Off/Downtrend or elevated vol → favor hedged/mean-revert/defined-risk."
    if "risk-on" in reg and "up" in trn and ("low" in vol or "medium" in vol):
        return "GREEN", "Risk-On/Uptrend with tame vol → favor directional income & trend-following."
    return "YELLOW", "Mixed → balance directional vs. defined-risk."

def print_header(cfg: EngineConfig, per: Dict[str, List[dict]], overall: List[dict], ages: Dict[str, int]):
    print("\n" + "═"*70)
    print("TRADE HUB — Interactive")
    print("═"*70)
    print(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}")

    state = load_market_state(cfg)
    regime = state.get("overall_regime", "Unknown")
    trend  = state.get("trend_bias", "?")
    vola   = state.get("volatility", "?")
    color, tip = color_for_day(state)
    print(f"\nMarket: Regime {regime} | Trend {trend} | Vol {vola}")
    print(f"Day Color: {color}  — {tip}")
    print("Focus today: covered_call, csp, diagonal, verticals\n")

    # Top 3 Overall
    top3 = overall[:3]
    print("Top 3 Overall (fresh, not taken)")
    if not top3:
        print("(none)")
    else:
        for i, r in enumerate(top3, 1):
            ic = STRAT_ICON.get(r["strategy"], "•")
            fl = FLAG_COLOR.get((r.get("flag") or "").upper(), "")
            exp = r.get("exp") or "?"
            strike = r.get("strike") or "-"
            print(f" {i}) {ic} {r['strategy']:<12} {_sym(r['symbol']):<6} {exp:<12} {strike:>8}  score={r['score']:>6.3f} flag={(r.get('flag') or '').upper()} {fl}")
    print("\nTop 3 by Strategy")
    print("(freshness ≤ current window; hide taken reflects current toggle)\n")
    for strat in STRATEGIES:
        name = strat.upper()
        ic = STRAT_ICON.get(strat, "•")
        age = ages.get(strat, -1)
        age_txt = f"{age}m" if age >= 0 else "N/A"
        print(f"{ic} {name:12} age {age_txt:>4}")
        picks = per.get(strat, [])[:3]
        if not picks:
            print("  (none)")
        else:
            for i, r in enumerate(picks, 1):
                exp = r.get("exp") or "?"
                strike = r.get("strike") or "-"
                fl = FLAG_COLOR.get((r.get("flag") or "").upper(), "")
                print(f"   {i}) {_sym(r['symbol']):<6} {exp:<12} {strike:>8}  score={r['score']:>6.3f} flag={(r.get('flag') or '').upper()} {fl}")
        print()

def pick_strategy(prompt="Choose a strategy:") -> str | None:
    print(prompt)
    for i, s in enumerate(STRATEGIES, 1):
        print(f"  {i}) {s}")
    try:
        sel = input("Select (or ENTER to cancel): ").strip()
    except EOFError:
        return None
    if not sel:
        return None
    try:
        idx = int(sel)
        if 1 <= idx <= len(STRATEGIES):
            return STRATEGIES[idx-1]
    except Exception:
        pass
    return None

def print_card(rec: dict):
    strat = rec.get("strategy")
    ic = STRAT_ICON.get(strat, "•")
    sym = _sym(rec.get("symbol"))
    exp = rec.get("exp") or "?"
    strike = rec.get("strike") or "-"
    flag = (rec.get("flag") or "YELLOW").upper()
    flag_ico = FLAG_COLOR.get(flag, "")
    score = rec.get("score")
    taken = bool(rec.get("taken", False))
    extras = []
    for k in ("bid","delta","otm_pct","ann","Ann%","annual","extr","Cov"):
        if k in rec and rec[k] not in (None, "", "-"):
            extras.append(f"  {k}: {rec[k]}")
    print("-"*60)
    print(f"{ic} {strat.upper():<12} {sym:<6}  exp={exp:<10}  strike={strike}")
    print(f"score={score}  flag={flag} {flag_ico}  taken={taken}")
    if extras:
        for line in extras[:6]:
            print(line)
    print("-"*60)

def persist_taken(cfg: EngineConfig, strat: str, rec_id: str, new_val: bool):
    filekey = STRAT_FILEKEY[strat]
    basefile = cfg.suggestions_dir / f"{filekey}_suggestions"
    fpath = _prefer_file(basefile, cfg.prefer_json)
    if not fpath or not fpath.exists():
        return
    blob = _load_yaml_or_json(fpath)
    if not isinstance(blob, dict):
        return
    top = blob.get("top") or []
    changed = False
    for r in top:
        rid = r.get("id") or ""
        if rid == rec_id:
            r["taken"] = bool(new_val)
            changed = True
            break
    if changed:
        if fpath.suffix.lower() == ".json":
            fpath.write_text(json.dumps(blob, indent=2))
        else:
            if yaml:
                fpath.write_text(yaml.safe_dump(blob, sort_keys=False))
        print(f"[saved] updated taken={new_val} for {rec_id} → {fpath.name}")

def write_ticket(rec: dict) -> Path:
    tickets_dir = Path("outputs") / "tickets"
    tickets_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    strat = rec.get("strategy","unknown").upper()
    sym = _sym(rec.get("symbol") or "UNK")
    path = tickets_dir / f"{ts}-{strat}-{sym}.md"
    body = []
    body.append(f"# Ticket — {strat} — {sym}")
    body.append("")
    body.append(f"- Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}")
    body.append(f"- Suggestion ID: `{rec.get('id','')}`")
    body.append("")
    body.append("```json")
    body.append(json.dumps(rec, indent=2))
    body.append("```")
    path.write_text("\n".join(body))
    return path

# ---- Position parsing & analysis (for Monitor flow) ----
MONTHS = {
    "jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
    "jul":"07","aug":"08","sep":"09","sept":"09","oct":"10","nov":"11","dec":"12"
}
def parse_position_blob(blob: str) -> dict:
    """
    Lenient parser for broker pastes.
    Returns: {"symbol": "NVDA", "shares": 100, "options":[
        {"side":"short","type":"call","exp":"2025-09-19","strike":"190.00","qty":1},
        {"side":"long","type":"call","exp":"2026-06-18","strike":"140.00","qty":1}
    ]}
    """
    lines = [l.strip() for l in (blob or "").splitlines() if l.strip()]
    pos = {"symbol": None, "shares": 0, "options": []}

    # guess symbol
    for ln in lines:
        m = re.match(r"^([A-Z]{1,6})(\s|,|$)", ln)
        if m:
            pos["symbol"] = _sym(m.group(1))
            break

    share_re = re.compile(r"(?P<sym>[A-Z]{1,6})?\s*(?P<qty>\d+)\s*(shares|sh)", re.I)
    opt_re = re.compile(
        r"(?P<sym>[A-Z]{1,6})?\s*"
        r"(?:(?P<mon>[A-Za-z]{3,5})\s*(?P<day>\d{1,2})\s*(?P<year>\d{2,4})|(?P<iso>\d{4}-\d{2}-\d{2}))\s+"
        r"(?P<strike>\d+(?:\.\d+)?)\s*"
        r"(?P<cp>Call|Put)\s*"
        r"(?P<sign>[+-])?(?P<qty>\d+)?",
        re.I
    )

    for ln in lines:
        ms = share_re.search(ln)
        if ms:
            pos["shares"] += int(ms.group("qty"))
            if not pos["symbol"] and ms.group("sym"):
                pos["symbol"] = _sym(ms.group("sym"))
            continue

        mo = opt_re.search(ln)
        if mo:
            sym = mo.group("sym")
            if sym and not pos["symbol"]:
                pos["symbol"] = _sym(sym)
            iso = mo.group("iso")
            if iso:
                exp = iso
            else:
                mon = (mo.group("mon") or "").lower()
                day = mo.group("day") or "1"
                year = mo.group("year") or "2099"
                if len(year) == 2:
                    year = "20" + year
                mm = MONTHS.get(mon, "01")
                dd = f"{int(day):02d}"
                exp = f"{year}-{mm}-{dd}"
            strike = mo.group("strike")
            cp = mo.group("cp").lower()
            sign = mo.group("sign") or "+"
            qty = int(mo.group("qty") or "1")
            side = "short" if sign == "-" else "long"
            pos["options"].append({
                "side": side, "type": cp, "exp": exp, "strike": strike, "qty": qty
            })
            continue

        mf = re.match(r"(?P<sym>[A-Z]{1,6})\s+(?P<qty>\d+)$", ln)
        if mf:
            if not pos["symbol"]:
                pos["symbol"] = _sym(mf.group("sym"))
            pos["shares"] += int(mf.group("qty"))

    if not pos["symbol"]:
        pos["symbol"] = "?"
    return pos

def analyze_position_against_suggestions(pos: dict, per: Dict[str, List[dict]]) -> str:
    """
    For Covered Call:
      - If shares > 0 and an existing short call is detected, propose roll choices (top 3).
      - Else propose opening a short call (top 3).
    For PMCC:
      - If long call (LEAP) present, propose short call choices (top 3) from pmcc list.

    Returns a printable markdown-ish block.
    """
    sym = _sym(pos.get("symbol"))
    shares = int(pos.get("shares") or 0)
    opts = pos.get("options") or []

    # detect existing short call (for CC)
    existing_short_call = None
    for o in opts:
        if o.get("type") == "call" and o.get("side") == "short":
            existing_short_call = o
            break

    out = []
    out.append(f"Position parsed → **{sym}** | shares={shares} | options={opts}")

    # Covered Call suggestions for this symbol
    cc = [r for r in per.get("covered_call", []) if _sym(r.get("symbol")) == sym]
    cc.sort(key=lambda r: float(r.get("score") or 0), reverse=True)
    cc = cc[:3]

    # PMCC suggestions for this symbol
    pm = [r for r in per.get("pmcc", []) if _sym(r.get("symbol")) == sym]
    pm.sort(key=lambda r: float(r.get("score") or 0), reverse=True)
    pm = pm[:3]

    if shares > 0:
        if existing_short_call:
            out.append("\n**Covered Call — Roll candidates (top 3 today):**")
        else:
            out.append("\n**Covered Call — Open short-call candidates (top 3 today):**")

        if not cc:
            out.append("  (none found today for this symbol)")
        else:
            for i, r in enumerate(cc, 1):
                out.append(f"  {i}) exp={r.get('exp','?')}  strike={r.get('strike','-')}  bid={r.get('bid','?')}  score={r.get('score')}")
    else:
        out.append("\n(Own 0 shares → skipping Covered Call suggestions.)")

    # PMCC
    has_long_call = any(o for o in opts if o.get("type")=="call" and o.get("side")=="long")
    if has_long_call:
        out.append("\n**PMCC — Short-call leg candidates (top 3 today):**")
        if not pm:
            out.append("  (none found today for this symbol)")
        else:
            for i, r in enumerate(pm, 1):
                out.append(f"  {i}) short exp={r.get('s_exp','?') or r.get('exp','?')}  short strike={r.get('s_strike','-') or r.get('strike','-')}  score={r.get('score')}")
    else:
        out.append("\n(No long call detected → PMCC suggestions only if you have/plan a LEAP.)")

    return "\n".join(out)

# ---- UI flows ----
def monitor_flow(cfg: EngineConfig, per: Dict[str, List[dict]]):
    strat = pick_strategy("Monitor by strategy — choose a strategy:")
    if not strat:
        return
    items = per.get(strat, [])
    if not items:
        print("(no items)")
        return
    idx = 0
    while True:
        rec = items[idx]
        print_card(rec)
        print("[n] next  [p] prev  [t] toggle taken  [a] analyze my position  [w] ticket  [ENTER] back")
        try:
            choice = input("> ").strip().lower()
        except EOFError:
            return
        if choice == "":
            return
        elif choice == "n":
            idx = (idx + 1) % len(items)
        elif choice == "p":
            idx = (idx - 1) % len(items)
        elif choice == "t":
            new_val = not bool(rec.get("taken", False))
            rec["taken"] = new_val
            persist_taken(cfg, strat, rec.get("id",""), new_val)
        elif choice == "w":
            path = write_ticket(rec)
            print(f"[ticket] {path}")
        elif choice == "a":
            print("\nPaste your broker position for this symbol (end with an empty line):")
            lines = []
            while True:
                try:
                    ln = input()
                except EOFError:
                    ln = ""
                if not ln:
                    break
                lines.append(ln)
            blob = "\n".join(lines)
            pos = parse_position_blob(blob)
            print()
            print(analyze_position_against_suggestions(pos, per))
            print()
        else:
            print("(unrecognized)")

def trade_flow(cfg: EngineConfig, per: Dict[str, List[dict]]):
    strat = pick_strategy("Trade from a strategy — choose a strategy:")
    if not strat:
        return
    items = per.get(strat, [])
    if not items:
        print("(no items)")
        return
    # list
    print(f"\n{strat.upper()} — top {min(10,len(items))} candidates")
    for i, r in enumerate(items[:10], 1):
        exp = r.get("exp") or "?"
        strike = r.get("strike") or "-"
        fl = (r.get("flag") or "").upper()
        ico = FLAG_COLOR.get(fl, "")
        print(f" {i:>2}) {_sym(r['symbol']):<6} {exp:<12} {strike:>8}  score={r['score']:>6.3f} flag={fl} {ico}")
    print("Select a row number to open a card; ENTER to cancel.")
    try:
        sel = input("> ").strip()
    except EOFError:
        return
    if not sel:
        return
    try:
        k = int(sel)
        if not (1 <= k <= min(10,len(items))):
            return
    except Exception:
        return
    rec = items[k-1]
    # detail card
    while True:
        print_card(rec)
        print("[w] ticket  [t] toggle taken  [ENTER] back")
        try:
            c = input("> ").strip().lower()
        except EOFError:
            return
        if c == "":
            return
        elif c == "w":
            path = write_ticket(rec)
            print(f"[ticket] {path}")
        elif c == "t":
            new_val = not bool(rec.get("taken", False))
            rec["taken"] = new_val
            persist_taken(cfg, strat, rec.get("id",""), new_val)

def rerun_rankers():
    cmds = [
        ["python","-m","scripts.rank_csp"],
        ["python","-m","scripts.rank_covered_call"],
        ["python","-m","scripts.rank_pmcc"],
        ["python","-m","scripts.rank_verticals"],
        ["python","-m","scripts.rank_diagonal"],
        ["python","-m","scripts.rank_iron_condor"],
    ]
    env = os.environ.copy()
    env["SAVE_SUGGESTIONS"] = "1"
    for c in cmds:
        try:
            print("[run]", " ".join(c))
            subprocess.run(c, check=False, env=env)
        except Exception as e:
            print("[warn] failed:", c, e)

def main():
    cfg = load_engine_config()
    # session toggles (mutable)
    hide_taken = cfg.hide_taken_default
    fresh_min = cfg.freshness_min

    while True:
        per, overall, ages, counts = load_suggestions(cfg, hide_taken=hide_taken, fresh_min=fresh_min)
        print_header(cfg, per, overall, ages)
        print("Menu")
        print("  1) Monitor by strategy")
        print("  2) Trade from a strategy")
        print("  3) Top 3 Overall")
        print(f"  4) Toggle hide-taken  (currently: {hide_taken})")
        print("  5) Re-run rankers now")
        print("  6) Refresh")
        print("  7) Quit")
        try:
            sel = input("\nSelect: ").strip()
        except EOFError:
            return
        if sel == "1":
            monitor_flow(cfg, per)
        elif sel == "2":
            trade_flow(cfg, per)
        elif sel == "3":
            # print top 3 with cards
            for r in overall[:3]:
                print_card(r)
            input("(ENTER to continue)")
        elif sel == "4":
            hide_taken = not hide_taken
        elif sel == "5":
            rerun_rankers()
        elif sel == "6":
            # rebuild loop
            pass
        elif sel == "7":
            return
        else:
            # allow quick change of freshness window via hidden command like: f=120
            m = re.match(r"f=(\d+)", sel or "")
            if m:
                fresh_min = int(m.group(1))
            # otherwise ignore and loop

if __name__ == "__main__":
    main()