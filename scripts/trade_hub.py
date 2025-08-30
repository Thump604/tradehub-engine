#!/usr/bin/env python3
# scripts/trade_hub.py — Non-interactive Trade Hub (polished)
# - Pretty ANSI output with icons/colors
# - Reads suggestions from JSON/YAML (prefers JSON unless overridden)
# - Honors engine.yaml hub/market + ENV overrides:
#     SUGGESTIONS_DIR, FRESH_MIN, HIDE_TAKEN, PREFER_JSON, COLLAPSE_BY_SYMBOL, MARKET_STATE_FILE

from __future__ import annotations
import os, sys, json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Tuple

try:
    import yaml  # optional
except Exception:
    yaml = None

# ─────────────────────────── UI / Colors ────────────────────────────
class C:
    B  = "\033[34m"; CY = "\033[36m"; G = "\033[32m"; Y = "\033[33m"; R = "\033[31m"; W = "\033[37m"
    DIM = "\033[2m"; BOLD = "\033[1m"; RESET = "\033[0m"
    @staticmethod
    def on_tty() -> bool:
        return sys.stdout.isatty()
    @staticmethod
    def wrap(s, color):
        return f"{color}{s}{C.RESET}" if C.on_tty() else s

ICON_OK   = "✅"
ICON_INFO = "ℹ️"
ICON_WARN = "⚠️"
ICON_BULL = "📈"
ICON_CLOCK= "⏱️"
ICON_STAR = "★"
ICON_DOT  = "•"

def flag_color(flag: str) -> str:
    f = (flag or "").upper()
    if f == "GREEN":  return C.G
    if f == "YELLOW": return C.Y
    if f == "RED":    return C.R
    return C.W

def hr():
    print(C.wrap("─"*70, C.DIM))

def header(title: str):
    print(C.wrap("═"*70, C.DIM))
    print(C.wrap("TRADE HUB — " + title, C.BOLD))
    print(C.wrap("═"*70, C.DIM))

def ago_minutes(iso_or_space: str) -> int:
    if not iso_or_space: return 99999
    s = iso_or_space.replace(" ", "T")
    try:
        dt = datetime.fromisoformat(s.replace("Z","")).replace(tzinfo=timezone.utc)
    except Exception:
        return 99999
    return max(0, int((datetime.now(timezone.utc) - dt).total_seconds() // 60))

def fmt_age(mins: int) -> str:
    return f"{mins}m"

def fmt_score(x) -> str:
    try:
        return f"{float(x):.3f}"
    except Exception:
        return "0.000"

def safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

# ───────────────────────── Config & Loading ─────────────────────────
@dataclass
class HubConfig:
    suggestions_dir: str
    freshness_min: int
    hide_taken_default: bool
    prefer_json: bool
    market_state_file: str
    collapse_by_symbol: bool

def _yaml_or_empty(path: str) -> dict:
    if not os.path.isfile(path) or yaml is None:
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def load_engine_config() -> HubConfig:
    defaults = {
        "hub": {
            "suggestions_dir": "./outputs",
            "freshness_min": 30,
            "hide_taken_default": True,
            "prefer_json": True,
            "collapse_by_symbol": False,
        },
        "market": { "state_file": "./outputs/market_state.yml" }
    }
    cfg_path = os.path.join(os.getcwd(), "engine.yaml")
    base = defaults | _yaml_or_empty(cfg_path)

    hub = (base.get("hub") or {}) | {}
    market = (base.get("market") or {}) | {}

    suggestions_dir = os.environ.get("SUGGESTIONS_DIR", hub.get("suggestions_dir", "./outputs"))
    fresh_min = int(os.environ.get("FRESH_MIN", hub.get("freshness_min", 30)))
    hide_taken = os.environ.get("HIDE_TAKEN", str(hub.get("hide_taken_default", True))).lower() in ("1","true","yes","y")
    prefer_json = os.environ.get("PREFER_JSON", str(hub.get("prefer_json", True))).lower() in ("1","true","yes","y")
    collapse = os.environ.get("COLLAPSE_BY_SYMBOL", str(hub.get("collapse_by_symbol", False))).lower() in ("1","true","yes","y")
    ms_file = os.environ.get("MARKET_STATE_FILE", market.get("state_file", "./outputs/market_state.yml"))

    return HubConfig(
        suggestions_dir=suggestions_dir,
        freshness_min=fresh_min,
        hide_taken_default=hide_taken,
        prefer_json=prefer_json,
        market_state_file=ms_file,
        collapse_by_symbol=collapse,
    )

def _strategy_files(dirpath: str, prefer_json=True) -> Dict[str, str]:
    keys = ["covered_call", "csp", "diagonal", "iron_condor", "pmcc", "vertical"]
    out = {}
    for k in keys:
        jp = os.path.join(dirpath, f"{k}_suggestions.json")
        yp = os.path.join(dirpath, f"{k}_suggestions.yml")
        if prefer_json and os.path.isfile(jp): out[k] = jp
        elif (not prefer_json) and os.path.isfile(yp): out[k] = yp
        elif os.path.isfile(jp): out[k] = jp
        elif os.path.isfile(yp): out[k] = yp
    return out

def _load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _normalize_doc(obj: dict) -> Tuple[List[dict], int]:
    # Expect {strategy, generated_at, top:[…], count}
    top = obj.get("top") or []
    for t in top:
        if "taken" not in t: t["taken"] = False
        if "flag"  not in t: t["flag"]  = "YELLOW"
        if "score" not in t: t["score"] = 0.0
        if "exp" not in t and "Exp" in t: t["exp"] = t["Exp"]
        if "strike" not in t and "Strike" in t: t["strike"] = t["Strike"]
    gen = obj.get("generated_at")
    age = ago_minutes(gen) if gen else 99999
    return top, age

def _dedupe(items: List[dict]) -> List[dict]:
    seen = set(); out = []
    for r in items:
        key = r.get("id") or (r.get("symbol"), r.get("exp"), r.get("strike"), r.get("strategy"))
        if key in seen: 
            continue
        seen.add(key); out.append(r)
    return out

def _collapse_best_by_symbol(items: List[dict]) -> List[dict]:
    best: Dict[str, dict] = {}
    for r in items:
        sym = str(r.get("symbol") or "")
        if not sym: continue
        if sym not in best or safe_float(r.get("score"), -1e9) > safe_float(best[sym].get("score"), -1e9):
            best[sym] = r
    return list(best.values())

def load_suggestions(cfg: HubConfig, hide_taken: bool, fresh_min: int) -> Tuple[Dict[str, List[dict]], List[dict], Dict[str,int]]:
    files = _strategy_files(cfg.suggestions_dir, cfg.prefer_json)
    per: Dict[str, List[dict]] = {}
    ages: Dict[str, int] = {}

    for strat, path in files.items():
        try:
            doc = _load_json(path) if path.endswith(".json") else _load_yaml(path)
        except Exception:
            continue
        top, age = _normalize_doc(doc)
        ages[strat] = age
        rows = []
        for r in top:
            if hide_taken and r.get("taken") is True: 
                continue
            if fresh_min and age > fresh_min: 
                continue
            rr = dict(r)
            rr["strategy"] = strat
            rows.append(rr)
        rows = _dedupe(rows)
        if cfg.collapse_by_symbol:
            rows = _collapse_best_by_symbol(rows)
        per[strat] = rows

    overall = [x for v in per.values() for x in v]
    overall.sort(key=lambda r: safe_float(r.get("score"), 0.0), reverse=True)
    return per, overall, ages

def load_market(cfg: HubConfig) -> Tuple[str,str,str]:
    st = _yaml_or_empty(cfg.market_state_file)
    return st.get("overall_regime","?"), st.get("trend_bias","?"), st.get("volatility","?")

# ─────────────────────────── Printing ───────────────────────────────
def _fmt_row(r: dict) -> str:
    sym    = f"{r.get('symbol',''):<6}"
    exp    = f"{(r.get('exp') or '?'):<10}"
    strike = f"{str(r.get('strike') or ''):>8}"
    sc     = fmt_score(r.get("score"))
    flag   = (r.get("flag") or "").upper()
    return f"{sym} {exp} {strike}  score={sc} flag={C.wrap(flag, flag_color(flag))}"

def main():
    cfg = load_engine_config()
    hide_taken = cfg.hide_taken_default
    fresh_min  = cfg.freshness_min

    per, overall, ages = load_suggestions(cfg, hide_taken, fresh_min)
    regime, trend, vol = load_market(cfg)

    header("Summary (non-interactive)")
    gen = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(C.wrap(f"Generated: {gen}", C.DIM))
    print(f"Suggestions dir: {C.wrap(os.path.abspath(cfg.suggestions_dir), C.CY)}")
    print(f"Freshness window: {C.wrap(str(fresh_min)+' min', C.CY)}")
    print(f"Hide taken: {C.wrap(str(hide_taken), C.CY)}")
    print()
    print(f"Market state: Regime={C.wrap(regime, C.BOLD)}, Trend={C.wrap(trend, C.BOLD)}, Vol={C.wrap(vol, C.BOLD)}")
    print()

    order = ["covered_call","csp","diagonal","iron_condor","pmcc","vertical"]
    labels= {
        "covered_call":"covered_call",
        "csp":"csp",
        "diagonal":"diagonal",
        "iron_condor":"iron_condor",
        "pmcc":"pmcc",
        "vertical":"vertical",
    }

    for k in order:
        rows = per.get(k, [])
        print(f"[{labels[k]}] count={len(rows)}", end="")
        age = ages.get(k)
        if age is not None:
            print(f"  {ICON_CLOCK} age {fmt_age(age)}")
        else:
            print()
        if rows:
            for r in rows[:5]:
                print("  " + _fmt_row(r))
        print()

    total = len(overall)
    print(f"Overall suggestions: {C.wrap(str(total), C.BOLD)}")
    if overall:
        print("Top 5 overall:")
        for r in overall[:5]:
            print(f"  {r.get('strategy', ''):<12} {_fmt_row(r)}")

if __name__ == "__main__":
    main()