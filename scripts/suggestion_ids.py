#!/usr/bin/env python3
"""
Suggestion ID backfiller

Reads suggestion files in outputs/suggestions/, computes a stable `id`
for each record in `top`, and writes updated .yml and .json in place.

Formats (stable, human-readable, machine-parseable):

- CSP:        CSP:{SYM}:{EXP}:{STRIKE}:P
- CoveredCall CC:{SYM}:{EXP}:{STRIKE}:C
- PMCC:       PMCC:{SYM}:LEAP{L_STRIKE}C@{L_EXP}|S{S_STRIKE}C@{S_EXP}
- BCALL:      BCALL:{SYM}:{EXP}:{LONG}-{SHORT}:C
- BPUT:       BPUT:{SYM}:{EXP}:{LONG}-{SHORT}:P
- DIAG:       DIAG:{SYM}:L{L_STRIKE}C@{L_EXP}|S{S_STRIKE}C@{S_EXP}
- IRONCONDOR  IC:{SYM}:DTE{DTE}:RR{RISK_REWARD}

NOTE (Iron Condor):
Current ranker output is aggregate (no 4 legs), so ID uses symbol+DTE+risk/reward
as a temporary unique key. When we upgrade the condor ranker to include all four
strikes, we’ll switch to a full 4-leg ID.
"""
from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

try:
    import yaml  # PyYAML
except Exception as e:
    print("[ERROR] PyYAML is required. Activate your venv and run:\n  python3 -m pip install PyYAML")
    raise

SUG_DIR = Path("outputs/suggestions")

def fmt_strike(val: Any) -> str:
    try:
        x = float(val)
        # Keep up to 2 decimals but drop trailing zeros
        s = f"{x:.2f}"
        if s.endswith("00"): return s[:-3]
        if s.endswith("0"):  return s[:-1]
        return s
    except Exception:
        return str(val)

def make_id_csp(rec: Dict[str, Any]) -> Optional[str]:
    sym = rec.get("symbol")
    exp = rec.get("exp")
    strike = rec.get("strike")
    if not (sym and exp and strike is not None): return None
    return f"CSP:{sym}:{exp}:{fmt_strike(strike)}:P"

def make_id_cc(rec: Dict[str, Any]) -> Optional[str]:
    # Covered Call suggestions use: symbol, exp, strike
    sym = rec.get("symbol")
    exp = rec.get("exp")
    strike = rec.get("strike")
    if not (sym and exp and strike is not None): return None
    return f"CC:{sym}:{exp}:{fmt_strike(strike)}:C"

def make_id_pmcc(rec: Dict[str, Any]) -> Optional[str]:
    sym = rec.get("symbol")
    leap = rec.get("leap") or {}
    short = rec.get("short") or {}
    l_exp = leap.get("exp"); l_strike = leap.get("strike")
    s_exp = short.get("exp"); s_strike = short.get("strike")
    if not (sym and l_exp and s_exp and l_strike is not None and s_strike is not None):
        return None
    return f"PMCC:{sym}:LEAP{fmt_strike(l_strike)}C@{l_exp}|S{fmt_strike(s_strike)}C@{s_exp}"

def make_id_bcall(rec: Dict[str, Any]) -> Optional[str]:
    sym = rec.get("symbol") or rec.get("sym")  # tolerate 'sym'
    exp = rec.get("exp")
    # vertical ranker outputs often don’t include exp in top; if missing, omit from ID
    long_k = rec.get("long") or rec.get("long_strike")
    short_k = rec.get("short") or rec.get("short_strike")
    if not (sym and long_k is not None and short_k is not None):
        return None
    base = f"BCALL:{sym}:{fmt_strike(long_k)}-{fmt_strike(short_k)}:C"
    return f"{base}@{exp}" if exp else base

def make_id_bput(rec: Dict[str, Any]) -> Optional[str]:
    sym = rec.get("symbol") or rec.get("sym")
    exp = rec.get("exp")
    long_k = rec.get("long") or rec.get("long_strike")
    short_k = rec.get("short") or rec.get("short_strike")
    if not (sym and long_k is not None and short_k is not None):
        return None
    base = f"BPUT:{sym}:{fmt_strike(long_k)}-{fmt_strike(short_k)}:P"
    return f"{base}@{exp}" if exp else base

def make_id_diag(rec: Dict[str, Any]) -> Optional[str]:
    sym = rec.get("symbol")
    l_exp = rec.get("long_exp"); l_strike = rec.get("long_strike")
    s_exp = rec.get("short_exp"); s_strike = rec.get("short_strike")
    if not (sym and l_exp and s_exp and l_strike is not None and s_strike is not None):
        return None
    return f"DIAG:{sym}:L{fmt_strike(l_strike)}C@{l_exp}|S{fmt_strike(s_strike)}C@{s_exp}"

def make_id_condor(rec: Dict[str, Any]) -> Optional[str]:
    # Temporary aggregate ID (no legs in current output)
    sym = rec.get("symbol")
    dte = rec.get("dte")
    rr = rec.get("risk_reward")
    if not (sym and dte is not None and rr is not None):
        return None
    return f"IC:{sym}:DTE{int(dte)}:RR{rr}"

def detect_kind(obj: Dict[str, Any], fname: str) -> str:
    name = Path(fname).name
    if "csp_suggestions" in name: return "csp"
    if "pmcc_suggestions" in name: return "pmcc"
    if "vertical_suggestions" in name:
        # The records carry enough fields to tell call vs put:
        # our vertical ranker writes one list for calls and one for puts into same file?
        # Here we infer per-record.
        return "vertical"
    if "covered_call_suggestions" in name: return "cc"
    if "diagonal_suggestions" in name: return "diag"
    if "iron_condor_suggestions" in name: return "condor"
    # fallback: try to infer by keys in the first record
    top = obj.get("top") or []
    if not top: return "unknown"
    r0 = top[0]
    keys = set(r0.keys())
    if {"leap","short"}.issubset(keys): return "pmcc"
    if {"short_prob","roc_annual"}.issubset(keys): return "csp"
    if {"long_exp","short_exp","long_strike","short_strike"}.issubset(keys): return "diag"
    if {"risk_reward","loss_prob"}.issubset(keys): return "condor"
    if {"long","short","debit"}.issubset(keys): return "vertical"
    if {"strike","ask"}.issubset(keys): return "cc"
    return "unknown"

def add_ids(obj: Dict[str, Any], fname: str) -> Tuple[int,int]:
    added = 0
    updated = 0
    kind = detect_kind(obj, fname)
    top: List[Dict[str, Any]] = obj.get("top") or []
    for rec in top:
        existing = rec.get("id")
        new_id: Optional[str] = None
        if kind == "csp":
            new_id = make_id_csp(rec)
        elif kind == "cc":
            new_id = make_id_cc(rec)
        elif kind == "pmcc":
            new_id = make_id_pmcc(rec)
        elif kind == "diag":
            new_id = make_id_diag(rec)
        elif kind == "condor":
            new_id = make_id_condor(rec)
        elif kind == "vertical":
            # Guess call vs put per record by presence of 'credit' vs 'debit'
            if "debit" in rec or rec.get("type") == "CALL":
                new_id = make_id_bcall(rec)
            elif "credit" in rec or rec.get("type") == "PUT":
                new_id = make_id_bput(rec)
            else:
                # try presence of 'R/D' (call debit ratio) vs 'C/W' etc. — fallback to call
                new_id = make_id_bcall(rec)
        else:
            continue

        if not new_id:
            continue

        if existing and existing != new_id:
            rec["id"] = new_id
            updated += 1
        elif not existing:
            rec["id"] = new_id
            added += 1
    return added, updated

def load_any(path: Path) -> Dict[str, Any]:
    if path.suffix.lower() in (".yml", ".yaml"):
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    else:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

def save_any(path: Path, obj: Dict[str, Any]) -> None:
    if path.suffix.lower() in (".yml", ".yaml"):
        with path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(obj, f, sort_keys=False)
    else:
        with path.open("w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2)

def main():
    if not SUG_DIR.exists():
        print(f"[ERROR] {SUG_DIR} not found.")
        return
    files = sorted(list(SUG_DIR.glob("*_suggestions.yml"))) + \
            sorted(list(SUG_DIR.glob("*_suggestions.json")))

    if not files:
        print(f"[WARN] No suggestion files found under {SUG_DIR}/")
        return

    total_added = 0
    total_updated = 0
    print("\n──────────────────────────────────────────────────────────────────────")
    print("SUGGESTION ID BACKFILL — adding `id` to suggestion records")
    print("──────────────────────────────────────────────────────────────────────\n")

    for p in files:
        obj = load_any(p)
        added, updated = add_ids(obj, p.name)
        if (added + updated) > 0:
            save_any(p, obj)
        print(f"{p.name:<34}  added: {added:>3}   updated: {updated:>3}")

        total_added += added
        total_updated += updated

    print("\nSummary:")
    print(f"  Total added:   {total_added}")
    print(f"  Total updated: {total_updated}")
    print("\n[OK] ID backfill complete.")

if __name__ == "__main__":
    main()