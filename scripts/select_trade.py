# scripts/select_trade.py
from __future__ import annotations
import argparse, json, re, sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

SUG_DIR = Path("outputs/suggestions")
TIX_DIR = Path("outputs/tickets")
TIX_DIR.mkdir(parents=True, exist_ok=True)

def _load_all() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for base in [
        "covered_call_suggestions.json",
        "csp_suggestions.json",
        "pmcc_suggestions.json",
        "diagonal_suggestions.json",
        "vertical_suggestions.json",
        "iron_condor_suggestions.json",
    ]:
        p = SUG_DIR / base
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        # verticals may store keys differently; normalize to a flat "top" list
        if "top" in data and isinstance(data["top"], list):
            for rec in data["top"]:
                if isinstance(rec, dict):
                    rec["_source"] = base
                    out.append(rec)
        else:
            # verticals json keys: top_bull_call / top_bull_put
            for k in ["top_bull_call", "top_bull_put"]:
                if k in data and isinstance(data[k], list):
                    for rec in data[k]:
                        if isinstance(rec, dict):
                            rec["_source"] = base
                            out.append(rec)
    return out

def _norm_strike(s) -> str:
    try:
        return f"{float(str(s).replace(',', '')):.2f}"
    except Exception:
        return str(s)

def _normalize_id(raw: str) -> str:
    s = raw.strip()
    s = s.replace("|", ":").replace("C", "").replace("P", "")
    s = re.sub(r"\s+", "", s)
    parts = s.split(":")
    if len(parts) < 4:
        return raw  # leave unchanged
    t = parts[0].upper()
    if t in ("CC", "CSP"):
        # CC:SYM:YYYY-MM-DD:STRIKE
        parts[3] = _norm_strike(parts[3])
        return f"{t}:{parts[1].upper()}:{parts[2]}:{parts[3]}:{'C' if t=='CC' else 'P'}"
    if t in ("PMCC", "DIAG"):
        # PMCC/DIAG:SYM:LEXP:LSTRK:SEXP:SSTRK
        if len(parts) >= 6:
            parts[3] = _norm_strike(parts[3])
            parts[5] = _norm_strike(parts[5])
            return f"{t}:{parts[1].upper()}:{parts[2]}:{parts[3]}:{parts[4]}:{parts[5]}"
    if t in ("BCALL", "BPUT"):
        # BCALL:SYM:EXP:LONG:SHORT   /  BPUT:SYM:EXP:SHORT:LONG
        if len(parts) >= 5:
            parts[3] = _norm_strike(parts[3])
            parts[4] = _norm_strike(parts[4])
            return f"{t}:{parts[1].upper()}:{parts[2]}:{parts[3]}:{parts[4]}"
    return raw

def _matches_id(rec: Dict[str, Any], want: str) -> bool:
    rid = rec.get("id") or ""
    if not rid:
        # synthesize from fields if present
        if all(k in rec for k in ("symbol","exp","strike")) and rec.get("flag") is not None:
            guess = f"CC:{rec['symbol']}:{rec['exp']}:{_norm_strike(rec['strike'])}:C"
            rid = guess
        elif set(rec.keys()) >= {"symbol","long_exp","long_strike","short_exp","short_strike"}:
            rid = f"DIAG:{rec['symbol']}:{rec['long_exp']}:{_norm_strike(rec['long_strike'])}:{rec['short_exp']}:{_norm_strike(rec['short_strike'])}"
    return _normalize_id(rid) == _normalize_id(want)

def main():
    ap = argparse.ArgumentParser(description="Create a trade ticket from a suggestion ID")
    ap.add_argument("--id", required=True, help="Suggestion ID (lenient; 150 equals 150.00, '|' or ':' ok)")
    ap.add_argument("--gtc", default=None, help="Optional take-profit tiers e.g. '50,75'")
    args = ap.parse_args()

    want = _normalize_id(args.id)
    records = _load_all()
    match = None
    for rec in records:
        if _matches_id(rec, want):
            match = rec
            break
    if not match:
        print("[ERROR] Could not find a matching candidate in suggestions. Make sure suggestions are freshly generated.")
        sys.exit(1)

    sym = match.get("symbol", "UNKNOWN")
    # Construct a deterministic ticket name
    if want.startswith("CC:"):
        _, _, exp, strike, _ = want.split(":")
        tname = f"CC_{sym}_{exp}_{strike}.yml"
    elif want.startswith("CSP:"):
        _, _, exp, strike, _ = want.split(":")
        tname = f"CSP_{sym}_{exp}_{strike}.yml"
    elif want.startswith("PMCC:") or want.startswith("DIAG:"):
        parts = want.split(":")
        tname = f"DIAG_{sym}_{parts[2]}_{parts[3]}_{parts[4]}_{parts[5]}.yml"
    elif want.startswith("BCALL:") or want.startswith("BPUT:"):
        parts = want.split(":")
        tname = f"{parts[0]}_{sym}_{parts[2]}_{parts[3]}_{parts[4]}.yml"
    else:
        tname = f"TICKET_{sym}.yml"

    payload = {
        "id": want,
        "symbol": sym,
        "from_suggestions": match.get("_source"),
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "gtc": args.gtc,
        "suggestion_snapshot": match,
        "status": "PLANNED",
    }
    path = TIX_DIR / tname
    # write as YAML (no dependency): simple key: value
    lines = []
    for k, v in payload.items():
        if isinstance(v, dict) or isinstance(v, list):
            # embed JSON for nested to keep dependency-free
            lines.append(f"{k}: {json.dumps(v)}")
        else:
            lines.append(f"{k}: {v}")
    path.write_text("\n".join(lines), encoding="utf-8")
    print("[OK] Ticket created:\n  {}".format(path))

if __name__ == "__main__":
    main()