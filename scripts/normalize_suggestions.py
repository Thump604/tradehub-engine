#!/usr/bin/env python3
# scripts/normalize_suggestions.py — unify *_suggestions.{json,yml} for the Hub

import os, sys, json, argparse, math
from datetime import datetime, timezone
from glob import glob

try:
    import yaml
except Exception:
    yaml = None

ISO_Z = "%Y-%m-%dT%H:%M:%SZ"

SAFE_ROW_KEYS = {
    "id","symbol","exp","dte","strike","bid","ask","mid","mark","delta","gamma","theta",
    "vega","rho","otm_pct","ann","pop","score","flag","s_exp","s_dte","s_strike",
    "l_exp","l_dte","l_strike"
}

def parse_iso(s: str):
    if not s: return None
    # Accept "YYYY-MM-DD HH:MM:SSZ" or with 'T'
    s = s.strip()
    if s.endswith("Z") and " " in s:
        s = s.replace(" ", "T", 1)
    # Python can parse 'Z' if we handle manually
    if s.endswith("Z"):
        try:
            dt = datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass
    # Fallback tries common patterns
    for fmt in ("%Y-%m-%d %H:%M:%SZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None

def iso_now():
    return datetime.now(timezone.utc).strftime(ISO_Z)

def to_iso(dt: datetime):
    if not dt: return iso_now()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime(ISO_Z)

def coerce_flag(s):
    if not s: return ""
    u = str(s).upper()
    if u not in ("GREEN","YELLOW","RED"):
        return u  # leave custom tags as-is
    return u

def ensure_id(row, strategy):
    rid = row.get("id")
    if rid: return rid
    sym = str(row.get("symbol","")).upper()
    exp = str(row.get("exp",""))
    strike = str(row.get("strike",""))
    # strategy-specific fallbacks permitted
    rid = f"{strategy}:{sym}:{exp}:{strike}"
    row["id"] = rid
    return rid

def clean_row(row: dict, strategy: str):
    # keep only safe keys + anything already present that’s a string/number
    out = {}
    for k,v in row.items():
        if k in SAFE_ROW_KEYS or isinstance(v, (str,int,float,bool)) or v is None:
            out[k] = v
    out["flag"] = coerce_flag(out.get("flag"))
    ensure_id(out, strategy)
    # numeric tidy
    for nk in ("score","delta","bid","ask","mid","mark","otm_pct","ann","pop"):
        if nk in out and out[nk] is not None:
            try:
                out[nk] = float(out[nk])
            except Exception:
                pass
    for nk in ("dte","s_dte","l_dte"):
        if nk in out and out[nk] is not None:
            try:
                out[nk] = int(out[nk])
            except Exception:
                pass
    return out

def load_json(p):
    with open(p,"r") as f:
        return json.load(f)

def dump_json(p, data):
    with open(p,"w") as f:
        json.dump(data, f, indent=2, sort_keys=False)

def load_yaml(p):
    if yaml is None: return None
    with open(p,"r") as f:
        return yaml.safe_load(f)

def dump_yaml(p, data):
    if yaml is None: return
    with open(p,"w") as f:
        yaml.safe_dump(data, f, sort_keys=False)

def normalize_one(path, fresh_min):
    base = os.path.basename(path)
    if not base.endswith("_suggestions.json") and not base.endswith("_suggestions.yml"):
        return None

    strategy = base.split("_suggestions.")[0]
    data = None

    try:
        if base.endswith(".json"):
            data = load_json(path)
        else:
            data = load_yaml(path)
    except Exception as e:
        print(f"  ! failed to load {path}: {e}")
        return None

    if not isinstance(data, dict):
        print(f"  ! {path}: not a dict, skipping")
        return None

    strat = data.get("strategy") or strategy
    top = data.get("top") or []
    if not isinstance(top, list):
        top = []

    # Clean rows + ensure flags upper + make ids
    top2 = [clean_row(r if isinstance(r, dict) else {}, strat) for r in top]

    # generated_at
    raw_gen = data.get("generated_at")
    dt = parse_iso(raw_gen) or parse_iso(data.get("generatedAt") or "") or parse_iso(data.get("generated") or "")
    if dt is None:
        dt = datetime.now(timezone.utc)
    gen_iso = to_iso(dt)
    age_min = int((datetime.now(timezone.utc) - dt).total_seconds() / 60.0)

    out = {
        "strategy": strat,
        "generated_at": gen_iso,
        "count": len(top2),
        "top": top2,
    }

    # Write back to both json and yml next to the chosen directory
    root = path.rsplit("/",1)[0] if "/" in path else "."
    jpath = os.path.join(root, f"{strategy}_suggestions.json")
    ypath = os.path.join(root, f"{strategy}_suggestions.yml")
    dump_json(jpath, out)
    dump_yaml(ypath, out)

    fresh = (age_min <= fresh_min) if fresh_min is not None else True
    return (strategy, out["count"], age_min, fresh, jpath, ypath)

def main():
    ap = argparse.ArgumentParser(description="Normalize suggestion files for the Trade Hub")
    ap.add_argument("--dir", default=os.environ.get("SUGGESTIONS_DIR", "outputs"),
                    help="Directory to scan (default: outputs or $SUGGESTIONS_DIR)")
    ap.add_argument("--fresh-min", type=int, default=int(os.environ.get("FRESH_MIN", "600")),
                    help="Freshness window in minutes (for summary only)")
    args = ap.parse_args()

    root = os.path.abspath(args.dir)
    if not os.path.isdir(root):
        print(f"[normalize] directory not found: {root}")
        sys.exit(1)

    pats = [
        os.path.join(root, "*_suggestions.json"),
        os.path.join(root, "*_suggestions.yml"),
    ]
    files = []
    for pat in pats:
        files.extend(glob(pat))

    if not files:
        print(f"[normalize] no suggestion files under {root}")
        sys.exit(0)

    print(f"[normalize] scanning {len(files)} file(s) in {root} …")
    seen = {}
    for p in sorted(files):
        res = normalize_one(p, args.fresh_min)
        if not res: continue
        strat, cnt, age, fresh, jpath, ypath = res
        seen[strat] = (cnt, age, fresh, jpath, ypath)

    print("\nSummary (per strategy):")
    if not seen:
        print("  (none)")
    else:
        for k in sorted(seen.keys()):
            cnt, age, fresh, jpath, ypath = seen[k]
            flag = "OK " if cnt > 0 else "N/A"
            fr = "fresh" if fresh else "stale"
            print(f"  {k:<15} count={cnt:<3} age={age:>4} min  {fr:<6}  → {os.path.basename(jpath)}, {os.path.basename(ypath)}")

    print("\nDone.")

if __name__ == "__main__":
    main()