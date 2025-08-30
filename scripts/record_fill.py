# scripts/record_fill.py
#!/usr/bin/env python3
import os, sys, argparse
from datetime import datetime

TIX_DIR = "outputs/tickets"

def _load_yaml(path):
    try:
        import yaml
    except Exception:
        print("[ERROR] PyYAML is required. Activate your venv and run:\n  python3 -m pip install PyYAML")
        sys.exit(1)
    with open(path,"r") as f:
        return yaml.safe_load(f) or {}

def _dump_yaml(path, data):
    import yaml
    with open(path,"w") as f:
        yaml.safe_dump(data, f, sort_keys=False)

def compute_targets(ticket, fill):
    tiers = ticket.get("gtc_tiers_pct") or []
    targets=[]
    if not tiers: return targets
    if "credit" in ticket:
        for p in tiers:
            tgt = round(fill*(1 - p/100.0), 4)
            targets.append({"pct":p, "target_price":tgt, "direction":"buy_to_close"})
    elif "debit" in ticket:
        for p in tiers:
            tgt = round(fill*(1 + p/100.0), 4)
            targets.append({"pct":p, "target_price":tgt, "direction":"sell_to_close"})
    return targets

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--fill", required=True, type=float)
    args = ap.parse_args()

    fname = f"{args.id.replace(':','_').replace('|','__')}.yml"
    path = os.path.join(TIX_DIR, fname)
    if not os.path.exists(path):
        print(f"[ERROR] Ticket not found: {path}")
        sys.exit(1)

    t = _load_yaml(path)
    t.setdefault("fills",[])
    nowz = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    t["fills"].append({"price": float(args.fill), "at": nowz})

    # update GTC targets from actual fill
    t["gtc_targets"] = compute_targets(t, float(args.fill))
    t["status"] = "open"

    _dump_yaml(path, t)

    print("\n[OK] Recorded fill")
    print(f"ID: {t['id']}")
    print(f"Status: {t['status']}")
    if t.get("gtc_targets"):
        print("GTC targets:")
        for g in t["gtc_targets"]:
            print(f"  • {g['pct']}% → {g['target_price']} ({g['direction']})")
    print()

if __name__ == "__main__":
    main()