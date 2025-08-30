# scripts/account_update.py
#!/usr/bin/env python3
import argparse, os, sys, json
from datetime import datetime

DEFAULTS = {
    "total_value": 300000,
    "alloc_pct_to_options": 0.50,
    "cash_available": 15000,
    "per_trade_cap_pct": 0.02,
}

def _fmt_money(x): return f"${x:,.2f}"
def _fmt_pct(x):   return f"{x*100:.2f}%"

def load_yaml(path):
    try:
        import yaml
    except Exception:
        print("[ERROR] PyYAML is required. Activate your venv and run:\n  python3 -m pip install PyYAML")
        sys.exit(1)
    if not os.path.exists(path):
        return dict(DEFAULTS)
    with open(path,"r") as f:
        return yaml.safe_load(f) or {}

def dump_yaml(path, data):
    import yaml
    with open(path,"w") as f:
        yaml.safe_dump(data, f, sort_keys=False)

def show_state(data):
    print("\nAccount state (current):")
    print(f"  total_value          : {_fmt_money(float(data.get('total_value',0)))}")
    print(f"  alloc_pct_to_options : {_fmt_pct(float(data.get('alloc_pct_to_options',0)))}")
    print(f"  cash_available       : {_fmt_money(float(data.get('cash_available',0)))}")
    print(f"  per_trade_cap_pct    : {_fmt_pct(float(data.get('per_trade_cap_pct',0)))}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default="data/account_state.yml")
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--cash", type=float)
    ap.add_argument("--total", type=float)
    ap.add_argument("--alloc", type=float, help="fraction (e.g., 0.55)")
    ap.add_argument("--cap",   type=float, help="per-trade cap fraction (e.g., 0.03)")
    ap.add_argument("--reset", action="store_true", help="reset to baseline defaults")
    args = ap.parse_args()

    state = load_yaml(args.path)

    if args.show and not any([args.cash, args.total, args.alloc, args.cap, args.reset]):
        show_state(state); return

    before = dict(state)

    if args.reset:
        state = dict(DEFAULTS)

    if args.total is not None: state["total_value"] = float(args.total)
    if args.alloc is not None: state["alloc_pct_to_options"] = float(args.alloc)
    if args.cash  is not None: state["cash_available"] = float(args.cash)
    if args.cap   is not None: state["per_trade_cap_pct"] = float(args.cap)

    dump_yaml(args.path, state)

    print("\n[OK] Updated account_state.yml")
    print("Before:")
    show_state(before)
    print("After:")
    show_state(state)

if __name__ == "__main__":
    main()