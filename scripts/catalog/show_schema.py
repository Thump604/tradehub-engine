from __future__ import annotations
import argparse, json, sys
try:
    from catalog.schemas import SCHEMAS
except Exception as e:
    print(f"error: cannot load catalog.schemas: {e}", file=sys.stderr)
    sys.exit(1)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--screener", required=True)
    args = ap.parse_args()
    key = args.screener
    if key not in SCHEMAS:
        print(f"error: screener not found: {key}", file=sys.stderr)
        print("available:", ", ".join(sorted(SCHEMAS.keys())), file=sys.stderr)
        sys.exit(2)
    print(json.dumps({key: SCHEMAS[key]}, indent=2))

if __name__ == "__main__":
    main()
