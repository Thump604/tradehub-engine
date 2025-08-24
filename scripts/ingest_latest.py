#!/usr/bin/env python3
# ingest_latest.py — Stage the newest Barchart export per strategy from
# data/incoming/ into data/<strategy>-latest.csv and write data/data_catalog_runtime.yml.
# All incoming files are archived to data/archive/<YYYYMMDD-HHMMSS>/, and
# 'sourced_from' points to the archived copy (stable path).
#
# Self-contained: NO ~/Downloads fallback.
#
# Usage:
#   python3 scripts/ingest_latest.py [-n|--dry-run]
#
# Produces:
#   data/<strategy>-latest.csv
#   data/data_catalog_runtime.yml
#
# Strategies detected:
#   leap                -> long-call LEAP screener
#   csp                 -> naked put (CSP)
#   covered_call        -> covered call
#   vertical_bull_call  -> bull call spread
#   vertical_bull_put   -> bull put spread
#   long_put            -> long put
#   protective_collar   -> protective collar

import os, sys, shutil, argparse
from datetime import datetime, UTC
from pathlib import Path
import yaml

# ------------------------- Paths -------------------------
HERE        = Path(__file__).resolve().parent
BASE_DIR    = HERE.parent
DATA_DIR    = BASE_DIR / "data"
INCOMING    = DATA_DIR / "incoming"
ARCHIVE     = DATA_DIR / "archive"
OUT_CATALOG = DATA_DIR / "data_catalog_runtime.yml"

# ------------------------- Strategy name patterns -------------------------
STRATEGY_PATTERNS = {
    "leap": [
        ["long-call-options-screener", "long-call-leap"],
        ["long-call", "leap"],
    ],
    "csp": [
        ["naked-put-option-screener", "csp"],
        ["naked-put", "csp"],
    ],
    "covered_call": [
        ["covered-call-option-screener", "covered-call"],
        ["covered-call"],
    ],
    "vertical_bull_call": [
        ["bull-call-spread-option-screener", "vertical-bull-call"],
        ["bull-call-spread", "vertical"],
    ],
    "vertical_bull_put": [
        ["bull-put-spread-option-screener", "vertical-bull-put"],
        ["bull-put-spread", "vertical"],
    ],
    "long_put": [
        ["long-put-options-screener", "long-put"],
        ["long-put"],
    ],
    "protective_collar": [
        ["protective-collar-option-screener", "protective-collar"],
        ["protective", "collar"],
    ],
}

def matches_strategy(filename: str, patterns) -> bool:
    fn = filename.lower()
    fn_c = fn.replace(" ", "").replace("_", "-")
    for alt in patterns:
        if all((needle in fn) or (needle in fn_c) for needle in alt):
            return True
    return False

def detect_strategy(filename: str) -> str | None:
    for key, alts in STRATEGY_PATTERNS.items():
        if matches_strategy(filename, alts):
            return key
    return None

def ensure_dirs():
    for p in (DATA_DIR, INCOMING, ARCHIVE):
        p.mkdir(parents=True, exist_ok=True)

def pick_latest(files):
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)

def archive_all(incoming_files, dry_run=False):
    """Move ALL incoming files into a timestamped archive directory. Return (archive_dir, moved_map)."""
    if not incoming_files:
        return None, {}
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    dest = ARCHIVE / stamp
    moved_map = {}
    if dry_run:
        print(f"[DRY-RUN] Would create archive dir: {dest}")
    else:
        dest.mkdir(parents=True, exist_ok=True)
    for src in incoming_files:
        dst = dest / src.name
        if dry_run:
            print(f"[DRY-RUN] Would archive: {src} -> {dst}")
        else:
            shutil.move(str(src), str(dst))
        moved_map[str(src)] = str(dst)
    return str(dest), moved_map

def write_runtime_yaml(mapping, dry_run=False):
    doc = {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ"),
        "datasets": mapping
    }
    if dry_run:
        print("[DRY-RUN] Would write:", OUT_CATALOG)
        print(yaml.safe_dump(doc, sort_keys=False, width=4096))
    else:
        with open(OUT_CATALOG, "w") as f:
            yaml.safe_dump(doc, f, sort_keys=False, width=4096)  # prevent line wrapping
    return doc

def copy_latest(latest_archived_map, dry_run=False):
    """Copy each archived latest file to data/<strategy>-latest.csv"""
    staged = {}
    for key, archived_src in latest_archived_map.items():
        out_path = DATA_DIR / f"{key}-latest.csv"
        staged[key] = str(out_path)
        if dry_run:
            print(f"[DRY-RUN] Would copy: {archived_src} -> {out_path}")
        else:
            shutil.copy2(archived_src, out_path)
    return staged

def main():
    ap = argparse.ArgumentParser(description="Stage newest Barchart export per strategy from data/incoming/")
    ap.add_argument("-n","--dry-run", action="store_true", help="Show actions without modifying files")
    args = ap.parse_args()

    ensure_dirs()

    incoming = [p for p in INCOMING.iterdir() if p.is_file() and p.suffix.lower()==".csv"]
    if not incoming:
        print("[WARN] No CSV files found in data/incoming/. Nothing to ingest.")
        print("       Place your Barchart exports into:", INCOMING)
        sys.exit(0)

    # Partition by strategy
    bucket = {}
    unknown = []
    for p in incoming:
        key = detect_strategy(p.name)
        if key:
            bucket.setdefault(key, []).append(p)
        else:
            unknown.append(p)

    if unknown:
        print("[INFO] Skipping unknown files (no strategy match):")
        for u in sorted(unknown):
            print("  -", u.name)

    # Pick latest per strategy (pre-archive paths)
    latest_pre = {}
    for key, files in bucket.items():
        latest = pick_latest(files)
        if latest:
            latest_pre[key] = str(latest)

    if not latest_pre:
        print("[WARN] No recognized strategy files in incoming/. Nothing to stage.")
        sys.exit(0)

    # Archive ALL incoming files; map pre-archive paths -> archived paths
    arch_dir, moved_map = archive_all(incoming, dry_run=args.dry_run)

    # Build map of latest files using their archived paths
    latest_archived_map = {}
    for key, pre_path in latest_pre.items():
        if pre_path not in moved_map:
            # Shouldn't happen; be defensive
            print(f"[WARN] Latest file for {key} wasn't archived? {pre_path}")
            continue
        latest_archived_map[key] = moved_map[pre_path]

    if not latest_archived_map:
        print("[WARN] Could not resolve archived latest files. Aborting.")
        sys.exit(1)

    # Copy archived latest to canonical data/<key>-latest.csv
    staged_paths = copy_latest(latest_archived_map, dry_run=args.dry_run)

    # Build runtime catalog (sourced_from = archived path)
    now_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    catalog = {}
    for key, archived_src in latest_archived_map.items():
        catalog[key] = {
            "file": staged_paths[key],
            "sourced_from": archived_src,
            "updated_at": now_iso
        }

    # Write runtime YAML
    write_runtime_yaml(catalog, dry_run=args.dry_run)

    # Summary
    if args.dry_run:
        print("\n[DRY-RUN] Done.")
    else:
        print("\n[OK] Staged the latest CSV per strategy to data/<strategy>-latest.csv")
        print("[OK] Wrote", OUT_CATALOG)
        if arch_dir:
            total_archived = len(moved_map)
            print(f"[OK] Archived {total_archived} file(s) to {arch_dir}")
        print("\nSummary:")
        for k in sorted(latest_archived_map.keys()):
            print(f"  {k:<20} -> {staged_paths[k]}  (from {Path(latest_archived_map[k]).name})")

if __name__ == "__main__":
    main()