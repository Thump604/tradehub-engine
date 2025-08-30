#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import shutil, re, sys, csv, time
from pathlib import Path
from datetime import datetime

try:
    import yaml  # type: ignore

    HAVE_YAML = True
except Exception:
    HAVE_YAML = False

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
INCOMING = DATA / "incoming"
ARCHIVE = DATA / "archive"
DATA.mkdir(exist_ok=True)
INCOMING.mkdir(parents=True, exist_ok=True)
ARCHIVE.mkdir(parents=True, exist_ok=True)

# ---- strategy → filename keywords & staged name
PATTERNS = {
    "covered_call": (["covered-call", "covered_call"], "covered_call-latest.csv"),
    "csp": (["naked-put", "csp"], "csp-latest.csv"),
    "vertical_bull_call": (
        ["bull-call-spread", "vertical-bull-call"],
        "vertical_bull_call-latest.csv",
    ),
    "vertical_bull_put": (
        ["bull-put-spread", "vertical-bull-put"],
        "vertical_bull_put-latest.csv",
    ),
    "diagonal": (
        ["long-call-diagonal", "long_call_diagonal"],
        "long_call_diagonal-latest.csv",
    ),
    "iron_condor": (["short-iron-condor", "iron_condor"], "iron_condor-latest.csv"),
    "leap": (["long-call-options", "long_call_options", "leap"], "leap-latest.csv"),
    # new:
    "long_call": (
        ["long-call-options", "long_call_options", "leap"],
        "long_call-latest.csv",
    ),
    "short_call": (
        ["covered-call-option", "covered_call"],
        "short_call-latest.csv",
    ),  # proxy via covered-call screener
    # market snapshot
    "market": (
        ["market-indices", "market indices", "indices"],
        "market-indices-latest.csv",
    ),
}


def newest_matching(keywords: list[str]) -> list[Path]:
    files = []
    for p in INCOMING.glob("*.csv"):
        name = p.name.lower()
        if any(k in name for k in keywords):
            files.append(p)
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def stage_file(src: Path, dst_name: str):
    dst = DATA / dst_name
    shutil.copy2(src, dst)  # copy to staged
    return dst


def archive_moved(files: list[Path]):
    if not files:
        return None
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    dest_dir = ARCHIVE / stamp
    dest_dir.mkdir(parents=True, exist_ok=True)
    for f in files:
        try:
            shutil.move(str(f), dest_dir / f.name)
        except Exception:
            # if already moved by user, ignore
            pass
    return dest_dir


def write_runtime_catalog(staged: dict[str, str]):
    y = DATA / "data_catalog_runtime.yml"
    doc = {"staged": staged, "generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    if HAVE_YAML:
        y.write_text(yaml.safe_dump(doc, sort_keys=False))
    else:
        # minimal fallback
        lines = ["staged:"]
        for k, v in staged.items():
            lines.append(f"  {k}: {v}")
        lines.append(f"generated: {doc['generated']}")
        y.write_text("\n".join(lines))


def process():
    staged = {}
    moved_all = []

    for key, (kw, staged_name) in PATTERNS.items():
        cand = newest_matching(kw)
        if not cand:
            continue
        # We stage the newest only; archive all that we touched (every matching file)
        newest = cand[0]
        stage_file(newest, staged_name)
        staged[key] = staged_name
        moved_all.extend(cand)

    # After staging, archive every processed incoming file to keep INCOMING clean
    archived_dir = archive_moved(moved_all)

    write_runtime_catalog(staged)

    # Summary
    print("Summary:")
    for k, v in staged.items():
        print(f"  {k:18s} -> data/{v}")
    if archived_dir:
        print(f"Archived {len(moved_all)} file(s) → {archived_dir}")
    else:
        print("Archived 0 file(s).")
    sys.stdout.flush()


if __name__ == "__main__":
    process()
