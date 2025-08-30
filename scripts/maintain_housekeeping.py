#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
import os, sys, shutil, gzip
from pathlib import Path
from datetime import datetime, timedelta, timezone

ENGINE = Path(__file__).resolve().parents[1]
ROOT = ENGINE
DATA = ROOT / "data"
INCOMING = DATA / "incoming"
ARCHIVE = DATA / "archive"
LOGS = ROOT / "logs"
OUTPUTS = ROOT / "outputs"

UTC = timezone.utc
NOW = datetime.now(UTC)

# ---- settings (tune as you like) -------------------------------------------
# rotate any *.log over this many bytes (5 MB), keep 10 gz rotations
LOG_ROTATE_BYTES = 5 * 1024 * 1024
LOG_MAX_ROTATIONS = 10
# prune archived CSVs older than N days
ARCHIVE_RETENTION_DAYS = 21
# prune *.log.*.gz older than N days
LOG_RETENTION_DAYS = 30
# move any stray files left behind in incoming into an archive folder stamp
INCOMING_LEFTOVER_BUCKET = ARCHIVE / f"leftovers-{NOW.strftime('%Y%m%d-%H%M%S')}"


def ts() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ")


def log(msg: str):
    print(f"[{ts()}] {msg}", flush=True)


def rotate_log(path: Path):
    if not path.exists():
        return
    if path.stat().st_size < LOG_ROTATE_BYTES:
        return
    # find next index
    for idx in range(LOG_MAX_ROTATIONS, 0, -1):
        older = path.with_name(f"{path.name}.{idx}.gz")
        newer = path.with_name(f"{path.name}.{idx-1}.gz")
        if idx == LOG_MAX_ROTATIONS and older.exists():
            older.unlink(missing_ok=True)
        if newer.exists():
            newer.rename(older)
    # .0 becomes freshly gzipped current
    zero = path.with_name(f"{path.name}.0.gz")
    with path.open("rb") as src, gzip.open(zero, "wb") as dst:
        shutil.copyfileobj(src, dst)
    path.write_text("", encoding="utf-8")


def prune_gz_logs(log_dir: Path, days: int):
    cutoff = NOW - timedelta(days=days)
    for p in log_dir.glob("*.log.*.gz"):
        # gz don’t preserve mtime reliably across copies; use file mtime anyway
        mtime = datetime.fromtimestamp(p.stat().st_mtime, UTC)
        if mtime < cutoff:
            p.unlink(missing_ok=True)


def prune_archives(arch_dir: Path, days: int):
    cutoff = NOW - timedelta(days=days)
    for p in arch_dir.rglob("*"):
        if p.is_dir():
            # remove empty dirs created by earlier runs
            try:
                next(p.iterdir())
            except StopIteration:
                # empty
                # if dir mtime < cutoff, remove; otherwise leave to collect more
                if datetime.fromtimestamp(p.stat().st_mtime, UTC) < cutoff:
                    p.rmdir()
            continue
        mtime = datetime.fromtimestamp(p.stat().st_mtime, UTC)
        if mtime < cutoff:
            p.unlink(missing_ok=True)


def sweep_incoming_leftovers():
    if not INCOMING.exists():
        return
    leftovers = [p for p in INCOMING.iterdir() if p.is_file()]
    if not leftovers:
        return
    INCOMING_LEFTOVER_BUCKET.mkdir(parents=True, exist_ok=True)
    for p in leftovers:
        p.rename(INCOMING_LEFTOVER_BUCKET / p.name)
    log(f"archived {len(leftovers)} leftover file(s) → {INCOMING_LEFTOVER_BUCKET}")


def main():
    LOGS.mkdir(parents=True, exist_ok=True)
    log("housekeeping start")

    # 1) rotate big logs
    for name in ("watch_incoming.log", "web_server.log", "maintenance.log"):
        rotate_log(LOGS / name)

    # 2) prune old gz rotations & archives
    prune_gz_logs(LOGS, LOG_RETENTION_DAYS)
    prune_archives(ARCHIVE, ARCHIVE_RETENTION_DAYS)

    # 3) sweep any stray incoming files (very occasionally a ranker crash leaves them)
    sweep_incoming_leftovers()

    log("housekeeping done")


if __name__ == "__main__":
    sys.exit(main())
