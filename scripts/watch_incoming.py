#!/usr/bin/env python3
"""
TradeHub – resilient watcher for data/incoming

Behavior:
- Polls data/incoming/*.csv
- If any files exist, triggers the one-shot pipeline:
    1) scripts.ingest_latest --from-dir data/incoming --archive-to data/processed
    2) scripts.make_web_site_feed
    3) scripts.suggestions_merge
- Archives processed files under data/processed/YYYY-MM-DD/
- No filename regex games; routing is delegated to ingest_latest by reading headers.
- Uses a file lock to avoid overlapping runs.
- Poll every 30s during market hours (08:00–17:00 Central), else every 300s by default.
  You can override intervals via env.

Env (optional):
  WATCH_DIR           default: data/incoming
  ARCHIVE_DIR         default: data/processed
  ACTIVE_INTERVAL_S   default: 30
  OFFHOURS_INTERVAL_S default: 300
  TZ                  default: America/Chicago
"""

from __future__ import annotations
import os, sys, time, json, subprocess, pathlib, datetime
from zoneinfo import ZoneInfo

ROOT = pathlib.Path(__file__).resolve().parents[1]
LOGS = ROOT / "logs"
LOGS.mkdir(exist_ok=True)

WATCH_DIR = pathlib.Path(os.environ.get("WATCH_DIR", ROOT / "data" / "incoming"))
ARCHIVE_DIR = pathlib.Path(os.environ.get("ARCHIVE_DIR", ROOT / "data" / "processed"))
TZ = ZoneInfo(os.environ.get("TZ", "America/Chicago"))

ACTIVE_INTERVAL_S = int(os.environ.get("ACTIVE_INTERVAL_S", "30"))
OFFHOURS_INTERVAL_S = int(os.environ.get("OFFHOURS_INTERVAL_S", "300"))

LOCKFILE = LOGS / "watch_incoming.lock"
LOGFILE = LOGS / "watch_incoming.log"
ERRFILE = LOGS / "watch_incoming.err"


def log(msg: str):
    ts = datetime.datetime.now(tz=ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%SZ")
    with LOGFILE.open("a") as f:
        f.write(f"[{ts}] INFO: {msg}\n")
    print(msg, flush=True)


def log_err(msg: str):
    ts = datetime.datetime.now(tz=ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%SZ")
    with ERRFILE.open("a") as f:
        f.write(f"[{ts}] ERROR: {msg}\n")
    print(msg, file=sys.stderr, flush=True)


def is_market_active_now(dt: datetime.datetime) -> bool:
    # Simple: fast polling 08:00–17:00 Central
    hr = dt.astimezone(TZ).hour
    return 8 <= hr < 17


def acquire_lock() -> bool:
    try:
        fd = os.open(LOCKFILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return True
    except FileExistsError:
        return False


def release_lock():
    try:
        LOCKFILE.unlink(missing_ok=True)
    except Exception:
        pass


def run(cmd: list[str]) -> int:
    p = subprocess.run(cmd, cwd=str(ROOT))
    return p.returncode


def files_in_incoming() -> list[pathlib.Path]:
    return sorted(WATCH_DIR.glob("*.csv"))


def one_shot_pipeline():
    # Ingest + archive everything present
    rc1 = run(
        [
            sys.executable,
            "-m",
            "scripts.ingest_latest",
            "--from-dir",
            str(WATCH_DIR),
            "--archive-to",
            str(ARCHIVE_DIR),
        ]
    )
    if rc1 != 0:
        log_err(f"ingest_latest failed rc={rc1}")
        return

    # Rebuild site feeds
    rc2 = run([sys.executable, "-m", "scripts.make_web_site_feed"])
    if rc2 != 0:
        log_err(f"make_web_site_feed failed rc={rc2}")

    # Merge suggestions for web hub
    rc3 = run([sys.executable, "-m", "scripts.suggestions_merge"])
    if rc3 != 0:
        log_err(f"suggestions_merge failed rc={rc3}")


def main():
    WATCH_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    log(f"watch_incoming started — monitoring {WATCH_DIR} (archive -> {ARCHIVE_DIR})")
    while True:
        try:
            now = datetime.datetime.now(tz=TZ)
            interval = (
                ACTIVE_INTERVAL_S if is_market_active_now(now) else OFFHOURS_INTERVAL_S
            )

            matches = files_in_incoming()
            if matches:
                if acquire_lock():
                    try:
                        log(
                            f"detected {len(matches)} file(s): "
                            + ", ".join(p.name for p in matches)
                        )
                        one_shot_pipeline()
                    finally:
                        release_lock()
                else:
                    log("skip: pipeline already running (lock held)")
            else:
                log("heartbeat (no matching *.csv in incoming)")

            time.sleep(interval)
        except KeyboardInterrupt:
            log("stopping (KeyboardInterrupt)")
            break
        except Exception as e:
            log_err(f"watch loop error: {e!r}")
            time.sleep(10)


if __name__ == "__main__":
    main()
