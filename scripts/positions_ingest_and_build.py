# scripts/positions_ingest_and_build.py
from __future__ import annotations
import sys, subprocess, shlex, os
from pathlib import Path

ENGINE_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    # Pass-through CLI (we only care about --strategy and any future flags)
    args = sys.argv[1:]

    # Slurp stdin once (supports pbpaste | python -m scripts.positions_ingest_and_build --strategy pmcc)
    buf = sys.stdin.buffer.read()

    # 1) Run the existing ingestor
    ingest_cmd = [sys.executable, "-m", "scripts.positions_ingest_symbol", *args]
    r1 = subprocess.run(ingest_cmd, input=buf, capture_output=True)

    # Echo through the ingestor’s stdout/stderr verbatim so your UX stays the same
    if r1.stdout:
        sys.stdout.buffer.write(r1.stdout)
    if r1.stderr:
        sys.stderr.buffer.write(r1.stderr)

    if r1.returncode != 0:
        sys.exit(r1.returncode)

    # 2) Immediately refresh the dashboard feed & timeseries
    build_cmd = [sys.executable, "-m", "scripts.positions_build_dashboard"]
    r2 = subprocess.run(build_cmd, capture_output=True)
    if r2.stdout:
        sys.stdout.buffer.write(r2.stdout)
    if r2.stderr:
        sys.stderr.buffer.write(r2.stderr)

    sys.exit(r2.returncode)


if __name__ == "__main__":
    main()
