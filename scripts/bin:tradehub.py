#!/usr/bin/env python3
"""
TradeHub one-word launcher.

- Default: launch/ensure the web server is running and open the app in your browser.
- Optional CLI menu (-m): paste positions (stdin), rebuild feeds, open web, or quit.
"""

from __future__ import annotations
import argparse, os, sys, json, subprocess, time, webbrowser
from pathlib import Path

ENGINE = Path(__file__).resolve().parents[1]
PY = sys.executable if sys.executable else "python3"

LOGS = ENGINE / "logs"
PIDFILE = LOGS / "web_server.pid"
WEB_LOG = LOGS / "web_server.log"


def is_proc_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def ensure_logs():
    LOGS.mkdir(parents=True, exist_ok=True)


def start_web_server():
    ensure_logs()
    # If already running, keep it
    if PIDFILE.exists():
        try:
            pid = int(PIDFILE.read_text().strip())
            if is_proc_alive(pid):
                return pid
        except Exception:
            pass
        # stale pidfile
        PIDFILE.unlink(missing_ok=True)

    # Start server
    cmd = [PY, "-m", "scripts.run_web_server"]
    with open(WEB_LOG, "ab") as lf:
        proc = subprocess.Popen(cmd, cwd=str(ENGINE), stdout=lf, stderr=lf)
    PIDFILE.write_text(str(proc.pid))
    # give it a sec to bind
    time.sleep(0.8)
    return proc.pid


def open_web():
    # sync legacy market json & market_state -> web feed
    try:
        subprocess.run(
            [PY, "-m", "scripts.make_legacy_market_json"], cwd=str(ENGINE), check=False
        )
        subprocess.run(
            [PY, "-m", "scripts.make_web_site_feed"], cwd=str(ENGINE), check=False
        )
    except Exception:
        pass
    pid = start_web_server()
    # Your web server prints the URL into web_server.log; assume http://127.0.0.1:8899 by default
    url = "http://127.0.0.1:8899/"
    webbrowser.open(url)
    print(f"[tradehub] web server pid={pid} • {url}")


def ingest_positions_interactive():
    print(
        "\nPaste your broker rows for ONE SYMBOL, then press Ctrl-D.\n"
        "Strategy options: pmcc | csp | covered_call | short_call | long_call | "
        "vertical_bull_put | vertical_bull_call | iron_condor | diagonal\n"
    )
    strategy = input("Strategy: ").strip()
    if not strategy:
        print("Aborted: no strategy.")
        return
    print("\n--- PASTE BELOW (Ctrl-D to finish) ---\n")
    buf = sys.stdin.buffer.read()

    # Use your wrapper so dashboard updates automatically
    cmd = [PY, "-m", "scripts.positions_ingest_and_build", "--strategy", strategy]
    r = subprocess.run(cmd, input=buf, cwd=str(ENGINE))
    if r.returncode != 0:
        print("[tradehub] ingest failed.", file=sys.stderr)


def rebuild_everything():
    # Market state (indices/VIX) + site feed (suggestions + positions)
    steps = [
        [PY, "-m", "scripts.build_market_state"],
        [PY, "-m", "scripts.make_legacy_market_json"],
        [PY, "-m", "scripts.make_web_site_feed"],
        [PY, "-m", "scripts.positions_build_dashboard"],
    ]
    for cmd in steps:
        print(f"[tradehub] run: {' '.join(cmd)}")
        subprocess.run(cmd, cwd=str(ENGINE))


def cli_menu():
    while True:
        print(
            "\nTradeHub — Quick Menu\n"
            "1) Open Web (recommended)\n"
            "2) Paste Positions (ingest ➜ dashboard)\n"
            "3) Rebuild Feeds (market + suggestions + positions)\n"
            "4) Quit\n"
        )
        choice = input("> ").strip()
        if choice == "1":
            open_web()
        elif choice == "2":
            ingest_positions_interactive()
        elif choice == "3":
            rebuild_everything()
        elif choice == "4" or choice.lower().startswith("q"):
            return
        else:
            print("Pick 1–4.")


def main():
    os.chdir(ENGINE)
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument(
        "-m",
        "--menu",
        action="store_true",
        help="Show CLI menu instead of auto-opening the web UI",
    )
    args, _ = ap.parse_known_args()

    if args.menu:
        cli_menu()
    else:
        open_web()


if __name__ == "__main__":
    main()
