# scripts/positions_build_dashboard.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .rank_base import WEB_FEED_DIR, write_json

# This is a no-op builder unless you have a brokerage source.
# It preserves existing fields and ensures a top-level "rows" key exists.


def main() -> None:
    p = WEB_FEED_DIR / "positions.json"
    rows = []
    payload = {
        "ok": True,
        "rows": rows,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    if p.exists():
        try:
            cur = json.loads(p.read_text(encoding="utf-8"))
            rows = cur.get("rows") or cur.get("items") or []
            payload.update(cur)
            payload["rows"] = rows
        except Exception:
            pass
    write_json(p, payload)
    print(f"[positions] dashboard feed -> {p}")
    print(
        f"[positions] timeseries CSVs -> {WEB_FEED_DIR.parent / 'positions' / 'timeseries' / '<SYM>.csv'}"
    )


if __name__ == "__main__":
    main()
