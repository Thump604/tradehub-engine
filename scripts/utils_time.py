# scripts/utils_time.py
from __future__ import annotations
from datetime import datetime, date


def parse_expiry(s: str) -> date:
    """
    Accepts 'YYYY-MM-DD' or 'MM/DD/YYYY' (typical across our CSVs/feeds).
    """
    s = (s or "").strip()
    if not s:
        raise ValueError("empty expiry string")
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    # Fallback like 'YYYY/MM/DD'
    parts = s.replace("-", "/").split("/")
    if len(parts) == 3 and len(parts[0]) == 4:
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    raise ValueError(f"Unrecognized expiry format: {s!r}")


def compute_dte(expiry_str: str, *, today: date | None = None) -> int:
    """
    Days to expiry (floor). Non-negative (clamps at 0).
    """
    d0 = today or date.today()
    d1 = parse_expiry(expiry_str)
    return max(0, (d1 - d0).days)
