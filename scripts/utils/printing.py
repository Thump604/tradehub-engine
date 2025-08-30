# printing.py — common printing helpers for rank scripts

def colorize_flag(flag: str) -> str:
    """Return flag unchanged or add color if desired."""
    return flag or ""

def hr():
    """Print a horizontal rule."""
    print("-" * 70)