# scripts/utils_render.py
from __future__ import annotations
from typing import Iterable, Tuple, Union


# Simple color/flag passthrough (kept minimal to avoid terminal deps)
# You can later replace with rich/colored output if you like.
def color_flag(flag: str) -> str:
    return (flag or "").upper()


def card_text(
    title: str,
    rows: Union[Iterable[Tuple[str, str]], Iterable[str]],
    *,
    width: int = 80,
) -> str:
    """
    Render a lightweight text 'card' used by rankers.

    rows can be:
      - list[tuple[label, value]] -> "Label: Value"
      - list[str] -> printed verbatim

    width is only used to clamp the title underline.
    """
    out: list[str] = []
    t = (title or "").strip()
    if t:
        out.append(t)
        out.append("-" * min(max(len(t), 8), width))

    # Try tuple rows first
    try:
        as_pairs = list(rows)  # type: ignore
    except TypeError:
        as_pairs = []

    if as_pairs and isinstance(as_pairs[0], tuple):
        for k, v in as_pairs:  # type: ignore
            out.append(f"{str(k).strip()}: {str(v).strip()}")
    else:
        for line in rows:  # type: ignore
            out.append(str(line))

    return "\n".join(out)
