# scripts/suggestions_merge.py
from __future__ import annotations
from typing import List
from .rank_base import WEB_FEED, read_items_forgiving, write_json

FILES = [
    "csp_suggestions.json",
    "covered_call_suggestions.json",
    "pmcc_suggestions.json",
    "vertical_bull_call_suggestions.json",
    "vertical_bull_put_suggestions.json",
    "diagonal_suggestions.json",
    "iron_condor_suggestions.json",
]


def flatten_items(x):
    # Accept list, dict-with-items, or single dict
    out = []
    if isinstance(x, list):
        for el in x:
            if isinstance(el, list):
                out.extend(flatten_items(el))
            elif isinstance(el, dict):
                out.append(el)
    elif isinstance(x, dict):
        if "items" in x and isinstance(x["items"], list):
            out.extend(flatten_items(x["items"]))
        else:
            out.append(x)
    return out

def main():
    merged: List[dict] = []
    for name in FILES:
        items = read_items_forgiving(WEB_FEED / name)
        if items:
            merged.extend(items)
    write_json(WEB_FEED / "suggestions_merged.json", merged)
    print(
        f"[merge] suggestions -> {WEB_FEED/'suggestions_merged.json'} (rows={len(merged)} files={len(FILES)})"
    )


if __name__ == "__main__":
    main()
