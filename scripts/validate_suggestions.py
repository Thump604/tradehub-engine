#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
validate_suggestions.py
- Inspect original outputs/*_suggestions.{json,yml,yaml}
- Report missing keys that the web expects
- Non-destructive; exits 0 if OK, 1 if issues found
"""

from __future__ import annotations
import os, sys, glob, json
from pathlib import Path
from typing import Any, Dict, List

try:
    import yaml

    HAVE_YAML = True
except Exception:
    HAVE_YAML = False

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs"
SRC_GLOB = str(OUT_DIR / "*_suggestions.*")

REQ_KEYS_ITEM = {
    "symbol",
    "score",
    "flag",
}  # id/exp/strike/taken are normalized if missing


def load_any(path: Path) -> Any:
    txt = path.read_text(encoding="utf-8").strip()
    if path.suffix.lower() in (".yml", ".yaml"):
        if not HAVE_YAML:
            raise RuntimeError("PyYAML not installed")
        return yaml.safe_load(txt)
    return json.loads(txt) if txt else {}


def iter_items(payload: Any):
    if isinstance(payload, dict) and isinstance(payload.get("top"), list):
        yield from payload["top"]
        return
    if isinstance(payload, list):
        yield from payload
        return
    if isinstance(payload, dict):
        for v in payload.values():
            if isinstance(v, list):
                yield from v
                return


def main() -> int:
    files = [Path(p) for p in glob.glob(SRC_GLOB) if "web_normalized" not in p]
    if not files:
        print("[validate] No suggestion files found.")
        return 1

    bad = 0
    for p in sorted(files):
        try:
            data = load_any(p)
        except Exception as e:
            print(f"[validate] {p.name}: cannot load ({e})")
            bad += 1
            continue
        missing_any = False
        count = 0
        for row in iter_items(data) or []:
            if not isinstance(row, dict):
                missing_any = True
                continue
            missing = [k for k in REQ_KEYS_ITEM if k not in row]
            if missing:
                missing_any = True
            count += 1
        if missing_any:
            print(f"[validate] {p.name}: {count} items (some missing {REQ_KEYS_ITEM})")
            bad += 1
        else:
            print(f"[validate] {p.name}: OK ({count} items)")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
