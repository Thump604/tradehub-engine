# scripts/utils_io.py
from __future__ import annotations

import json
import os
import pathlib
import tempfile
from typing import Any


def _as_path(pathlike: os.PathLike | str) -> pathlib.Path:
    return pathlib.Path(pathlike)


def ensure_dir(pathlike: os.PathLike | str) -> None:
    """
    Ensure a directory exists. If a file path is provided, ensure its parent exists.
    """
    p = _as_path(pathlike)
    target = p if p.suffix == "" else p.parent
    target.mkdir(parents=True, exist_ok=True)


def _atomic_write_bytes(dest: pathlib.Path, data: bytes) -> None:
    """
    Write bytes to `dest` atomically (write to a temp file in the same dir, then replace).
    """
    ensure_dir(dest)
    with tempfile.NamedTemporaryFile(dir=str(dest.parent), delete=False) as tmp:
        tmp_name = tmp.name
        tmp.write(data)
        tmp.flush()
        os.fsync(tmp.fileno())
    os.replace(tmp_name, dest)  # atomic within same filesystem


def write_json(
    pathlike: os.PathLike | str, obj: Any, *, indent: int = 2
) -> pathlib.Path:
    """
    Pretty-write JSON atomically. Returns the Path written.
    """
    dest = _as_path(pathlike)
    text = json.dumps(obj, indent=indent, ensure_ascii=False)
    _atomic_write_bytes(dest, text.encode("utf-8"))
    return dest


def write_yaml(pathlike: os.PathLike | str, obj: Any) -> pathlib.Path:
    """
    Write YAML if PyYAML available; otherwise JSON-with-.yml fallback.
    Returns the Path written.
    """
    dest = _as_path(pathlike)
    try:
        import yaml  # type: ignore
    except Exception:
        # Fallback: still write something human-readable
        text = json.dumps(obj, indent=2, ensure_ascii=False)
        _atomic_write_bytes(dest, text.encode("utf-8"))
        return dest

    ensure_dir(dest)
    with tempfile.NamedTemporaryFile(
        dir=str(dest.parent), delete=False, mode="w", encoding="utf-8"
    ) as tmp:
        tmp_name = tmp.name
        yaml.safe_dump(
            obj, tmp, sort_keys=False, default_flow_style=False, allow_unicode=True
        )
        tmp.flush()
        os.fsync(tmp.fileno())
    os.replace(tmp_name, dest)
    return dest
