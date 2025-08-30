# scripts/utils/data_loader.py
from __future__ import annotations
import csv, sys
from pathlib import Path
from typing import List, Dict, Optional

# ---- project roots ----
def _project_root() -> Path:
    # .../engine/scripts/utils/data_loader.py -> engine root
    return Path(__file__).resolve().parents[2]

ROOT = _project_root()
DATA_DIR = ROOT / "data"
RUNTIME_CATALOG = DATA_DIR / "data_catalog_runtime.yml"
STATIC_CATALOG = DATA_DIR / "data_catalog.yml"

# Optional dependency, used only for YAML catalogs.
def _load_yaml(path: Path) -> Optional[dict]:
    try:
        import yaml  # type: ignore
    except Exception:
        return None
    if not path.exists():
        return None
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def _strip_footer(lines: List[str]) -> List[str]:
    if not lines:
        return lines
    last = lines[-1].strip().lower()
    if last.startswith("downloaded from barchart.com"):
        return lines[:-1]
    # Some files quote the footer
    if "downloaded from barchart.com" in last:
        return lines[:-1]
    return lines

def get_dataset_path(key: str) -> Path:
    """
    Resolve a dataset key to a CSV path.
    Resolution order:
      1) data_catalog_runtime.yml -> datasets[key].file
      2) data_catalog.yml         -> datasets[key].file (or .filename)
      3) fallback: data/<key>-latest.csv
    """
    # 1) runtime
    cat = _load_yaml(RUNTIME_CATALOG)
    if cat and isinstance(cat.get("datasets"), dict):
        ds = cat["datasets"].get(key)
        if isinstance(ds, dict) and ds.get("file"):
            p = Path(ds["file"]).expanduser()
            if p.exists():
                return p

    # 2) static
    cat2 = _load_yaml(STATIC_CATALOG)
    if cat2 and isinstance(cat2.get("datasets"), dict):
        ds2 = cat2["datasets"].get(key)
        if isinstance(ds2, dict):
            p2 = ds2.get("file") or ds2.get("filename")
            if p2:
                pth = Path(p2).expanduser()
                if not pth.is_absolute():
                    pth = (DATA_DIR / pth).resolve()
                if pth.exists():
                    return pth

    # 3) fallback
    fallback = DATA_DIR / f"{key}-latest.csv"
    if fallback.exists():
        return fallback

    tried = [
        str(RUNTIME_CATALOG),
        str(STATIC_CATALOG),
        str(fallback),
    ]
    raise FileNotFoundError(
        f"Could not resolve dataset for key '{key}'. Tried:\n  - " + "\n  - ".join(tried)
    )

def load_barchart_csv(key_or_path: str | Path, *, strip_footer: bool = True) -> List[Dict[str, str]]:
    """
    Load a Barchart CSV as list[dict], optionally stripping the trailing footer.
    - No schema enforcement, no cleaning. Your schemas drive usage elsewhere.
    - Accepts a dataset key (resolved via catalogs) or a file path.
    """
    if isinstance(key_or_path, (str, Path)):
        kp = str(key_or_path)
        if kp.endswith(".csv") or "/" in kp or "\\" in kp:
            path = Path(kp)
        else:
            path = get_dataset_path(kp)
    else:
        path = Path(key_or_path)

    if not path.exists():
        raise FileNotFoundError(path)

    text = path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    if strip_footer:
        lines = _strip_footer(lines)
    if not lines:
        return []

    reader = csv.DictReader(lines)
    rows: List[Dict[str, str]] = []
    for r in reader:
        if r is None:
            continue
        # Keep raw; callers decide which columns to read.
        rows.append({k: v for k, v in r.items()})
    return rows