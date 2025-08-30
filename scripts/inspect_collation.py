#!/usr/bin/env python3
# scripts/inspect_collation.py
from __future__ import annotations
import os, json, glob, sys, datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs"
SETTINGS_PATH = OUT / "web_settings.json"


def load_settings():
    # Defaults mirror the web Settings page
    settings = {
        "account_size": 310_000.0,
        "available_capital": 35_000.0,
        "min_score": 0.0,
        "max_age_min": 600,  # 10h default in web; yours likely 480
        "suggestion_globs": None,
        "hide_taken": True,
    }
    if SETTINGS_PATH.exists():
        try:
            s = json.loads(SETTINGS_PATH.read_text())
            settings.update({k: s.get(k, settings[k]) for k in settings})
        except Exception as e:
            print(f"[inspect] WARN: could not parse {SETTINGS_PATH.name}: {e}")
    # Env var overrides (this is what the site uses first)
    env_glob = os.getenv("SUGGESTION_GLOBS")
    if env_glob:
        settings["suggestion_globs"] = env_glob
    if not settings["suggestion_globs"]:
        # Fallback to normalized if present, else raw outputs
        norm = OUT / "web_normalized"
        settings["suggestion_globs"] = (
            str(norm / "*_suggestions.json")
            if norm.exists()
            else str(OUT / "*_suggestions.json")
        )
    return settings


def utc_now():
    return dt.datetime.now(dt.timezone.utc)


def age_minutes(path: Path) -> int:
    m = dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.timezone.utc)
    return int((utc_now() - m).total_seconds() // 60)


def load_one(path: Path):
    try:
        data = json.loads(path.read_text())
        # normalize shape just like the web expects
        if isinstance(data, list):
            items = data
            generated_at = None
        elif isinstance(data, dict):
            items = (
                data.get("top") or data.get("items") or data.get("suggestions") or []
            )
            generated_at = data.get("generated_at")
        else:
            items = []
            generated_at = None
        # ensure each item has required keys
        fixed = []
        for it in items:
            if not isinstance(it, dict):
                continue
            sym = it.get("symbol") or it.get("Symbol") or it.get("ticker")
            score = it.get("score") or it.get("Score")
            flag = it.get("flag") or it.get("Flag")
            if sym is None or score is None or flag is None:
                continue
            # normalize
            fixed.append(
                {
                    "id": it.get("id")
                    or f"{sym}:{it.get('exp','?')}:{it.get('strike','-')}",
                    "symbol": sym,
                    "exp": it.get("exp", "?"),
                    "strike": it.get("strike", "-"),
                    "score": float(score),
                    "flag": str(flag).upper(),
                    "taken": bool(it.get("taken", False)),
                    # pass through some helpful fields if present
                    "delta": it.get("delta"),
                    "bid": it.get("bid"),
                    "pop": it.get("pop"),
                    "credit": it.get("credit"),
                    "debit": it.get("debit"),
                    "strategy": it.get("strategy") or it.get("Strategy"),
                }
            )
        # infer strategy from filename if missing
        strat = path.name.split("_suggestions.", 1)[0]
        for f in fixed:
            if not f.get("strategy"):
                f["strategy"] = strat
        return fixed
    except Exception as e:
        print(f"[inspect] ERROR reading {path.name}: {e}")
        return []


def main():
    settings = load_settings()
    globs = [
        g.strip() for g in str(settings["suggestion_globs"]).split(",") if g.strip()
    ]
    print(f"[inspect] Using globs: {globs}")
    print(
        f"[inspect] Filters: min_score>={settings['min_score']}, max_age_min<={settings['max_age_min']}, hide_taken={settings['hide_taken']}"
    )
    files = []
    for g in globs:
        for p in glob.glob(g):
            files.append(Path(p))
    if not files:
        print("[inspect] NO FILES matched. Check your glob(s).")
        sys.exit(2)
    by_file = {}
    for p in sorted(files):
        a = age_minutes(p)
        by_file[p] = {"age_min": a, "items": load_one(p)}
    total_kept = 0
    for p, info in by_file.items():
        age_ok = info["age_min"] <= settings["max_age_min"]
        kept = []
        for it in info["items"]:
            if settings["hide_taken"] and it.get("taken"):
                continue
            if float(it["score"]) < float(settings["min_score"]):
                continue
            kept.append(it)
        total_kept += len(kept)
        print(
            f"[inspect] {p.name}: age={info['age_min']}m | raw={len(info['items'])} | kept={len(kept)}"
        )
        if kept[:3]:
            top3 = ", ".join(
                f"{x['symbol']}({x['score']:.3f},{x['flag']})" for x in kept[:3]
            )
            print(f"          top: {top3}")
    print(f"[inspect] TOTAL kept across all = {total_kept}")
    if total_kept == 0:
        print(
            "[inspect] If raw>0 but kept=0, your filters/age/glob may be excluding everything."
        )


if __name__ == "__main__":
    main()
