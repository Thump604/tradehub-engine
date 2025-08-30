#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TradeHub Web — single-file Flask app
- Reads config from env and outputs/web_feed/settings.json
- Merges suggestions across multiple globs
- Serves health, suggestions, positions, cards, and symbol pages
- Uses in-memory Jinja templates with DictLoader (no filesystem templates)

ENV:
  WEB_HOST            default 127.0.0.1
  WEB_PORT            default 8000
  SUGGESTION_GLOBS    default "outputs/web_feed/*_suggestions.json"
"""

from __future__ import annotations

import os
import json
import time
import glob
import pathlib
from typing import Any, Dict, List, Tuple

from flask import Flask, jsonify, render_template, request, abort
from jinja2 import DictLoader

# --------------------------------------------------------------------------------------
# Config & constants

ROOT = pathlib.Path(__file__).resolve().parent
WEB_HOST = os.environ.get("WEB_HOST", "127.0.0.1")
WEB_PORT = int(os.environ.get("WEB_PORT", "8000"))
SUGGESTION_GLOBS_ENV = os.environ.get(
    "SUGGESTION_GLOBS",
    "outputs/web_feed/*_suggestions.json",
)

# Canonical feed paths
WEB_FEED_DIR = ROOT / "outputs" / "web_feed"
POS_PATH = WEB_FEED_DIR / "positions.json"
CARDS_PATH = WEB_FEED_DIR / "cards.json"
SETTINGS_PATH = WEB_FEED_DIR / "settings.json"

# --------------------------------------------------------------------------------------
# App & templates (DictLoader so we avoid TemplateNotFound)

app = Flask(__name__)

TEMPLATES: Dict[str, str] = {
    "base.html": """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>TradeHub · live</title>
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 16px; color: #111; }
    header { display:flex; gap:12px; align-items:baseline; flex-wrap:wrap; }
    small, code { color:#666; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border-bottom: 1px solid #eee; padding: 6px 8px; text-align:left; }
    th { background:#fafafa; position: sticky; top:0; }
    .pill { border:1px solid #ddd; border-radius:999px; padding:2px 8px; font-size:12px; color:#333; background:#f8f8f8; }
    .health-ok { color: #0a7e24; font-weight:600; }
    .health-bad { color: #b00020; font-weight:600; }
    .grid { display:grid; grid-template-columns: 1fr; gap:18px; }
    @media (min-width: 1024px) {
      .grid { grid-template-columns: 2fr 1fr; }
    }
    .card { border:1px solid #eee; border-radius:12px; padding:12px; box-shadow: 0 1px 2px rgba(0,0,0,0.03); background:#fff; }
    .muted { color:#777; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; }
    .nowrap { white-space: nowrap; }
  </style>
</head>
<body>
  <header>
    <h2 style="margin:0">TradeHub · live</h2>
    <span class="pill">Health: {{ health_bad and "check" or "ok" }}</span>
    <a class="pill" href="/" title="Reload">Reload</a>
    <small class="muted">Globs: <code>{{ globs|e }}</code></small>
  </header>
  {% if health_note %}<p class="{{ 'health-bad' if health_bad else 'health-ok' }}">{{ health_note }}</p>{% endif %}
  {% block body %}{% endblock %}
</body>
</html>
""",
    "dashboard.html": """
{% extends "base.html" %}
{% block body %}
<div class="grid">
  <section class="card">
    <h3 style="margin-top:0">Suggestions <small class="muted">from {{ globs }}</small></h3>
    <div style="margin:8px 0">
      <span class="pill">Total {{ counts_total }}</span>
      {% for k, v in counts_by_strategy.items() %}
        <span class="pill">{{ k }}:{{ v }}</span>
      {% endfor %}
      {% if has_cards %}<span class="pill">cards.json</span>{% endif %}
    </div>
    <div class="muted" style="margin:6px 0">
      Showing top {{ rows|length }} row(s).
    </div>
    <div style="max-height: 45vh; overflow:auto; border:1px solid #eee; border-radius:8px;">
      <table>
        <thead><tr>
          <th>Symbol</th><th>Strategy</th><th>Expiry</th><th>Score</th>
        </tr></thead>
        <tbody>
        {% for r in rows %}
          <tr>
            <td class="nowrap"><a href="{{ url_for('symbol', sym=r.symbol) }}">{{ r.symbol }}</a></td>
            <td>{{ r.strategy or "—" }}</td>
            <td class="nowrap">{{ r.expiry or "—" }}</td>
            <td class="mono">{{ r.score_fmt }}</td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
    </div>
  </section>

  <aside class="card">
    <h3 style="margin-top:0">Positions <small class="muted">Feed: positions.json</small></h3>
    {% if positions.rows %}
    <div class="muted" style="margin:6px 0">
      {{ positions.rows|length }} row(s)
    </div>
    <div style="max-height: 45vh; overflow:auto; border:1px solid #eee; border-radius:8px;">
      <table>
        <thead><tr><th>Symbol</th><th>Legs</th><th>Updated</th></tr></thead>
        <tbody>
        {% for p in positions.rows %}
          <tr>
            <td class="nowrap"><a href="{{ url_for('symbol', sym=p.symbol) }}">{{ p.symbol }}</a></td>
            <td class="mono">{{ p.legs if p.legs is not none else "—" }}</td>
            <td class="mono">{{ p.as_of or "—" }}</td>
          </tr>
        {% endfor %}
        </tbody>
      </table>
    </div>
    {% else %}
      <div class="muted">No positions feed.</div>
    {% endif %}
  </aside>
</div>
{% endblock %}
""",
    "symbol.html": """
{% extends "base.html" %}
{% block body %}
<section class="card">
  <h3 style="margin-top:0">Symbol: {{ sym }}</h3>
  {% if sugg_rows %}
    <h4>Suggestions</h4>
    <table>
      <thead><tr><th>Strategy</th><th>Expiry</th><th>Score</th></tr></thead>
      <tbody>
        {% for r in sugg_rows %}
        <tr>
          <td>{{ r.strategy or "—" }}</td>
          <td class="nowrap">{{ r.expiry or "—" }}</td>
          <td class="mono">{{ r.score_fmt }}</td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  {% else %}
    <div class="muted">No suggestions for {{ sym }}.</div>
  {% endif %}

  {% if cards %}
    <h4 style="margin-top:16px;">Cards</h4>
    <div class="muted">({{ cards|length }} card(s))</div>
    <ul>
    {% for c in cards %}
      <li><span class="mono">{{ c.kind or "trade" }}</span>
        — {{ (c.title or c.summary or c.note or "—")|e }}</li>
    {% endfor %}
    </ul>
  {% endif %}
</section>
{% endblock %}
""",
}

app.jinja_loader = DictLoader(TEMPLATES)

# --------------------------------------------------------------------------------------
# Utilities


def _json_load(path: pathlib.Path) -> Dict[str, Any] | List[Any] | None:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return None


def _coerce_list(x: Any) -> List[Any]:
    if isinstance(x, list):
        return x
    return []


def _utc_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _file_age_minutes(path: str | pathlib.Path) -> float | None:
    try:
        st = os.stat(str(path))
        return max(0.0, (time.time() - st.st_mtime) / 60.0)
    except Exception:
        return None


def settings_read() -> Dict[str, Any]:
    defaults = {"freshness_minutes": {"suggestions": 180, "positions": 60}}
    try:
        data = _json_load(SETTINGS_PATH) or {}
        fm = data.get("freshness_minutes") or {}
        sug = int(fm.get("suggestions", defaults["freshness_minutes"]["suggestions"]))
        pos = int(fm.get("positions", defaults["freshness_minutes"]["positions"]))
        return {"freshness_minutes": {"suggestions": sug, "positions": pos}}
    except Exception:
        return defaults


def _normalize_suggestion_row(row: Dict[str, Any]) -> Dict[str, Any]:
    sym = (row.get("symbol") or row.get("sym") or "").strip().upper()
    strat = (row.get("strategy") or row.get("kind") or row.get("type") or "").strip()
    exp = row.get("expiry") or row.get("exp") or row.get("expiration") or "—"
    score = row.get("score")
    # format score
    try:
        score_fmt = f"{float(score):g}" if score is not None else "—"
    except Exception:
        score_fmt = str(score or "—")
    return {
        "symbol": sym or "—",
        "strategy": strat or "other",
        "expiry": exp,
        "score": score,
        "score_fmt": score_fmt,
        "_raw": row,
    }


def _load_positions() -> Dict[str, Any]:
    data = _json_load(POS_PATH) or {}
    rows = data.get("rows")
    if not rows:
        items = _coerce_list(data.get("items"))
        if items:
            rows = items
    # map to small shape for table
    mapped = []
    for r in _coerce_list(rows):
        mapped.append(
            {
                "symbol": (r.get("symbol") or "").upper(),
                "as_of": r.get("as_of"),
                "legs": r.get("legs") or r.get("num_legs") or r.get("count") or "—",
            }
        )
    return {"rows": mapped, "ok": True}


def _merge_suggestions(
    globs_csv: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, int], bool]:
    """
    Returns (rows, counts_by_strategy, has_cards)
    """
    files: List[str] = []
    for g in globs_csv.split(","):
        g = g.strip()
        if not g:
            continue
        files.extend(glob.glob(g))
    rows: List[Dict[str, Any]] = []
    counts: Dict[str, int] = {}
    has_cards = False

    # priority: if a merged file exists, include it naturally in the merge like any other
    for f in sorted(set(files)):
        p = pathlib.Path(f)
        if p.name == "cards.json":
            has_cards = True
            continue
        data = _json_load(p)
        if not data:
            continue
        rs = data.get("rows") if isinstance(data, dict) else None
        if rs is None and isinstance(data, dict):
            rs = data.get("items")
        if not isinstance(rs, list):
            continue
        for r in rs:
            if not isinstance(r, dict):
                continue
            nr = _normalize_suggestion_row(r)
            rows.append(nr)
            counts[nr["strategy"]] = counts.get(nr["strategy"], 0) + 1

    # Limit displayed rows for the dashboard (fast render)
    rows.sort(
        key=lambda r: (
            r.get("symbol", ""),
            str(r.get("expiry", "")),
            -(float(r.get("score") or 0)),
        ),
        reverse=False,
    )
    return rows[:500], counts, has_cards


# --------------------------------------------------------------------------------------
# Routes


@app.get("/healthz")
def healthz():
    s = settings_read()
    fmins = s["freshness_minutes"]

    sugg_globs = SUGGESTION_GLOBS_ENV.split(",")
    sug_candidates: List[str] = []
    for g in sugg_globs:
        sug_candidates.extend(glob.glob(g.strip()))
    sugg_age = None
    if sug_candidates:
        try:
            newest = max(sug_candidates, key=lambda p: os.stat(p).st_mtime)
            sugg_age = _file_age_minutes(newest)
        except Exception:
            pass

    pos_age = _file_age_minutes(POS_PATH)

    stale = {
        "suggestions": (sugg_age is not None and sugg_age > fmins["suggestions"]),
        "positions": (pos_age is not None and pos_age > fmins["positions"]),
    }
    return jsonify(
        {
            "ok": not any(stale.values()),
            "stale": stale,
            "ages_min": {"suggestions": sugg_age, "positions": pos_age},
            "thresholds_min": fmins,
            "ts": _utc_ts(),
        }
    )


@app.get("/suggestions.json")
def suggestions_json():
    rows, counts, has_cards = _merge_suggestions(SUGGESTION_GLOBS_ENV)
    return jsonify(
        {
            "ok": True,
            "globs": SUGGESTION_GLOBS_ENV,
            "counts": counts,
            "rows": rows,
            "has_cards": has_cards,
            "ts": _utc_ts(),
        }
    )


@app.get("/positions.json")
def positions_json():
    data = _load_positions()
    data["ts"] = _utc_ts()
    return jsonify(data)


@app.get("/cards.json")
def cards_json():
    data = _json_load(CARDS_PATH)
    rows = []
    if isinstance(data, dict):
        rows = _coerce_list(data.get("rows"))
    elif isinstance(data, list):
        rows = data
    return jsonify({"ok": True, "rows": rows, "ts": _utc_ts()})


@app.get("/")
def dashboard():
    rows, counts, has_cards = _merge_suggestions(SUGGESTION_GLOBS_ENV)
    positions = _load_positions()

    # health banner
    hz = healthz().json
    health_bad = not hz.get("ok", True)
    reasons = []
    st = hz.get("stale") or {}
    if st.get("suggestions"):
        reasons.append("suggestions stale")
    if st.get("positions"):
        reasons.append("positions stale")
    health_note = " · ".join(reasons) if reasons else "ok"

    return render_template(
        "dashboard.html",
        globs=SUGGESTION_GLOBS_ENV,
        rows=rows,
        counts_by_strategy=counts,
        counts_total=sum(counts.values()),
        positions=positions,
        has_cards=has_cards,
        health_bad=health_bad,
        health_note=health_note,
    )


@app.get("/symbol/<sym>")
def symbol(sym: str):
    sym = (sym or "").strip().upper()
    if not sym:
        abort(404)
    rows, _, _ = _merge_suggestions(SUGGESTION_GLOBS_ENV)
    sugg_rows = [r for r in rows if r.get("symbol") == sym]

    # cards for that symbol
    cards_data = cards_json().json
    cards_all = cards_data.get("rows") or []
    cards = [c for c in cards_all if (c.get("symbol") or "").upper() == sym]

    hz = healthz().json
    health_bad = not hz.get("ok", True)
    st = hz.get("stale") or {}
    reasons = []
    if st.get("suggestions"):
        reasons.append("suggestions stale")
    if st.get("positions"):
        reasons.append("positions stale")
    health_note = " · ".join(reasons) if reasons else "ok"

    return render_template(
        "symbol.html",
        sym=sym,
        sugg_rows=sugg_rows,
        cards=cards,
        globs=SUGGESTION_GLOBS_ENV,
        health_bad=health_bad,
        health_note=health_note,
    )


# --------------------------------------------------------------------------------------
# Entrypoint


def main():
    print(
        f"[web] TradeHub Web starting — http://{WEB_HOST}:{WEB_PORT} • globs={SUGGESTION_GLOBS_ENV}"
    )
    app.run(host=WEB_HOST, port=WEB_PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
