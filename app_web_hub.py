#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TradeHub Web — minimal, robust Flask UI.
- No Jinja inheritance (no TemplateNotFound).
- Reads suggestions via env SUGGESTION_GLOBS (comma-separated) or defaults to merged+split.
- /suggestions.json, /positions.json, /cards.json are pass-throughs (safe if missing).
- honors outputs/web_feed/settings.json freshness for display only (UI note).
"""

from __future__ import annotations
import glob, json, os, time
from pathlib import Path
from typing import Any, Dict, List, Tuple
from flask import (
    Flask,
    Response,
    jsonify,
    render_template_string,
    send_from_directory,
    abort,
)

ROOT = Path(__file__).resolve().parent
WF = ROOT / "outputs" / "web_feed"
WF.mkdir(parents=True, exist_ok=True)

WEB_HOST = os.environ.get("WEB_HOST", "127.0.0.1")
WEB_PORT = int(os.environ.get("WEB_PORT", "8000"))

DEFAULT_GLOBS = [
    str(WF / "suggestions_merged.json"),
    str(WF / "*_suggestions.json"),
    str(WF / "cards.json"),
]
SUGGESTION_GLOBS = [
    g.strip()
    for g in os.environ.get("SUGGESTION_GLOBS", ",".join(DEFAULT_GLOBS)).split(",")
    if g.strip()
]


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"ok": True, "rows": []}
    try:
        return json.loads(path.read_text() or "{}")
    except Exception:
        return {"ok": False, "rows": []}


def _glob_rows() -> Tuple[List[Dict[str, Any]], List[str]]:
    files: List[str] = []
    rows: List[Dict[str, Any]] = []
    for pat in SUGGESTION_GLOBS:
        for fp in sorted(glob.glob(pat)):
            if fp in files:  # de-dupe
                continue
            files.append(fp)
            try:
                data = json.loads(Path(fp).read_text() or "{}")
            except Exception:
                continue
            rs = data.get("rows") or []
            # normalize for UI
            for r in rs:
                rows.append(
                    {
                        "symbol": r.get("symbol"),
                        "strategy": r.get("strategy"),
                        "expiry": r.get("expiry"),
                        "score": r.get("score"),
                    }
                )
    return rows, files


def _counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    c: Dict[str, int] = {}
    for r in rows:
        s = r.get("strategy") or "other"
        c[s] = c.get(s, 0) + 1
    return c


def _read_settings() -> Dict[str, Any]:
    p = WF / "settings.json"
    try:
        return json.loads(p.read_text() or "{}")
    except Exception:
        return {}


BASE_HTML = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>TradeHub · live</title>
  <style>
    body{font-family: -apple-system, system-ui, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin:20px;}
    h1{margin:0 0 6px 0;font-size:22px}
    .meta{color:#666;margin-bottom:12px}
    table{border-collapse:collapse;width:100%}
    th,td{border-bottom:1px solid #eee;padding:6px 8px;text-align:left;font-size:13px}
    th{background:#fafafa;position:sticky;top:0}
    .pill{display:inline-block;padding:2px 6px;border:1px solid #ccc;border-radius:999px;font-size:12px;margin-left:6px}
    .ok{color:#0a0}
    .err{color:#a00}
    .grid{display:grid;grid-template-columns:1fr; gap:18px;}
    .card{border:1px solid #eee;border-radius:12px;padding:12px}
    .muted{color:#777}
    .mono{font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace}
    .right{float:right}
  </style>
</head>
<body>
  <h1>TradeHub · live</h1>
  <div class="meta">
    Suggestions from <span class="mono">{{ globs }}</span><span class="pill">Total {{ total }}
    {% for k,v in counts.items() %} · {{ k }}:{{ v }}{% endfor %}</span>
    {% if freshness_note %}<span class="pill">{{ freshness_note }}</span>{% endif %}
  </div>

  <div class="grid">
    <div class="card">
      <div><b>Symbol</b> &nbsp; <span class="muted">Strategy</span> &nbsp; <span class="muted">Expiry</span> &nbsp; <span class="muted">Score</span></div>
      <table>
        <thead><tr><th>Symbol</th><th>Strategy</th><th>Expiry</th><th>Score</th></tr></thead>
        <tbody>
        {% for r in rows %}
          <tr><td>{{ r.symbol }}</td><td>{{ r.strategy }}</td><td>{{ r.expiry or "—" }}</td><td>{{ r.score }}</td></tr>
        {% endfor %}
        {% if not rows %}
          <tr><td colspan="4" class="muted">No suggestions</td></tr>
        {% endif %}
        </tbody>
      </table>
    </div>

    <div class="card">
      <div class="muted">Positions — {{ pos_rows|length }} row(s)</div>
      <div class="muted">Feed: positions.json</div>
      <table>
        <thead><tr><th>Symbol</th><th>Legs</th><th>Updated</th></tr></thead>
        <tbody>
        {% for p in pos_rows %}
          <tr><td>{{ p.symbol }}</td><td>{{ p.legs if p.legs is not none else "—" }}</td><td>{{ p.as_of or "—" }}</td></tr>
        {% endfor %}
        {% if not pos_rows %}
          <tr><td colspan="3" class="muted">No positions feed.</td></tr>
        {% endif %}
        </tbody>
      </table>
    </div>

    <div class="muted">Health: <span class="ok">ok</span> · <span class="mono">Reload</span> (⌘+R)</div>
  </div>
</body>
</html>
"""

app = Flask(__name__)


@app.get("/healthz")
def healthz():
    return jsonify(
        {"ok": True, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    )


@app.get("/suggestions.json")
def suggestions_json():
    rows, files = _glob_rows()
    return jsonify(
        {
            "ok": True,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "globs": ",".join(SUGGESTION_GLOBS),
            "files": [
                str(Path(f).relative_to(ROOT)) if f.startswith(str(ROOT)) else f
                for f in files
            ],
            "rows": rows,
            "counts": _counts(rows),
        }
    )


@app.get("/positions.json")
def positions_json():
    p = WF / "positions.json"
    data = _read_json(p)
    # normalize legacy 'items' into 'rows'
    rows = data.get("rows") or data.get("items") or []
    return jsonify(
        {
            "ok": True,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "rows": rows,
        }
    )


@app.get("/cards.json")
def cards_json():
    p = WF / "cards.json"
    return jsonify(_read_json(p))


@app.get("/")
def dashboard():
    rows, files = _glob_rows()
    pos = _read_json(WF / "positions.json")
    pos_rows = pos.get("rows") or pos.get("items") or []

    s = _read_settings()
    fset = s.get("freshness_minutes") or {}
    fresh_sug = fset.get("suggestions")
    fresh_pos = fset.get("positions")
    freshness_note = None
    if fresh_sug or fresh_pos:
        freshness_note = f"freshness mins — suggestions:{fresh_sug or '-'} positions:{fresh_pos or '-'}"

    html = render_template_string(
        BASE_HTML,
        rows=rows,
        globs=",".join(SUGGESTION_GLOBS),
        counts=_counts(rows),
        total=len(rows),
        pos_rows=pos_rows,
        freshness_note=freshness_note,
    )
    return html


def run():
    app.run(host=WEB_HOST, port=WEB_PORT, debug=False, use_reloader=False)


if __name__ == "__main__":
    run()
