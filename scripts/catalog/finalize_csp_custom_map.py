from __future__ import annotations
from pathlib import Path
from pprint import pformat

CAT = Path("catalog/schemas.py")
ns = {}
exec(compile(CAT.read_text(encoding="utf-8"), str(CAT), "exec"), ns, ns)
SCHEMAS = dict(ns.get("SCHEMAS", {}))

def build_position_aware_map(physical_cols: list[str]) -> dict[str, str]:
    out, ask_seen = {}, 0
    for c in physical_cols:
        if c == "Ask":
            ask_seen += 1
            out[c] = "Ask Price" if ask_seen == 1 else "Option Ask Price"
        elif c == "Bid":
            out[c] = "Bid Price"
        elif c == "Last":
            out[c] = "Option Last Price"
        elif c == "Volume":
            out[c] = "Option Volume"
        elif c == "%Chg~":
            out[c] = "Percent Change"
        elif c == "Volume~":
            out[c] = "Volume"
        elif c == "Exp Date":
            out[c] = "Expiration Date"
        elif c == "Strike":
            out[c] = "Strike Price"
        elif c == "Type":
            out[c] = "Option Type"
        elif c == "BE (Ask)":
            out[c] = "Break Even (Ask)"
        elif c == "%BE (Ask)":
            out[c] = "% to Break Even (Ask)"
        elif c == "Ann Yield to Strike%":
            out[c] = "Yield to Strike Annual Rtn%"
        elif c == "ITM Prob":
            out[c] = "ITM Probability"
        elif c == "OTM Prob":
            out[c] = "OTM Probability"
        elif c == "Total OI":
            out[c] = "Total Options Open Interest"
        elif c == "Short Term~":
            out[c] = "Short Term Opinion Signal"
        else:
            out[c] = c
    return out

changed = False

if "covered_call_custom" in SCHEMAS:
    e = dict(SCHEMAS["covered_call_custom"])
    hm = dict(e.get("header_map") or {})
    if hm.get("Ask") != "Option Ask Price":
        hm["Ask"] = "Option Ask Price"
        e["header_map"] = hm
        e["logical_columns"] = [hm.get(c, c) for c in e.get("columns", [])]
        SCHEMAS["covered_call_custom"] = e
        changed = True

if "csp_custom" in SCHEMAS:
    e = dict(SCHEMAS["csp_custom"])
    cols = e.get("columns", [])
    pa = build_position_aware_map(cols)
    e["header_map"] = pa
    e["logical_columns"] = [pa.get(c, c) for c in cols]
    SCHEMAS["csp_custom"] = e
    changed = True

if changed:
    CAT.write_text("SCHEMAS = " + pformat(SCHEMAS, sort_dicts=False) + "\n", encoding="utf-8")
    print("updated:", CAT)
else:
    print("no changes needed")
