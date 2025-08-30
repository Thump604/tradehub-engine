# scripts/positions_enrich.py
import json, pathlib, math, datetime as dt, glob

ROOT = pathlib.Path(__file__).resolve().parents[1]
BY_SYMBOL_DIR = ROOT / "outputs" / "positions" / "by_symbol"
OUT = ROOT / "outputs" / "web_feed" / "positions_rich.json"


def read_json(p):
    try:
        return json.loads(pathlib.Path(p).read_text())
    except Exception:
        return None


def leg_side(qty):
    try:
        q = float(qty)
    except Exception:
        return "unknown"
    if q > 0:
        return "long"
    if q < 0:
        return "short"
    return "flat"


def as_float(x, default=None):
    try:
        if x in (None, "", "N/A"):
            return default
        return float(x)
    except Exception:
        return default


def flag_assignment_risk(spot, kind, strike, side):
    if spot is None or kind not in ("C", "P"):
        return False
    if as_float(strike) is None:
        return False
    s = float(spot)
    k = float(strike)
    if kind == "C":
        # call assignment if spot >= strike * 0.99 for shorts
        return side == "short" and s >= 0.99 * k
    else:
        # put assignment if spot <= strike * 1.01 for shorts
        return side == "short" and s <= 1.01 * k


def flag_short_delta_high(delta, kind, side, thresh=0.35):
    d = as_float(delta, 0.0)
    if kind == "P":
        d = -d  # puts usually negative; use absolute exposure
    return side == "short" and abs(d) >= thresh


def flag_low_dte(dte, thresh=10):
    d = as_float(dte, None)
    return d is not None and d <= thresh


def pnl_est(price, basis, qty):
    p = as_float(price, None)
    b = as_float(basis, None)
    q = as_float(qty, None)
    if p is None or b is None or q is None:
        return None
    # options ×100; we’ll assume legs include an "multiplier" when/if present later; for now 100 by default
    return (p - b) * q * 100.0


def load_symbol_latest():
    # returns {SYM: {"symbol":..., "spot":..., "legs":[...]}}
    out = {}
    for p in sorted(BY_SYMBOL_DIR.glob("*-latest.json")):
        data = read_json(p)
        if not data:
            continue
        sym = data.get("symbol") or data.get("SYM") or p.name.split("-latest.json")[0]
        legs = data.get("legs") or data.get("rows") or []
        spot = as_float(data.get("spot"), None)
        # normalize legs
        norm_legs = []
        for lg in legs:
            kind = (lg.get("kind") or lg.get("type") or "").upper()[
                :1
            ]  # "C" / "P" / ""
            qty = lg.get("qty") or lg.get("quantity") or lg.get("position_qty")
            side = leg_side(qty)
            d = lg.get("delta")
            dte = lg.get("dte") or lg.get("DTE")
            strike = lg.get("strike") or lg.get("strike_price")
            expiry = lg.get("expiry") or lg.get("exp") or lg.get("expiration")
            price = lg.get("price") or lg.get("mark") or lg.get("last")
            basis = lg.get("cost_basis") or lg.get("basis") or lg.get("entry_price")
            oi = lg.get("open_interest") or lg.get("open_int") or lg.get("oi")
            itm = lg.get("itm") or lg.get("ITM")
            norm = {
                "kind": kind,
                "side": side,
                "qty": as_float(qty, 0),
                "delta": as_float(d, None),
                "dte": as_float(dte, None),
                "strike": as_float(strike, None),
                "expiry": expiry,
                "price": as_float(price, None),
                "cost_basis": as_float(basis, None),
                "open_int": as_float(oi, None),
                "itm": itm,
            }
            norm["pnl_est"] = pnl_est(norm["price"], norm["cost_basis"], norm["qty"])
            # risk flags
            flags = []
            if flag_assignment_risk(spot, norm["kind"], norm["strike"], norm["side"]):
                flags.append("assignment_risk")
            if flag_short_delta_high(norm["delta"], norm["kind"], norm["side"]):
                flags.append("short_delta_high")
            if flag_low_dte(norm["dte"]):
                flags.append("low_dte")
            norm["risk_flags"] = flags
            norm_legs.append(norm)
        out[sym] = {
            "symbol": sym,
            "spot": spot,
            "legs": norm_legs,
            "as_of": data.get("as_of"),
        }
    return out


def main():
    symmap = load_symbol_latest()
    rows = []
    for sym, pack in sorted(symmap.items()):
        legs = pack["legs"]
        pnl = (
            sum(
                [
                    x["pnl_est"]
                    for x in legs
                    if isinstance(x.get("pnl_est"), (int, float))
                ]
            )
            if legs
            else 0.0
        )
        flags = sorted(list({f for lg in legs for f in (lg.get("risk_flags") or [])}))
        rows.append(
            {
                "symbol": sym,
                "as_of": pack.get("as_of"),
                "spot": pack.get("spot"),
                "legs": legs,
                "pnl_est_total": pnl,
                "risk_flags": flags,
                "legs_count": len(legs),
            }
        )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(
        json.dumps(
            {
                "ok": True,
                "rows": rows,
                "ts": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            indent=2,
        )
    )
    print(f"[enrich] wrote {OUT} (rows={len(rows)})")


if __name__ == "__main__":
    main()
