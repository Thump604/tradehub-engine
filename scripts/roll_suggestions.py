# scripts/roll_suggestions.py
import json, pathlib, datetime as dt

ROOT = pathlib.Path(__file__).resolve().parents[1]
RICH = ROOT / "outputs" / "web_feed" / "positions_rich.json"
OUT = ROOT / "outputs" / "web_feed" / "roll_suggestions.json"

TARGET_DTE = (21, 35)
TARGET_DELTA = {
    "CSP": (0.20, 0.30),  # we quote absolute delta
    "COVERED_CALL": (0.25, 0.35),
    "PMCC_SHORT": (0.25, 0.35),
}
MIN_CREDIT = {
    "CSP": 0.20,
    "COVERED_CALL": 0.15,
    "PMCC_SHORT": 0.15,
}


def read_json(p):
    try:
        return json.loads(pathlib.Path(p).read_text())
    except Exception:
        return None


def absf(x, default=None):
    try:
        return abs(float(x))
    except Exception:
        return default


def asf(x, default=None):
    try:
        return float(x)
    except Exception:
        return default


def classify_leg(lg):
    # returns one of: CSP, COVERED_CALL, PMCC_SHORT, IGNORE
    kind = lg.get("kind")
    side = lg.get("side")
    if side != "short":
        return "IGNORE"
    if kind == "P":
        return "CSP"
    if kind == "C":
        # We'll separate CC vs PMCC using “is there a long call in this symbol?” in caller
        return "CC_OR_PMCC"
    return "IGNORE"


def find_any_long_call(legs):
    return any(l.get("kind") == "C" and l.get("side") == "long" for l in legs)


def rank_score(credit, target_mid_dte, new_dte):
    # prefer ~28 DTE, more credit better
    dte_pen = abs((new_dte or target_mid_dte) - target_mid_dte)
    return (credit or 0) * 100.0 - dte_pen


def make_ticket(symbol, old, new, strat):
    # Broker-agnostic textual ticket (what you can paste/adapt)
    legs = []
    # close old short
    old_k = f"{symbol} {old.get('expiry')} {old.get('strike'):.2f} {old.get('kind')}"
    legs.append(
        f"STC {abs(int(old.get('qty',-1))) if old.get('qty') else 1} {old_k} @ MKT"
    )
    # open new short
    new_k = f"{symbol} {new.get('expiry')} {new.get('strike'):.2f} {new.get('kind')}"
    legs.append(
        f"STO {abs(int(new.get('qty',-1))) if new.get('qty') else 1} {new_k} @ LMT {new.get('limit', 'X.XX')}"
    )
    return " ; ".join(legs)


def propose_rolls_for_symbol(symrow):
    sym = symrow["symbol"]
    legs = symrow["legs"]
    has_long_call = find_any_long_call(legs)
    rows = []

    # Gather short legs we can roll
    shorts = [
        l for l in legs if l.get("side") == "short" and l.get("kind") in ("C", "P")
    ]
    if not shorts:
        return rows

    target_mid_dte = sum(TARGET_DTE) / 2.0

    for sh in shorts:
        k = sh["kind"]
        dte = asf(sh.get("dte"), None)
        delta = asf(sh.get("delta"), None)
        strike = asf(sh.get("strike"), None)
        qty = sh.get("qty") or -1

        # classify
        strat = classify_leg(sh)
        if strat == "CC_OR_PMCC":
            strat = "PMCC_SHORT" if has_long_call else "COVERED_CALL"
        if strat == "IGNORE":
            continue

        # Heuristic new strikes: start at same strike; if credit weak later we’d step strikes
        candidate_strikes = [strike] if strike is not None else [None]

        # Heuristic expiries: +28 DTE equivalent (we only record DTE target; actual expiry text is unknown from screeners)
        # Because we don't have an options chain source here, we generate a synthetic “proposal”.
        # The web’s job is to present the structure + numbers; you’ll key the exact chain in broker.
        lo, hi = TARGET_DTE
        tgt_low, tgt_high = TARGET_DELTA[strat]
        min_credit = MIN_CREDIT[strat]

        # Use the current mid/mark as proxy for premium with a very rough delta mapping.
        # If we have delta and price, estimate new credit using proportionality to delta ratio.
        est_old_credit = asf(sh.get("price"), 0.0)

        # Build 3 candidates: 21, 28, 35 DTE equivalents
        for new_dte in (21, 28, 35):
            # delta target midpoint
            tgt_delta = (tgt_low + tgt_high) / 2.0
            # crude reprice: scale by ratio of target delta to current delta (bounded)
            cur_abs = absf(delta, 0.25) or 0.25
            scale = max(0.5, min(1.5, tgt_delta / cur_abs))
            new_credit = max(0.0, est_old_credit * scale)
            # prefer slightly higher credit when rolling further
            if new_dte > 28:
                new_credit *= 1.05

            if new_credit < min_credit:
                continue

            for new_strike in candidate_strikes:
                cand = {
                    "symbol": sym,
                    "strategy": strat.lower(),  # to match your site’s lowercase
                    "score": round(rank_score(new_credit, target_mid_dte, new_dte), 2),
                    "why": {
                        "min_credit": min_credit,
                        "target_delta_range": [tgt_low, tgt_high],
                        "target_dte_range": [lo, hi],
                        "reason": "roll out-in-time to lower risk and collect credit",
                    },
                    "leg_old": {
                        "kind": k,
                        "qty": qty,
                        "strike": strike,
                        "expiry": sh.get("expiry"),
                        "dte": dte,
                        "delta": delta,
                        "price": asf(sh.get("price"), None),
                    },
                    "leg_new": {
                        "kind": k,
                        "qty": qty,
                        "strike": new_strike,
                        "expiry": f"+{new_dte}DTE",
                        "dte": new_dte,
                        "target_delta_mid": (tgt_low + tgt_high) / 2.0,
                        "limit": round(new_credit, 2),
                    },
                }
                cand["ticket"] = make_ticket(
                    sym, cand["leg_old"], cand["leg_new"], strat
                )
                rows.append(cand)

    # keep top 4 per symbol by score
    rows.sort(key=lambda r: r["score"], reverse=True)
    keep = []
    seen = 0
    for r in rows:
        keep.append(r)
        seen += 1
        if seen >= 4:
            break
    return keep


def main():
    data = read_json(RICH)
    rows = []
    if not data or not data.get("rows"):
        print(f"[rolls] no positions found in {RICH}")
    else:
        for symrow in data["rows"]:
            rows.extend(propose_rolls_for_symbol(symrow))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ok": True,
        "strategy": "rolls",
        "rows": rows,
        "files": [],
        "globs": "",
        "ts": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "counts": {"rolls": len(rows)},
    }
    OUT.write_text(json.dumps(payload, indent=2))
    print(f"[rolls] wrote {OUT} (rows={len(rows)})")


if __name__ == "__main__":
    main()
