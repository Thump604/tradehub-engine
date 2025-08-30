# scripts/build_trade_cards.py
from __future__ import annotations

from typing import Any, Dict, List
from .rank_base import WEB_FEED, read_json_items, write_json

INP = WEB_FEED / "suggestions_merged.json"
OUT = WEB_FEED / "cards.json"


def _payoff(item: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Try to compute payoff metrics when spread width & debit/credit are present.
    Returns a dict or None if insufficient data.
    """
    # Width
    width = None
    for k in ("spread_width", "width"):
        if k in item:
            try:
                width = float(item[k])
                break
            except Exception:
                pass

    # Price: net debit (for calls) or net credit (for puts)
    debit = None
    credit = None
    for k in ("net_debit", "debit"):
        if k in item:
            try:
                debit = float(item[k])
                break
            except Exception:
                pass
    if debit is None:
        for k in ("net_credit", "credit"):
            if k in item:
                try:
                    credit = float(item[k])
                    break
                except Exception:
                    pass

    if width is None:
        return None

    if debit is not None:
        # long call spread
        max_value = width
        max_profit = max_value - debit
        max_loss = debit
        return {
            "type": "debit_spread",
            "width": width,
            "debit": debit,
            "max_value": max_value,
            "max_profit": max_profit,
            "max_loss": max_loss,
        }
    if credit is not None:
        # bull put (credit)
        max_profit = credit
        max_loss = max(0.0, width - credit)
        return {
            "type": "credit_spread",
            "width": width,
            "credit": credit,
            "max_profit": max_profit,
            "max_loss": max_loss,
        }
    return None


def main() -> None:
    suggestions = read_json_items(INP)
    cards: List[Dict[str, Any]] = []
    for it in suggestions:
        card = {
            "symbol": it.get("symbol"),
            "strategy": it.get("strategy"),
            "expiry": it.get("expiry", "—"),
            "score": it.get("score", 0.0),
        }
        p = _payoff(it)
        if p:
            card.update({"payoff": p})
        cards.append(card)

    write_json(OUT, cards)
    print(f"[saved] {OUT}")


if __name__ == "__main__":
    main()
