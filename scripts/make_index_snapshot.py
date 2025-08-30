#!/usr/bin/env python3
"""
Build a rich market snapshot for the Web Hub.

Outputs:
  outputs/index_snapshot.json  (rich indices panel)
  outputs/market_state.json    (keeps your simple regime/trend/vol keys)

Install:  pip install yfinance pandas numpy ta
Run:      python -m scripts.make_index_snapshot
"""

import json, datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs"
OUT.mkdir(exist_ok=True, parents=True)

# What we show
TICKERS = {
    "SPY": "S&P 500",
    "QQQ": "NASDAQ 100",
    "IWM": "Russell 2000",
    "DIA": "Dow Jones",
    "TLT": "20Y+ Treasuries",
    "^VIX": "VIX",
    "GLD": "Gold",
    "DXY": "US Dollar",
}


# Helper: basic techs
def calc_metrics(df: pd.DataFrame) -> dict:
    close = df["Close"].dropna()
    if len(close) < 2:
        return {}
    last = float(close.iloc[-1])
    prev = float(close.iloc[-2])
    pct = (last / prev - 1) * 100

    # moving avgs
    ma20 = float(close.rolling(20).mean().iloc[-1]) if len(close) >= 20 else np.nan
    ma50 = float(close.rolling(50).mean().iloc[-1]) if len(close) >= 50 else np.nan
    ma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else np.nan

    # ATR (14)
    high = df["High"].dropna()
    low = df["Low"].dropna()
    tr = pd.concat(
        [(high - low), (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)
    atr14 = float(tr.rolling(14).mean().iloc[-1]) if len(tr) >= 14 else np.nan

    # Simple RSI(14)
    delta = close.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.rolling(14).mean()
    roll_down = down.rolling(14).mean()
    rs = (roll_up / (roll_down.replace(0, np.nan))).iloc[-1]
    rsi = float(100 - (100 / (1 + rs))) if np.isfinite(rs) else np.nan

    trend = (
        "Up"
        if (np.isfinite(ma50) and last > ma50 and ma50 > ma200)
        else (
            "Down" if (np.isfinite(ma50) and last < ma50 and ma50 < ma200) else "Mixed"
        )
    )

    return {
        "price": round(last, 2),
        "pct": round(pct, 2),
        "ma20": round(ma20, 2) if np.isfinite(ma20) else None,
        "ma50": round(ma50, 2) if np.isfinite(ma50) else None,
        "ma200": round(ma200, 2) if np.isfinite(ma200) else None,
        "atr14": round(atr14, 2) if np.isfinite(atr14) else None,
        "rsi14": round(rsi, 1) if np.isfinite(rsi) else None,
        "trend": trend,
    }


def main():
    end = dt.datetime.utcnow()
    start = end - dt.timedelta(days=365 * 2)  # 2y lookback

    panel = {}
    for tkr, label in TICKERS.items():
        try:
            df = yf.download(
                tkr,
                start=start.date().isoformat(),
                end=end.date().isoformat(),
                progress=False,
            )
            if isinstance(df, pd.DataFrame) and not df.empty:
                panel[tkr] = {"label": label, **calc_metrics(df)}
        except Exception as e:
            panel[tkr] = {"label": label, "error": str(e)}

    # Derive high-level regime from SPY/QQQ trend and VIX level
    regime = "Neutral"
    trend_bias = "Mixed"
    vol = "Unknown"
    try:
        spy = panel.get("SPY", {})
        qqq = panel.get("QQQ", {})
        vix = panel.get("^VIX", {})
        bull = (spy.get("trend") == "Up") and (qqq.get("trend") == "Up")
        bear = (spy.get("trend") == "Down") and (qqq.get("trend") == "Down")
        if bull:
            trend_bias = "Up"
        elif bear:
            trend_bias = "Down"
        else:
            trend_bias = "Mixed"

        v = vix.get("price")
        if v is not None:
            if v < 15:
                vol = "Low"
            elif v > 25:
                vol = "High"
            else:
                vol = "Normal"

        regime = (
            "Risk-On"
            if (trend_bias == "Up" and vol in ("Low", "Normal"))
            else ("Risk-Off" if (trend_bias == "Down" or vol == "High") else "Neutral")
        )
    except Exception:
        pass

    OUT.joinpath("index_snapshot.json").write_text(
        json.dumps(
            {
                "generated_at": dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ"),
                "panel": panel,
            },
            indent=2,
        )
    )

    # keep the old file in sync so the header still shows something
    OUT.joinpath("market_state.json").write_text(
        json.dumps(
            {"overall_regime": regime, "trend_bias": trend_bias, "volatility": vol},
            indent=2,
        )
    )

    print("Wrote:", OUT / "index_snapshot.json")
    print("Updated:", OUT / "market_state.json")


if __name__ == "__main__":
    main()
