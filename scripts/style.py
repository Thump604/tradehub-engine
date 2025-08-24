#!/usr/bin/env python3
# style.py — shared formatting helpers for tickets (ANSI, grid, icons).

import os
from datetime import datetime, timezone

# ----- ANSI -----
class S:
    RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
    RED="\033[31m"; GREEN="\033[32m"; YEL="\033[33m"; BLU="\033[34m"; MAG="\033[35m"; CYA="\033[36m"

def c(txt, color): return f"{color}{txt}{S.RESET}"
def b(txt): return f"{S.BOLD}{txt}{S.RESET}"
def dim(txt): return f"{S.DIM}{txt}{S.RESET}"
ok=lambda s: c(s, S.GREEN); warn=lambda s: c(s, S.YEL); bad=lambda s: c(s, S.RED); info=lambda s: c(s, S.CYA)

# Icons
ICON_PASS = ok("PASS")
ICON_FAIL = bad("FAIL")
ICON_WARN = warn("WARN")
ICON_OK   = ok("OK")
ICON_ATT  = warn("•")
ICON_BUL  = info("•")

# ----- Utils -----
def now_utc_str(fmt="%Y-%m-%d %H:%M UTC"):
    return datetime.now(timezone.utc).strftime(fmt)

def sep(char="─", width=70):
    return char*width

def section(title, width=70):
    t = f" {title} "
    pad = max(0, width-len(t))
    left = pad//2; right = pad-left
    return f"{sep('─', left)}{t}{sep('─', right)}"

# 2-col metric grid with fixed widths (no “jiggle”)
def grid_pairs(pairs, left_w=24, right_w=18):
    """
    pairs: list of tuples [(label, value), ...]
    Prints two pairs per row: (L1,V1) | (L2,V2).
    """
    out=[]
    row=[]
    for p in pairs:
        row.append(p)
        if len(row)==2:
            (l1,v1),(l2,v2)=row
            out.append(f"{l1:<{left_w}} {v1:>{right_w}}   {l2:<{left_w}} {v2:>{right_w}}")
            row=[]
    if row:  # last odd
        (l1,v1)=row[0]
        out.append(f"{l1:<{left_w}} {v1:>{right_w}}")
    return "\n".join(out)

# Formatting
def fmt_money(x):
    if x is None: return "N/A"
    try: return f"${x:,.2f}"
    except: return str(x)

def fmt_num(x, nd=3):
    if x is None: return "N/A"
    try: return f"{x:.{nd}f}"
    except: return str(x)

def fmt_int(x):
    if x is None: return "N/A"
    try: return f"{int(x)}"
    except: return str(x)

def fmt_pct(x):  # x as fraction
    if x is None: return "N/A"
    try: return f"{x*100:.2f}%"
    except: return str(x)

def fmt_pct_plain(x):  # x already in %
    if x is None: return "N/A"
    try: return f"{x:.2f}%"
    except: return str(x)

def bullet_line(text, tone="info"):
    style = {"ok": ok, "warn": warn, "bad": bad, "info": info}.get(tone, info)
    return f"{style('•')} {text}"

def market_header(market):
    if not market:
        return "REGIME N/A | TREND N/A | VOL N/A"
    reg = market.get("overall_regime") or market.get("regime")
    tr  = market.get("trend_bias") or market.get("trend")
    vol = market.get("volatility") or market.get("vol")
    return f"REGIME {reg or 'N/A'} | TREND {tr or 'N/A'} | VOL {vol or 'N/A'}"