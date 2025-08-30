#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ui.py — Shared console UX helpers (colors, flags, headers, tables).

- Keep artifacts clean: never writes files here, just prints.
- Scripts can add --quiet to suppress printing (set ConsoleUX(enabled=False)).
"""

import sys
from typing import Iterable, List, Optional

def _supports_color() -> bool:
    return sys.stdout.isatty()

class Colors:
    def __init__(self, enabled: bool = True):
        self.enabled = enabled and _supports_color()

    def wrap(self, code: str, txt: str) -> str:
        if not self.enabled: return txt
        return f"\033[{code}m{txt}\033[0m"

    def bold(self, txt: str) -> str:   return self.wrap("1", txt)
    def red(self, txt: str) -> str:    return self.wrap("31", txt)
    def green(self, txt: str) -> str:  return self.wrap("32", txt)
    def yellow(self, txt: str) -> str: return self.wrap("33", txt)
    def blue(self, txt: str) -> str:   return self.wrap("34", txt)
    def cyan(self, txt: str) -> str:   return self.wrap("36", txt)

def flag_icon(flag: Optional[str]) -> str:
    if not flag: return ""
    f = flag.strip().upper()
    if f == "GREEN":  return "🟢 GREEN"
    if f == "YELLOW": return "🟡 YELLOW"
    if f == "RED":    return "🔴 RED"
    return f

def color_flag(flag: Optional[str], C: Colors) -> str:
    base = flag_icon(flag)
    if not base: return ""
    f = (flag or "").strip().upper()
    if f == "GREEN":  return C.green(base)
    if f == "YELLOW": return C.yellow(base)
    if f == "RED":    return C.red(base)
    return base

def pct(x, digits=1):
    try:
        return f"{float(x):.{digits}f}%"
    except Exception:
        return str(x)

def f2(x, digits=2):
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return str(x)

def line(width=70, ch="─"):
    return ch * width

class ConsoleUX:
    """Lightweight console UI helper."""
    def __init__(self, enabled: bool = True, width: int = 78):
        self.enabled = enabled
        self.C = Colors(enabled=enabled)
        self.width = width

    def header(self, title: str, subtitle: str = ""):
        if not self.enabled: return
        print()
        print(line(self.width))
        if subtitle:
            print(f"{self.C.bold(title)} — {subtitle}")
        else:
            print(self.C.bold(title))
        print(line(self.width))
        print()

    def subheader(self, txt: str):
        if not self.enabled: return
        print(self.C.bold(txt))

    def info(self, txt: str):
        if not self.enabled: return
        print(txt)

    def warn(self, txt: str):
        if not self.enabled: return
        print(self.C.yellow(txt))

    def error(self, txt: str):
        if not self.enabled: return
        print(self.C.red(txt))

    def table(self, headers: List[str], rows: Iterable[List[str]]):
        if not self.enabled: return
        # compute col widths
        cols = len(headers)
        w = [len(h) for h in headers]
        cache = []
        for r in rows:
            rr = ["" if v is None else str(v) for v in r]
            cache.append(rr)
            for i in range(cols):
                if i < len(rr): w[i] = max(w[i], len(rr[i]))
        # print
        hdr = "  ".join(f"{headers[i]:<{w[i]}}" for i in range(cols))
        print(hdr)
        print("-" * len(hdr))
        for rr in cache:
            line = "  ".join(f"{rr[i]:<{w[i]}}" for i in range(cols))
            print(line)