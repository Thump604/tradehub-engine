cat > pmcc_monitor.py <<'PY'
#!/usr/bin/env python3
# PMCC Monitor — v0.4.1 (no utcnow, wide parser window, works with your broker paste)

import sys, re, math, os
from datetime import datetime, timezone

# ---------- color helpers ----------
def c(s, code): return f"\x1b[{code}m{s}\x1b[0m"
GREEN = lambda s: c(s, "32")
RED   = lambda s: c(s, "31")
YEL   = lambda s: c(s, "33")
CYAN  = lambda s: c(s, "36")
BOLD  = lambda s: c(s, "1")

DEBUG = bool(os.environ.get("DEBUG_PMCC"))

def utc_today():
    return datetime.now(timezone.utc).date()

# Matches lines like: "AAPL 09/19/2025 235.00 C"
OPT_HDR = re.compile(
    r'^(?P<sym>[A-Z][A-Z0-9.\-]{0,9})\s+'
    r'(?P<date>\d{2}/\d{2}/\d{4})\s+'
    r'(?P<strike>\d+(?:\.\d+)?)\s+'
    r'(?P<cp>[CP])\b'
)

MONEY = re.compile(r'[$]?(\d{1,5}(?:\.\d{1,4})?)')

def _to_iso(ds):
    mm, dd, yyyy = ds.split('/')
    return f"{yyyy}-{mm}-{dd}"

def _find_qty(text):
    # explicit column token or isolated ±1
    m = re.search(r'\bQty\D*(-?1)\b', text, re.IGNORECASE)
    if m: return int(m.group(1))
    m = re.search(r'(?<!\d)[\s\t](-?1)[\s\t](?!\d)', text)
    if m: return int(m.group(1))
    return None

def _find_dte(text):
    m = re.search(r'\bDTE\D*(\d{1,3})\b', text, re.IGNORECASE)
    if m: return int(m.group(1))
    # common broker paste has a lonely small int near OTM/ITM
    nums = [int(x) for x in re.findall(r'\b(\d{1,3})\b', text)]
    for v in nums:
        if 1 <= v <= 800:  # reasonable DTE range
            return v
    return None

def _find_delta(text):
    # look for a 0.xxx or -0.xxx between -1 and 1
    for m in re.finditer(r'([\-+]?\d?\.\d{3,4})', text):
        try:
            v = float(m.group(1))
            if -1.0 <= v <= 1.0:
                return v
        except:
            pass
    return None

def _find_oi(text):
    m = re.search(r'Open\s*Int\D*(\d{2,7})', text, re.IGNORECASE)
    if m: return int(m.group(1))
    # fallback: any biggish integer
    for m in re.finditer(r'\b(\d{3,7})\b', text):
        v = int(m.group(1))
        if v >= 100:
            return v
    return None

def _find_prices(text):
    bid = ask = mid = None
    m = re.search(r'\bBid\s*([$]?[\d.]+)', text, re.IGNORECASE)
    if m: bid = float(m.group(1).replace('$',''))
    m = re.search(r'\bAsk\s*([$]?[\d.]+)', text, re.IGNORECASE)
    if m: ask = float(m.group(1).replace('$',''))
    m = re.search(r'\bMid\s*([$]?[\d.]+)', text, re.IGNORECASE)
    if m: mid = float(m.group(1).replace('$',''))
    if mid is None and (bid is not None and ask is not None):
        mid = round((bid+ask)/2, 2)

    if bid is None or ask is None:
        # fallback: first 3 money tokens in window
        vals = []
        for m in MONEY.finditer(text):
            try: vals.append(float(m.group(1)))
            except: pass
        if len(vals) >= 2:
            b, a = min(vals[:3]), max(vals[:3])
            if bid is None: bid = b
            if ask is None: ask = a
            if mid is None: mid = round((bid+ask)/2, 2)
    return bid, ask, mid

def parse_options(raw):
    lines = [ln.rstrip() for ln in raw.splitlines()]
    opts = []
    n = len(lines)
    for i, ln in enumerate(lines):
        m = OPT_HDR.match(ln.strip())
        if not m:
            continue
        sym    = m.group('sym').upper()
        expiso = _to_iso(m.group('date'))
        strike = float(m.group('strike'))
        cp     = m.group('cp')

        # wider scan window to capture your broker’s scattered columns
        lo = max(0, i-3)
        hi = min(n, i+10)
        window = " ".join(lines[lo:hi])

        qty   = _find_qty(window)
        dte   = _find_dte(window)
        delt  = _find_delta(window)
        oi    = _find_oi(window)
        bid, ask, mid = _find_prices(window)

        rec = dict(sym=sym, exp=expiso, strike=strike, cp=cp,
                   qty=qty, dte=dte, delta=delt, oi=oi,
                   bid=bid, ask=ask, mid=mid, line=ln.strip())

        if dte is None:
            try:
                y, mth, d = map(int, expiso.split('-'))
                rec['dte'] = (datetime(y, mth, d, tzinfo=timezone.utc).date() - utc_today()).days
            except:
                pass

        if DEBUG:
            print(CYAN(f"[DEBUG] parsed: {rec}"))
        opts.append(rec)
    return opts

def group_pmcc(opts):
    by = {}
    for o in opts:
        by.setdefault(o['sym'], []).append(o)
    pairs = []
    for sym, rows in by.items():
        longs  = [r for r in rows if r['cp']=='C' and (r.get('qty') or 0) > 0]
        shorts = [r for r in rows if r['cp']=='C' and (r.get('qty') or 0) < 0]
        if not longs or not shorts:
            continue

        # only pair long-dated with shorter-dated
        valids = []
        for L in longs:
            for S in shorts:
                dL = L.get('dte'); dS = S.get('dte')
                if isinstance(dL, int) and isinstance(dS, int) and dL > dS:
                    valids.append((L, S))
        if not valids:
            # fallback: just take one each
            pairs.append((sym, longs[0], shorts[0]))
            continue
        # choose max DTE gap pair
        L, S = max(valids, key=lambda t: (t[0]['dte'] - t[1]['dte']))
        pairs.append((sym, L, S))
    return pairs

def fmt$(x): return "N/A" if x is None else f"${x:,.2f}"

def main():
    raw = sys.stdin.read()
    if not raw.strip():
        print("Paste your full broker rows, then press Ctrl-D (Linux/Mac) or Ctrl-Z + Enter (Windows).")
        return

    opts = parse_options(raw)
    pairs = group_pmcc(opts)

    if not pairs:
        print(RED("No PMCC pairs detected (need a long-dated call and a shorter-dated short call on the same symbol)."))
        if DEBUG:
            print(CYAN("[DEBUG] parsed rows:"))
            for o in opts: print(o)
        return

    today = utc_today()
    print(BOLD(f"\nPMCC Monitor — v0.4.1 — {today.isoformat()}"))
    for sym, L, S in pairs:
        leap_dte  = L.get('dte')
        short_dte = S.get('dte')
        long_mid  = L.get('mid') or L.get('ask') or L.get('bid')
        short_mid = S.get('mid') or ( (S.get('bid')+S.get('ask'))/2.0 if S.get('bid') and S.get('ask') else None )

        cycles_left = None
        if isinstance(leap_dte, int):
            cycles_left = max(1, math.floor(leap_dte/30) - 1)

        short_gtc = round(max(0.05, (short_mid or 0)*0.50), 2) if short_mid else None
        roll_due_dte = isinstance(short_dte, int) and short_dte <= 21
        roll_due_del = (S.get('delta') is not None and abs(S['delta']) >= 0.55)

        net_delta = None
        if L.get('delta') is not None and S.get('delta') is not None:
            net_delta = round(L['delta'] + S['delta'], 3)

        print("\n" + BOLD(f"{sym} — PMCC"))
        print(f"  Long:  {L['line']}  {YEL(f'[{leap_dte} DTE]') if isinstance(leap_dte,int) else ''}")
        print(f"  Short: {S['line']}  {YEL(f'[{short_dte} DTE]') if isinstance(short_dte,int) else ''}")
        print("  Greeks/Price:")
        print(f"    Long  Δ: {L.get('delta') if L.get('delta') is not None else 'N/A'}   Mid: {fmt$(long_mid)}")
        print(f"    Short Δ: {S.get('delta') if S.get('delta') is not None else 'N/A'}   Mid: {fmt$(short_mid)}  Bid/Ask: {fmt$(S.get('bid'))}/{fmt$(S.get('ask'))}")
        print("  Cycle Planning:")
        print(f"    Cycles left (≈30D): {cycles_left if cycles_left is not None else 'N/A'}")
        flags = []
        if roll_due_dte: flags.append("short_DTE≤21")
        if roll_due_del: flags.append("short_Δ≥0.55")
        if flags:
            print(f"  {RED('Roll triggers: ')}" + ", ".join(flags))
        else:
            print(f"  {GREEN('No roll trigger hit')} (watchlist).")
        print("  GTC suggestion on short:")
        if short_gtc is not None:
            print(f"    {GREEN('Target:')} {fmt$(short_gtc)} (≈50% of current mid)")
        else:
            print("    N/A (no mid/bid/ask found for short).")
        print(f"  Net Δ (est): {net_delta if net_delta is not None else 'N/A'}  |  Policy: 25–50% winner GTC; roll @ 21 DTE or Δ≥0.55.")
    print()

if __name__ == "__main__":
    main()
PY
chmod +x pmcc_monitor.py