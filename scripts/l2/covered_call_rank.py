from __future__ import annotations
import argparse, json
from pathlib import Path
import pandas as pd

def jlog(**kw): print(json.dumps(kw, ensure_ascii=False))

# Utilities copied from earlier L2 (minimal)
from catalog.schemas import SCHEMAS

def align_logical(df: pd.DataFrame, schema_key: str) -> pd.DataFrame:
    e = SCHEMAS.get(schema_key, {})
    hm = e.get("header_map", {})
    # rename using physical->logical map where present, keep others
    cols = {}
    for c in df.columns:
        cols[c] = hm.get(c, c)
    out = df.rename(columns=cols).copy()
    return out

def get_join_keys_pair() -> list[str]:
    # prefer main's join_keys then custom
    for key in ("covered_call_main","covered_call_custom"):
        e = SCHEMAS.get(key)
        if e and e.get("join_keys"):
            return list(e["join_keys"])
    return ["symbol","Expiration Date","Strike Price"]

def coalesce_core_fields(df: pd.DataFrame) -> pd.DataFrame:
    # bring canonical columns together from *_m / *_c
    def co(a,b, target):
        if a in df.columns and b in df.columns:
            df[target] = df[a].where(df[a].notna(), df[b])
        elif a in df.columns:
            df[target] = df[a]
        elif b in df.columns:
            df[target] = df[b]
        return

    for col in ["symbol","Expiration Date","Strike Price","DTE","Delta",
                "IV Rank","Moneyness","Volume",
                "Ann Rtn","%Time Premium Ask Annual Rtn%","Static Annual Return%","Yield to Strike Annual Rtn%"]:
        a, b = f"{col}_m", f"{col}_c"
        if a in df.columns or b in df.columns:
            co(a,b,col)
    return df

def to_float(x):
    if x is None: return None
    if isinstance(x,(int,float)): return float(x)
    s = str(x).strip().replace(",","")
    if s.endswith("%"): s = s[:-1]
    try: return float(s)
    except: return None

def compute_rank(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Annual yield proxy: prefer the first non-null among the AY family
    out["__AY"] = None
    for c in ["Ann Rtn","%Time Premium Ask Annual Rtn%","Static Annual Return%","Yield to Strike Annual Rtn%"]:
        if c in out.columns:
            out["__AY"] = out["__AY"].where(out["__AY"].notna(), out[c])
    out["__AY"]       = out["__AY"].map(to_float)
    out["DTE"]       = out["DTE"].map(to_float)
    out["Delta"]     = out["Delta"].map(to_float).abs()
    out["IV Rank"]   = out["IV Rank"].map(to_float)
    out["Moneyness"] = out["Moneyness"].map(to_float)

    # Basic features (clip for safety)
    ay   = out["__AY"].clip(lower=-1000, upper=1000)
    dte  = out["DTE"].clip(lower=0, upper=3650)
    ivr  = out["IV Rank"].clip(lower=0, upper=100)
    mny  = out["Moneyness"]  # percent +/-; keep as-is
    delta= out["Delta"].clip(lower=0, upper=1)

    # Heuristic normalization (0..1). Keep simple & tunable.
    # More AY, moderate DTE, moderate |delta|, higher IVR, slightly OTM (negative moneyness small magnitude)
    s_ay   = (ay / 100).clip(0, 1)
    s_dte  = (30 - (dte-30).abs()) / 30  # peak ~30 DTE
    s_dte  = s_dte.clip(0,1)
    s_delta= (0.35 - (delta-0.35).abs()) / 0.35  # peak around 0.35
    s_delta= s_delta.clip(0,1)
    s_ivr  = (ivr / 50).clip(0,1)  # 50 = good IVR
    # prefer small negative moneyness (~ -2% to -5% best)
    s_mny  = (0.05 - (mny/100 + 0.02).abs()) / 0.05
    s_mny  = s_mny.clip(0,1)

    out["__score"] = 0.35*s_ay + 0.20*s_dte + 0.20*s_delta + 0.15*s_ivr + 0.10*s_mny

    # If any NaN slipped through, keep them but below all valid
    nans = out["__score"].isna().sum()
    if nans:
        jlog(stage="rank", info="NaN scores present; will rank after valid", nan_scores=int(nans), total=len(out))
        # sort will handle NaNs at the end; we'll number ranks on valid only
        ranks = out["__score"].rank(method="first", ascending=False, na_option="bottom")
    else:
        ranks = out["__score"].rank(method="first", ascending=False)

    out["__rank"] = ranks.fillna(0).astype(int)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in-main", required=True)
    ap.add_argument("--in-custom", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--top", type=int, default=10)
ap.add_argument("--exclude-pre-earnings", action="store_true", default=True)
args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    norm_p = Path("data/l1/covered_call/normalized.parquet")
    if norm_p.exists():
        df_joined = pd.read_parquet(norm_p)
        jlog(stage="source", using="normalized", rows=len(df_joined))
    else:
        main_pq, custom_pq = Path(args.in_main), Path(args.in_custom)
        df_m = pd.read_parquet(main_pq)
        df_c = pd.read_parquet(custom_pq)
        df_m = align_logical(df_m, "covered_call_main")
        df_c = align_logical(df_c, "covered_call_custom")

        join_keys = get_join_keys_pair()
        present = [k for k in join_keys if k in df_m.columns and k in df_c.columns]
        if not present:
            jlog(stage="join", mode="fallback_concat", reason="no join keys present in both views", join_keys=join_keys)
            df_joined = pd.concat([df_m, df_c], ignore_index=True)
        else:
            df_joined = pd.merge(df_m, df_c, on=present, how="outer", suffixes=("_m","_c"))
            jlog(stage="join", mode="merge", join_keys=present, rows=len(df_joined))
        df_joined = coalesce_core_fields(df_joined)

    ranked = compute_rank(df_joined)

    if norm_p.exists() and ranked["__score"].isna().any():
        bad = int(ranked["__score"].isna().sum())
        jlog(stage="rank", status="reject", reason="NaN after normalization", bad_rows=bad)
        ranked = ranked[ranked["__score"].notna()].copy()

    out_parquet = outdir / "ranked.parquet"
    ranked.sort_values(["__score","__rank"], ascending=[False, True]).to_parquet(out_parquet, index=False)

    subset = ["symbol","Expiration Date","Strike Price","DTE","__score","__rank"]
    subset = [c for c in subset if c in ranked.columns]
    head = ranked.sort_values(["__score","__rank"], ascending=[False, True]).head(10)
    preview = head[subset].to_dict(orient="records")
    jlog(stage="l2_rank", screener="covered_call", total=len(ranked), out=str(out_parquet), top=preview)

if __name__ == "__main__":
    main()
