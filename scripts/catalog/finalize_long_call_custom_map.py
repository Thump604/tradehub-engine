from __future__ import annotations
from pathlib import Path
from pprint import pformat
CAT=Path("catalog/schemas.py")
ns={}
exec(compile(CAT.read_text(encoding="utf-8"),str(CAT),"exec"),ns,ns)
SCHEMAS=dict(ns.get("SCHEMAS",{}))
e=SCHEMAS.get("long_call_custom")
if not e: 
    print("no long_call_custom present; nothing to do"); raise SystemExit(0)
cols=list(e.get("columns",[]))
LOGICAL=[
    "symbol","Expiration Date","DTE","Strike Price","Option Type","Bid Price","Ask Price","Option Ask Price",
    "Option Last Price","Percent Change","Volume","Moneyness","Option Volume","Option Open Interest","IV Rank",
    "Option Volume %Change","Delta","Theta","Gamma","Vega","Break Even (Ask)","% to Break Even (Ask)",
    "ITM Probability","Short Term Opinion Signal/Percent","52-Week High",
]
if len(cols)!=len(LOGICAL):
    print("WARNING: column count mismatch for long_call_custom")
    print("  physical:",len(cols)); print("  logical :",len(LOGICAL))
    for i,c in enumerate(cols): print(f"  {i:02d}: {c}")
    raise SystemExit(1)
header_map={cols[i]:LOGICAL[i] for i in range(len(cols))}
e["header_map"]=header_map
e["logical_columns"]=LOGICAL
SCHEMAS["long_call_custom"]=e
CAT.write_text("SCHEMAS = "+pformat(SCHEMAS,sort_dicts=False)+"\n",encoding="utf-8")
print("updated:",CAT)
