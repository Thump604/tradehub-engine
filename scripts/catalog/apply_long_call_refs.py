from __future__ import annotations
import csv, sys, subprocess
from pathlib import Path
REF_KEYS={"main":"long_call_main","custom":"long_call_custom"}
def read_header(p:Path)->list[str]:
    with p.open("r",encoding="utf-8",newline="") as f:
        return next(csv.reader(f))
def is_custom(h:list[str])->bool:
    s=set(h); return any(m in s for m in {"Gamma","Vega","Short Term~","52W High","Option Vol %Chg~","Vol %Chg~"})
def run_update(p:Path,k:str)->None:
    subprocess.check_call(["python","-m","scripts.catalog.update_schema","--in",str(p),"--screener",k])
def main():
    if len(sys.argv)<2: sys.exit("usage: apply_long_call_refs.py <csv> [<csv>...]")
    for arg in sys.argv[1:]:
        p=Path(arg); h=read_header(p); k=REF_KEYS["custom" if is_custom(h) else "main"]; run_update(p,k)
if __name__=="__main__": main()
