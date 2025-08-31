from __future__ import annotations
import argparse,csv,re,sys,json
from pathlib import Path
from pprint import pformat
FOOTER_RE=re.compile(r"^Downloaded from Barchart\.com as of (.+)$")
def read_header_rows_footer(p:Path):
    with p.open("r",encoding="utf-8",newline="") as f:
        rdr=csv.reader(f)
        try: header=next(rdr)
        except StopIteration: return [],0,None,None
        row_count=0
        sample=None
        for row in rdr:
            if len(row)==1:
                m=FOOTER_RE.match(row[0].strip())
                if m: return header,row_count,m.group(1),sample
            if len(row)==len(header):
                row_count+=1
                if sample is None: sample=row
        return header,row_count,None,sample
def load_existing_schemas(py_path:Path)->dict:
    if not py_path.exists(): return {}
    ns={}
    src=py_path.read_text(encoding="utf-8")
    exec(compile(src,str(py_path),"exec"),ns,ns)
    return dict(ns.get("SCHEMAS",{}))
def write_schemas(py_path:Path,schemas:dict)->None:
    py_path.parent.mkdir(parents=True,exist_ok=True)
    with py_path.open("w",encoding="utf-8") as f:
        f.write("SCHEMAS = ")
        f.write(pformat(schemas,sort_dicts=False))
        f.write("\n")
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--in",dest="in_file",required=True)
    ap.add_argument("--screener",required=True)
    ap.add_argument("--schemas",default="catalog/schemas.py")
    args=ap.parse_args()
    csv_path=Path(args.in_file)
    if not csv_path.exists(): raise SystemExit(f"File not found: {csv_path}")
    columns,row_count,footer_ts,sample=read_header_rows_footer(csv_path)
    sample_row=dict(zip(columns,sample)) if sample else None
    entry={"source_example":str(csv_path),"footer_timestamp":footer_ts,"row_count":row_count,"columns":columns,"sample_row":sample_row}
    out_path=Path(args.schemas)
    all_schemas=load_existing_schemas(out_path)
    all_schemas[args.screener]=entry
    write_schemas(out_path,all_schemas)
    print(json.dumps({args.screener:entry},indent=2))
if __name__=="__main__": main()
