# scripts/debug_barchart_files.py
from __future__ import annotations

from .rank_base import INCOMING_DIR, ARCHIVE_DIR, KIND_GROUPS, load_barchart_csv_any


def main() -> None:
    print("Search roots:")
    print(f"  INCOMING: {INCOMING_DIR}")
    print(f"  ARCHIVE : {ARCHIVE_DIR}\n")

    for kind, groups in KIND_GROUPS.items():
        group_label = "  OR  ".join(g.name for g in groups)
        print(f"[{kind}] groups=({group_label})")
        df = load_barchart_csv_any(kind)
        if df is None or df.empty:
            print("  ERR No matching file found.\n")
            continue
        print(
            f"  OK rows={len(df)} source={df.attrs.get('__source__')}  "
            f"(matched_group: {df.attrs.get('__matched_group__')})\n"
        )


if __name__ == "__main__":
    main()
