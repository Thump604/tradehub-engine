# sitecustomize.py
# Keep legacy rankers running by shimming helpers onto scripts.rank_base.

from __future__ import annotations


def _install_compute_dte_shim():
    try:
        from scripts.utils_time import compute_dte as _impl  # your actual fn if present
    except Exception:
        return
    try:
        import scripts.rank_base as rb  # type: ignore
    except Exception:
        return
    if not hasattr(rb, "compute_dte"):
        setattr(rb, "compute_dte", _impl)


def _install_color_flag_and_card_text_shim():
    # color_flag
    try:
        from scripts.utils_render import color_flag as _color
    except Exception:

        def _color(flag: str) -> str:
            return (flag or "").upper()

    # card_text
    try:
        from scripts.utils_render import card_text as _card_text
    except Exception:

        def _card_text(title: str, rows, *, width: int = 80) -> str:
            title = (title or "").strip()
            lines = []
            if title:
                lines.append(title)
                lines.append("-" * min(max(len(title), 8), width))
            try:
                rows_list = list(rows)
            except Exception:
                rows_list = []
            if rows_list and isinstance(rows_list[0], tuple):
                for k, v in rows_list:
                    lines.append(f"{k}: {v}")
            else:
                for r in rows_list:
                    lines.append(str(r))
            return "\n".join(lines)

    try:
        import scripts.rank_base as rb  # type: ignore
    except Exception:
        return
    if not hasattr(rb, "color_flag"):
        setattr(rb, "color_flag", _color)
    if not hasattr(rb, "card_text"):
        setattr(rb, "card_text", _card_text)


def _install_io_shims():
    try:
        from scripts.utils_io import (
            write_json as _wjson,
            write_yaml as _wyaml,
            ensure_dir as _edir,
        )
    except Exception:
        return
    try:
        import scripts.rank_base as rb  # type: ignore
    except Exception:
        return
    if not hasattr(rb, "write_json"):
        setattr(rb, "write_json", _wjson)
    if not hasattr(rb, "write_yaml"):
        setattr(rb, "write_yaml", _wyaml)
    if not hasattr(rb, "ensure_dir"):
        setattr(rb, "ensure_dir", _edir)


def _run():
    _install_compute_dte_shim()
    _install_color_flag_and_card_text_shim()
    _install_io_shims()


_run()
