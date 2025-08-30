#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import app_web_hub as app


def main():
    host = os.environ.get("WEB_HOST", "127.0.0.1")
    port = int(os.environ.get("WEB_PORT", "8000"))
    globs = os.environ.get("SUGGESTION_GLOBS", "")
    print(
        f"[web] TradeHub Web starting — http://{host}:{port} • globs={globs or '(default)'}"
    )
    app.run()


if __name__ == "__main__":
    main()
