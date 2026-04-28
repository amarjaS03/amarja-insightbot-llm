"""
Line-buffer stdout/stderr and ensure logging reaches the terminal.

When stdout is not a TTY (IDE run panel, Docker, Cloud Run log collection),
Python block-buffers output so prints and logging appear late or not at all
until flush. This module fixes that for the execution layer process.
"""

from __future__ import annotations

import logging
import os
import sys

_initialized = False


def ensure_terminal_friendly_io() -> None:
    """Enable line-buffered stdio and attach INFO logs to stdout (idempotent)."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    for stream in (sys.stdout, sys.stderr):
        try:
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(line_buffering=True)
        except Exception:
            pass

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    has_stdout_stream = any(
        isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout
        for h in root.handlers
    )
    for h in root.handlers:
        if isinstance(h, logging.StreamHandler):
            try:
                h.setLevel(logging.INFO)
            except Exception:
                pass

    if not has_stdout_stream:
        h = logging.StreamHandler(sys.stdout)
        h.setLevel(logging.INFO)
        h.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        root.addHandler(h)
