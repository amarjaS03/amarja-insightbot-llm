#!/usr/bin/env python3
"""Run the execution-layer Flask API from the repository root.

Use: python run_execution_api.py

The implementation lives in execution_layer/run_execution_api.py.
"""
import os
import sys

_root = os.path.dirname(os.path.abspath(__file__))
if _root not in sys.path:
    sys.path.insert(0, _root)

# Ensure .env is loaded when starting from repo root (child module also loads; this is redundant-safe).
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(_root, ".env"))
except Exception:
    pass

from execution_layer.run_execution_api import main

if __name__ == "__main__":
    main()
