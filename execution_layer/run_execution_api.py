#!/usr/bin/env python3

import os
import sys

# `logger` is v1/logger.py (flat import used across legacy code); script dir alone is not enough.
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_v1 = os.path.join(_repo_root, "v1")
if _v1 not in sys.path:
    sys.path.insert(0, _v1)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

# Load repo .env before ExecutionApi reads DATA_DIR / GCS_BUCKET (required locally and on Cloud Run).
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(_repo_root, ".env"))
except Exception:
    pass

_data_dir = (os.getenv("DATA_DIR") or "").strip()
if not _data_dir:
    _data_dir = os.path.join(_repo_root, "local_data_dir")
elif not os.path.isabs(_data_dir):
    _data_dir = os.path.normpath(os.path.join(_repo_root, _data_dir))
os.environ["DATA_DIR"] = _data_dir

if not (os.getenv("GCS_BUCKET") or "").strip():
    os.environ["GCS_BUCKET"] = "local-dev-bucket"

from execution_layer.runtime_io import ensure_terminal_friendly_io

ensure_terminal_friendly_io()

from execution_api import ExecutionApi
from logger import add_log


def main():
    """Main entry point for the execution API"""
    try:
        print("Starting Data Analysis Execution API...")
        
        # Create and run the execution API
        api = ExecutionApi()
        api.run(host='0.0.0.0', port=5001)
        
    except Exception as e:
        print(f"Error starting Execution API: {str(e)}")
        add_log(f"Error starting Execution API: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 