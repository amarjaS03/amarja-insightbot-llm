import os
import sys
import traceback
from pathlib import Path

from dotenv import load_dotenv

# Support both `python v1/main.py` and `from v1.main import main`.
if __package__ in (None, ""):
    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

try:
    from v1.logger import add_log
    from v1.utils.env import init_env
except ImportError:
    from logger import add_log
    from utils.env import init_env

load_dotenv()


def main() -> None:
    """Main entry point for the API application."""
    try:
        api_version = (os.getenv("API_VERSION", "v1") or "v1").strip().lower()
        constants = init_env()

        if api_version == "v2":
            add_log("Starting FastAPI v2 server")
            from v2.main import run_app

            run_app(host="0.0.0.0", port=8000, debug=True)
            return

        add_log("Starting Flask v1 server")
        try:
            from v1.api_layer.api_server import ApiServer
        except ImportError:
            from api_layer.api_server import ApiServer
        api_server = ApiServer(constants)
        api_server.run(host="0.0.0.0", port=5000, debug=True)
    except Exception as exc:
        add_log(f"Error starting API server: {str(exc)} | traceback: {traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    main()
