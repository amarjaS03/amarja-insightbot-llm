#!/usr/bin/env python3

import sys
import os
import argparse
import subprocess
import logging
import time

# Ensure execution_layer dir is on path for logger import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from logger import add_log


def _kill_port(port: int):
    """Kill any process already bound to *port* (Windows + Unix)."""
    try:
        if sys.platform == "win32":
            result = subprocess.run(
                ["netstat", "-ano"],
                capture_output=True, text=True
            )
            for line in result.stdout.splitlines():
                if f":{port}" in line and "LISTENING" in line:
                    parts = line.split()
                    pid = parts[-1]
                    subprocess.run(["taskkill", "/F", "/PID", pid],
                                   capture_output=True)
                    add_log(f"[STARTUP] Killed previous process on port {port} (PID {pid})")
        else:
            subprocess.run(["fuser", "-k", f"{port}/tcp"], capture_output=True)
            add_log(f"[STARTUP] Killed previous process on port {port}")
    except Exception as e:
        add_log(f"[STARTUP] Could not kill process on port {port}: {e}")


def _setup_logging():
    """Configure root logging so all INFO+ messages are visible in the terminal."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stdout,
        force=True,
    )


def _watch_and_restart(args):
    """
    Restart this script whenever a .py file in execution_layer changes.
    Uses watchdog if available, otherwise polls every 2 s.
    Runs as a wrapper so the Flask app always runs in a SINGLE process —
    avoids interleaved stdout from Flask's built-in dual-process reloader.
    """
    watch_dir = os.path.dirname(os.path.abspath(__file__))
    # Build child command without --reload to avoid infinite recursion
    cmd = [sys.executable] + [a for a in sys.argv if a != "--reload"]

    def _spawn():
        p = subprocess.Popen(cmd, env=os.environ.copy())
        add_log(f"[RELOADER] Server started (PID {p.pid})")
        return p

    proc = _spawn()
    add_log(f"[RELOADER] Watching {watch_dir} for .py changes")

    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler

        class _Handler(FileSystemEventHandler):
            def __init__(self):
                self.changed = False
            def on_modified(self, event):
                if event.src_path.endswith(".py"):
                    self.changed = True

        handler = _Handler()
        observer = Observer()
        observer.schedule(handler, watch_dir, recursive=True)
        observer.start()

        while True:
            time.sleep(1)
            if proc.poll() is not None:
                add_log("[RELOADER] Server process exited — restarting...")
                proc = _spawn()
                handler.changed = False
            elif handler.changed:
                add_log("[RELOADER] Source change detected — restarting server...")
                proc.terminate()
                proc.wait()
                proc = _spawn()
                handler.changed = False

    except ImportError:
        # watchdog not installed — fall back to polling mtimes
        add_log("[RELOADER] 'watchdog' not installed. Polling file mtimes every 2 s.")
        add_log("[RELOADER] Install it for instant reload:  pip install watchdog")

        def _snapshot():
            m = {}
            for root, _, files in os.walk(watch_dir):
                for f in files:
                    if f.endswith(".py"):
                        p = os.path.join(root, f)
                        try:
                            m[p] = os.path.getmtime(p)
                        except OSError:
                            pass
            return m

        mtimes = _snapshot()
        while True:
            time.sleep(2)
            if proc.poll() is not None:
                add_log("[RELOADER] Server process exited — restarting...")
                proc = _spawn()
                mtimes = _snapshot()
                continue
            new = _snapshot()
            if new != mtimes:
                add_log("[RELOADER] Source change detected — restarting server...")
                proc.terminate()
                proc.wait()
                proc = _spawn()
                mtimes = _snapshot()

    except KeyboardInterrupt:
        add_log("[RELOADER] Stopping...")
        proc.terminate()
        proc.wait()


def main():
    """Main entry point for the execution API"""
    parser = argparse.ArgumentParser(description="Start Data Analysis Execution API")
    parser.add_argument("--local",   action="store_true", help="Run in local mode (use local fs instead of GCS FUSE)")
    parser.add_argument("--port",    type=int, default=5001, help="Port to listen on (default: 5001)")
    parser.add_argument("--no-kill", action="store_true", help="Skip killing existing process on the port")
    parser.add_argument("--reload",  action="store_true", help="Auto-restart on .py file changes (single-process watcher, no garbled output)")
    args, _unknown = parser.parse_known_args()

    _setup_logging()

    if args.local:
        os.environ["LOCAL_MODE"] = "true"

    # --reload: delegate to the single-process watcher wrapper and exit
    if args.reload:
        _watch_and_restart(args)
        return

    if not args.no_kill:
        _kill_port(args.port)

    try:
        from execution_api import ExecutionApi

        add_log("Starting Data Analysis Execution API...")
        print("Starting Data Analysis Execution API...")

        api = ExecutionApi()

        # debug=True  → detailed tracebacks in responses
        # use_reloader=False → single process, no stdout interleaving
        # Use --reload flag above for auto-restart on file changes
        api.app.run(
            host="0.0.0.0",# server is accessible from any network interface (not just localhost)
            port=args.port,#the port number to listen on (default is 5001)
            debug=True, # means if an error happens, Flask shows a detailed traceback in the response — helpful for development
            use_reloader=False, #disables Flask's own reloader (we use our custom one instead)
        )

    except KeyboardInterrupt:
        add_log("Server stopped by user (Ctrl+C).")
        print("\n[EXECUTION LAYER] Server stopped cleanly.")
    except Exception as e:
        import traceback
        add_log(f"Error starting Execution API: {e}")
        print(f"Error starting Execution API: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":#"Only run main() if this script is executed directly (e.g., python run_execution_api.py), not if it's imported by another module."
    main()