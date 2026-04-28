"""
CLI: rebuild analysis_report.html from artifacts in a job output directory.

Usage (from repo root):
  python execution_layer/rebuild_analysis_report_cli.py "C:\\path\\to\\output_data\\<job_id>"

Prints JSON with status and checks to stdout.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_EXEC = Path(__file__).resolve().parent
if str(_EXEC) not in sys.path:
    sys.path.insert(0, str(_EXEC))

from agents.report_builder import rebuild_analysis_report_html  # noqa: E402


def main() -> int:
    if len(sys.argv) < 2:
        print(
            json.dumps(
                {
                    "status": "fail",
                    "error": "Usage: python rebuild_analysis_report_cli.py <output_dir>",
                    "checks": {
                        "tables_rendered": False,
                        "images_embedded": False,
                        "sections_created": False,
                    },
                }
            )
        )
        return 2
    out = sys.argv[1].strip().strip('"')
    result = rebuild_analysis_report_html(out)
    print(json.dumps(result, indent=2))
    return 0 if result.get("status") == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
