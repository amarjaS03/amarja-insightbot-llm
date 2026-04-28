import re
import json

content = r"""{
  "thinking_logs": [],
  "html_report": "<!DOCTYPE html>\n\\n<html lang='\\\"en\\\"'>\\n<head>\\n<title>Test</title>\n</head>\\n<body>\n  <h1>Hello</h1>\n</body>\n</html>"
}"""

try:
    result = json.loads(content)
    html_report = result.get("html_report", "")
except Exception:
    html_report = ""

# 1. Clean escaped characters
html_report = html_report.replace('\\"', '"').replace('\\n', '\n').replace('\\t', '\t').replace('\\\\', '\\')

# 2. Extract valid inner HTML to remove markdown/double wrappers
html_match = re.search(r'(<!DOCTYPE html[\s\S]*</html>)', html_report, re.IGNORECASE)
if html_match:
    html_report = html_match.group(1)

print("----- CLEANED -----")
print(html_report)