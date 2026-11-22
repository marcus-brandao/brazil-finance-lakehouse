#!/usr/bin/env python3
"""Block commits, PRs and issue text that carry mechanical AI tells.

Hooks cannot run a skill, so this only catches what a regex can: dashes and
attribution lines. The full /humanizer pass is still required by CLAUDE.md.
"""

import json
import re
import sys

cmd = json.load(sys.stdin).get("tool_input", {}).get("command", "")
if not re.search(r"git commit|gh (pr|issue) (create|edit|comment|review)", cmd):
    sys.exit(0)

text = cmd
# Body text often lives in a file, so scan it too.
for path in re.findall(r"(?:--body-file|-F)\s+(\S+)", cmd):
    try:
        with open(path.strip("'\"")) as f:
            text += "\n" + f.read()
    except OSError:
        pass

problems = []
if re.search("[–—]", text):
    problems.append("em/en dash: use a period, comma, colon or parentheses")
if re.search(
    r"co-authored-by|generated with|claude\.com|claude code", text, re.IGNORECASE
):
    problems.append("AI/tool attribution is not allowed in this repo")

if problems:
    print(
        "Humanizer check failed: " + "; ".join(problems) + ". "
        "Run /humanizer on the text, fix it, then retry.",
        file=sys.stderr,
    )
    sys.exit(2)
