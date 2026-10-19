#!/usr/bin/env bash
# Syntax-check every Python file, then run the pytest suite (macOS / Linux).
set -euo pipefail
cd "$(dirname "$0")/.."
PY=python3
[ -x .venv/bin/python ] && PY=.venv/bin/python
echo "== Compiling sources =="
"$PY" -m compileall -q app.py src scripts tests
echo "== Running tests (Spark starts once; ~1-2 minutes) =="
"$PY" -m pytest "$@"
