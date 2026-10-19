#!/usr/bin/env bash
# Launch the AirScope dashboard (macOS / Linux).
# Creates .venv and installs requirements on first run. Errors are shown, never hidden.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v java >/dev/null 2>&1; then
  echo "ERROR: Java was not found. Spark needs JDK 17 or 21."
  echo "  macOS:  brew install openjdk@17   (then follow brew's PATH instructions)"
  exit 1
fi

PY=${PYTHON:-python3}
if [ ! -d ".venv" ]; then
  echo "Creating virtual environment in .venv ..."
  "$PY" -m venv .venv
  .venv/bin/python -m pip install --upgrade pip setuptools wheel
  .venv/bin/python -m pip install -r requirements.txt
fi

echo "Starting AirScope at http://localhost:8501  (Ctrl+C to stop)"
exec .venv/bin/python -m streamlit run app.py
