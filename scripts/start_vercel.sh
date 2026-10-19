#!/usr/bin/env bash
# Runtime writes stay in Vercel's temporary filesystem.
set -euo pipefail
runtime_dir=$(mktemp -d /tmp/airscope.XXXXXX)
cp -R /app/. "$runtime_dir/"
cd "$runtime_dir"

# Leave room for Python, Arrow, and the JVM inside the function's memory limit.
python - <<'PY'
from pathlib import Path
import yaml

path = Path("config/settings.yaml")
settings = yaml.safe_load(path.read_text())
settings["spark"].update(master="local[1]", driver_memory="768m", shuffle_partitions=2)
settings["upload"]["max_upload_mb"] = 4
path.write_text(yaml.safe_dump(settings, sort_keys=False))
PY

exec python -m streamlit run app.py \
    --server.address=0.0.0.0 \
    --server.port="${PORT:-80}" \
    --server.headless=true \
    --server.maxUploadSize=4 \
    --server.fileWatcherType=none \
    --browser.gatherUsageStats=false
