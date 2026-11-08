#!/usr/bin/env bash
# Run the whole local pipeline end to end:
#   start the Docker stack -> prepare Python venv -> create tables
#   -> generate fake raw events -> build facts and aggregates.
#
# Usage:
#   bash run_all.sh
#   START=2026-11-01 DAYS=7 bash run_all.sh
#
# The script always starts from an empty stack (docker compose down):
# it regenerates all raw data, and generate_events appends rows, so keeping
# old data would duplicate it. Every run gives the same result for the same
# START / DAYS (the generator uses a fixed random seed).

set -euo pipefail

# Always run from the repository root (the folder of this script)
cd "$(dirname "$0")"

START="${START:-2026-10-01}"
DAYS="${DAYS:-31}"
END="$(date -d "$START + $((DAYS - 1)) days" +%F)"

echo "==> Removing old containers and data"
docker compose down

echo "==> Starting Docker stack"
docker compose up -d

echo "==> Preparing Python virtual environment"
if [[ ! -d .venv ]]; then
    python3 -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --quiet -r requirements.txt

echo "==> Waiting for Trino to start"
for _ in $(seq 1 60); do
    if python - <<'EOF' 2>/dev/null
import json, urllib.request
info = json.load(urllib.request.urlopen("http://localhost:8080/v1/info", timeout=2))
raise SystemExit(0 if not info["starting"] else 1)
EOF
    then
        break
    fi
    sleep 2
done

echo "==> Creating tables and loading seed dimensions"
python -m pipeline.init_db

echo "==> Generating fake events: $START, $DAYS days"
python -m pipeline.generate_events --start "$START" --days "$DAYS"

echo "==> Building the model: $START .. $END"
python -m pipeline.run_daily --start "$START" --end "$END"

echo
echo "Done. Open DataGrip (localhost:8080) or run: docker compose exec trino trino"
