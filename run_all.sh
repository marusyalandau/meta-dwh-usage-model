#!/usr/bin/env bash
# Run the whole local pipeline end to end:
#   start the Docker stack -> prepare Python venv -> create tables
#   -> generate fake raw events -> build facts and aggregates
#   -> run data quality checks -> run example queries -> run table maintenance.
# Results of the example queries are also saved to output/example_results.txt.
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

# Print a message and stop the script
fail() {
    echo >&2
    echo "ERROR: $1" >&2
    shift
    for line in "$@"; do
        echo "  $line" >&2
    done
    exit 1
}

echo "==> Checking requirements"
if ! command -v docker > /dev/null; then
    fail "Docker is not installed." \
        "On Windows with WSL: install Docker Desktop" \
        "  (https://docs.docker.com/desktop/setup/install/windows-install/)" \
        "  and turn on Settings -> Resources -> WSL integration for this distro." \
        "On Ubuntu (also inside WSL without Docker Desktop):" \
        "  curl -fsSL https://get.docker.com | sudo sh" \
        "  sudo usermod -aG docker \$USER    # then open a new terminal"
fi
if ! docker compose version > /dev/null 2>&1; then
    fail "Docker Compose v2 is not available (the 'docker compose' command)." \
        "Docker Desktop includes it. On Ubuntu: sudo apt install docker-compose-plugin"
fi
if ! docker_info="$(docker info 2>&1)"; then
    if grep -qi "permission denied" <<< "$docker_info"; then
        fail "No permission to use Docker." \
            "Add your user to the docker group, then open a new terminal:" \
            "  sudo usermod -aG docker \$USER"
    fi
    fail "Docker is installed but not running." \
        "Start Docker Desktop, or on Ubuntu run: sudo service docker start"
fi
if ! command -v python3 > /dev/null; then
    fail "Python 3 is not installed." \
        "On Ubuntu: sudo apt install python3 python3-venv"
fi
if [[ ! -d .venv ]] && ! python3 -c "import ensurepip" 2> /dev/null; then
    fail "Python cannot create virtual environments (python3-venv is missing)." \
        "On Ubuntu: sudo apt install python3-venv"
fi
echo "Docker and Python are available."

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
trino_ready=false
for _ in $(seq 1 60); do
    if python - <<'EOF' 2>/dev/null
import json, urllib.request
info = json.load(urllib.request.urlopen("http://localhost:8080/v1/info", timeout=2))
raise SystemExit(0 if not info["starting"] else 1)
EOF
    then
        trino_ready=true
        break
    fi
    sleep 2
done
if [[ "$trino_ready" != true ]]; then
    fail "Trino did not start in 2 minutes." \
        "Check the containers: docker compose ps" \
        "and the Trino log:    docker compose logs trino"
fi

echo "==> Creating tables and loading seed dimensions"
python -m pipeline.init_db

echo "==> Generating fake events: $START, $DAYS days"
python -m pipeline.generate_events --start "$START" --days "$DAYS"

echo "==> Building the model: $START .. $END"
python -m pipeline.run_daily --start "$START" --end "$END"

echo "==> Running data quality checks"
python -m pipeline.checks --start "$START" --end "$END"

mkdir -p output
echo "==> Running example queries (also saved to output/example_results.txt)"
python -m pipeline.run_examples | tee output/example_results.txt

# In production maintenance is a separate weekly job. Retention is counted
# from the last generated day, so the result does not depend on today's date.
echo "==> Running table maintenance as of $END"
python -m pipeline.maintenance --as-of "$END"

echo
echo "Done. Example results are in output/example_results.txt. To run your own"
echo "queries, open DataGrip (localhost:8080) or run: docker compose exec trino trino"
