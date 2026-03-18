#!/usr/bin/env bash
# Copies local state into Docker named volumes so containers start
# with your current dlt pipeline state, Dagster run history, etc.
#
# Usage: ./docker-seed-volumes.sh
#
# Run this once before the first 'docker compose up', or any time
# you want to reset the volumes to match your local state.

set -euo pipefail

PROJECT="dbt_duckdb_demo"
IMAGE="alpine"

copy_to_volume() {
    local src="$1"
    local volume="$2"

    if [ ! -d "$src" ] && [ ! -f "$src" ]; then
        echo "  SKIP  $src (not found)"
        return
    fi

    echo "  COPY  $src -> volume $volume"
    docker run --rm \
        -v "$(cd "$src" && pwd)":/src:ro \
        -v "${PROJECT}_${volume}":/dst \
        "$IMAGE" \
        sh -c 'cp -a /src/. /dst/'
}

echo "Seeding Docker volumes from local state..."
echo

copy_to_volume "dlt/pipelines_dir" "dlt_pipelines"
copy_to_volume ".dagster"          "dagster_data"
copy_to_volume "dbt/logs"          "dbt_logs"

echo
echo "Done. Volumes seeded. Start services with: docker compose up dagster"
