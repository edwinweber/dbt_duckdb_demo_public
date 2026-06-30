#!/usr/bin/env bash
# Copy the DuckDB database from the Docker volume to ./data/duckdb/
# for local querying with DBeaver or DuckDB CLI.
set -euo pipefail

DEST="./data/duckdb"
CONTAINER="$(docker compose ps -q dagster 2>/dev/null || docker compose ps -q run 2>/dev/null || true)"

if [ -z "$CONTAINER" ]; then
    echo "No running container found. Starting a temporary one..."
    mkdir -p "$DEST"
    docker compose run --rm --no-deps -v "$(pwd)/$DEST:/backup" --entrypoint bash run \
        -c 'cp /data/duckdb/danish_democracy_data.duckdb /backup/'
else
    mkdir -p "$DEST"
    docker compose cp dagster:/data/duckdb/danish_democracy_data.duckdb "$DEST/"
fi

echo "Copied to: $DEST/danish_democracy_data.duckdb"
echo "Size: $(du -h "$DEST/danish_democracy_data.duckdb" | cut -f1)"
