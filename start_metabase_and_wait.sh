#!/bin/bash

set -euo pipefail

# Start Metabase container and wait for a given interval (default: 120s)
WAIT_SECS="${1:-120}"
METABASE_CONTAINER_NAME="${METABASE_CONTAINER_NAME:-ddd-metabase}"

docker_cmd() {
	if docker info >/dev/null 2>&1; then
		docker "$@"
	elif command -v sudo >/dev/null 2>&1; then
		sudo docker "$@"
	else
		echo "Docker is not accessible and sudo is unavailable." >&2
		return 1
	fi
}

echo "Starting Metabase container..."
docker_cmd start "$METABASE_CONTAINER_NAME"
echo "Waiting $WAIT_SECS seconds for Metabase to initialize..."
sleep "$WAIT_SECS"
echo "Metabase started and wait complete."
