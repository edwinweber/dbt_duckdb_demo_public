#!/bin/bash

set -euo pipefail

# Stop Metabase container and wait for a given interval (default: 120s)
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

echo "Stopping Metabase container..."
docker_cmd stop "$METABASE_CONTAINER_NAME"
echo "Waiting $WAIT_SECS seconds for locks to clear..."
sleep "$WAIT_SECS"
echo "Metabase stopped and wait complete."
