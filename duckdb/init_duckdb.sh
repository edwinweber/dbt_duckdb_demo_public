#!/usr/bin/env bash
# Initialize a DuckDB database with Azure persistent secret and extensions.
#
# Usage:
#   ./duckdb/init_duckdb.sh                     # uses DUCKDB_DATABASE_LOCATION from .env
#   ./duckdb/init_duckdb.sh /path/to/my.duckdb  # explicit path
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Load .env if present
if [ -f "$REPO_ROOT/.env" ]; then
    set -a
    source "$REPO_ROOT/.env"
    set +a
fi

# Validate prerequisites
if ! command -v duckdb &>/dev/null; then
    echo "Error: duckdb CLI not found. Install it from https://duckdb.org/docs/installation/" >&2
    exit 1
fi

for var in AZURE_TENANT_ID AZURE_CLIENT_ID AZURE_CLIENT_SECRET; do
    if [ -z "${!var:-}" ]; then
        echo "Error: $var is not set. Check your .env file." >&2
        exit 1
    fi
done

DB_PATH="${1:-${DUCKDB_DATABASE_LOCATION:?Set DUCKDB_DATABASE_LOCATION in .env or pass as argument}}"

echo "Initializing DuckDB at: $DB_PATH"

# Ensure the parent directory exists
mkdir -p "$(dirname "$DB_PATH")"

duckdb "$DB_PATH" < "$SCRIPT_DIR/init_duckdb.sql"

echo "Done. Persistent Azure secret created, extensions installed."
echo "Verify with: duckdb \"$DB_PATH\" \"FROM duckdb_secrets();\""
