#!/usr/bin/env bash
# Ensure the Azure persistent secret is up to date before running the main command.
set -euo pipefail

DB_PATH="${DUCKDB_DATABASE_LOCATION:-/data/duckdb/danish_democracy_data.duckdb}"

# Only init if credentials are set and DuckDB CLI is available
if [ -n "${AZURE_TENANT_ID:-}" ] && command -v duckdb &>/dev/null; then
    echo "[entrypoint] Creating/updating DuckDB Azure secret..."
    mkdir -p "$(dirname "$DB_PATH")"
    duckdb "$DB_PATH" <<SQL
INSTALL httpfs; INSTALL azure; INSTALL delta;
LOAD httpfs; LOAD azure; LOAD delta;
CREATE OR REPLACE PERSISTENT SECRET azure_sp (
    TYPE azure,
    PROVIDER service_principal,
    TENANT_ID getenv('AZURE_TENANT_ID'),
    CLIENT_ID getenv('AZURE_CLIENT_ID'),
    CLIENT_SECRET getenv('AZURE_CLIENT_SECRET'),
    ACCOUNT_NAME 'onelake'
);
SQL
    echo "[entrypoint] DuckDB Azure secret ready."
fi

exec "$@"
