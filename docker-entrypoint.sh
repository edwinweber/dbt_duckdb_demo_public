#!/usr/bin/env bash
# Entrypoint for the Danish Democracy Data container.
# Sets up storage (local dirs or Azure secrets) before running the main command.
set -euo pipefail

DB_PATH="${DUCKDB_DATABASE_LOCATION:-/data/duckdb/danish_democracy_data.duckdb}"
STORAGE="${STORAGE_TARGET:-local}"

if [ "$STORAGE" = "local" ]; then
    # ── Local storage mode ──────────────────────────────────────────────
    echo "[entrypoint] Storage mode: local"
    LOCAL_BASE="${LOCAL_STORAGE_PATH:-/data/local}"
    mkdir -p "$LOCAL_BASE/Files/Silver" "$LOCAL_BASE/Files/Gold" "$(dirname "$DB_PATH")"
    echo "[entrypoint] Local storage directories ready at $LOCAL_BASE"

elif [ "$STORAGE" = "onelake" ]; then
    # ── OneLake mode — requires Azure credentials ───────────────────────
    if [ -z "${AZURE_TENANT_ID:-}" ] || [ -z "${AZURE_CLIENT_ID:-}" ] || [ -z "${AZURE_CLIENT_SECRET:-}" ]; then
        echo "[entrypoint] ERROR: STORAGE_TARGET=onelake requires AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET." >&2
        echo "[entrypoint] Copy .env.example to .env and fill in your Azure credentials." >&2
        exit 1
    fi

    if command -v duckdb &>/dev/null; then
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
else
    echo "[entrypoint] WARNING: Unknown STORAGE_TARGET='$STORAGE'. Expected 'local' or 'onelake'." >&2
fi

exec "$@"
