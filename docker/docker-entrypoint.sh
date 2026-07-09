#!/usr/bin/env bash
# Entrypoint for the Danish Democracy Data container.
# Sets up storage (local dirs or Azure secrets) before running the main command.
set -euo pipefail

DB_PATH="${DUCKDB_DATABASE_LOCATION:-/data/duckdb/danish_democracy_data.duckdb}"
STORAGE="${STORAGE_TARGET:-local}"

if [ "$STORAGE" = "onelake" ]; then
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
    # ── Local infrastructure — covers local, s3, and any future non-onelake targets ──
    # STORAGE_TARGET only controls the Delta Lake export destination; the .duckdb
    # file and Dagster state are always local regardless of STORAGE_TARGET.
    # S3 secret creation (when RAW_STORAGE_TARGET=s3) is handled by init_duckdb.py
    # and dbt's on-run-start hook, not here.
    if [ "$STORAGE" != "local" ] && [ "$STORAGE" != "s3" ]; then
        echo "[entrypoint] WARNING: Unrecognized STORAGE_TARGET='$STORAGE'; treating as local." >&2
    fi
    echo "[entrypoint] Storage mode: ${STORAGE} (local infrastructure)"
    LOCAL_BASE="${LOCAL_STORAGE_PATH:-/data/local}"
    DAGSTER_HOME="${DAGSTER_HOME:-/data/dagster}"
    mkdir -p "$LOCAL_BASE/Files/Silver" "$LOCAL_BASE/Files/Gold" "$(dirname "$DB_PATH")" "$DAGSTER_HOME"
    # Write dagster.yaml only if not already present, so run history persists across restarts.
    if [ ! -f "$DAGSTER_HOME/dagster.yaml" ]; then
        cat > "$DAGSTER_HOME/dagster.yaml" <<YAML
storage:
  sqlite:
    base_dir: ${DAGSTER_HOME}
YAML
        echo "[entrypoint] Created $DAGSTER_HOME/dagster.yaml"
    fi
    for required_dir in \
        "$DAGSTER_HOME" \
        "$DAGSTER_HOME/history" \
        "$DAGSTER_HOME/schedules" \
        "$DAGSTER_HOME/storage"
    do
        mkdir -p "$required_dir"
        if [ ! -w "$required_dir" ]; then
            echo "[entrypoint] ERROR: $required_dir is not writable by $(id -u):$(id -g)." >&2
            echo "[entrypoint] Run: sudo scripts/setup_host_permissions.sh" >&2
            exit 1
        fi
    done
    echo "[entrypoint] Local storage directories ready at $LOCAL_BASE"
fi

exec "$@"
