#!/usr/bin/env bash
# Entrypoint for the Dagster image (services dagster and run).
# Sets up storage (local dirs or Azure secrets) before running the main command.
set -euo pipefail

STORAGE="${STORAGE_TARGET:-local}"

if [ "$STORAGE" = "onelake" ]; then
    # ── OneLake mode — requires Azure credentials ───────────────────────
    if [ -z "${AZURE_TENANT_ID:-}" ] || [ -z "${AZURE_CLIENT_ID:-}" ] || [ -z "${AZURE_CLIENT_SECRET:-}" ]; then
        echo "[entrypoint] ERROR: STORAGE_TARGET=onelake requires AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET." >&2
        echo "[entrypoint] Copy .env.example to .env and fill in your Azure credentials." >&2
        exit 1
    fi

    echo "[entrypoint] Creating/updating DuckDB Azure secret..."
    # In memory: a persistent secret is stored under ~/.duckdb, not in a database
    # file, and the .duckdb file belongs to duckdb_server. The same function creates
    # the server's secrets (and the S3 secret when a storage switch is s3).
    python - <<'PY'
import duckdb

from ddd_python.ddd_dbt.init_duckdb import init_secrets_on_connection

con = duckdb.connect()
for ext in ("httpfs", "azure", "delta"):
    con.execute(f"INSTALL {ext}; LOAD {ext};")
init_secrets_on_connection(con)
con.close()
PY
    echo "[entrypoint] DuckDB Azure secret ready."
else
    # ── Local infrastructure — covers local, s3, and any future non-onelake targets ──
    # STORAGE_TARGET only controls the Delta Lake export destination; the .duckdb
    # file and Dagster state are always local regardless of STORAGE_TARGET. The
    # .duckdb file belongs to duckdb_server, so its directory is not created here.
    # S3 secret creation (when RAW_STORAGE_TARGET=s3) is handled by init_duckdb.py
    # and dbt's on-run-start hook, not here.
    if [ "$STORAGE" != "local" ] && [ "$STORAGE" != "s3" ]; then
        echo "[entrypoint] WARNING: Unrecognized STORAGE_TARGET='$STORAGE'; treating as local." >&2
    fi
    echo "[entrypoint] Storage mode: ${STORAGE} (local infrastructure)"
    LOCAL_BASE="${LOCAL_STORAGE_PATH:-/data/local}"
    DAGSTER_HOME="${DAGSTER_HOME:-/data/dagster}"
    mkdir -p "$LOCAL_BASE/Files/Silver" "$LOCAL_BASE/Files/Gold" "$DAGSTER_HOME"
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
