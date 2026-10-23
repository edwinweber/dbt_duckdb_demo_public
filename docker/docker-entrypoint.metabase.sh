#!/bin/sh
# Metabase entrypoint:
#   1. Stage baked-in DuckDB extensions into the persisted extensions volume.
#   2. Render docker/metabase-duckdb-init.sql with QUACK_SERVER_TOKEN substituted
#      into /metabase-data/duckdb-init.sql so the admin can copy-paste it into
#      the Metabase DuckDB connection's "Init SQL" field without typing the token.
#   3. Launch Metabase.
#
# Why stage extensions:
#   docker-compose mounts a host volume over /home/metabase/.duckdb/extensions
#   so installed DuckDB extensions persist across container restarts.  That mount
#   SHADOWS any extensions baked into the image at build time (e.g. quack), so
#   on a fresh deploy — or after a DuckDB version bump — the volume would be
#   missing quack and the ATTACH would fail.
#   We bake extensions into a staging dir OUTSIDE the mount ($EXT_STAGING) and
#   copy any that are missing into the live extensions dir ($EXT_LIVE) on every
#   startup.  Existing files are never overwritten.
set -eu

EXT_STAGING=/opt/duckdb-extensions
EXT_LIVE="${HOME}/.duckdb/extensions"

if [ -d "$EXT_STAGING" ]; then
    find "$EXT_STAGING" -type f | while IFS= read -r src; do
        rel=${src#"$EXT_STAGING"/}
        dest="$EXT_LIVE/$rel"
        if [ ! -f "$dest" ]; then
            mkdir -p "$(dirname "$dest")"
            cp "$src" "$dest"
            echo "[entrypoint] staged DuckDB extension: $rel"
        fi
    done
fi

# Render the quack init SQL with the token substituted.
# The source template is baked into the image at /app/metabase-duckdb-init.sql.
# The rendered output lands on the persistent /metabase-data volume so the admin
# can read it at $METABASE_DATA_DIR/duckdb-init.sql on the host.
# QUACK_SERVER_TOKEN must be set; the :? check in docker-compose catches missing
# values before the container even starts.
INIT_SQL_TEMPLATE=/app/metabase-duckdb-init.sql
INIT_SQL_RENDERED=/metabase-data/duckdb-init.sql

if [ -f "$INIT_SQL_TEMPLATE" ] && [ -n "${QUACK_SERVER_TOKEN:-}" ]; then
    # Use sed with a unique delimiter to avoid issues with slashes in the token.
    # The token is treated as a literal string (no regex special chars expected,
    # but we escape backslashes and & just in case).
    ESCAPED_TOKEN=$(printf '%s' "$QUACK_SERVER_TOKEN" | sed 's/[\\&]/\\&/g')
    sed "s|<QUACK_SERVER_TOKEN>|${ESCAPED_TOKEN}|g" \
        "$INIT_SQL_TEMPLATE" > "$INIT_SQL_RENDERED"
    echo "[entrypoint] rendered DuckDB init SQL to $INIT_SQL_RENDERED"
    echo "[entrypoint] paste $INIT_SQL_RENDERED into the Metabase DuckDB connection Init SQL field"
else
    echo "[entrypoint] WARNING: QUACK_SERVER_TOKEN not set — init SQL not rendered"
fi

exec java -jar /app/metabase.jar
