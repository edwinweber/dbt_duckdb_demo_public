#!/bin/sh
# Metabase entrypoint: stage baked-in DuckDB extensions into the persisted
# extensions volume, then launch Metabase.
#
# Why this exists:
#   docker-compose mounts a host volume over /home/metabase/.duckdb/extensions
#   so installed DuckDB extensions persist across container restarts.  That mount
#   SHADOWS any extensions baked into the image at build time (e.g. ducklake),
#   so on a fresh deploy — or after a DuckDB version bump — the volume would be
#   missing ducklake and Metabase's `LOAD ducklake` would fail.
#
#   We therefore bake extensions into a staging dir OUTSIDE the mount
#   ($EXT_STAGING) and copy any that are missing into the live extensions dir
#   ($EXT_LIVE) on every startup.  Existing files are never overwritten, so
#   extensions Metabase self-installs via INSTALL (icu/httpfs/delta/sqlite) are
#   left untouched.
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

exec java -jar /app/metabase.jar
