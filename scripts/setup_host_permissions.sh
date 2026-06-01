#!/usr/bin/env bash
# =============================================================================
# DDD Platform — fix host directory permissions for non-root containers
#
# Run this script ONCE on the host after cloning the repo, and again whenever
# you recreate the /data or /data_backup directory trees.
#
# What it does:
#   1. Creates any missing data and backup directories.
#   2. Sets ownership so the pipeline containers (app, UID 1000) can write to
#      their directories and Metabase (UID 2000) can write to its own.
#   3. Sets cross-UID read/write bits so:
#        - Metabase (UID 2000) can write DuckDB WAL files in a UID-1000 dir.
#        - backup (UID 1000) can read Metabase data owned by UID 2000.
#   4. Detects the Docker socket GID and writes DOCKER_GID to .env (once).
#   5. Prints a summary so you can verify each directory looks right.
#
# Usage:
#   sudo scripts/setup_host_permissions.sh
#   sudo scripts/setup_host_permissions.sh --dry-run   # print without applying
# =============================================================================

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${REPO_DIR}/.env"

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
fi

# ── Colour helpers ────────────────────────────────────────────────────────────

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

ok()   { printf "${GREEN}  ✓${NC}  %s\n" "$*"; }
info() { printf "${YELLOW}  →${NC}  %s\n" "$*"; }
err()  { printf "${RED}  ✗${NC}  %s\n" "$*" >&2; }

run() {
    # Print and optionally execute a command.
    if $DRY_RUN; then
        printf '     (dry-run) %s\n' "$*"
    else
        "$@"
    fi
}

# ── Guards ────────────────────────────────────────────────────────────────────

if [[ $EUID -ne 0 ]] && ! $DRY_RUN; then
    err "This script must be run as root (use sudo)."
    exit 1
fi

if $DRY_RUN; then
    printf '\n%s\n\n' "=== DRY RUN — no changes will be made ==="
fi

printf '\n%s\n\n' "=== DDD Platform — host permission setup ==="

# ── Step 1: create directories ────────────────────────────────────────────────

printf 'Step 1: create directories\n'

for dir in \
    /data/dlt_pipelines \
    /data/duckdb \
    /data/dbt_logs \
    /data/dagster \
    /data/local \
    /data/metabase/data \
    /data/metabase/duckdb-extensions \
    /data_backup/dagster \
    /data_backup/metabase \
    /data_backup/logs
do
    if [[ -d "$dir" ]]; then
        ok "exists  $dir"
    else
        info "creating $dir"
        run mkdir -p "$dir"
    fi
done

printf '\n'

# ── Step 2: ownership ─────────────────────────────────────────────────────────

printf 'Step 2: ownership\n'

info "chown 1000:1000  /data/dlt_pipelines /data/duckdb /data/dbt_logs /data/dagster /data/local"
run chown -R 1000:1000 \
    /data/dlt_pipelines \
    /data/duckdb \
    /data/dbt_logs \
    /data/dagster \
    /data/local

info "chown 1000:1000  /data_backup"
run chown -R 1000:1000 /data_backup

info "chown 2000:2000  /data/metabase/data /data/metabase/duckdb-extensions"
run chown -R 2000:2000 /data/metabase/data /data/metabase/duckdb-extensions

printf '\n'

# ── Step 3: cross-UID access ──────────────────────────────────────────────────

printf 'Step 3: cross-UID access bits\n'

# Metabase (UID 2000) creates WAL/lock files inside the UID-1000-owned DuckDB
# directory even during read-only queries — o+rwx on the directory and files.
info "chmod o+rwx  /data/duckdb  (Metabase WAL access)"
run chmod -R o+rwx /data/duckdb

# backup (UID 1000) reads Metabase data (UID 2000) to create zip archives.
# o+rX = read files, traverse directories; no write needed.
info "chmod o+rX   /data/metabase/data  (backup read access)"
run chmod -R o+rX /data/metabase/data

printf '\n'

# ── Step 4: DOCKER_GID in .env ───────────────────────────────────────────────

printf 'Step 4: Docker socket GID\n'

if [[ ! -S /var/run/docker.sock ]]; then
    err "/var/run/docker.sock not found — is Docker running?"
    err "Set DOCKER_GID manually in .env after Docker starts."
else
    DOCKER_GID="$(stat -c '%g' /var/run/docker.sock)"

    if [[ ! -f "$ENV_FILE" ]]; then
        info ".env not found at ${ENV_FILE} — skipping DOCKER_GID write (set it manually)"
    elif grep -q '^DOCKER_GID=' "$ENV_FILE"; then
        EXISTING="$(grep '^DOCKER_GID=' "$ENV_FILE" | head -1 | cut -d= -f2)"
        if [[ "$EXISTING" == "$DOCKER_GID" ]]; then
            ok "DOCKER_GID=${DOCKER_GID} already set in .env"
        else
            info "Updating DOCKER_GID from ${EXISTING} to ${DOCKER_GID} in .env"
            run sed -i "s/^DOCKER_GID=.*/DOCKER_GID=${DOCKER_GID}/" "$ENV_FILE"
        fi
    else
        info "Adding DOCKER_GID=${DOCKER_GID} to .env"
        run bash -c "echo 'DOCKER_GID=${DOCKER_GID}' >> '${ENV_FILE}'"
    fi
fi

printf '\n'

# ── Step 5: verification summary ──────────────────────────────────────────────

if $DRY_RUN; then
    printf '%s\n\n' "=== Dry run complete — no changes were made ==="
    exit 0
fi

printf 'Step 5: verification\n\n'

printf '  %-45s  %s\n' "Path" "Owner (UID:GID)"
printf '  %-45s  %s\n' "----" "---------------"

for dir in \
    /data/dlt_pipelines \
    /data/duckdb \
    /data/dbt_logs \
    /data/dagster \
    /data/local \
    /data/metabase/data \
    /data/metabase/duckdb-extensions \
    /data_backup
do
    owner="$(stat -c '%u:%g' "$dir" 2>/dev/null || echo 'missing')"
    perms="$(stat -c '%A' "$dir" 2>/dev/null || echo '?')"
    printf '  %-45s  %s  %s\n' "$dir" "$owner" "$perms"
done

printf '\n'

if [[ -f "$ENV_FILE" ]] && grep -q '^DOCKER_GID=' "$ENV_FILE"; then
    ok "$(grep '^DOCKER_GID=' "$ENV_FILE") (in .env)"
else
    info "DOCKER_GID not found in .env — set it manually."
fi

printf '\n%s\n' "=== Done. Restart your containers to apply the new permissions. ==="
printf '%s\n\n' "    docker compose up -d dagster metabase"
