#!/usr/bin/env bash
# Upgrade the project's DuckDB database files from the 1.5.x to the DuckDB 2.0 storage format.
#
# DuckDB 2.0 opens 1.5.x files, but 1.5.x cannot open files written by 2.0, so the
# upgrade is one-way. This script makes it reversible by backing up first.
#
# What it does:
#   1. Stops the containers that hold the files (duckdb_server, and Dagster and Metabase
#      from before the quack server) and checks that no other process has them open.
#   2. Backs up the main .duckdb file, the DuckLake catalog (ducklake mode) and the
#      Azure secret file to a timestamped directory, with SHA-256 checksums.
#   3. Creates a throw-away venv with duckdb==2.0.0 and copies each file into a new
#      file in the 2.0 format (COPY FROM DATABASE), next to the original.
#   4. Verifies the copies against the originals: same tables and row counts, same
#      views, DuckLake tables read through the extension, every view binds.
#   5. Only when everything matches, replaces the originals with the copies.
#
# The DuckLake Parquet files are never touched (the Parquet format does not change).
# Containers are left stopped: Metabase (DuckDB 1.5 driver) cannot read a 2.0 file.
#
# Usage:
#   scripts/upgrade_duckdb_to_v2.sh --dry-run                # steps 1, 3 and 4 on copies; nothing replaced
#   scripts/upgrade_duckdb_to_v2.sh                          # asks for confirmation, then upgrades
#   scripts/upgrade_duckdb_to_v2.sh --yes --backup-dir /data/backup_pre_duckdb2
#
# Options:
#   --dry-run          do everything except replacing the originals (the backup is skipped too)
#   --backup-dir DIR   backup root (default: $DUCKDB_BACKUP_DIR, then ./backup); a timestamped
#                      sub-directory pre_duckdb2_<time> is created in it
#   --yes              do not ask for confirmation
#   --no-stop          do not stop containers (fails when something still holds the files)
#
# Rollback: copy the files from the backup directory back (the script prints the command)
# and run DuckDB 1.5.6 again.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$PROJECT_DIR/.env"
DUCKDB_VERSION="2.0.0"

DRY_RUN=0
ASSUME_YES=0
STOP_CONTAINERS=1
BACKUP_ROOT=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1 ;;
        --yes) ASSUME_YES=1 ;;
        --no-stop) STOP_CONTAINERS=0 ;;
        --backup-dir) BACKUP_ROOT="${2:?--backup-dir needs a directory}"; shift ;;
        -h|--help) sed -n '2,31p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 2 ;;
    esac
    shift
done

[[ -f "$ENV_FILE" ]] || { echo "Error: .env file not found at $ENV_FILE" >&2; exit 1; }
set -a
# shellcheck source=.env
source "$ENV_FILE"
set +a

DB_FILE="${DUCKDB_DATABASE_LOCATION:?DUCKDB_DATABASE_LOCATION is not set}"
DB_ALIAS="${DUCKDB_DATABASE:-$(basename "$DB_FILE" .duckdb)}"
DB_DIR="$(dirname "$DB_FILE")"
CATALOG_FILE=""
DATA_PATH=""
if [[ "${SILVER_STORAGE_FORMAT:-duckdb}" == "ducklake" ]]; then
    CATALOG_FILE="${DUCKLAKE_CATALOG_LOCATION:?DUCKLAKE_CATALOG_LOCATION is not set}"
    if [[ "${RAW_STORAGE_TARGET:-local}" == "s3" ]]; then
        echo "Error: DuckLake data on S3 is not supported by this script (the verification needs an S3 secret)." >&2
        exit 1
    fi
    DATA_PATH="${DUCKLAKE_DATA_PATH:?DUCKLAKE_DATA_PATH is not set}"
fi

FILES=("$DB_FILE")
[[ -n "$CATALOG_FILE" ]] && FILES+=("$CATALOG_FILE")
for f in "${FILES[@]}"; do
    [[ -f "$f" ]] || { echo "Error: $f does not exist." >&2; exit 1; }
    [[ -e "$f.wal" ]] && { echo "Error: $f.wal exists (unclean shutdown). Open the file once with DuckDB 1.5.6 so it checkpoints, then retry." >&2; exit 1; }
done

# Header bytes 12-15 hold 999 in files written by DuckDB 2.0.
is_v2_format() { [[ "$(od -An -tu4 -j12 -N4 "$1" | tr -d ' ')" == "999" ]]; }
ALREADY_V2=0
for f in "${FILES[@]}"; do
    if is_v2_format "$f"; then echo "Already in the 2.0 format: $f"; ALREADY_V2=$((ALREADY_V2 + 1)); fi
done
if [[ "$ALREADY_V2" -eq "${#FILES[@]}" ]]; then echo "Nothing to upgrade."; exit 0; fi
if [[ "$ALREADY_V2" -ne 0 ]]; then
    echo "Error: some files are already 2.0 and some are not; resolve this manually." >&2
    exit 1
fi

docker_cmd() {
    if docker info >/dev/null 2>&1; then docker "$@"
    elif command -v sudo >/dev/null 2>&1; then sudo docker "$@"
    else echo "Docker is not accessible." >&2; return 1; fi
}

echo "Files to upgrade:"
for f in "${FILES[@]}"; do echo "  $f ($(du -h "$f" | cut -f1))"; done
if [[ "$DRY_RUN" -eq 0 && "$ASSUME_YES" -eq 0 ]]; then
    read -r -p "Back up and upgrade these files to DuckDB $DUCKDB_VERSION? This cannot be undone without the backup. [y/N] " answer
    [[ "$answer" == "y" || "$answer" == "Y" ]] || { echo "Aborted."; exit 1; }
fi

# 1. Stop whatever holds the files (a dry run only reads, so it leaves everything running)
if [[ "$DRY_RUN" -eq 0 && "$STOP_CONTAINERS" -eq 1 ]] && command -v docker >/dev/null 2>&1; then
    for name in "${DUCKDB_SERVER_CONTAINER_NAME:-ddd-duckdb-server}" "${DAGSTER_CONTAINER_NAME:-ddd-dagster}" "${METABASE_CONTAINER_NAME:-ddd-metabase}"; do
        if [[ "$(docker_cmd inspect -f '{{.State.Running}}' "$name" 2>/dev/null || true)" == "true" ]]; then
            echo "Stopping $name ..."
            docker_cmd stop "$name" >/dev/null
        fi
    done
fi
for f in "${FILES[@]}"; do
    [[ "$DRY_RUN" -eq 1 ]] && break
    if fuser "$f" >/dev/null 2>&1; then
        echo "Error: $f is still open by a process:" >&2
        fuser -v "$f" >&2 || true
        exit 1
    fi
done

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"

# 2. Backup (skipped for a dry run, which changes nothing)
BACKUP_DIR=""
if [[ "$DRY_RUN" -eq 0 ]]; then
    BACKUP_ROOT="${BACKUP_ROOT:-${DUCKDB_BACKUP_DIR:-$PROJECT_DIR/backup}}"
    BACKUP_DIR="$BACKUP_ROOT/pre_duckdb2_$TIMESTAMP"
    mkdir -p "$BACKUP_DIR" 2>/dev/null || { echo "Error: cannot create $BACKUP_DIR; pass a writable --backup-dir." >&2; exit 1; }
    echo "Backing up to $BACKUP_DIR ..."
    for f in "${FILES[@]}"; do cp -p "$f" "$BACKUP_DIR/"; done
    cp -p "$DB_DIR"/*.duckdb_secret "$BACKUP_DIR/" 2>/dev/null || true
    (cd "$BACKUP_DIR" && sha256sum -- * > SHA256SUMS.tmp && mv SHA256SUMS.tmp SHA256SUMS)
    # The checksum of each original must equal the checksum of its backup copy.
    for f in "${FILES[@]}"; do
        orig="$(sha256sum "$f" | cut -d' ' -f1)"
        grep -q "^$orig  $(basename "$f")$" "$BACKUP_DIR/SHA256SUMS" || { echo "Error: backup checksum mismatch for $f" >&2; exit 1; }
    done
    echo "Backup verified ($(ls "$BACKUP_DIR" | grep -vc SHA256SUMS) files)."
fi

# 3. Copy into the 2.0 format in a venv that has DuckDB 2.0
VENV_DIR="${UPGRADE_VENV_DIR:-$(mktemp -d -t duckdb2_venv_XXXXXX)}"
OUT_DIR="$DB_DIR/.upgrade_v2_$TIMESTAMP"
cleanup() {
    rm -rf "$OUT_DIR"
    [[ -z "${UPGRADE_VENV_DIR:-}" ]] && rm -rf "$VENV_DIR"
    return 0
}
trap cleanup EXIT
echo "Creating a venv with duckdb==$DUCKDB_VERSION ..."
python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install --quiet "duckdb==$DUCKDB_VERSION"

WORKER_ARGS=(--out-dir "$OUT_DIR" --duckdb-file "$DB_FILE" --duckdb-alias "$DB_ALIAS")
[[ -n "$CATALOG_FILE" ]] && WORKER_ARGS+=(--catalog-file "$CATALOG_FILE" --data-path "$DATA_PATH")
"$VENV_DIR/bin/python" -I "$SCRIPT_DIR/upgrade_duckdb_to_v2_worker.py" "${WORKER_ARGS[@]}"

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "Dry run complete: the copies verified, nothing was replaced."
    exit 0
fi

# 4. Replace the originals (same directory, so mv is a rename)
for f in "${FILES[@]}"; do
    new="$OUT_DIR/$(basename "$f")"
    chmod --reference="$BACKUP_DIR/$(basename "$f")" "$new"
    mv -f "$new" "$f"
done
for f in "${FILES[@]}"; do
    is_v2_format "$f" || { echo "Error: $f is not in the 2.0 format after the swap." >&2; exit 1; }
    "$VENV_DIR/bin/python" -I -c "
import sys, duckdb
con = duckdb.connect(sys.argv[1], read_only=True)
print('  opens with DuckDB', duckdb.__version__, '-', con.execute('SELECT count(*) FROM duckdb_tables()').fetchone()[0], 'tables:', sys.argv[1])
" "$f"
done

echo
echo "Upgrade complete."
echo "  Backup: $BACKUP_DIR"
echo "  Rollback (with the old DuckDB 1.5.6 stack):"
for f in "${FILES[@]}"; do echo "    cp -p \"$BACKUP_DIR/$(basename "$f")\" \"$f\""; done
echo
echo "Dagster and Metabase were left stopped. Anything that opens these files must now use"
echo "DuckDB 2.0 (the project pin duckdb<1.6, the Metabase DuckDB driver and the Docker images)."
