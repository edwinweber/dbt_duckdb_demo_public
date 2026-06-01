"""Shared configuration, data types, and Docker helpers for the DDD backup system.

This module is the single source of truth for backup targets, directories,
and container lifecycle management.  Both ``backup_platform`` and
``restore_platform`` import from here; nothing else should.
"""

from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

__all__ = [
    "BackupTarget",
    "BACKUP_LOG_DIR",
    "BACKUP_TARGETS",
    "REPO_ROOT",
    "RESTORE_DOCKER_IMAGE",
    "TARGET_NAMES",
    "available_timestamps",
    "start_containers",
    "stop_containers",
]

logger = logging.getLogger(__name__)


# ── Target descriptor ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class BackupTarget:
    """Immutable descriptor for a single backup/restore target."""

    name: str
    """Archive filename prefix — archives are named ``{name}_{YYYYMMDD_HHMMSS}.zip``."""

    source: Path
    """Host directory to archive (must exist at backup time)."""

    backup_dir: Path
    """Local directory where zip archives are stored."""

    containers: tuple[str, ...]
    """Docker Compose service(s) to stop before archiving and restart after."""

    restore_uid: str | None = None
    """When set, restore runs extraction via ``docker run --user {restore_uid}``
    so that restored files are owned by the correct UID.  Required when the
    target directory is not writable by the process running the restore script."""

    max_archive_age_days: int = 62
    """Local archives older than this many days are pruned after a successful backup."""


# ── Paths (overridable via environment variables) ─────────────────────────────

BACKUP_LOG_DIR: Path = Path(os.environ.get("BACKUP_LOG_DIR", "/data_backup/logs"))

_DAGSTER_BACKUP_DIR  = Path(os.environ.get("DAGSTER_BACKUP_DIR",  "/data_backup/dagster"))
_METABASE_BACKUP_DIR = Path(os.environ.get("METABASE_BACKUP_DIR", "/data_backup/metabase"))
_DUCKDB_BACKUP_DIR   = Path(os.environ.get("DUCKDB_BACKUP_DIR",   "/data_backup/duckdb"))

_DAGSTER_BACKUP_MAX_AGE_DAYS:  int = max(1, int(os.environ.get("DAGSTER_BACKUP_MAX_AGE_DAYS",  "62")))
_METABASE_BACKUP_MAX_AGE_DAYS: int = max(1, int(os.environ.get("METABASE_BACKUP_MAX_AGE_DAYS", "62")))
_DUCKDB_BACKUP_MAX_AGE_DAYS:   int = max(1, int(os.environ.get("DUCKDB_BACKUP_MAX_AGE_DAYS",   "7")))

# Single source of truth for each service's data directory.
# DAGSTER_HOME and METABASE_DATA_DIR are the same variables used by the
# services themselves — no separate backup-source variables needed.
# DUCKDB_DATABASE_LOCATION points to the .duckdb file; .parent gives the directory.
_DAGSTER_HOME      = Path(os.environ.get("DAGSTER_HOME",      "/data/dagster"))
_METABASE_DATA_DIR = Path(os.environ.get("METABASE_DATA_DIR", "/data/metabase/data"))
_DUCKDB_DATA_DIR   = Path(
    os.environ.get("DUCKDB_DATABASE_LOCATION", "/data/duckdb/danish_democracy_data.duckdb")
).parent


# ── Backup targets ────────────────────────────────────────────────────────────
#
# ``containers`` holds Docker *container names* (not Compose service names).
# Container names are pinned via ``container_name:`` in docker-compose.yml so
# they are stable regardless of which directory the backup runs from.

BACKUP_TARGETS: tuple[BackupTarget, ...] = (
    BackupTarget(
        name="dagster",
        source=_DAGSTER_HOME,
        backup_dir=_DAGSTER_BACKUP_DIR,
        containers=("ddd-dagster",),
        # Dagster home is owned by the process user — direct extraction works.
        max_archive_age_days=_DAGSTER_BACKUP_MAX_AGE_DAYS,
    ),
    BackupTarget(
        name="metabase",
        source=_METABASE_DATA_DIR,
        backup_dir=_METABASE_BACKUP_DIR,
        containers=("ddd-metabase",),
        restore_uid="2000",  # /data/metabase is owned by UID 2000 (Metabase process)
        max_archive_age_days=_METABASE_BACKUP_MAX_AGE_DAYS,
    ),
    BackupTarget(
        name="duckdb",
        source=_DUCKDB_DATA_DIR,
        backup_dir=_DUCKDB_BACKUP_DIR,
        # Both services hold open DuckDB connections; stop both to ensure a
        # clean, WAL-flushed snapshot before archiving.
        containers=("ddd-dagster", "ddd-metabase"),
        max_archive_age_days=_DUCKDB_BACKUP_MAX_AGE_DAYS,
    ),
)

TARGET_NAMES: tuple[str, ...] = tuple(t.name for t in BACKUP_TARGETS)


# ── Docker ────────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parents[2]

# Image used for privileged restore extraction; must provide ``python3``.
# The project image is used so no additional pull is required.
RESTORE_DOCKER_IMAGE = "danish-democracy-data:latest"


def _is_container_running(container_name: str) -> bool:
    """Return True when *container_name* exists and its state is running."""
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Running}}", container_name],
        capture_output=True,
        text=True,
    )
    # returncode != 0 means the container does not exist.
    return result.returncode == 0 and result.stdout.strip() == "true"


def stop_containers(containers: list[str] | tuple[str, ...]) -> list[str]:
    """Gracefully stop the given containers (SIGTERM, waits for exit).

    Only containers that are currently running are stopped; already-stopped
    containers are silently skipped so the call is always safe.

    Uses ``docker stop`` directly (not ``docker compose``) so the backup works
    from inside a container without needing the compose project context or a
    ``.env`` file.

    Args:
        containers: Container *names* to stop (may include non-running ones).

    Returns:
        Names of the containers that were actually stopped.  Pass this list to
        :func:`start_containers` to restart exactly those containers — and no
        others — after the operation is complete.
    """
    to_stop = [c for c in containers if _is_container_running(c)]
    if to_stop:
        logger.info("Stopping container(s): %s", ", ".join(to_stop))
        subprocess.run(["docker", "stop", *to_stop], check=True)
    else:
        logger.info("No containers were running — nothing to stop.")
    return to_stop


def start_containers(containers: list[str]) -> None:
    """Start the given containers.

    Intended to be called with the list returned by :func:`stop_containers`
    so that only previously running containers are brought back up.

    Uses ``docker start`` directly (not ``docker compose``) for the same
    reasons as :func:`stop_containers`.

    Args:
        containers: Container names to start.  Does nothing when the list is empty.
    """
    if not containers:
        logger.info("No containers to restart.")
        return
    logger.info("Starting container(s): %s", ", ".join(containers))
    subprocess.run(["docker", "start", *containers], check=True)


# ── Archive discovery ─────────────────────────────────────────────────────────

def available_timestamps(backup_dir: Path) -> list[str]:
    """Return all backup timestamps found in *backup_dir*, sorted ascending.

    Archives follow the naming convention ``{name}_{YYYYMMDD_HHMMSS}.zip``;
    the timestamp is always the last 15 characters of the stem.

    Returns an empty list when *backup_dir* does not exist or contains no archives.
    """
    if not backup_dir.is_dir():
        return []
    return sorted({p.stem[-15:] for p in backup_dir.glob("*_????????_??????.zip")})
