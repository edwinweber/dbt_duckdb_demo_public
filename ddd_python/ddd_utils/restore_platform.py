"""Restore DDD platform databases from a local backup archive.

The script resolves the archive to restore, confirms with the operator, stops
the relevant containers, extracts the zip archive in-place (overwriting existing
files), then restarts the containers.

When a target's data directory is owned by a UID other than the calling process
(e.g. Metabase runs as UID 2000), the extraction runs inside a Docker container
as that UID so that restored files are owned correctly.

Backup directories (overridable via environment variables):
    /data_backup/dagster   — Dagster home directory archives
    /data_backup/metabase  — Metabase data directory archives
    /data_backup/duckdb    — DuckDB database directory archives

Every restore run appends one NDJSON record per target to:
    /data_backup/logs/restore_log_{YYYYMMDD_HHMMSS}.ndjson

Query all restore runs with DuckDB:
    SELECT * FROM read_json_auto('/data_backup/logs/restore_log_*.ndjson')
    ORDER BY run_started_at DESC;

Usage:
    # Restore all targets from the most recent backup (default):
    python -m ddd_python.ddd_utils.restore_platform

    # Non-interactive (skip confirmation prompt):
    python -m ddd_python.ddd_utils.restore_platform --yes

    # Restore from a specific timestamp:
    python -m ddd_python.ddd_utils.restore_platform --timestamp 20260513_020000

    # Restore a single target:
    python -m ddd_python.ddd_utils.restore_platform --targets dagster
    python -m ddd_python.ddd_utils.restore_platform --targets metabase
    python -m ddd_python.ddd_utils.restore_platform --targets duckdb

    # Combine flags:
    python -m ddd_python.ddd_utils.restore_platform --targets dagster --timestamp 20260513_020000 --yes

Environment variables:
    DAGSTER_BACKUP_DIR        Override for /data_backup/dagster
    METABASE_BACKUP_DIR       Override for /data_backup/metabase
    DUCKDB_BACKUP_DIR         Override for /data_backup/duckdb
    DAGSTER_HOME              Override for Dagster home directory
    METABASE_DATA_DIR         Override for Metabase data directory
    DUCKDB_DATABASE_LOCATION  Override for DuckDB file path (parent dir is restored)
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import NamedTuple, TypedDict

from ddd_python.ddd_utils.backup_common import (
    BACKUP_TARGETS,
    RESTORE_DOCKER_IMAGE,
    TARGET_NAMES,
    BackupTarget,
    available_timestamps,
    start_containers,
    stop_containers,
)

logger = logging.getLogger(__name__)

# Fast lookup by name; built once at import time.
_TARGETS: dict[str, BackupTarget] = {t.name: t for t in BACKUP_TARGETS}


# ── Log record ────────────────────────────────────────────────────────────────

class _RestoreLogRecord(TypedDict):
    """Schema for one NDJSON log entry written per restored target."""

    run_id: str
    run_started_at: str
    logged_at: str
    target: str
    archive_name: str
    archive_size_mb: float
    restore_dest: str
    status: str           # "success" | "error"
    error_message: str | None
    duration_seconds: float | None


def _write_log_record(log_file: Path, record: _RestoreLogRecord) -> None:
    """Append one JSON line to the run's NDJSON log file."""
    with log_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


# ── Restore plan ──────────────────────────────────────────────────────────────

class _RestoreItem(NamedTuple):
    """One resolved entry in the restore plan."""

    target: BackupTarget
    archive: Path


# ── Restore operations ────────────────────────────────────────────────────────

def _resolve_timestamp(target: BackupTarget, requested: str | None) -> str:
    """Return the timestamp to restore for *target*.

    When *requested* is ``None`` the most recent available timestamp is used.

    Raises:
        FileNotFoundError: When no archives exist in ``target.backup_dir`` or
            *requested* is not among the available timestamps.
    """
    timestamps = available_timestamps(target.backup_dir)
    if not timestamps:
        raise FileNotFoundError(
            f"No backup archives found in {target.backup_dir}"
        )
    if requested is None:
        return timestamps[-1]
    if requested not in timestamps:
        raise FileNotFoundError(
            f"Timestamp {requested!r} not found for target '{target.name}'. "
            f"Available: {', '.join(timestamps)}"
        )
    return requested


def _build_restore_plan(
    targets: list[str], timestamp: str | None
) -> list[_RestoreItem]:
    """Resolve archive paths for all requested targets before touching anything.

    Raises:
        FileNotFoundError: When a required archive cannot be found.
    """
    plan: list[_RestoreItem] = []
    for name in targets:
        target = _TARGETS[name]
        ts      = _resolve_timestamp(target, timestamp)
        archive = target.backup_dir / f"{name}_{ts}.zip"
        plan.append(_RestoreItem(target=target, archive=archive))
        logger.info("[%s] will restore from %s", name, archive.name)
    return plan


def _restore_one(item: _RestoreItem) -> None:
    """Extract *item.archive* into ``item.target.source.parent``, overwriting files.

    Archives contain entries as ``{source.name}/...`` so extracting to
    ``source.parent`` reconstructs the original directory tree in-place.

    When ``item.target.restore_uid`` is set the extraction runs inside Docker
    as that UID so that restored files carry the correct ownership.
    """
    if not item.archive.exists():
        raise FileNotFoundError(f"Archive not found: {item.archive}")

    target  = item.target
    dest    = target.source.parent

    if target.restore_uid:
        # Data directory is owned by a different UID.  Run extraction inside
        # Docker with the correct user so ownership is preserved.
        logger.info(
            "Extracting %s via Docker (--user %s) → %s",
            item.archive.name,
            target.restore_uid,
            dest,
        )
        subprocess.run(
            [
                "docker", "run", "--rm",
                "--user", target.restore_uid,
                "--entrypoint", "python3",      # bypass the image's entrypoint script
                "-v", f"{item.archive.parent}:/backup:ro",
                "-v", f"{dest}:/target",
                RESTORE_DOCKER_IMAGE,
                "-c",
                (
                    "import zipfile; "
                    f"zipfile.ZipFile('/backup/{item.archive.name}').extractall('/target')"
                ),
            ],
            check=True,
        )
    else:
        logger.info("Extracting %s → %s (overwrite)", item.archive.name, dest)
        with zipfile.ZipFile(item.archive, "r") as zf:
            zf.extractall(dest)


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description=(
            "Restore DDD platform databases from a local backup archive.\n"
            "Defaults to the most recent backup when --timestamp is omitted."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available targets: {', '.join(TARGET_NAMES)}",
    )
    parser.add_argument(
        "--timestamp",
        metavar="YYYYMMDD_HHMMSS",
        default=None,
        help="Restore from a specific backup run (default: most recent).",
    )
    parser.add_argument(
        "--targets",
        nargs="+",
        choices=list(TARGET_NAMES),
        default=list(TARGET_NAMES),
        metavar="TARGET",
        help=(
            f"One or more targets to restore (default: all). "
            f"Choices: {{{', '.join(TARGET_NAMES)}}}"
        ),
    )
    parser.add_argument(
        "--yes", "-y",
        action="store_true",
        help="Skip the confirmation prompt (use in non-interactive / scripted contexts).",
    )
    args = parser.parse_args()

    # ── Resolve plan (fail fast, before touching any live data) ───────────────
    try:
        plan = _build_restore_plan(args.targets, args.timestamp)
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        sys.exit(1)

    # ── Confirmation ──────────────────────────────────────────────────────────
    logger.warning(
        "The following archives will overwrite live data:\n%s",
        "\n".join(f"  {item.target.name}: {item.archive.name}" for item in plan),
    )
    if not args.yes:
        try:
            answer = input("Type 'yes' to continue: ").strip().lower()
        except EOFError:
            answer = ""
        if answer != "yes":
            logger.info("Aborted.")
            sys.exit(0)

    # ── Log file setup ────────────────────────────────────────────────────────
    now        = datetime.now()
    timestamp  = now.strftime("%Y%m%d_%H%M%S")
    started_at = now.isoformat(timespec="seconds")

    from ddd_python.ddd_utils.backup_common import BACKUP_LOG_DIR
    BACKUP_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = BACKUP_LOG_DIR / f"restore_log_{timestamp}.ndjson"
    logger.info("Log: %s", log_file)

    # ── Container lifecycle ───────────────────────────────────────────────────
    containers_to_stop: list[str] = list(
        dict.fromkeys(c for item in plan for c in item.target.containers)
    )
    actually_stopped = stop_containers(containers_to_stop)

    # ── Restore ───────────────────────────────────────────────────────────────
    failed: list[str] = []
    try:
        for item in plan:
            logger.info("[%s] restoring …", item.target.name)
            t0 = time.monotonic()
            record: _RestoreLogRecord = {
                "run_id":           timestamp,
                "run_started_at":   started_at,
                "logged_at":        datetime.now().isoformat(timespec="seconds"),
                "target":           item.target.name,
                "archive_name":     item.archive.name,
                "archive_size_mb":  round(item.archive.stat().st_size / 1_048_576, 2),
                "restore_dest":     str(item.target.source),
                "status":           "error",
                "error_message":    None,
                "duration_seconds": None,
            }
            try:
                _restore_one(item)
                record["status"] = "success"
                logger.info("[%s] restored successfully.", item.target.name)
            except Exception as exc:
                record["error_message"] = str(exc)
                failed.append(item.target.name)
                logger.error("[%s] restore failed: %s", item.target.name, exc)
            finally:
                record["duration_seconds"] = round(time.monotonic() - t0, 1)
                _write_log_record(log_file, record)
    finally:
        try:
            start_containers(actually_stopped)
        except Exception:
            logger.exception(
                "Failed to restart container(s) — manual intervention required!"
            )

    if failed:
        logger.error(
            "Restore finished with errors — failed targets: %s", ", ".join(failed)
        )
        sys.exit(1)
    else:
        logger.info("Restore completed successfully.")


if __name__ == "__main__":
    main()
