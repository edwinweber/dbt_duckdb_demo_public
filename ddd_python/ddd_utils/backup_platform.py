"""Backup the DDD platform databases.

For each selected target the script:

1. Determines which Docker containers are running.
2. Stops only the containers relevant to the selected targets (graceful SIGTERM).
3. Waits briefly for databases to flush to disk (``FLUSH_WAIT_SECONDS``, default 30 s).
4. Creates a verified, deflate-compressed zip archive per target.
5. Optionally uploads the archive to a Hetzner StorageBox via rsync/SSH.
6. Prunes local archives older than 2 months.
7. Prunes remote StorageBox archives to a daily+monthly rotation (31 + 12).
8. Restarts only the containers that were stopped in step 2.

Backup directories (overridable via environment variables):
    /data_backup/dagster   — Dagster home directory
    /data_backup/metabase  — Metabase data directory
    /data_backup/ducklake  — DuckLake Parquet data files (DuckLake mode only)
    /data_backup/duckdb    — DuckDB database directory (incl. the DuckLake catalog)

DuckLake mode (``SILVER_STORAGE_FORMAT=ducklake``):
    The Silver layer is stored as DuckLake-managed Parquet files, so the backup
    additionally captures the DuckLake **data files** (``ducklake`` target).
    The DuckLake **catalog** (``.ducklake`` file) lives in the DuckDB directory
    and is captured by the ``duckdb`` target.  Targets run in the order
    ``… → ducklake → duckdb`` so the data files are archived **before** the
    catalog — the catalog references the files, so files-first guarantees the
    catalog snapshot never points at a file the backup missed.  The ``ducklake``
    target is omitted entirely in native ``duckdb`` mode.

Log files are written to /data_backup/logs/ as NDJSON.  Query all runs:

    SELECT * FROM read_json_auto('/data_backup/logs/backup_log_*.ndjson');

Usage:
    python -m ddd_python.ddd_utils.backup_platform                 # all targets
    python -m ddd_python.ddd_utils.backup_platform --targets dagster
    python -m ddd_python.ddd_utils.backup_platform --targets metabase
    python -m ddd_python.ddd_utils.backup_platform --targets duckdb
    python -m ddd_python.ddd_utils.backup_platform --targets dagster metabase

Cron (daily at 02:00 UTC — see scripts/backup_platform.sh):
    0 2 * * * /path/to/repo/scripts/backup_platform.sh >> /data_backup/logs/cron.log 2>&1

Environment variables:
    ENVIRONMENT                   DEV | PROD (default: PROD); StorageBox subdirectory
    FLUSH_WAIT_SECONDS            Seconds to wait after stopping containers (default: 30)
    DAGSTER_BACKUP_DIR            Override for /data_backup/dagster
    METABASE_BACKUP_DIR           Override for /data_backup/metabase
    DUCKDB_BACKUP_DIR             Override for /data_backup/duckdb
    DUCKLAKE_BACKUP_DIR           Override for /data_backup/ducklake
    BACKUP_LOG_DIR                Override for /data_backup/logs
    DAGSTER_HOME                  Override for Dagster home directory
    METABASE_DATA_DIR             Override for Metabase data directory
    DUCKDB_DATABASE_LOCATION      Override for DuckDB file path (parent dir is backed up)
    SILVER_STORAGE_FORMAT         duckdb | ducklake — ducklake adds the ducklake target
    DUCKLAKE_DATA_PATH            DuckLake data directory backed up by the ducklake target
    HETZNER_STORAGEBOX_HOST       StorageBox hostname  (upload skipped when absent)
    HETZNER_STORAGEBOX_USER       StorageBox SSH user
    HETZNER_STORAGEBOX_REMOTE_DIR Base remote path; archives go to <base>/<env>/
    HETZNER_STORAGEBOX_PORT       SSH port (default: 23)
    HETZNER_STORAGEBOX_SSH_KEY    Path to private key (uses SSH agent when absent)
    HETZNER_STORAGEBOX_KEEP_DAILY    Remote daily archives to keep (default: 31; 0 disables)
    HETZNER_STORAGEBOX_KEEP_MONTHLY  Remote monthly archives to keep (default: 12; 0 disables)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import NamedTuple, TypedDict

from ddd_python.ddd_utils.backup_common import (
    BACKUP_LOG_DIR,
    BACKUP_TARGETS,
    TARGET_NAMES,
    BackupTarget,
    start_containers,
    stop_containers,
)

logger = logging.getLogger(__name__)


# ── Constants ─────────────────────────────────────────────────────────────────

_VALID_ENVIRONMENTS: frozenset[str] = frozenset({"DEV", "PROD"})

# Seconds to wait after containers stop so databases can flush WAL to disk.
# Set to 0 in tests via the FLUSH_WAIT_SECONDS env var.
_FLUSH_WAIT_SECONDS: int = max(0, int(os.environ.get("FLUSH_WAIT_SECONDS", "30")))


# ── Hetzner StorageBox ────────────────────────────────────────────────────────

_HETZNER_HOST: str | None = os.environ.get("HETZNER_STORAGEBOX_HOST")
_HETZNER_USER: str | None = os.environ.get("HETZNER_STORAGEBOX_USER")
_HETZNER_PORT: int = int(
    os.environ.get("HETZNER_STORAGEBOX_PORT", "23")
)  # 23 is Hetzner's documented SSH port for StorageBox
_HETZNER_REMOTE_BASE: str | None = os.environ.get("HETZNER_STORAGEBOX_REMOTE_DIR")
_HETZNER_SSH_KEY: str | None = os.environ.get("HETZNER_STORAGEBOX_SSH_KEY")

# Remote (StorageBox) retention. Unlike the local purge, the StorageBox keeps a
# grandfather-father-son rotation: the most recent N daily archives plus the most
# recent M monthly archives (one representative per day/month). Set either to 0
# to disable that tier; both 0 disables remote pruning entirely (keep forever).
_STORAGEBOX_KEEP_DAILY: int = max(0, int(os.environ.get("HETZNER_STORAGEBOX_KEEP_DAILY", "31")))
_STORAGEBOX_KEEP_MONTHLY: int = max(0, int(os.environ.get("HETZNER_STORAGEBOX_KEEP_MONTHLY", "12")))


# ── Typed structures ──────────────────────────────────────────────────────────


class _ArchiveResult(NamedTuple):
    """Result of a single archive creation."""

    path: Path
    """Path to the created zip file."""

    skipped: list[str]
    """Archive-relative paths of files that could not be read (permission errors)."""


class _LogRecord(TypedDict):
    """Schema for one NDJSON log entry."""

    run_id: str
    run_started_at: str
    logged_at: str
    environment: str
    target: str
    source_path: str
    backup_dir: str
    archive_name: str | None
    archive_size_mb: float | None
    archive_verified: bool
    uploaded_to_storagebox: bool
    skipped_files: list[str]
    status: str  # "success" | "error"
    error_message: str | None
    duration_seconds: float | None


# ── Archive operations ────────────────────────────────────────────────────────


def _create_archive(target: BackupTarget, timestamp: str) -> _ArchiveResult:
    """Create a deflate-compressed zip archive of *target.source*.

    Archive name: ``{target.name}_{YYYYMMDD_HHMMSS}.zip``.
    Entries are stored as ``{source.name}/...`` so that extracting to
    ``source.parent`` reconstructs the original directory tree.

    Files that cannot be read due to a ``PermissionError`` are skipped with a
    ``WARNING`` and listed in :attr:`_ArchiveResult.skipped`.

    Raises:
        FileNotFoundError: When ``target.source`` does not exist.
    """
    if not target.source.exists():
        raise FileNotFoundError(f"Source path not found: {target.source}")

    target.backup_dir.mkdir(parents=True, exist_ok=True)
    archive_path = target.backup_dir / f"{target.name}_{timestamp}.zip"
    skipped: list[str] = []

    logger.info("Archiving %s → %s", target.source, archive_path.name)
    with zipfile.ZipFile(
        archive_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
    ) as zf:
        for file_path in sorted(target.source.rglob("*")):
            arcname = file_path.relative_to(target.source.parent)
            if file_path.is_dir():
                # Preserve empty directories as a trailing-slash entry.
                zf.writestr(zipfile.ZipInfo(str(arcname) + "/"), "")
            else:
                try:
                    zf.write(file_path, arcname)
                except PermissionError:
                    logger.warning("Skipping unreadable file: %s", file_path)
                    skipped.append(str(arcname))

    if skipped:
        logger.warning(
            "%d file(s) skipped due to permissions: %s",
            len(skipped),
            ", ".join(skipped),
        )

    size_mb = archive_path.stat().st_size / 1_048_576
    logger.info("Archive size: %.1f MB", size_mb)
    return _ArchiveResult(path=archive_path, skipped=skipped)


def _verify_archive(archive_path: Path) -> int:
    """Test every entry in the zip archive for CRC corruption.

    Args:
        archive_path: Path to the zip file to verify.

    Returns:
        Number of members verified.

    Raises:
        ValueError: On CRC error or empty archive.
    """
    with zipfile.ZipFile(archive_path, "r") as zf:
        bad = zf.testzip()
        if bad is not None:
            raise ValueError(f"Corrupt entry in archive: {bad}")
        count = len(zf.namelist())
    if count == 0:
        raise ValueError(f"Archive is empty: {archive_path.name}")
    logger.info("Verified — %d member(s) readable", count)
    return count


def _storagebox_ssh_args() -> list[str]:
    """Build the shared ``ssh`` option list for StorageBox connections."""
    args = [
        "-p",
        str(_HETZNER_PORT),
        "-o",
        "StrictHostKeyChecking=accept-new",
    ]
    if _HETZNER_SSH_KEY:
        args += ["-i", _HETZNER_SSH_KEY]
    return args


def _upload_to_storagebox(archive_path: Path, environment: str) -> bool:
    """Upload *archive_path* to the environment-specific StorageBox subdirectory.

    Uses rsync over SSH so interrupted transfers are resumable.

    Returns:
        ``True`` when the upload succeeded; ``False`` when StorageBox
        credentials are not configured (upload silently skipped).
    """
    if not _HETZNER_HOST or not _HETZNER_USER or not _HETZNER_REMOTE_BASE:
        logger.warning("StorageBox credentials not configured — skipping remote upload.")
        return False

    ssh_args = _storagebox_ssh_args()
    remote_dir = f"{_HETZNER_REMOTE_BASE}/{environment.lower()}"

    # Pre-create the remote directory tree. rsync only creates the final
    # destination dir when its parent already exists, and Hetzner's StorageBox
    # rsync does not reliably honour --mkpath. The StorageBox restricted shell
    # does support `mkdir -p`, so create the path explicitly first.
    subprocess.run(
        ["ssh"] + ssh_args + [f"{_HETZNER_USER}@{_HETZNER_HOST}", "mkdir", "-p", remote_dir],
        check=True,
    )

    remote = f"{_HETZNER_USER}@{_HETZNER_HOST}:{remote_dir}/"
    logger.info("Uploading %s → %s", archive_path.name, remote)
    subprocess.run(
        [
            "rsync",
            "--archive",
            "--compress",
            "--progress",
            "-e",
            " ".join(["ssh"] + ssh_args),
            str(archive_path),
            remote,
        ],
        check=True,
    )
    return True


def _select_retained_archives(
    archives: list[tuple[datetime, str]],
    keep_daily: int,
    keep_monthly: int,
) -> set[str]:
    """Return the set of archive names to keep under a daily+monthly rotation.

    For each calendar day and each calendar month the most recent archive is the
    representative for that period. The *keep_daily* most recent daily
    representatives and *keep_monthly* most recent monthly representatives are
    retained; everything else is eligible for deletion.
    """
    latest_per_day: dict[date, tuple[datetime, str]] = {}
    latest_per_month: dict[tuple[int, int], tuple[datetime, str]] = {}
    for dt, name in archives:
        day = dt.date()
        if day not in latest_per_day or dt > latest_per_day[day][0]:
            latest_per_day[day] = (dt, name)
        month = (dt.year, dt.month)
        if month not in latest_per_month or dt > latest_per_month[month][0]:
            latest_per_month[month] = (dt, name)

    keep: set[str] = set()
    for day in sorted(latest_per_day, reverse=True)[:keep_daily]:
        keep.add(latest_per_day[day][1])
    for month in sorted(latest_per_month, reverse=True)[:keep_monthly]:
        keep.add(latest_per_month[month][1])
    return keep


def _prune_storagebox(target_name: str, environment: str) -> None:
    """Apply daily+monthly retention to *target_name* archives on the StorageBox.

    Lists the remote directory, keeps the most recent
    ``_STORAGEBOX_KEEP_DAILY`` daily and ``_STORAGEBOX_KEEP_MONTHLY`` monthly
    archives, and deletes the rest. Never raises: a pruning failure is logged as
    a warning and does not fail the (already-completed) backup. A no-op when
    StorageBox credentials are absent or both retention counts are 0.
    """
    if not _HETZNER_HOST or not _HETZNER_USER or not _HETZNER_REMOTE_BASE:
        return
    if _STORAGEBOX_KEEP_DAILY == 0 and _STORAGEBOX_KEEP_MONTHLY == 0:
        return  # retention disabled — keep everything remotely

    remote_dir = f"{_HETZNER_REMOTE_BASE}/{environment.lower()}"
    host = f"{_HETZNER_USER}@{_HETZNER_HOST}"
    ssh = ["ssh"] + _storagebox_ssh_args()

    try:
        listing = subprocess.run(
            ssh + [host, "ls", remote_dir],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except subprocess.CalledProcessError as exc:
        logger.warning(
            "[%s] StorageBox prune: could not list %s (%s) — skipping.",
            target_name,
            remote_dir,
            exc.stderr.strip() if exc.stderr else exc,
        )
        return

    pattern = re.compile(rf"^{re.escape(target_name)}_(\d{{8}})_(\d{{6}})\.zip$")
    archives: list[tuple[datetime, str]] = []
    for token in listing.split():
        m = pattern.match(token)
        if not m:
            continue
        try:
            dt = datetime.strptime(f"{m.group(1)}_{m.group(2)}", "%Y%m%d_%H%M%S")
        except ValueError:
            continue
        archives.append((dt, token))

    if not archives:
        return

    keep = _select_retained_archives(archives, _STORAGEBOX_KEEP_DAILY, _STORAGEBOX_KEEP_MONTHLY)
    to_delete = sorted(name for _, name in archives if name not in keep)
    if not to_delete:
        logger.info(
            "[%s] StorageBox prune: nothing to remove (%d archive(s) kept).",
            target_name,
            len(keep),
        )
        return

    paths = [f"{remote_dir}/{name}" for name in to_delete]
    try:
        subprocess.run(ssh + [host, "rm"] + paths, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as exc:
        logger.warning(
            "[%s] StorageBox prune: rm failed (%s) — leaving archives in place.",
            target_name,
            exc.stderr.strip() if exc.stderr else exc,
        )
        return

    logger.info(
        "[%s] StorageBox prune: removed %d archive(s), kept %d.",
        target_name,
        len(to_delete),
        len(keep),
    )


def _write_log_record(log_file: Path, record: _LogRecord) -> None:
    """Append one JSON line to the run's NDJSON log file."""
    with log_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def _purge_old_archives(backup_dir: Path, max_age_days: int) -> None:
    """Delete archives older than *max_age_days* from *backup_dir*."""
    cutoff = datetime.now() - timedelta(days=max_age_days)
    expired = [
        f
        for f in backup_dir.glob("*_????????_??????.zip")
        if datetime.fromtimestamp(f.stat().st_mtime) < cutoff
    ]
    for f in expired:
        f.unlink()

    if expired:
        names = ", ".join(sorted(f.name for f in expired))
        logger.info("Purged %d expired archive(s): %s", len(expired), names)
    else:
        logger.info("No expired archives to purge in %s.", backup_dir)


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Backup DDD platform data directories to zip archives.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"Available targets: {', '.join(TARGET_NAMES)}",
    )
    parser.add_argument(
        "--targets",
        nargs="+",
        choices=list(TARGET_NAMES),
        default=list(TARGET_NAMES),
        metavar="TARGET",
        help=(
            f"One or more targets to back up (default: all). Choices: {{{', '.join(TARGET_NAMES)}}}"
        ),
    )
    args = parser.parse_args()

    environment = os.environ.get("ENVIRONMENT", "PROD").upper()
    if environment not in _VALID_ENVIRONMENTS:
        logger.error(
            "ENVIRONMENT=%r is invalid. Must be one of: %s",
            environment,
            sorted(_VALID_ENVIRONMENTS),
        )
        sys.exit(1)

    # Preserve canonical BACKUP_TARGETS ordering for the selected subset.
    requested: set[str] = set(args.targets)
    selected: list[BackupTarget] = [t for t in BACKUP_TARGETS if t.name in requested]

    # Collect unique containers while preserving declaration order.
    containers_to_stop: list[str] = list(dict.fromkeys(c for t in selected for c in t.containers))

    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    started_at = now.isoformat(timespec="seconds")

    BACKUP_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = BACKUP_LOG_DIR / f"backup_log_{timestamp}.ndjson"

    logger.info("=== DDD Platform Backup — %s (%s) ===", timestamp, environment)
    logger.info("Targets : %s", ", ".join(t.name for t in selected))
    logger.info("Log     : %s", log_file)

    actually_stopped = stop_containers(containers_to_stop)
    if actually_stopped and _FLUSH_WAIT_SECONDS > 0:
        logger.info("Waiting %d s for databases to flush to disk …", _FLUSH_WAIT_SECONDS)
        time.sleep(_FLUSH_WAIT_SECONDS)

    failed: list[str] = []
    try:
        for target in selected:
            logger.info("[%s] backup started", target.name)
            t0 = time.monotonic()
            record: _LogRecord = {
                "run_id": timestamp,
                "run_started_at": started_at,
                "logged_at": datetime.now().isoformat(timespec="seconds"),
                "environment": environment,
                "target": target.name,
                "source_path": str(target.source),
                "backup_dir": str(target.backup_dir),
                "archive_name": None,
                "archive_size_mb": None,
                "archive_verified": False,
                "uploaded_to_storagebox": False,
                "skipped_files": [],
                "status": "error",
                "error_message": None,
                "duration_seconds": None,
            }
            try:
                result = _create_archive(target, timestamp)
                record["archive_name"] = result.path.name
                record["archive_size_mb"] = round(result.path.stat().st_size / 1_048_576, 2)
                record["skipped_files"] = result.skipped
                _verify_archive(result.path)
                record["archive_verified"] = True
                record["uploaded_to_storagebox"] = _upload_to_storagebox(result.path, environment)
                record["status"] = "success"
                _purge_old_archives(target.backup_dir, target.max_archive_age_days)
                if record["uploaded_to_storagebox"]:
                    _prune_storagebox(target.name, environment)
            except Exception as exc:
                record["error_message"] = str(exc)
                failed.append(target.name)
                logger.error("[%s] backup failed: %s", target.name, exc)
            finally:
                record["duration_seconds"] = round(time.monotonic() - t0, 1)
                _write_log_record(log_file, record)

        if failed:
            logger.error("Backup finished with errors — failed targets: %s", ", ".join(failed))
        else:
            logger.info("Backup completed successfully — %d archive(s) created.", len(selected))
    finally:
        try:
            start_containers(actually_stopped)
        except Exception:
            logger.exception("Failed to restart container(s) — manual intervention required!")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
