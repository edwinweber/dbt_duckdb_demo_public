"""Move processed Bronze files to an archive directory.

Silver is the state: a Silver run only reads the Bronze files newer than its bookmark
(``<model>_last_file``) and compares them with Silver.  Processed files are therefore not
needed any more, but every Bronze view still opens all files in its folder, and DuckDB 2.0
reserves 32 MiB of its memory limit for each.  This script moves processed files out of the
folder.  It runs on demand, from the command line or as the Dagster job ``bronze_archive_job``;
it is not part of the daily schedule.

What is archived, per entity
----------------------------
Every file older than the newest processed file (the bookmark) whose timestamp is older than
``--keep-days``.  The newest processed file always stays, so the Bronze views have a file to
read and the bookmark points at a file that exists.  Files that are not processed yet are never
touched, and neither are late files: older than the bookmark but not in the model's ``_files``
log, which the next Silver run reports (runbook: late Bronze files).

What it does
------------
The default is a **dry run** that prints the plan.  With ``--execute`` every file is copied
(optionally gzip-compressed with ``--compress``) to ``<archive-dir>/<source>/<entity>/``, read
back and checked against its SHA-256, listed in ``<archive-dir>/MANIFEST.ndjson`` and only then
removed from Bronze.  A file that is already archived with the same content is simply removed
from Bronze.  ``--restore`` copies archived files back, checked against the manifest, for
example to replay Silver from the beginning.

Stop the extraction and dbt jobs before ``--execute``; the script cannot see running jobs.

Usage
-----
    python -m ddd_python.ddd_utils.archive_bronze                     # plan, default BRONZE_ARCHIVE_DIR
    python -m ddd_python.ddd_utils.archive_bronze --keep-days 31 --compress --execute
    python -m ddd_python.ddd_utils.archive_bronze --archive-dir DIR --restore --entity sag

Local files only (no ``s3://``).  Silver is read through ``open_export_connection()``.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import re
import shutil
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
from ddd_python.ddd_utils import configuration_variables, get_variables_from_env
from ddd_python.ddd_utils.path_utils import open_export_connection, silver_storage_is_ducklake
from ddd_python.ddd_utils.string_utils import normalize_danish_name

logger = logging.getLogger(__name__)

DEFAULT_KEEP_DAYS = 7
MANIFEST = "MANIFEST.ndjson"
_TIMESTAMP = re.compile(r"(\d{8}_\d{6})")
_FULL_FILE = re.compile(r"\d{8}_\d{6}_full\.")
_CHUNK = 1024 * 1024


@dataclass(frozen=True)
class Entity:
    source: str  # "DDD" or "RFAM"
    name: str  # normalised name, also the Bronze directory name
    incremental: bool

    @property
    def silver_table(self) -> str:
        return f"silver_{self.source.lower()}_{self.name}"


def list_entities(sources: list[str]) -> list[Entity]:
    """All Bronze entities of the given source systems, from the central configuration."""
    entities: list[Entity] = []
    if "DDD" in sources:
        incremental = {
            normalize_danish_name(n)
            for n in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL
        }
        for name in configuration_variables.DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS:
            entities.append(Entity("DDD", name, name in incremental))
    if "RFAM" in sources:
        incremental = set(configuration_variables.RFAM_TABLE_NAMES_INCREMENTAL)
        for name in configuration_variables.RFAM_TABLE_PRIMARY_KEYS:
            entities.append(Entity("RFAM", name, name in incremental))
    return entities


def silver_database() -> str:
    """Database holding the Silver tables (same rule as ``export_silver``)."""
    db = get_variables_from_env.DUCKDB_DATABASE
    if not db:
        raise OSError("DUCKDB_DATABASE must be set")
    if silver_storage_is_ducklake():
        return (
            f"{db}.ducklake_catalog"
            if get_variables_from_env.QUACK_SERVER_HOST
            else "ducklake_catalog"
        )
    return db


@dataclass
class Plan:
    entity: Entity
    files: int
    newest_processed: str | None = None
    archive: list[str] = field(default_factory=list)
    reason: str = ""


def file_timestamp(name: str) -> datetime:
    """Timestamp in a Bronze file name, as ``parse_filename_ts`` reads it (UTC)."""
    match = _TIMESTAMP.search(name)
    if not match:
        raise ValueError(f"No YYYYMMDD_HHMMSS timestamp in file name {name!r}")
    return datetime.strptime(match.group(1), "%Y%m%d_%H%M%S").replace(tzinfo=UTC)


def bronze_files(directory: Path, entity: Entity) -> list[Path]:
    return sorted(directory.glob(f"{entity.name}_*.json*"), key=lambda p: p.name)


def read_bookmark(silver: duckdb.DuckDBPyConnection, database: str, entity: Entity) -> str | None:
    """The newest processed Bronze file, from the model's ``_last_file`` table."""
    relation = f'{database}.main_silver."{entity.silver_table}_last_file"'
    try:
        row = silver.execute(f"SELECT MAX(LKHS_filename) FROM {relation}").fetchone()
    except duckdb.CatalogException:
        return None
    return row[0] if row and row[0] else None


def read_logged_files(
    silver: duckdb.DuckDBPyConnection, database: str, entity: Entity
) -> set[str] | None:
    """Files in the model's ``_files`` log (processed, seeded or skipped); None without a log."""
    relation = f'{database}.main_silver."{entity.silver_table}_files"'
    try:
        rows = silver.execute(
            f"SELECT LKHS_filename FROM {relation} WHERE LKHS_status != 'pending'"
        ).fetchall()
    except duckdb.CatalogException:
        return None
    return {row[0] for row in rows}


def is_full_file(name: str) -> bool:
    """True for a full extraction of an incremental entity (``..._<timestamp>_full.json``)."""
    return _FULL_FILE.search(name) is not None


def plan_entity(
    entity: Entity,
    files: list[Path],
    newest_processed: str | None,
    keep_days: int,
    now: datetime,
    logged: set[str] | None = None,
) -> Plan:
    """Decide which files may be archived; with a ``_files`` log (``logged``) late files stay."""
    plan = Plan(entity, len(files), newest_processed)
    if not files:
        plan.reason = "no files"
        return plan
    if newest_processed is None:
        plan.reason = "Silver has not processed this entity yet (no _last_file bookmark)"
        return plan
    if newest_processed not in {f.name for f in files}:
        plan.reason = (
            f"the newest processed file {newest_processed} is not in Bronze; refusing to guess"
        )
        return plan
    boundary = now - timedelta(days=keep_days)
    candidates = [
        f.name for f in files if f.name < newest_processed and file_timestamp(f.name) <= boundary
    ]
    late = [name for name in candidates if logged is not None and name not in logged]
    plan.archive = [name for name in candidates if name not in late]
    plan.reason = "ok" if plan.archive else f"nothing processed and older than {keep_days} days"
    if late:
        plan.reason += f"; {len(late)} late file(s) kept, not in the _files log: {', '.join(late)}"
    return plan


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def archive_file(
    source: Path, target_dir: Path, entity: Entity, compress: bool, manifest: Path
) -> None:
    """Copy, verify and list one file in the manifest, then remove it from Bronze."""
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / (source.name + (".gz" if compress else ""))
    digest = _sha256(source)
    if target.exists():
        if _sha256(target) != digest:
            raise RuntimeError(f"{target} exists with different content; refusing to overwrite")
    else:
        partial = target.with_name(target.name + ".part")
        with source.open("rb") as src, (gzip.open if compress else open)(partial, "wb") as dst:
            shutil.copyfileobj(src, dst, _CHUNK)
        partial.rename(target)
        if _sha256(target) != digest:
            target.unlink()
            raise RuntimeError(f"Copy of {source.name} does not match the original")
        with manifest.open("a", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "source": entity.source,
                        "entity": entity.name,
                        "file": source.name,
                        "archived_as": target.name,
                        "sha256": digest,
                        "bytes": source.stat().st_size,
                        "archived_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    }
                )
                + "\n"
            )
    source.unlink()


def restore_entity(entity: Entity, archive_dir: Path, bronze_dir: Path) -> int:
    """Copy the archived files of an entity back to Bronze; returns the number restored."""
    manifest = archive_dir / MANIFEST
    if not manifest.exists():
        return 0
    records = [json.loads(line) for line in manifest.read_text(encoding="utf-8").splitlines()]
    restored = 0
    bronze_dir.mkdir(parents=True, exist_ok=True)
    for record in records:
        if record["source"] != entity.source or record["entity"] != entity.name:
            continue
        archived = archive_dir / entity.source / entity.name / record["archived_as"]
        if _sha256(archived) != record["sha256"]:
            raise RuntimeError(f"{archived} does not match its manifest checksum")
        target = bronze_dir / record["file"]
        if target.exists():
            if _sha256(target) != record["sha256"]:
                raise RuntimeError(f"{target} exists with different content; refusing to overwrite")
            continue
        with (
            (gzip.open if archived.suffix == ".gz" else open)(archived, "rb") as src,
            target.open("wb") as dst,
        ):
            shutil.copyfileobj(src, dst, _CHUNK)
        restored += 1
    return restored


def run_archive(
    directories: dict[str, Path],
    archive_dir: Path,
    keep_days: int = DEFAULT_KEEP_DAYS,
    compress: bool = False,
    execute: bool = False,
    entity_names: list[str] | None = None,
) -> tuple[list[Plan], int]:
    """Plan (and with ``execute`` carry out) the archiving of every entity of the given source
    directories; returns the plans and the number of files moved."""
    entities = [
        e for e in list_entities(list(directories)) if not entity_names or e.name in entity_names
    ]
    silver = open_export_connection()
    database = silver_database()
    plans: list[Plan] = []
    moved = 0
    try:
        for entity in entities:
            bronze_dir = directories[entity.source] / entity.name
            plan = plan_entity(
                entity,
                bronze_files(bronze_dir, entity),
                read_bookmark(silver, database, entity),
                keep_days,
                datetime.now(UTC),
                read_logged_files(silver, database, entity),
            )
            plans.append(plan)
            if execute:
                target_dir = archive_dir / entity.source / entity.name
                for name in plan.archive:
                    archive_file(
                        bronze_dir / name, target_dir, entity, compress, archive_dir / MANIFEST
                    )
                    moved += 1
    finally:
        silver.close()
    return plans, moved


def local_bronze_directories(sources: list[str]) -> dict[str, Path]:
    """The local Bronze directory per source system; raises for S3 or a missing setting."""
    configured = {
        "DDD": get_variables_from_env.DANISH_DEMOCRACY_DATA_SOURCE,
        "RFAM": get_variables_from_env.RFAM_DATA_SOURCE,
    }
    directories: dict[str, Path] = {}
    for source in sources:
        directory = configured[source]
        if not directory or str(directory).startswith("s3://"):
            raise ValueError(f"{source}: a local Bronze directory is required (got {directory!r})")
        directories[source] = Path(directory)
    return directories


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="archive_bronze",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--archive-dir", type=Path, help="Archive directory (default BRONZE_ARCHIVE_DIR)."
    )
    parser.add_argument("--source", choices=["DDD", "RFAM", "all"], default="all")
    parser.add_argument(
        "--entity", action="append", help="Only this normalised entity (repeatable)."
    )
    parser.add_argument(
        "--ddd-dir", type=Path, help="Bronze DDD directory (default DANISH_DEMOCRACY_DATA_SOURCE)."
    )
    parser.add_argument(
        "--rfam-dir", type=Path, help="Bronze RFAM directory (default RFAM_DATA_SOURCE)."
    )
    parser.add_argument(
        "--keep-days",
        type=int,
        default=DEFAULT_KEEP_DAYS,
        help="Never archive files younger than this.",
    )
    parser.add_argument("--compress", action="store_true", help="gzip the archived files.")
    parser.add_argument(
        "--execute", action="store_true", help="Move the files (default: print the plan only)."
    )
    parser.add_argument(
        "--restore", action="store_true", help="Copy archived files back to Bronze."
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sources = ["DDD", "RFAM"] if args.source == "all" else [args.source]
    try:
        directories = local_bronze_directories(sources)
    except ValueError as exc:
        logger.error("%s", exc)
        sys.exit(1)
    if args.ddd_dir and "DDD" in directories:
        directories["DDD"] = args.ddd_dir
    if args.rfam_dir and "RFAM" in directories:
        directories["RFAM"] = args.rfam_dir
    archive_dir = args.archive_dir or Path(get_variables_from_env.BRONZE_ARCHIVE_DIR)

    if args.restore:
        for entity in list_entities(sources):
            if args.entity and entity.name not in args.entity:
                continue
            count = restore_entity(entity, archive_dir, directories[entity.source] / entity.name)
            logger.info("%s/%s: %d file(s) restored", entity.source, entity.name, count)
        return

    plans, moved = run_archive(
        directories, archive_dir, args.keep_days, args.compress, args.execute, args.entity
    )
    for plan in plans:
        logger.info(
            "%s/%-22s %4d files: archive %4d, keep from %s (%s)",
            plan.entity.source,
            plan.entity.name,
            plan.files,
            len(plan.archive),
            plan.newest_processed or "-",
            plan.reason,
        )
    if args.execute:
        logger.info("%d file(s) moved to %s", moved, archive_dir)
    else:
        logger.info("Dry run: nothing moved (add --execute)")


if __name__ == "__main__":
    main()
