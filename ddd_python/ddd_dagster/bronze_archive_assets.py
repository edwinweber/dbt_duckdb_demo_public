"""Dagster asset that archives processed Bronze files, on demand.

Silver is the state and only reads the Bronze files newer than its bookmark, so processed
files can leave the Bronze folders.  That keeps the Bronze views small: each file a view opens
costs DuckDB 2.0 a 32 MiB memory reservation.  The asset wraps
:func:`ddd_python.ddd_utils.archive_bronze.run_archive`; the rule (keep the newest processed
file and everything younger than ``keep_days``) is described there.

There is no schedule: run ``bronze_archive_job`` from the Dagster UI when Bronze has grown.
Only local Bronze directories are archived; with ``RAW_STORAGE_TARGET=s3`` the asset logs and
does nothing.
"""

from pathlib import Path

from dagster import AssetExecutionContext, Config, MaterializeResult, MetadataValue, asset

from ddd_python.ddd_utils import get_variables_from_env
from ddd_python.ddd_utils.archive_bronze import (
    DEFAULT_KEEP_DAYS,
    local_bronze_directories,
    run_archive,
)


class BronzeArchiveConfig(Config):
    """Launchpad settings of ``bronze_archive_job``."""

    keep_days: int = DEFAULT_KEEP_DAYS
    compress: bool = True
    dry_run: bool = False


@asset(
    name="bronze_archive_asset",
    group_name="maintenance",
    description=(
        "Moves processed Bronze files to BRONZE_ARCHIVE_DIR (checksummed, listed in "
        "MANIFEST.ndjson). Keeps the newest processed file of every entity and every file "
        "younger than keep_days. Run on demand; no-op with RAW_STORAGE_TARGET=s3."
    ),
)
def bronze_archive_asset(
    context: AssetExecutionContext, config: BronzeArchiveConfig
) -> MaterializeResult:
    if get_variables_from_env.RAW_STORAGE_TARGET != "local":
        context.log.info(
            "RAW_STORAGE_TARGET=%s — Bronze archiving skipped.",
            get_variables_from_env.RAW_STORAGE_TARGET,
        )
        return MaterializeResult(metadata={"skipped": True})
    archive_dir = Path(get_variables_from_env.BRONZE_ARCHIVE_DIR)
    plans, moved = run_archive(
        local_bronze_directories(["DDD", "RFAM"]),
        archive_dir,
        keep_days=config.keep_days,
        compress=config.compress,
        execute=not config.dry_run,
    )
    for plan in plans:
        context.log.info(
            "%s/%s: %d file(s), %d to archive (%s)",
            plan.entity.source,
            plan.entity.name,
            plan.files,
            len(plan.archive),
            plan.reason,
        )
    return MaterializeResult(
        metadata={
            "files_moved": moved,
            "files_planned": sum(len(p.archive) for p in plans),
            "dry_run": config.dry_run,
            "archive_dir": MetadataValue.path(str(archive_dir)),
        }
    )
