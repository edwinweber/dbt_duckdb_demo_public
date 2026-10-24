"""Dagster asset for DuckLake catalog maintenance.

Runs three cleanup steps against the DuckLake catalog:

1. ``ducklake_expire_snapshots`` — marks snapshots older than
   :data:`SNAPSHOT_RETENTION_DAYS` days as expired so their data files become
   eligible for deletion.  Recent snapshots are retained for time-travel.
2. ``ducklake_delete_orphaned_files`` — removes Parquet files that are no
   longer referenced by any live snapshot.
3. Filesystem sweep — deletes any residual ``*_current_temp`` directories
   left by earlier versions of the Silver macros (the current macros do not
   create them).  They are invisible to DuckLake's vacuum because dbt created
   and dropped them via plain DuckDB DDL, not through the DuckLake catalog.  ``*__dbt_tmp`` directories are intentionally
   preserved: DuckLake stores live table data there (dbt's incremental-append
   strategy writes staging files into ``__dbt_tmp/`` and the main table's
   catalog snapshot references them directly).  Deleting ``__dbt_tmp``
   directories would corrupt the Silver tables.

   In Docker deployments the Dagster container does not mount the DuckLake data
   directory, so the sweep runs inside ``duckdb_server`` and is called as the SQL
   function ``ducklake_sweep_current_temp`` (see ``ddd_utils/ducklake_maintenance.py``).
   Only directories untouched for ``CURRENT_TEMP_MIN_AGE_MINUTES`` are removed, so a
   dbt run that is still using its temp table is left alone.

The asset is a no-op (skips with a log message) when
``SILVER_STORAGE_FORMAT`` is not set to ``ducklake``.
"""

from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from ddd_python.ddd_utils import get_variables_from_env
from ddd_python.ddd_utils.ducklake_maintenance import (
    CURRENT_TEMP_MIN_AGE_MINUTES,
    SWEEP_FUNCTION_NAME,
    sweep_current_temp_dirs,
)
from ddd_python.ddd_utils.path_utils import open_server_session, silver_storage_is_ducklake

# Snapshots older than this many days are expired (their data files then become
# eligible for orphan deletion).  More recent snapshots are kept so DuckLake
# time-travel queries remain available for that window.
SNAPSHOT_RETENTION_DAYS = 31


@asset(
    name="ducklake_cleanup_asset",
    group_name="maintenance",
    description=(
        "Vacuums the DuckLake catalog: expires old snapshots, deletes "
        "orphaned Parquet files, and removes residual _current_temp directories "
        "left by earlier Silver macro versions. __dbt_tmp directories are intentionally "
        "preserved because DuckLake stores live table data there."
    ),
)
def ducklake_cleanup_asset(context: AssetExecutionContext) -> MaterializeResult:
    if not silver_storage_is_ducklake():
        silver_format = getattr(get_variables_from_env, "SILVER_STORAGE_FORMAT", "duckdb")
        context.log.info("SILVER_STORAGE_FORMAT=%s — DuckLake cleanup skipped.", silver_format)
        return MaterializeResult(metadata={"skipped": MetadataValue.bool(True)})

    data_path = get_variables_from_env.DUCKLAKE_DATA_PATH
    assert data_path is not None, "DUCKLAKE_DATA_PATH must be set"

    context.log.info("Connecting to DuckDB (via quack server or direct file)")
    con = open_server_session()
    try:
        context.log.info("Expiring snapshots older than %d day(s)", SNAPSHOT_RETENTION_DAYS)
        expired = con.execute(
            "CALL ducklake_expire_snapshots("
            "'ducklake_catalog', "
            f"older_than=NOW() - INTERVAL '{SNAPSHOT_RETENTION_DAYS} days')"
        ).fetchall()
        context.log.info("Expired %d snapshot(s)", len(expired))

        context.log.info("Deleting orphaned files tracked by DuckLake catalog")
        deleted_catalog = con.execute(
            "CALL ducklake_delete_orphaned_files('ducklake_catalog')"
        ).fetchall()
        context.log.info("Deleted %d catalog-tracked orphaned file(s)", len(deleted_catalog))

        # Remove residual _current_temp directories left by earlier versions of the Silver hooks.
        # __dbt_tmp directories are excluded: DuckLake stores live table data there.
        if get_variables_from_env.QUACK_SERVER_HOST:
            # In Docker only duckdb_server mounts the data path, so the sweep runs there
            # (SQL function registered by the server, see ddd_utils/ducklake_maintenance.py).
            row = con.execute(
                f"SELECT {SWEEP_FUNCTION_NAME}({int(CURRENT_TEMP_MIN_AGE_MINUTES)})"
            ).fetchone()
            swept_dirs = int(row[0]) if row and row[0] is not None else 0
        else:
            swept_dirs = sweep_current_temp_dirs(Path(data_path), CURRENT_TEMP_MIN_AGE_MINUTES)
        context.log.info("Removed %d orphaned _current_temp director(ies)", swept_dirs)
    finally:
        con.close()

    orphan_dirs_removed = swept_dirs
    context.log.info(
        "DuckLake cleanup complete — %d snapshot(s) expired, "
        "%d catalog file(s) deleted, %d orphaned director(ies) removed.",
        len(expired),
        len(deleted_catalog),
        orphan_dirs_removed,
    )

    return MaterializeResult(
        metadata={
            "snapshots_expired": MetadataValue.int(len(expired)),
            "catalog_files_deleted": MetadataValue.int(len(deleted_catalog)),
            "orphan_dirs_removed": MetadataValue.int(orphan_dirs_removed),
        }
    )
