"""Dagster asset for DuckLake catalog maintenance.

Runs three cleanup steps against the DuckLake catalog:

1. ``ducklake_expire_snapshots`` — marks snapshots older than
   :data:`SNAPSHOT_RETENTION_DAYS` days as expired so their data files become
   eligible for deletion.  Recent snapshots are retained for time-travel.
2. ``ducklake_delete_orphaned_files`` — removes Parquet files that are no
   longer referenced by any live snapshot.
3. Filesystem sweep — deletes any residual ``*_current_temp`` and
   ``*__dbt_tmp`` directories created by dbt's incremental-append strategy
   or the Silver pre/post-hooks.  These are invisible to DuckLake's vacuum
   because dbt creates and drops them via plain DuckDB DDL, not through the
   DuckLake catalog.

The asset is a no-op (skips with a log message) when
``SILVER_STORAGE_FORMAT`` is not set to ``ducklake``.
"""

import shutil
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, MetadataValue, asset

import duckdb
from ddd_python.ddd_utils import get_variables_from_env

_STOP_METABASE_KEY = AssetKey(["stop_metabase_asset"])

# Snapshots older than this many days are expired (their data files then become
# eligible for orphan deletion).  More recent snapshots are kept so DuckLake
# time-travel queries remain available for that window.
SNAPSHOT_RETENTION_DAYS = 31


@asset(
    name="ducklake_cleanup_asset",
    group_name="maintenance",
    description=(
        "Vacuums the DuckLake catalog: expires old snapshots, deletes "
        "orphaned Parquet files, and removes residual _current_temp / "
        "__dbt_tmp directories left by dbt's incremental strategy."
    ),
    deps=[_STOP_METABASE_KEY],
)
def ducklake_cleanup_asset(context: AssetExecutionContext) -> MaterializeResult:
    silver_format = getattr(get_variables_from_env, "SILVER_STORAGE_FORMAT", "duckdb")
    if silver_format != "ducklake":
        context.log.info("SILVER_STORAGE_FORMAT=%s — DuckLake cleanup skipped.", silver_format)
        return MaterializeResult(metadata={"skipped": MetadataValue.bool(True)})

    catalog_location = get_variables_from_env.DUCKLAKE_CATALOG_LOCATION
    data_path = get_variables_from_env.DUCKLAKE_DATA_PATH
    db_location = get_variables_from_env.DUCKDB_DATABASE_LOCATION
    assert catalog_location is not None, "DUCKLAKE_CATALOG_LOCATION must be set"
    assert data_path is not None, "DUCKLAKE_DATA_PATH must be set"
    assert db_location is not None, "DUCKDB_DATABASE_LOCATION must be set"

    context.log.info("Connecting to DuckDB at %s", db_location)
    con = duckdb.connect(db_location)
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(
        f"ATTACH 'ducklake:{catalog_location}' AS ducklake_catalog (DATA_PATH '{data_path}')"
    )

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

    con.close()

    # Remove residual _current_temp directories left by dbt's Silver pre/post-hooks.
    # These are created during --full-refresh, dropped by the post-hook, and their
    # Parquet files become orphaned in the catalog (confirmed by ducklake_delete_orphaned_files).
    # __dbt_tmp directories are intentionally excluded: DuckLake stores live table data
    # there (dbt's incremental-append strategy writes staging files into __dbt_tmp/ and
    # the main table's catalog snapshot references them directly). Deleting __dbt_tmp
    # would corrupt the Silver tables.
    data_root = Path(data_path)
    orphan_dirs: list[Path] = []
    if data_root.exists():
        for entry in data_root.rglob("*"):
            if entry.is_dir() and entry.name.endswith("_current_temp"):
                orphan_dirs.append(entry)

    for d in orphan_dirs:
        context.log.info("Removing orphaned directory: %s", d)
        shutil.rmtree(d, ignore_errors=True)

    # Ensure Metabase (UID 2000) can read new Parquet files written by the
    # pipeline container (UID 1000). New dbt runs create files with 0o644 /
    # dirs with 0o755 by default, which already grants "other" read access,
    # but we enforce it explicitly so the DuckLake data is always readable.
    if data_root.exists():
        for entry in data_root.rglob("*"):
            try:
                mode = entry.stat().st_mode
                if entry.is_dir():
                    entry.chmod(mode | 0o005)  # o+rx
                else:
                    entry.chmod(mode | 0o004)  # o+r
            except OSError:
                pass

    context.log.info(
        "DuckLake cleanup complete — %d snapshot(s) expired, "
        "%d catalog file(s) deleted, %d orphaned director(ies) removed.",
        len(expired),
        len(deleted_catalog),
        len(orphan_dirs),
    )

    return MaterializeResult(
        metadata={
            "snapshots_expired": MetadataValue.int(len(expired)),
            "catalog_files_deleted": MetadataValue.int(len(deleted_catalog)),
            "orphan_dirs_removed": MetadataValue.int(len(orphan_dirs)),
        }
    )
