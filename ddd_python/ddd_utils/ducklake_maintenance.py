"""DuckLake data-directory maintenance that has to run where the Parquet files are.

Earlier versions of dbt's Silver hooks left ``*_current_temp`` directories in the DuckLake
data path; the current macros no longer create them. In Docker only ``duckdb_server`` mounts that path, so the sweep runs
inside the server process and is exposed to clients as a SQL function
(:data:`SWEEP_FUNCTION_NAME`), which the DuckLake cleanup asset calls over quack.

``__dbt_tmp`` directories are never touched: DuckLake stores live table data there
(the Silver incremental-append strategy writes its staging files into ``__dbt_tmp/``
and the table's catalog snapshot references them directly).
"""

import logging
import shutil
import time
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

SWEEP_FUNCTION_NAME = "ducklake_sweep_current_temp"

# A directory is only removed once nothing inside it changed for this long, so a dbt
# run that is still using its ``_current_temp`` table is never swept (dbt steps can
# run in parallel).
CURRENT_TEMP_MIN_AGE_MINUTES = 60


def _newest_mtime(directory: Path) -> float:
    """Most recent modification time of the directory and everything below it."""
    newest = directory.stat().st_mtime
    for entry in directory.rglob("*"):
        newest = max(newest, entry.stat().st_mtime)
    return newest


def sweep_current_temp_dirs(data_root: Path, min_age_minutes: int = 0) -> int:
    """Remove ``*_current_temp`` directories under *data_root*; return how many.

    Only directories whose newest file is at least *min_age_minutes* old are removed.
    """
    if not data_root.exists():
        return 0
    cutoff = time.time() - min_age_minutes * 60
    removed = 0
    for entry in sorted(data_root.rglob("*_current_temp")):
        if not entry.is_dir():
            continue
        try:
            if _newest_mtime(entry) > cutoff:
                continue
        except FileNotFoundError:  # removed by a parallel sweep or a dbt hook
            continue
        logger.info("Removing orphaned directory: %s", entry)
        shutil.rmtree(entry, ignore_errors=True)
        removed += 1
    return removed


def register_sweep_function(connection: duckdb.DuckDBPyConnection, data_path: str) -> None:
    """Expose the sweep as ``ducklake_sweep_current_temp(min_age_minutes)`` in SQL.

    Register it on the connection that serves quack, before ``quack_serve``; client
    sessions (``CONNECT``) can then call it. DuckDB 2.0 needs numpy for Python UDFs.
    """
    root = Path(data_path)

    def _sweep(min_age_minutes: int) -> int:
        return sweep_current_temp_dirs(root, min_age_minutes)

    connection.create_function(
        SWEEP_FUNCTION_NAME, _sweep, ["INTEGER"], "INTEGER", side_effects=True
    )
