"""Export Gold-layer tables from DuckDB to the configured Delta Lake destination.

Full overwrite: reads each Gold view (which reference Silver via ``ducklake_catalog``
in DuckLake mode), converts to PyArrow, and overwrites the Delta table. Connection
comes from ``open_export_connection()`` which attaches the DuckLake catalog if needed.
"""

import argparse
import logging

from deltalake.writer import write_deltalake

import duckdb
from ddd_python.ddd_utils import configuration_variables, get_variables_from_env
from ddd_python.ddd_utils.path_utils import build_delta_export_path, open_export_connection

logger = logging.getLogger(__name__)


def export_single_gold_table(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    """Export one Gold table from DuckDB to the configured Delta Lake destination.

    Gold tables are always fully overwritten (``mode="overwrite"``).

    Returns:
        Number of rows written.
    """
    target_table_path, storage_options = build_delta_export_path("gold", table)
    db = get_variables_from_env.DUCKDB_DATABASE
    if db is None:
        raise OSError("DUCKDB_DATABASE must be set")
    query = f"SELECT * FROM {db}.main_gold.{table}"
    result = connection.execute(query)
    df = result.to_arrow_table()
    write_deltalake(
        target_table_path,
        df,
        mode="overwrite",
        schema_mode="merge",
        storage_options=storage_options,
    )
    logger.info("Replaced Gold Delta-table %s — %d rows written.", table, df.num_rows)
    return int(df.num_rows)


def write_tables_to_delta_gold(connection: duckdb.DuckDBPyConnection, tables: list[str]) -> None:
    """Exports Gold tables as Delta Lake tables to the configured destination (local filesystem, S3-compatible storage, or Fabric OneLake).

    Convenience wrapper that calls :func:`export_single_gold_table` for each
    table in *tables*.

    Raises:
        RuntimeError: If one or more tables failed to export, after attempting
            all tables.  Partial failures are logged at ERROR level.
    """
    failed: list[str] = []
    for table in tables:
        try:
            export_single_gold_table(connection, table)
        except Exception as e:
            logger.error("Failed to export Gold table %s: %s", table, e, exc_info=True)
            failed.append(table)

    if failed:
        raise RuntimeError(f"Gold export failed for {len(failed)} table(s): {', '.join(failed)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export Gold tables to the configured Delta Lake destination (local, S3, or OneLake)."
    )
    parser.add_argument(
        "--tables", nargs="+", required=False, help="Tables to export (default: all Gold tables)."
    )
    args = parser.parse_args()

    tables = args.tables or configuration_variables.DANISH_DEMOCRACY_MODELS_GOLD

    connection = open_export_connection()
    try:
        write_tables_to_delta_gold(connection, tables)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
