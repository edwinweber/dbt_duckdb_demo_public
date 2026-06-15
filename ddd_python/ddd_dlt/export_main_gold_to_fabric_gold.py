"""Export Gold-layer tables from DuckDB to Fabric OneLake as Delta Lake tables.

Reads each Gold table via DuckDB, converts it to a PyArrow table, and writes
it as a Delta Lake table to OneLake using ``deltalake``.  Gold tables are
always fully overwritten (``mode="overwrite"``).

The connection comes from ``open_export_connection()``: in DuckLake mode it
attaches the DuckLake catalog read-only, because the Gold views (which live in
the main DuckDB database) reference ``ducklake_catalog.main_silver.*`` and would
otherwise fail to resolve.

Usage::

    python -m ddd_python.ddd_dlt.export_main_gold_to_fabric_gold
    python -m ddd_python.ddd_dlt.export_main_gold_to_fabric_gold --tables actor vote
"""

import argparse
import logging

from deltalake.writer import write_deltalake

import duckdb
from ddd_python.ddd_utils import configuration_variables, get_variables_from_env
from ddd_python.ddd_utils.path_utils import build_delta_export_path, open_export_connection

logger = logging.getLogger(__name__)


def export_single_gold_table(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    """Export one Gold table from DuckDB to OneLake as a Delta Lake table.

    Gold tables are always fully overwritten (``mode="overwrite"``).

    Returns:
        Number of rows written.
    """
    target_table_path, storage_options = build_delta_export_path("gold", table)
    query = f"SELECT * FROM {get_variables_from_env.DUCKDB_DATABASE}.main_gold.{table}"
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
    return df.num_rows


def write_tables_to_onelake_gold(connection: duckdb.DuckDBPyConnection, tables: list[str]) -> None:
    """Write *tables* from the DuckDB Gold schema to OneLake as Delta Lake tables.

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
            logger.error("Failed to export Gold table %s: %s", table, e)
            failed.append(table)

    if failed:
        raise RuntimeError(f"Gold export failed for {len(failed)} table(s): {', '.join(failed)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Gold tables to Fabric OneLake.")
    parser.add_argument(
        "--tables", nargs="+", required=False, help="Tables to export (default: all Gold tables)."
    )
    args = parser.parse_args()

    tables = args.tables or configuration_variables.DANISH_DEMOCRACY_MODELS_GOLD

    connection = open_export_connection()
    try:
        write_tables_to_onelake_gold(connection, tables)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
