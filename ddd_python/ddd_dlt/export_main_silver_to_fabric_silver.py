"""Export Silver-layer tables from DuckDB to Fabric OneLake as Delta Lake tables.

Reads each Silver table via DuckDB, determines new rows by LEFT-JOINing
against the existing Delta table on OneLake (if present), and appends only
the delta.  On first load (no existing Delta table), the full table is
written with ``mode="overwrite"``.

Usage::

    python -m ddd_python.ddd_dlt.export_main_silver_to_fabric_silver
    python -m ddd_python.ddd_dlt.export_main_silver_to_fabric_silver --tables silver_ddd_aktoer silver_ddd_moede
"""

import argparse
import logging
import os

import duckdb
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

from ddd_python.ddd_utils import get_variables_from_env, configuration_variables

logger = logging.getLogger(__name__)


def _get_primary_key(table: str) -> str:
    """Return the primary key column for a Silver model name."""
    return configuration_variables.SILVER_TABLE_PRIMARY_KEYS.get(table, "id")


def export_single_silver_table(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    """Export one Silver table from DuckDB to OneLake as a Delta Lake table.

    Tries to read the existing Delta table on OneLake and inserts only new
    rows (incremental append via LEFT JOIN on the table's primary key +
    ``LKHS_date_valid_from``).  If the Delta table does not exist yet, it is
    created with a full overwrite.

    Returns:
        Number of rows written.
    """
    if get_variables_from_env.STORAGE_TARGET == "local":
        target_table_path = f"{get_variables_from_env.LOCAL_STORAGE_PATH}/Files/Silver/{table}/"
        os.makedirs(target_table_path, exist_ok=True)
        storage_options = {}
    else:
        from ddd_python.ddd_utils import get_fabric_onelake_clients
        token = get_fabric_onelake_clients.get_fabric_token()
        target_table_path = (
            f"abfss://{get_variables_from_env.FABRIC_WORKSPACE}"
            f"@{get_variables_from_env.FABRIC_ONELAKE_STORAGE_ACCOUNT}"
            f".dfs.fabric.microsoft.com/{get_variables_from_env.FABRIC_ONELAKE_FOLDER_SILVER}/{table}/"
        )
        storage_options = {"bearer_token": token, "use_fabric_endpoint": "true"}

    pk = _get_primary_key(table)

    if DeltaTable.is_deltatable(target_table_path, storage_options=storage_options):
        target_table = DeltaTable(target_table_path, storage_options=storage_options)
        connection.register(f"target_table_{table}", target_table.to_pyarrow_table())
        query = (
            f"SELECT src.* FROM {get_variables_from_env.DUCKDB_DATABASE}.main_silver.{table} src "
            f"LEFT JOIN target_table_{table} tgt ON src.{pk} = tgt.{pk} AND src.LKHS_date_valid_from = tgt.LKHS_date_valid_from "
            f"WHERE tgt.{pk} IS NULL"
        )
        result = connection.execute(query)
        df = result.to_arrow_table()
        if df.num_rows > 0:
            write_deltalake(
                target_table_path, df,
                mode="append", schema_mode="merge",
                storage_options=storage_options,
            )
        logger.info("Updated Silver Delta-table %s — %d rows inserted.", table, df.num_rows)
        return df.num_rows
    else:
        query = f"SELECT src.* FROM {get_variables_from_env.DUCKDB_DATABASE}.main_silver.{table} src"
        result = connection.execute(query)
        df = result.to_arrow_table()
        if df.num_rows > 0:
            write_deltalake(
                target_table_path, df,
                mode="overwrite",
                storage_options=storage_options,
            )
        logger.info("Created Silver Delta-table %s — %d rows inserted.", table, df.num_rows)
        return df.num_rows


def write_tables_to_onelake_silver(connection: duckdb.DuckDBPyConnection, tables: list[str]) -> None:
    """Write *tables* from the DuckDB Silver schema to OneLake as Delta Lake tables.

    Convenience wrapper that calls :func:`export_single_silver_table` for each
    table in *tables*.
    """
    for table in tables:
        try:
            export_single_silver_table(connection, table)
        except Exception as e:
            logger.error("Failed to export Silver table %s: %s", table, e)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Silver tables to Fabric OneLake.")
    parser.add_argument("--tables", nargs="+", required=False, help="Tables to export (default: all Silver tables).")
    args = parser.parse_args()

    tables = args.tables or (
        configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER
        + configuration_variables.RFAM_MODELS_SILVER
    )

    connection = duckdb.connect(get_variables_from_env.DUCKDB_DATABASE_LOCATION, read_only=True)
    try:
        write_tables_to_onelake_silver(connection, tables)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
