"""Export Gold-layer tables from DuckDB to Fabric OneLake as Delta Lake tables.

Reads each Gold table via DuckDB, converts it to a PyArrow table, and writes
it as a Delta Lake table to OneLake using ``deltalake``.  Gold tables are
always fully overwritten (``mode="overwrite"``).

Usage::

    python -m ddd_python.ddd_dlt.export_main_gold_to_fabric_gold
    python -m ddd_python.ddd_dlt.export_main_gold_to_fabric_gold --tables actor vote
"""

import argparse
import logging
import os

import duckdb
from deltalake.writer import write_deltalake

from ddd_python.ddd_utils import get_variables_from_env, configuration_variables

logger = logging.getLogger(__name__)


def export_single_gold_table(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    """Export one Gold table from DuckDB to OneLake as a Delta Lake table.

    Gold tables are always fully overwritten (``mode="overwrite"``).

    Returns:
        Number of rows written.
    """
    if get_variables_from_env.STORAGE_TARGET == "local":
        target_table_path = f"{get_variables_from_env.LOCAL_STORAGE_PATH}/Files/Gold/{table}/"
        os.makedirs(target_table_path, exist_ok=True)
        storage_options = {}
    else:
        from ddd_python.ddd_utils import get_fabric_onelake_clients
        token = get_fabric_onelake_clients.get_fabric_token()
        target_table_path = (
            f"abfss://{get_variables_from_env.FABRIC_WORKSPACE}"
            f"@{get_variables_from_env.FABRIC_ONELAKE_STORAGE_ACCOUNT}"
            f".dfs.fabric.microsoft.com/{get_variables_from_env.FABRIC_ONELAKE_FOLDER_GOLD}/{table}/"
        )
        storage_options = {"bearer_token": token, "use_fabric_endpoint": "true"}
    query = f"SELECT * FROM {get_variables_from_env.DUCKDB_DATABASE}.main_gold.{table}"
    result = connection.execute(query)
    df = result.to_arrow_table()
    write_deltalake(
        target_table_path, df,
        mode="overwrite", schema_mode="merge",
        storage_options=storage_options,
    )
    logger.info("Replaced Gold Delta-table %s — %d rows written.", table, df.num_rows)
    return df.num_rows


def write_tables_to_onelake_gold(connection: duckdb.DuckDBPyConnection, tables: list[str]) -> None:
    """Write *tables* from the DuckDB Gold schema to OneLake as Delta Lake tables.

    Convenience wrapper that calls :func:`export_single_gold_table` for each
    table in *tables*.
    """
    for table in tables:
        try:
            export_single_gold_table(connection, table)
        except Exception as e:
            logger.error("Failed to export Gold table %s: %s", table, e)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Gold tables to Fabric OneLake.")
    parser.add_argument("--tables", nargs="+", required=False, help="Tables to export (default: all Gold tables).")
    args = parser.parse_args()

    tables = args.tables or configuration_variables.DANISH_DEMOCRACY_MODELS_GOLD

    connection = duckdb.connect(get_variables_from_env.DUCKDB_DATABASE_LOCATION, read_only=True)
    try:
        write_tables_to_onelake_gold(connection, tables)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
