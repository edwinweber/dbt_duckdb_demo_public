"""Export Silver-layer tables from DuckDB to Fabric OneLake as Delta Lake tables.

For each Silver table, new rows are determined by anti-joining the DuckDB
Silver table against the **existing** Delta table — read in-place with DuckDB's
``delta_scan`` (delta extension), so the target is never materialised into
Python/PyArrow memory.  Only the delta is appended.  On first load (no existing
Delta table) the full table is written with ``mode="overwrite"``.

Note on the read/write split: DuckDB's delta extension is **read-only**
(``delta_scan`` reads; there is no ``COPY ... (FORMAT delta)``), so the existence
check stays on ``deltalake.DeltaTable.is_deltatable`` and the actual write stays
on ``deltalake.write_deltalake``.  The expensive part that moved into DuckDB is
the dedup read.

Usage::

    python -m ddd_python.ddd_dlt.export_main_silver_to_fabric_silver
    python -m ddd_python.ddd_dlt.export_main_silver_to_fabric_silver --tables silver_ddd_aktoer silver_ddd_moede
"""

import argparse
import logging
import os

from deltalake import DeltaTable
from deltalake.writer import write_deltalake

import duckdb
from ddd_python.ddd_utils import configuration_variables, get_variables_from_env
from ddd_python.ddd_utils.path_utils import (
    build_delta_export_path,
    open_export_connection,
    silver_storage_is_ducklake,
)

logger = logging.getLogger(__name__)


def _get_primary_key(table: str) -> str:
    """Return the primary key column for a Silver model name."""
    return configuration_variables.SILVER_TABLE_PRIMARY_KEYS.get(table, "id")


def _silver_source_database() -> str:
    """Database that holds the Silver tables.

    In DuckLake mode the Silver tables live in the attached ``ducklake_catalog``
    database; otherwise they live in the main DuckDB database.
    """
    if silver_storage_is_ducklake():
        return "ducklake_catalog"
    db = get_variables_from_env.DUCKDB_DATABASE
    assert db is not None, "DUCKDB_DATABASE must be set"
    return db


def _prepare_delta_read(connection: duckdb.DuckDBPyConnection) -> None:
    """Load the DuckDB ``delta`` extension so ``delta_scan`` can read existing
    Delta tables in-place.

    For OneLake targets the Azure stack is also loaded and the persistent
    ``azure_sp`` service-principal secret (created by ``duckdb/init_duckdb.sql``
    next to the DuckDB file) is made available, so ``delta_scan`` can
    authenticate against ``abfss://`` paths the same way the dbt ``onelake``
    target does.

    Idempotent: safe to call once per table export.
    """
    connection.execute("INSTALL delta; LOAD delta;")
    if get_variables_from_env.STORAGE_TARGET == "onelake":
        db_loc = get_variables_from_env.DUCKDB_DATABASE_LOCATION
        assert db_loc is not None, "DUCKDB_DATABASE_LOCATION must be set"
        secret_dir = os.path.dirname(db_loc)
        connection.execute(f"SET secret_directory='{secret_dir}';")
        connection.execute("INSTALL azure; LOAD azure;")
        connection.execute("INSTALL httpfs; LOAD httpfs;")
        connection.execute("SET azure_transport_option_type='curl';")


def export_single_silver_table(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    """Export one Silver table from DuckDB to OneLake as a Delta Lake table.

    If the Delta table already exists, only rows whose ``(primary_key,
    LKHS_date_valid_from)`` are not already present are appended — the existing
    rows are read directly from the Delta table via ``delta_scan`` and
    anti-joined inside DuckDB (no full target materialisation).  If the Delta
    table does not exist yet, it is created with a full overwrite.

    Returns:
        Number of rows written.
    """
    target_table_path, storage_options = build_delta_export_path("silver", table)
    pk = _get_primary_key(table)
    source = f"{_silver_source_database()}.main_silver.{table}"

    if DeltaTable.is_deltatable(target_table_path, storage_options=storage_options):
        _prepare_delta_read(connection)
        # Anti-join against the existing Delta table via delta_scan.  DuckDB
        # reads only the join keys it needs from the target (projection
        # pushdown) instead of loading the whole table into PyArrow memory.
        query = (
            f"SELECT src.* FROM {source} src "
            f"LEFT JOIN delta_scan('{target_table_path}') tgt "
            f"  ON src.{pk} = tgt.{pk} "
            f"  AND src.LKHS_date_valid_from = tgt.LKHS_date_valid_from "
            f"WHERE tgt.{pk} IS NULL"
        )
        df = connection.execute(query).to_arrow_table()
        if df.num_rows > 0:
            write_deltalake(
                target_table_path,
                df,
                mode="append",
                schema_mode="merge",
                storage_options=storage_options,
            )
        logger.info("Updated Silver Delta-table %s — %d rows inserted.", table, df.num_rows)
        return df.num_rows
    else:
        query = f"SELECT src.* FROM {source} src"
        df = connection.execute(query).to_arrow_table()
        if df.num_rows > 0:
            write_deltalake(
                target_table_path,
                df,
                mode="overwrite",
                storage_options=storage_options,
            )
        logger.info("Created Silver Delta-table %s — %d rows inserted.", table, df.num_rows)
        return df.num_rows


def write_tables_to_onelake_silver(
    connection: duckdb.DuckDBPyConnection, tables: list[str]
) -> None:
    """Write *tables* from the DuckDB Silver schema to OneLake as Delta Lake tables.

    Convenience wrapper that calls :func:`export_single_silver_table` for each
    table in *tables*.

    Raises:
        RuntimeError: If one or more tables failed to export, after attempting
            all tables.  Partial failures are logged at ERROR level.
    """
    failed: list[str] = []
    for table in tables:
        try:
            export_single_silver_table(connection, table)
        except Exception as e:
            logger.error("Failed to export Silver table %s: %s", table, e)
            failed.append(table)

    if failed:
        raise RuntimeError(f"Silver export failed for {len(failed)} table(s): {', '.join(failed)}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Silver tables to Fabric OneLake.")
    parser.add_argument(
        "--tables", nargs="+", required=False, help="Tables to export (default: all Silver tables)."
    )
    args = parser.parse_args()

    tables = args.tables or (
        configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER
        + configuration_variables.RFAM_MODELS_SILVER
    )

    connection = open_export_connection()
    try:
        write_tables_to_onelake_silver(connection, tables)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
