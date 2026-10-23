"""Export Silver-layer tables from DuckDB to the configured Delta Lake destination.

Incremental append: reads the target Delta table via DuckDB's ``delta_scan``
(no Python materialization) and anti-joins to find new rows. Dedup read stays
in DuckDB; PyArrow write via ``deltalake`` (DuckDB's delta extension is
read-only). First load overwrites; subsequent runs append.

See documentation/project_reference.md (Silver Storage Format) for context on storage mode switching.
"""

import argparse
import contextlib
import logging
import os

from deltalake import DeltaTable
from deltalake.writer import write_deltalake

import duckdb
from ddd_python.ddd_dbt.init_duckdb import init_secrets_on_connection
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
    database; otherwise they live in the main DuckDB database. Through the quack
    server (ATTACH only) the DuckLake catalog is addressed below the attached
    server database: ``<DUCKDB_DATABASE>.ducklake_catalog``.
    """
    db = get_variables_from_env.DUCKDB_DATABASE
    if not db:
        raise OSError("DUCKDB_DATABASE must be set")
    if silver_storage_is_ducklake():
        if get_variables_from_env.QUACK_SERVER_HOST:
            return f"{db}.ducklake_catalog"
        return "ducklake_catalog"
    return db


def _prepare_delta_read(connection: duckdb.DuckDBPyConnection) -> None:
    """Load the DuckDB ``delta`` extension so ``delta_scan`` can read existing
    Delta tables in-place.

    For OneLake targets the Azure stack is also loaded and the persistent
    ``azure_sp`` service-principal secret (created by ``duckdb/init_duckdb.sql``
    next to the DuckDB file) is made available, so ``delta_scan`` can
    authenticate against ``abfss://`` paths the same way the dbt ``onelake``
    target does.

    Through the quack server the connection is ATTACH only, so ``delta_scan`` runs
    in this process and the S3 / Azure secrets are created here as well (the
    ``azure_sp`` secret file next to the DuckDB file is not mounted in this container).

    Idempotent: safe to call once per table export.
    """
    quack_client = bool(get_variables_from_env.QUACK_SERVER_HOST)
    connection.execute("INSTALL delta; LOAD delta;")
    if get_variables_from_env.STORAGE_TARGET == "onelake":
        if not quack_client:
            secret_dir = os.path.dirname(get_variables_from_env.DUCKDB_DATABASE_LOCATION)
            connection.execute(f"SET secret_directory='{secret_dir}';")
        connection.execute("INSTALL azure; LOAD azure;")
        connection.execute("INSTALL httpfs; LOAD httpfs;")
        connection.execute("SET azure_transport_option_type='curl';")
    elif get_variables_from_env.STORAGE_TARGET == "s3":
        with contextlib.suppress(Exception):
            connection.execute("INSTALL httpfs;")
        connection.execute("LOAD httpfs;")
    if quack_client and get_variables_from_env.STORAGE_TARGET in ("onelake", "s3"):
        init_secrets_on_connection(connection)


def export_single_silver_table(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    """Export one Silver table from DuckDB to the configured Delta Lake destination.

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
        arrow_table = connection.sql(query).to_arrow_table()
        if arrow_table is None:
            raise RuntimeError(f"DuckDB returned None for Silver query on {table}")
        if arrow_table.num_rows > 0:
            write_deltalake(
                target_table_path,
                arrow_table,
                mode="append",
                schema_mode="merge",
                storage_options=storage_options,
            )
        logger.info(
            "Updated Silver Delta-table %s — %d rows inserted.", table, arrow_table.num_rows
        )
        return int(arrow_table.num_rows)
    else:
        query = f"SELECT src.* FROM {source} src"
        arrow_table = connection.sql(query).to_arrow_table()
        if arrow_table is None:
            raise RuntimeError(f"DuckDB returned None for Silver query on {table}")
        if arrow_table.num_rows > 0:
            write_deltalake(
                target_table_path,
                arrow_table,
                mode="overwrite",
                storage_options=storage_options,
            )
        logger.info(
            "Created Silver Delta-table %s — %d rows inserted.", table, arrow_table.num_rows
        )
        return int(arrow_table.num_rows)


def write_tables_to_delta_silver(connection: duckdb.DuckDBPyConnection, tables: list[str]) -> None:
    """Exports Silver tables as Delta Lake tables to the configured destination (local filesystem, S3-compatible storage, or Fabric OneLake).

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
            logger.error("Failed to export Silver table %s: %s", table, e, exc_info=True)
            failed.append(table)

    if failed:
        raise RuntimeError(f"Silver export failed for {len(failed)} table(s): {', '.join(failed)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export Silver tables to the configured Delta Lake destination (local, S3, or OneLake)."
    )
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
        write_tables_to_delta_silver(connection, tables)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
