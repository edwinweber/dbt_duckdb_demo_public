"""Storage path utilities for the Danish Democracy Data project.

Centralises all logic for constructing Bronze / Silver / Gold storage paths
so that each call site does not have to know whether the target is local
disk or Fabric OneLake.

Used by:
- ``ddd_dlt`` extraction scripts (Bronze destination directories for dlt)
- ``ddd_dlt`` export scripts (Delta Lake table paths for Silver / Gold)
- ``ddd_dagster._constants`` (re-exports ``build_bronze_destination_path``
  for the Dagster asset factories)
"""

import contextlib
import os

import duckdb
from ddd_python.ddd_utils import get_variables_from_env


def silver_storage_is_ducklake() -> bool:
    """True when Silver tables are stored in DuckLake (``SILVER_STORAGE_FORMAT=ducklake``).

    This is the predicate that decides whether an export connection must attach
    the DuckLake catalog: in DuckLake mode the Silver tables live in
    ``ducklake_catalog.main_silver`` and the Gold views reference them, so both
    the Silver and Gold exports need the catalog attached.
    """
    return getattr(get_variables_from_env, "SILVER_STORAGE_FORMAT", "duckdb") == "ducklake"


def _configure_s3_secret(conn: duckdb.DuckDBPyConnection) -> None:
    """Create a DuckDB S3 secret on *conn* using the S3_* environment variables.

    Called before any DuckDB operation that needs to reach S3 — currently only
    the DuckLake ATTACH when ``DUCKLAKE_DATA_PATH`` starts with ``s3://``.
    The secret is session-scoped (not persistent) because this connection is
    read-only and short-lived.
    """
    gve = get_variables_from_env
    use_ssl_val = "true" if str(gve.S3_USE_SSL).lower() in ("true", "1", "yes") else "false"
    # Escape single quotes so a credential value containing "'" can't break the DDL.
    # DuckDB's CREATE SECRET does not support bound parameters, so SQL-escaping is
    # the correct mitigation here.
    key_id = gve.S3_ACCESS_KEY_ID.replace("'", "''")
    secret = gve.S3_SECRET_ACCESS_KEY.replace("'", "''")
    region = gve.S3_REGION.replace("'", "''")

    endpoint_clause = ""
    if gve.S3_ENDPOINT:
        # DuckDB ENDPOINT takes host:port only — strip protocol prefix if present.
        raw_endpoint = gve.S3_ENDPOINT
        endpoint_host = raw_endpoint.split("://", 1)[-1] if "://" in raw_endpoint else raw_endpoint
        endpoint = endpoint_host.replace("'", "''")
        url_style = gve.S3_URL_STYLE.replace("'", "''")
        endpoint_clause = f"ENDPOINT '{endpoint}', URL_STYLE '{url_style}', USE_SSL {use_ssl_val}, "

    conn.execute(
        f"CREATE OR REPLACE SECRET ddd_s3_secret ("
        f"TYPE s3, KEY_ID '{key_id}', SECRET '{secret}', "
        f"{endpoint_clause}"
        f"REGION '{region}')"
    )


def open_export_connection() -> duckdb.DuckDBPyConnection:
    """Open the read-only DuckDB connection used by the Silver/Gold Delta exports.

    Always opens the main DuckDB file read-only (it holds the Gold views and, in
    native ``duckdb`` mode, the Silver tables).  In DuckLake mode it additionally
    attaches the DuckLake catalog read-only as ``ducklake_catalog`` so that
    ``ducklake_catalog.main_silver.*`` tables — and the Gold views that reference
    them — resolve.  When ``DUCKLAKE_DATA_PATH`` starts with ``s3://``, an S3
    secret is created on the connection before the ATTACH so DuckLake can reach
    the Parquet data files on S3.
    """
    db_path = get_variables_from_env.DUCKDB_DATABASE_LOCATION
    connection = duckdb.connect(db_path, read_only=True)
    # S3 secret is needed when:
    #   - DuckLake data path is on S3 (delta_scan reads from ducklake_catalog), or
    #   - STORAGE_TARGET=s3 (Silver export uses delta_scan on the s3:// export target).
    storage_target = get_variables_from_env.STORAGE_TARGET
    if silver_storage_is_ducklake():
        catalog = get_variables_from_env.DUCKLAKE_CATALOG_LOCATION
        data_path = get_variables_from_env.DUCKLAKE_DATA_PATH
        if not catalog:
            raise OSError(
                "DUCKLAKE_CATALOG_LOCATION must be set when SILVER_STORAGE_FORMAT=ducklake"
            )
        if not data_path:
            raise OSError("DUCKLAKE_DATA_PATH must be set when SILVER_STORAGE_FORMAT=ducklake")
        with contextlib.suppress(Exception):  # Already installed — safe under concurrency
            connection.execute("INSTALL ducklake;")
        connection.execute("LOAD ducklake;")
        if data_path.startswith("s3://") or storage_target == "s3":
            _configure_s3_secret(connection)
        catalog_safe = catalog.replace("'", "''")
        data_path_safe = data_path.replace("'", "''")
        connection.execute(
            f"ATTACH 'ducklake:{catalog_safe}' AS ducklake_catalog (DATA_PATH '{data_path_safe}', READ_ONLY)"
        )
    elif storage_target == "s3":
        # Not DuckLake, but delta_scan needs S3 access for the export target.
        _configure_s3_secret(connection)
    return connection


def build_bronze_destination_path(source_system_code: str, entity_name: str) -> str:
    """Build the Bronze directory path for a source system entity.

    Args:
        source_system_code: Short code for the source system
            (e.g. ``"DDD"``, ``"RFAM"``).
        entity_name: Normalised entity / table name
            (e.g. ``"aktoer"``, ``"family"``).

    Returns:
        A relative path ``Files/Bronze/{source}/{entity}`` used as the dlt
        dataset name.  The actual storage destination (local, S3, or OneLake)
        is determined independently by ``_make_destination()`` using
        ``RAW_STORAGE_TARGET``.  Bronze files never go directly to OneLake.
    """
    return f"Files/Bronze/{source_system_code}/{entity_name}"


def build_delta_export_path(layer: str, table: str) -> tuple[str, dict]:
    """Build the Delta Lake export path and storage options for a given layer.

    Handles the local-vs-OneLake switch and, for local storage, creates the
    target directory as a side effect so callers do not need an ``os.makedirs``
    after calling this function.

    Args:
        layer: Medallion layer name, lowercase (``"silver"`` or ``"gold"``).
        table: Table name used as the leaf directory
            (e.g. ``"silver_ddd_aktoer"``).

    Returns:
        A ``(path, storage_options)`` tuple ready for use with
        ``deltalake.write_deltalake`` / ``DeltaTable``.  ``storage_options``
        is an empty dict for local storage and contains a bearer token for
        OneLake.
    """
    layer_cap = layer.capitalize()  # "Silver" / "Gold"
    layer_upper = layer.upper()  # "SILVER" / "GOLD"

    if get_variables_from_env.STORAGE_TARGET == "local":
        path = f"{get_variables_from_env.LOCAL_STORAGE_PATH}/Files/{layer_cap}/{table}/"
        os.makedirs(path, exist_ok=True)
        return path, {}

    if get_variables_from_env.STORAGE_TARGET == "s3":
        gve = get_variables_from_env
        bucket = gve.S3_BUCKET_DELTA
        prefix_part = gve.S3_PREFIX_DELTA.strip("/") + "/" if gve.S3_PREFIX_DELTA else ""
        path = f"s3://{bucket}/{prefix_part}Files/{layer_cap}/{table}/"
        storage_options: dict[str, str] = {
            "AWS_ACCESS_KEY_ID": gve.S3_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": gve.S3_SECRET_ACCESS_KEY,
            "AWS_REGION": gve.S3_REGION,
        }
        if gve.S3_ENDPOINT:
            storage_options["AWS_ENDPOINT_URL"] = gve.S3_ENDPOINT
            storage_options["AWS_S3_ADDRESSING_STYLE"] = gve.S3_URL_STYLE
            storage_options["AWS_ALLOW_HTTP"] = (
                "true" if str(gve.S3_USE_SSL).lower() not in ("true", "1", "yes") else "false"
            )
        return path, storage_options

    from ddd_python.ddd_utils import (
        get_fabric_onelake_clients,  # lazy — avoids loading Azure SDK when not needed
    )

    token = get_fabric_onelake_clients.get_fabric_token()
    folder = getattr(get_variables_from_env, f"FABRIC_ONELAKE_FOLDER_{layer_upper}")
    path = (
        f"abfss://{get_variables_from_env.FABRIC_WORKSPACE}"
        f"@{get_variables_from_env.FABRIC_ONELAKE_STORAGE_ACCOUNT}"
        f".dfs.fabric.microsoft.com/{folder}/{table}/"
    )
    return path, {"bearer_token": token, "use_fabric_endpoint": "true"}
