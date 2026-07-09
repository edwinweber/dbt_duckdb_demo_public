"""Initialize a DuckDB database with Azure persistent secret and extensions.

Designed for Docker usage where the ``duckdb`` CLI is not available.
Reads credentials from environment variables (passed via docker-compose).

Usage::

    python -m ddd_python.ddd_dbt.init_duckdb
"""

import logging
import os

import duckdb
from ddd_python.ddd_utils import get_variables_from_env

logger = logging.getLogger(__name__)


def init_s3_secret(con: duckdb.DuckDBPyConnection) -> None:
    """Create a persistent S3 secret when RAW_STORAGE_TARGET=s3 or STORAGE_TARGET=s3.

    Always runs when either S3 mode is active.  The secret is written as
    persistent so non-dbt sessions (e.g. ad-hoc DBeaver queries) can reach
    S3-backed Bronze files, DuckLake Parquet data, or Delta Lake export
    targets without extra setup.
    KEY_ID, SECRET, and REGION are always set; ENDPOINT, URL_STYLE, and
    USE_SSL are only added when S3_ENDPOINT is non-empty (MinIO).  Omitting
    them lets DuckDB fall back to AWS defaults for real AWS S3.
    This statement is intentionally NOT logged to prevent credentials appearing
    in log output.
    """
    raw = get_variables_from_env.RAW_STORAGE_TARGET
    export = get_variables_from_env.STORAGE_TARGET
    if raw != "s3" and export != "s3":
        return

    s3_endpoint = get_variables_from_env.S3_ENDPOINT
    use_ssl_raw = get_variables_from_env.S3_USE_SSL
    use_ssl_val = "true" if str(use_ssl_raw).lower() in ("true", "1", "yes") else "false"
    # Escape single quotes so a credential value containing "'" can't break the DDL.
    # DuckDB's CREATE SECRET does not support bound parameters, so SQL-escaping is
    # the correct mitigation here.
    access_key = get_variables_from_env.S3_ACCESS_KEY_ID.replace("'", "''")
    secret_key = get_variables_from_env.S3_SECRET_ACCESS_KEY.replace("'", "''")
    region = get_variables_from_env.S3_REGION.replace("'", "''")

    if s3_endpoint:
        # DuckDB ENDPOINT takes host:port only — strip protocol prefix if present
        # (S3_ENDPOINT keeps the full URL for dlt/boto3 which needs it).
        endpoint_host = s3_endpoint.split("://", 1)[-1] if "://" in s3_endpoint else s3_endpoint
        endpoint = endpoint_host.replace("'", "''")
        url_style = get_variables_from_env.S3_URL_STYLE.replace("'", "''")
        endpoint_clause = (
            f"    ENDPOINT '{endpoint}',\n"
            f"    URL_STYLE '{url_style}',\n"
            f"    USE_SSL {use_ssl_val},\n"
        )
    else:
        logger.info(
            "S3_ENDPOINT is empty — using AWS default endpoint (omitting ENDPOINT from secret DDL)."
        )
        endpoint_clause = ""

    secret_sql = (
        "CREATE OR REPLACE PERSISTENT SECRET ddd_s3_secret (\n"
        "    TYPE s3,\n"
        f"    KEY_ID '{access_key}',\n"
        f"    SECRET '{secret_key}',\n"
        f"{endpoint_clause}"
        f"    REGION '{region}'\n"
        ");"
    )
    con.execute(secret_sql)

    result = con.execute(
        "SELECT name, type FROM duckdb_secrets() WHERE name = 'ddd_s3_secret'"
    ).fetchall()
    logger.info("S3 secret created: %s", result)


def init_duckdb() -> None:
    db_path = get_variables_from_env.DUCKDB_DATABASE_LOCATION

    dirname = os.path.dirname(db_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)

    logger.info("Initializing DuckDB at: %s", db_path)

    with duckdb.connect(db_path) as con:
        # Install and load extensions
        for ext in ("httpfs", "azure", "delta"):
            con.execute(f"INSTALL {ext};")
            con.execute(f"LOAD {ext};")

        # Set Azure transport to curl (required for OneLake / ADLS Gen2)
        con.execute("SET azure_transport_option_type = 'curl';")

        if get_variables_from_env.STORAGE_TARGET == "onelake":
            # Read credentials from environment
            tenant_id = get_variables_from_env.AZURE_TENANT_ID
            client_id = get_variables_from_env.AZURE_CLIENT_ID
            client_secret = get_variables_from_env.AZURE_CLIENT_SECRET

            # DuckDB's CREATE SECRET DDL does not support bound parameters, so
            # values are interpolated here.  This statement is intentionally NOT
            # logged to prevent credentials appearing in log output.
            secret_sql = (
                "CREATE OR REPLACE PERSISTENT SECRET azure_sp ("
                "    TYPE azure,"
                "    PROVIDER service_principal,"
                f"    TENANT_ID '{tenant_id}',"
                f"    CLIENT_ID '{client_id}',"
                f"    CLIENT_SECRET '{client_secret}',"
                "    ACCOUNT_NAME 'onelake'"
                ");"
            )
            con.execute(secret_sql)

            # Verify — only surface the metadata, never the secret values
            result = con.execute(
                "SELECT name, type, provider FROM duckdb_secrets() WHERE name = 'azure_sp'"
            ).fetchall()
            logger.info("Secret created: %s", result)
        else:
            logger.info("STORAGE_TARGET is not 'onelake' — skipping Azure secret creation.")

        init_s3_secret(con)

    logger.info("Done. Persistent secrets created, extensions installed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_duckdb()
