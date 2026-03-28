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


def init_duckdb() -> None:
    db_path = os.environ.get(
        "DUCKDB_DATABASE_LOCATION",
        get_variables_from_env.DUCKDB_DATABASE_LOCATION,
    )
    if not db_path:
        raise EnvironmentError("DUCKDB_DATABASE_LOCATION is not set")

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

    logger.info("Done. Persistent Azure secret created, extensions installed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_duckdb()
