"""Initialize a DuckDB database with Azure persistent secret and extensions.

Designed for Docker usage where the ``duckdb`` CLI is not available.
Reads credentials from environment variables (passed via docker-compose).

Usage::

    python -m ddd_python.ddd_dbt.init_duckdb
"""

import os
import duckdb

from ddd_python.ddd_utils import get_variables_from_env


def init_duckdb() -> None:
    db_path = os.environ.get(
        "DUCKDB_DATABASE_LOCATION",
        get_variables_from_env.DUCKDB_DATABASE_LOCATION,
    )
    if not db_path:
        raise EnvironmentError("DUCKDB_DATABASE_LOCATION is not set")

    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    print(f"Initializing DuckDB at: {db_path}")

    con = duckdb.connect(db_path)
    try:
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

        # Create persistent secret — values are passed inline because
        # DuckDB's getenv() is not available in the Python API.
        con.execute(f"""
            CREATE OR REPLACE PERSISTENT SECRET azure_sp (
                TYPE azure,
                PROVIDER service_principal,
                TENANT_ID '{tenant_id}',
                CLIENT_ID '{client_id}',
                CLIENT_SECRET '{client_secret}',
                ACCOUNT_NAME 'onelake'
            );
        """)

        # Verify
        result = con.execute(
            "SELECT name, type, provider FROM duckdb_secrets() WHERE name = 'azure_sp'"
        ).fetchall()
        print(f"Secret created: {result}")
    finally:
        con.close()

    print("Done. Persistent Azure secret created, extensions installed.")


if __name__ == "__main__":
    init_duckdb()
