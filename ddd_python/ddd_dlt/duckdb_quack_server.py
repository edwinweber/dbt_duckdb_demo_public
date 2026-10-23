"""DuckDB 2.0 quack RPC server — the single read-write owner of the database files.

Opens the main DuckDB 2.0 database read-write, loads extensions, optionally
attaches the DuckLake catalog, creates persistent secrets, then starts the quack
RPC server so that all other containers (Dagster, Metabase, dbt) can connect as
clients without mounting any database files themselves.

Environment variables (set via docker-compose or .env):
    QUACK_SERVER_TOKEN       — required; bearer token, min 4 chars
    QUACK_SERVER_PORT        — optional; default 9494
    QUACK_SERVER_MEMORY_LIMIT— optional; DuckDB memory_limit, default 12GB (see below)
    DUCKDB_DATABASE_LOCATION — path to the .duckdb file
    SILVER_STORAGE_FORMAT    — "duckdb" (default) or "ducklake"
    DUCKLAKE_CATALOG_LOCATION— required when SILVER_STORAGE_FORMAT=ducklake
    DUCKLAKE_DATA_PATH       — required when SILVER_STORAGE_FORMAT=ducklake
"""

import contextlib
import logging
import os
import signal
import time

import duckdb
from ddd_python.ddd_dbt.init_duckdb import init_secrets_on_connection
from ddd_python.ddd_utils import get_variables_from_env
from ddd_python.ddd_utils.ducklake_maintenance import SWEEP_FUNCTION_NAME, register_sweep_function

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

logger.info("duckdb version: %s", duckdb.__version__)

_QUACK_HOST = "0.0.0.0"

_connection: duckdb.DuckDBPyConnection | None = None
_shutdown_requested = False


def _handle_signal(signum: int, frame: object) -> None:  # noqa: ARG001
    global _shutdown_requested
    logger.info("Signal %d received — initiating clean shutdown.", signum)
    _shutdown_requested = True


def main() -> None:
    global _connection, _shutdown_requested

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    token = get_variables_from_env.QUACK_SERVER_TOKEN
    if not token:
        raise OSError("QUACK_SERVER_TOKEN must be set and non-empty")
    if len(token) < 4:
        raise OSError("QUACK_SERVER_TOKEN must be at least 4 characters (quack requirement)")

    port = get_variables_from_env.QUACK_SERVER_PORT
    db_path = get_variables_from_env.DUCKDB_DATABASE_LOCATION

    dirname = os.path.dirname(db_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)

    logger.info("Opening DuckDB at %s", db_path)
    _connection = duckdb.connect(db_path)

    try:
        # Bronze views use read_json_auto(..., union_by_name=true); DuckDB 2.0 reserves 32 MiB
        # per file at bind time, so the default limit (80% of the container's cgroup limit)
        # is hit at ~100 files although the real RSS stays around 1 GB. Set the logical
        # limit above the container limit; the container limit stays the hard cap.
        memory_limit = get_variables_from_env.QUACK_SERVER_MEMORY_LIMIT.replace("'", "''")
        _connection.execute(f"SET memory_limit='{memory_limit}'")
        logger.info("DuckDB memory_limit set to %s", memory_limit)

        # Load extensions needed server-side; INSTALL is idempotent.
        for ext in ("parquet", "httpfs", "sqlite", "delta"):
            with contextlib.suppress(Exception):
                _connection.execute(f"INSTALL {ext};")
            try:
                _connection.execute(f"LOAD {ext};")
                logger.info("Extension loaded: %s", ext)
            except Exception as exc:
                logger.warning("Could not load extension %s: %s", ext, exc)

        # Create persistent S3 / Azure secrets (no-ops when storage is local).
        init_secrets_on_connection(_connection)

        # DuckLake: conditionally load + attach the Silver catalog.
        if get_variables_from_env.SILVER_STORAGE_FORMAT == "ducklake":
            catalog_location = get_variables_from_env.DUCKLAKE_CATALOG_LOCATION
            data_path = get_variables_from_env.DUCKLAKE_DATA_PATH
            if not catalog_location:
                raise OSError(
                    "DUCKLAKE_CATALOG_LOCATION must be set when SILVER_STORAGE_FORMAT=ducklake"
                )
            if not data_path:
                raise OSError("DUCKLAKE_DATA_PATH must be set when SILVER_STORAGE_FORMAT=ducklake")

            with contextlib.suppress(Exception):
                _connection.execute("INSTALL ducklake;")
            _connection.execute("LOAD ducklake;")
            logger.info("ducklake extension loaded")

            catalog_safe = catalog_location.replace("'", "''")
            data_path_safe = data_path.replace("'", "''")
            _connection.execute(
                f"ATTACH 'ducklake:{catalog_safe}' AS ducklake_catalog "
                f"(DATA_PATH '{data_path_safe}')"
            )
            logger.info(
                "DuckLake catalog attached: %s (DATA_PATH: %s)",
                catalog_location,
                data_path,
            )

            # The data directory is only mounted here, so the _current_temp sweep runs in
            # this process; clients call it as a SQL function (DuckLake cleanup asset).
            if not data_path.startswith("s3://"):
                register_sweep_function(_connection, data_path)
                logger.info("Registered SQL function %s", SWEEP_FUNCTION_NAME)

        # Start the quack RPC server (non-blocking background thread).
        # allow_other_hostname: quack refuses to listen on anything but localhost otherwise,
        # and the other containers (and remote clients, via the published port) connect by name.
        # disable_ssl: plain HTTP, acceptable on the internal Docker network only; remote
        # clients should use a VPN or SSH tunnel (see docker/DOCKER_USAGE.md).
        token_safe = token.replace("'", "''")
        _connection.execute(
            f"CALL quack_serve('quack:{_QUACK_HOST}:{port}', "
            f"token => '{token_safe}', "
            f"allow_other_hostname => true, "
            f"disable_ssl => true)"
        )
        logger.info("quack server listening on %s:%d", _QUACK_HOST, port)

        try:
            schemas = _connection.execute(
                "SELECT catalog_name, schema_name FROM information_schema.schemata ORDER BY 1, 2"
            ).fetchall()
            logger.info("Schemas visible to server: %s", schemas)
        except Exception as exc:
            logger.warning("Could not list schemas: %s", exc)

        while not _shutdown_requested:
            time.sleep(1)

    finally:
        if _connection is not None:
            logger.info("Closing DuckDB connection (WAL checkpoint).")
            _connection.close()
            _connection = None
        logger.info("quack server stopped.")


if __name__ == "__main__":
    main()
