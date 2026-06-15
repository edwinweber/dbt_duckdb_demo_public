"""Integration tests for Gold Delta Lake export logic.

Uses an in-memory DuckDB database with a mock Gold table and patches the
external dependencies to verify the full-overwrite export behaviour.
"""

import os
from unittest.mock import patch

import pytest
from deltalake import DeltaTable

import ddd_python.ddd_dlt.export_main_gold_to_fabric_gold as gold_mod
import duckdb
from ddd_python.ddd_utils.path_utils import build_delta_export_path, open_export_connection

_ENV_PATCHES = {
    "STORAGE_TARGET": "onelake",
    "FABRIC_WORKSPACE": "test-workspace",
    "FABRIC_ONELAKE_STORAGE_ACCOUNT": "testaccount",
    "FABRIC_ONELAKE_FOLDER_GOLD": "Lakehouse/Files/Gold",
    "DUCKDB_DATABASE": "memory",
}


def _patch_env():
    """Patch env vars on the already-imported module.

    Uses patch.dict on __dict__ to bypass __getattr__ (which would call
    _require() and raise for lazy-required vars like FABRIC_WORKSPACE
    when no .env file is present, e.g. in CI).
    """
    return patch.dict(
        "ddd_python.ddd_dlt.export_main_gold_to_fabric_gold.get_variables_from_env.__dict__",
        _ENV_PATCHES,
        clear=False,
    )


@pytest.fixture
def gold_connection():
    """Create an in-memory DuckDB with a sample Gold table."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS main_gold")
    conn.execute("""
        CREATE TABLE main_gold.actor AS
        SELECT 1 AS actor_bk, 'Alice' AS name, 'Active' AS status
        UNION ALL
        SELECT 2, 'Bob', 'Inactive'
    """)
    yield conn
    conn.close()


def test_gold_export_overwrites(gold_connection, mock_fabric_clients):
    """Gold export should always use mode='overwrite'."""
    with (
        _patch_env(),
        patch.object(gold_mod, "write_deltalake") as mock_write,
    ):
        rows = gold_mod.export_single_gold_table(gold_connection, "actor")

    assert rows == 2
    mock_write.assert_called_once()
    call_kwargs = mock_write.call_args
    assert (
        call_kwargs.kwargs.get("mode") == "overwrite" or call_kwargs[1].get("mode") == "overwrite"
    )


def test_gold_export_returns_row_count(gold_connection, mock_fabric_clients):
    """Return value should be the number of rows in the table."""
    with (
        _patch_env(),
        patch.object(gold_mod, "write_deltalake"),
    ):
        rows = gold_mod.export_single_gold_table(gold_connection, "actor")

    assert rows == 2


def test_gold_export_correct_target_path(gold_connection, mock_fabric_clients):
    """The target path should follow the expected pattern."""
    with (
        _patch_env(),
        patch.object(gold_mod, "write_deltalake") as mock_write,
    ):
        gold_mod.export_single_gold_table(gold_connection, "actor")

    target_path = mock_write.call_args[0][0]
    assert "Lakehouse/Files/Gold/actor/" in target_path
    assert target_path.startswith("abfss://")


# ── DuckLake mode (Gold views reference ducklake_catalog.main_silver) ─────────


def _has_ducklake() -> bool:
    try:
        c = duckdb.connect()
        c.execute("INSTALL ducklake; LOAD ducklake;")
        c.close()
        return True
    except Exception:
        return False


@pytest.mark.skipif(not _has_ducklake(), reason="DuckDB 'ducklake' extension not available")
def test_gold_export_ducklake_mode_resolves_silver_refs(tmp_path):
    """In ducklake mode the Gold export connection must attach the DuckLake catalog
    so Gold views referencing ducklake_catalog.main_silver resolve and export."""
    catalog = str(tmp_path / "catalog.ducklake")
    dldata = str(tmp_path / "dldata")
    warehouse = str(tmp_path / "warehouse.duckdb")
    os.makedirs(dldata, exist_ok=True)

    # Build the main DuckDB file with a Gold VIEW that reads from DuckLake silver.
    setup = duckdb.connect(warehouse)
    setup.execute("INSTALL ducklake; LOAD ducklake;")
    setup.execute(f"ATTACH 'ducklake:{catalog}' AS ducklake_catalog (DATA_PATH '{dldata}')")
    setup.execute("CREATE SCHEMA ducklake_catalog.main_silver")
    setup.execute(
        "CREATE TABLE ducklake_catalog.main_silver.silver_ddd_aktoer AS "
        "SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'"
    )
    setup.execute("CREATE SCHEMA main_gold")
    setup.execute(
        "CREATE VIEW main_gold.actor AS "
        "SELECT id AS actor_bk, name FROM ducklake_catalog.main_silver.silver_ddd_aktoer"
    )
    setup.close()

    env = {
        "STORAGE_TARGET": "local",
        "LOCAL_STORAGE_PATH": str(tmp_path),
        "SILVER_STORAGE_FORMAT": "ducklake",
        "DUCKDB_DATABASE_LOCATION": warehouse,
        "DUCKDB_DATABASE": "warehouse",
        "DUCKLAKE_CATALOG_LOCATION": catalog,
        "DUCKLAKE_DATA_PATH": dldata,
    }
    with patch.dict(gold_mod.get_variables_from_env.__dict__, env, clear=False):
        path, _ = build_delta_export_path("gold", "actor")
        connection = open_export_connection()
        try:
            rows = gold_mod.export_single_gold_table(connection, "actor")
        finally:
            connection.close()

    assert rows == 2
    assert DeltaTable(path).to_pyarrow_table().num_rows == 2
