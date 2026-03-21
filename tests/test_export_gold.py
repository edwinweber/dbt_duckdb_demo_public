"""Integration tests for Gold Delta Lake export logic.

Uses an in-memory DuckDB database with a mock Gold table and patches the
external dependencies to verify the full-overwrite export behaviour.
"""

import duckdb
import pytest
from unittest.mock import patch

_ENV_PATCHES = {
    "STORAGE_TARGET": "onelake",
    "FABRIC_WORKSPACE": "test-workspace",
    "FABRIC_ONELAKE_STORAGE_ACCOUNT": "testaccount",
    "FABRIC_ONELAKE_FOLDER_GOLD": "Lakehouse/Files/Gold",
    "DUCKDB_DATABASE": "memory",
}


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


@patch.dict("ddd_python.ddd_dlt.export_main_gold_to_fabric_gold.get_variables_from_env.__dict__", _ENV_PATCHES, clear=False)
@patch("ddd_python.ddd_dlt.export_main_gold_to_fabric_gold.write_deltalake")
def test_gold_export_overwrites(mock_write, gold_connection, mock_fabric_clients):
    """Gold export should always use mode='overwrite'."""
    from ddd_python.ddd_dlt.export_main_gold_to_fabric_gold import export_single_gold_table

    rows = export_single_gold_table(gold_connection, "actor")

    assert rows == 2
    mock_write.assert_called_once()
    call_kwargs = mock_write.call_args
    assert call_kwargs.kwargs.get("mode") == "overwrite" or call_kwargs[1].get("mode") == "overwrite"


@patch.dict("ddd_python.ddd_dlt.export_main_gold_to_fabric_gold.get_variables_from_env.__dict__", _ENV_PATCHES, clear=False)
@patch("ddd_python.ddd_dlt.export_main_gold_to_fabric_gold.write_deltalake")
def test_gold_export_returns_row_count(mock_write, gold_connection, mock_fabric_clients):
    """Return value should be the number of rows in the table."""
    from ddd_python.ddd_dlt.export_main_gold_to_fabric_gold import export_single_gold_table

    rows = export_single_gold_table(gold_connection, "actor")
    assert rows == 2


@patch.dict("ddd_python.ddd_dlt.export_main_gold_to_fabric_gold.get_variables_from_env.__dict__", _ENV_PATCHES, clear=False)
@patch("ddd_python.ddd_dlt.export_main_gold_to_fabric_gold.write_deltalake")
def test_gold_export_correct_target_path(mock_write, gold_connection, mock_fabric_clients):
    """The target path should follow the expected pattern."""
    from ddd_python.ddd_dlt.export_main_gold_to_fabric_gold import export_single_gold_table

    export_single_gold_table(gold_connection, "actor")

    target_path = mock_write.call_args[0][0]
    assert "Lakehouse/Files/Gold/actor/" in target_path
    assert target_path.startswith("abfss://")
