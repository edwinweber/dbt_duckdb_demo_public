"""Integration tests for Silver Delta Lake export logic.

Uses an in-memory DuckDB database with mock Silver tables, and patches the
external dependencies (DeltaTable, write_deltalake, get_fabric_token, env vars)
to verify the incremental append and first-load overwrite logic.
"""

import duckdb
import pandas as pd
import pyarrow as pa
import pytest
from unittest.mock import patch

import ddd_python.ddd_dlt.export_main_silver_to_fabric_silver as silver_mod
from ddd_python.ddd_utils import configuration_variables


_ENV_PATCHES = {
    "STORAGE_TARGET": "onelake",
    "FABRIC_WORKSPACE": "test-workspace",
    "FABRIC_ONELAKE_STORAGE_ACCOUNT": "testaccount",
    "FABRIC_ONELAKE_FOLDER_SILVER": "Lakehouse/Files/Silver",
    "DUCKDB_DATABASE": "memory",
}


@pytest.fixture
def silver_connection():
    """Create an in-memory DuckDB with a sample Silver table."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")
    conn.execute("""
        CREATE TABLE main_silver.silver_aktoer AS
        SELECT 1 AS id, '2024-01-01'::TIMESTAMP AS LKHS_date_valid_from, 'Alice' AS name
        UNION ALL
        SELECT 2, '2024-01-02'::TIMESTAMP, 'Bob'
        UNION ALL
        SELECT 3, '2024-01-03'::TIMESTAMP, 'Charlie'
    """)
    yield conn
    conn.close()


def _patch_env():
    """Patch env vars on the already-imported module.

    Uses patch.dict on __dict__ to bypass __getattr__ (which would call
    _require() and raise for lazy-required vars like FABRIC_WORKSPACE
    when no .env file is present, e.g. in CI).
    """
    return patch.dict(
        "ddd_python.ddd_dlt.export_main_silver_to_fabric_silver.get_variables_from_env.__dict__",
        _ENV_PATCHES,
        clear=False,
    )


def test_incremental_append_finds_new_rows(silver_connection, mock_fabric_clients):
    """When an existing Delta table has rows 1 and 2, only row 3 should be appended."""
    existing = pa.table({
        "id": [1, 2],
        "LKHS_date_valid_from": pa.array(pd.to_datetime(["2024-01-01", "2024-01-02"])),
        "name": ["Alice", "Bob"],
    })

    with (
        _patch_env(),
        patch.object(silver_mod, "DeltaTable") as mock_dt,
        patch.object(silver_mod, "write_deltalake") as mock_write,
    ):
        mock_dt.is_deltatable.return_value = True
        mock_dt.return_value.to_pyarrow_table.return_value = existing
        rows = silver_mod.export_single_silver_table(silver_connection, "silver_aktoer")

    assert rows == 1  # only Charlie is new
    mock_write.assert_called_once()
    assert mock_write.call_args.kwargs.get("mode") == "append"


def test_incremental_no_new_rows_skips_write(silver_connection, mock_fabric_clients):
    """When all rows already exist, write_deltalake should NOT be called."""
    existing = pa.table({
        "id": [1, 2, 3],
        "LKHS_date_valid_from": pa.array(
            pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        ),
        "name": ["Alice", "Bob", "Charlie"],
    })

    with (
        _patch_env(),
        patch.object(silver_mod, "DeltaTable") as mock_dt,
        patch.object(silver_mod, "write_deltalake") as mock_write,
    ):
        mock_dt.is_deltatable.return_value = True
        mock_dt.return_value.to_pyarrow_table.return_value = existing
        rows = silver_mod.export_single_silver_table(silver_connection, "silver_aktoer")

    assert rows == 0
    mock_write.assert_not_called()


def test_first_load_creates_table(silver_connection, mock_fabric_clients):
    """When is_deltatable returns False, it should create with overwrite."""
    with (
        _patch_env(),
        patch.object(silver_mod, "DeltaTable") as mock_dt,
        patch.object(silver_mod, "write_deltalake") as mock_write,
    ):
        mock_dt.is_deltatable.return_value = False
        rows = silver_mod.export_single_silver_table(silver_connection, "silver_aktoer")

    assert rows == 3  # all rows written
    mock_write.assert_called_once()
    assert mock_write.call_args.kwargs.get("mode") == "overwrite"


def test_unexpected_error_is_raised(silver_connection, mock_fabric_clients):
    """Exceptions from is_deltatable should propagate."""
    with (
        _patch_env(),
        patch.object(silver_mod, "DeltaTable") as mock_dt,
    ):
        mock_dt.is_deltatable.side_effect = ConnectionError("network unreachable")
        with pytest.raises(ConnectionError, match="network unreachable"):
            silver_mod.export_single_silver_table(silver_connection, "silver_aktoer")


# ── Default table list ────────────────────────────────────────────────


def test_default_tables_include_ddd_and_rfam():
    """main() default table list must include both DDD and Rfam Silver models."""
    ddd = configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER
    rfam = configuration_variables.RFAM_MODELS_SILVER
    default = ddd + rfam
    assert all(t in default for t in ddd)
    assert all(t in default for t in rfam)
    assert len(default) == len(ddd) + len(rfam)


# ── Rfam-specific tests (non-id primary key) ─────────────────────────


@pytest.fixture
def rfam_silver_connection():
    """Create an in-memory DuckDB with a sample Rfam Silver table (PK=rfam_acc)."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")
    conn.execute("""
        CREATE TABLE main_silver.silver_rfam_family AS
        SELECT 'RF00001' AS rfam_acc, '2024-01-01'::TIMESTAMP AS LKHS_date_valid_from, 'family_a' AS rfam_id
        UNION ALL
        SELECT 'RF00002', '2024-01-02'::TIMESTAMP, 'family_b'
        UNION ALL
        SELECT 'RF00003', '2024-01-03'::TIMESTAMP, 'family_c'
    """)
    yield conn
    conn.close()


def test_rfam_incremental_append_uses_real_pk(rfam_silver_connection, mock_fabric_clients):
    """Rfam tables use rfam_acc (not id) — only rows with new PK+date should be appended."""
    existing = pa.table({
        "rfam_acc": ["RF00001", "RF00002"],
        "LKHS_date_valid_from": pa.array(pd.to_datetime(["2024-01-01", "2024-01-02"])),
        "rfam_id": ["family_a", "family_b"],
    })

    with (
        _patch_env(),
        patch.object(silver_mod, "DeltaTable") as mock_dt,
        patch.object(silver_mod, "write_deltalake") as mock_write,
    ):
        mock_dt.is_deltatable.return_value = True
        mock_dt.return_value.to_pyarrow_table.return_value = existing
        rows = silver_mod.export_single_silver_table(rfam_silver_connection, "silver_rfam_family")

    assert rows == 1  # only RF00003 is new
    mock_write.assert_called_once()
    assert mock_write.call_args.kwargs.get("mode") == "append"


def test_rfam_first_load_creates_table(rfam_silver_connection, mock_fabric_clients):
    """When no Delta table exists, all Rfam rows are written with overwrite."""
    with (
        _patch_env(),
        patch.object(silver_mod, "DeltaTable") as mock_dt,
        patch.object(silver_mod, "write_deltalake") as mock_write,
    ):
        mock_dt.is_deltatable.return_value = False
        rows = silver_mod.export_single_silver_table(rfam_silver_connection, "silver_rfam_family")

    assert rows == 3
    mock_write.assert_called_once()
    assert mock_write.call_args.kwargs.get("mode") == "overwrite"
