"""Integration tests for Silver Delta Lake export logic.

The dedup read uses DuckDB's ``delta_scan`` against the **real** existing Delta
table, so these tests create actual local Delta tables in a ``tmp_path`` and run
the export end-to-end (``STORAGE_TARGET=local``, no Azure).  Only the write path
still uses ``deltalake.write_deltalake`` (DuckDB cannot write Delta).
"""

import os
from datetime import datetime
from unittest.mock import patch

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

import ddd_python.ddd_dlt.export_main_silver_to_fabric_silver as silver_mod
import duckdb
from ddd_python.ddd_utils import configuration_variables
from ddd_python.ddd_utils.path_utils import build_delta_export_path, open_export_connection


def _delta_extension_available() -> bool:
    try:
        c = duckdb.connect()
        c.execute("INSTALL delta; LOAD delta;")
        c.close()
        return True
    except Exception:
        return False


# delta_scan is required for the dedup read; skip the module if unavailable.
pytestmark = pytest.mark.skipif(
    not _delta_extension_available(),
    reason="DuckDB 'delta' extension not available (needed for delta_scan)",
)


def _ts(*values):
    """Build a pyarrow timestamp[us] array from datetime values."""
    return pa.array(list(values), type=pa.timestamp("us"))


@pytest.fixture
def silver_connection():
    """In-memory DuckDB with a sample Silver table and the delta extension."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL delta; LOAD delta;")
    conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")
    conn.execute("""
        CREATE TABLE main_silver.silver_ddd_aktoer AS
        SELECT 1 AS id, '2024-01-01'::TIMESTAMP AS LKHS_date_valid_from, 'Alice' AS name
        UNION ALL
        SELECT 2, '2024-01-02'::TIMESTAMP, 'Bob'
        UNION ALL
        SELECT 3, '2024-01-03'::TIMESTAMP, 'Charlie'
    """)
    yield conn
    conn.close()


def _local_env(tmp_path):
    """Patch env vars so the export targets a local Delta path under tmp_path.

    Patches the shared lazy-env module __dict__ (bypassing __getattr__), which
    both this module and path_utils reference by identity.
    """
    return patch.dict(
        "ddd_python.ddd_dlt.export_main_silver_to_fabric_silver.get_variables_from_env.__dict__",
        {
            "STORAGE_TARGET": "local",
            "LOCAL_STORAGE_PATH": str(tmp_path),
            "DUCKDB_DATABASE": "memory",
            "SILVER_STORAGE_FORMAT": "duckdb",
        },
        clear=False,
    )


def test_incremental_append_finds_new_rows(silver_connection, tmp_path):
    """Existing Delta table has rows 1 and 2 → only row 3 (Charlie) is appended."""
    with _local_env(tmp_path):
        path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        write_deltalake(
            path,
            pa.table(
                {
                    "id": [1, 2],
                    "LKHS_date_valid_from": _ts(datetime(2024, 1, 1), datetime(2024, 1, 2)),
                    "name": ["Alice", "Bob"],
                }
            ),
            mode="overwrite",
        )
        rows = silver_mod.export_single_silver_table(silver_connection, "silver_ddd_aktoer")

        assert rows == 1  # only Charlie is new
        final = DeltaTable(path).to_pyarrow_table()
        assert final.num_rows == 3
        assert set(final.column("id").to_pylist()) == {1, 2, 3}


def test_incremental_no_new_rows_skips_write(silver_connection, tmp_path):
    """When all rows already exist, nothing is appended and the table is unchanged."""
    with _local_env(tmp_path):
        path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        write_deltalake(
            path,
            pa.table(
                {
                    "id": [1, 2, 3],
                    "LKHS_date_valid_from": _ts(
                        datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 3)
                    ),
                    "name": ["Alice", "Bob", "Charlie"],
                }
            ),
            mode="overwrite",
        )
        version_before = DeltaTable(path).version()

        # Patch write_deltalake to prove it is NOT called when there are no new rows.
        with patch.object(silver_mod, "write_deltalake") as mock_write:
            rows = silver_mod.export_single_silver_table(silver_connection, "silver_ddd_aktoer")

        assert rows == 0
        mock_write.assert_not_called()
        assert DeltaTable(path).version() == version_before  # untouched


def test_first_load_creates_table(silver_connection, tmp_path):
    """With no existing Delta table, all rows are written via overwrite."""
    with _local_env(tmp_path):
        path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        rows = silver_mod.export_single_silver_table(silver_connection, "silver_ddd_aktoer")

        assert rows == 3
        assert DeltaTable.is_deltatable(path)
        assert DeltaTable(path).to_pyarrow_table().num_rows == 3


def test_unexpected_error_is_raised(silver_connection, tmp_path):
    """Exceptions from the existence check should propagate."""
    with _local_env(tmp_path), patch.object(silver_mod, "DeltaTable") as mock_dt:
        mock_dt.is_deltatable.side_effect = ConnectionError("network unreachable")
        with pytest.raises(ConnectionError, match="network unreachable"):
            silver_mod.export_single_silver_table(silver_connection, "silver_ddd_aktoer")


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
    """In-memory DuckDB with a sample Rfam Silver table (PK=rfam_acc)."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL delta; LOAD delta;")
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


def test_rfam_incremental_append_uses_real_pk(rfam_silver_connection, tmp_path):
    """Rfam tables use rfam_acc (not id) — only the new PK+date row is appended."""
    with _local_env(tmp_path):
        path, _ = build_delta_export_path("silver", "silver_rfam_family")
        write_deltalake(
            path,
            pa.table(
                {
                    "rfam_acc": ["RF00001", "RF00002"],
                    "LKHS_date_valid_from": _ts(datetime(2024, 1, 1), datetime(2024, 1, 2)),
                    "rfam_id": ["family_a", "family_b"],
                }
            ),
            mode="overwrite",
        )
        rows = silver_mod.export_single_silver_table(rfam_silver_connection, "silver_rfam_family")

        assert rows == 1  # only RF00003 is new
        final = DeltaTable(path).to_pyarrow_table()
        assert final.num_rows == 3
        assert set(final.column("rfam_acc").to_pylist()) == {"RF00001", "RF00002", "RF00003"}


def test_rfam_first_load_creates_table(rfam_silver_connection, tmp_path):
    """When no Delta table exists, all Rfam rows are written with overwrite."""
    with _local_env(tmp_path):
        path, _ = build_delta_export_path("silver", "silver_rfam_family")
        rows = silver_mod.export_single_silver_table(rfam_silver_connection, "silver_rfam_family")

        assert rows == 3
        assert DeltaTable(path).to_pyarrow_table().num_rows == 3


# ── DuckLake mode (SILVER_STORAGE_FORMAT=ducklake) ───────────────────


def _has_ducklake() -> bool:
    try:
        c = duckdb.connect()
        c.execute("INSTALL ducklake; LOAD ducklake;")
        c.close()
        return True
    except Exception:
        return False


def _make_ducklake_silver_table(
    catalog_path: str, data_path: str, table: str, rows_sql: str
) -> None:
    """Create ``ducklake_catalog.main_silver.<table>`` in a fresh DuckLake catalog."""
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH 'ducklake:{catalog_path}' AS ducklake_catalog (DATA_PATH '{data_path}')")
    con.execute("CREATE SCHEMA IF NOT EXISTS ducklake_catalog.main_silver")
    con.execute(f"CREATE TABLE ducklake_catalog.main_silver.{table} AS {rows_sql}")
    con.close()


@pytest.mark.skipif(not _has_ducklake(), reason="DuckDB 'ducklake' extension not available")
def test_ducklake_mode_reads_silver_from_catalog_and_dedups(tmp_path):
    """In ducklake mode the export reads Silver from the attached DuckLake catalog
    (not the main DuckDB file) and still does first-load overwrite + append dedup."""
    catalog = str(tmp_path / "catalog.ducklake")
    dldata = str(tmp_path / "dldata")
    warehouse = str(tmp_path / "warehouse.duckdb")
    os.makedirs(dldata, exist_ok=True)
    duckdb.connect(warehouse).close()  # main DuckDB file must exist for read-only connect

    _make_ducklake_silver_table(
        catalog,
        dldata,
        "silver_ddd_aktoer",
        "SELECT 1 AS id, TIMESTAMP '2024-01-01' AS LKHS_date_valid_from, 'Alice' AS name "
        "UNION ALL SELECT 2, TIMESTAMP '2024-01-02', 'Bob' "
        "UNION ALL SELECT 3, TIMESTAMP '2024-01-03', 'Charlie'",
    )

    env = {
        "STORAGE_TARGET": "local",
        "LOCAL_STORAGE_PATH": str(tmp_path),
        "SILVER_STORAGE_FORMAT": "ducklake",
        "DUCKDB_DATABASE_LOCATION": warehouse,
        "DUCKDB_DATABASE": "warehouse",
        "DUCKLAKE_CATALOG_LOCATION": catalog,
        "DUCKLAKE_DATA_PATH": dldata,
    }
    with patch.dict(silver_mod.get_variables_from_env.__dict__, env, clear=False):
        assert silver_mod._silver_source_database() == "ducklake_catalog"
        path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")

        connection = open_export_connection()
        try:
            # First load: all 3 rows written (overwrite).
            first = silver_mod.export_single_silver_table(connection, "silver_ddd_aktoer")
            # Second run: nothing new (delta_scan dedup against the just-written table).
            second = silver_mod.export_single_silver_table(connection, "silver_ddd_aktoer")
        finally:
            connection.close()

    assert first == 3
    assert second == 0
    assert DeltaTable(path).to_pyarrow_table().num_rows == 3
