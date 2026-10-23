"""Tests for the DuckLake cleanup asset.

The cleanup test runs against a real local DuckLake catalog in ``tmp_path`` and
is skipped when the ``ducklake`` extension cannot be installed (offline).
"""

import os
import time
from unittest.mock import patch

import pytest
from dagster import materialize

import duckdb
from ddd_python.ddd_dagster.ducklake_cleanup_assets import ducklake_cleanup_asset

_GVE = "ddd_python.ddd_dagster.ducklake_cleanup_assets.get_variables_from_env.__dict__"
_PATH_UTILS = "ddd_python.ddd_utils.path_utils.get_variables_from_env.__dict__"


def _ducklake_available() -> bool:
    try:
        c = duckdb.connect()
        c.execute("INSTALL ducklake; LOAD ducklake;")
        c.close()
        return True
    except Exception:
        return False


def _materialize():
    result = materialize([ducklake_cleanup_asset])
    assert result.success
    (event,) = result.get_asset_materialization_events()
    return event.materialization.metadata


def test_skipped_when_silver_is_native_duckdb():
    with patch.dict(_PATH_UTILS, {"SILVER_STORAGE_FORMAT": "duckdb"}):
        metadata = _materialize()
    assert metadata["skipped"].value is True


@pytest.mark.skipif(not _ducklake_available(), reason="DuckDB 'ducklake' extension unavailable")
def test_cleanup_removes_current_temp_but_keeps_dbt_tmp(tmp_path):
    data_path = tmp_path / "data"
    stale = data_path / "main_silver" / "silver_x_current_temp"
    live = data_path / "main_silver" / "silver_x__dbt_tmp"
    stale.mkdir(parents=True)
    live.mkdir(parents=True)
    (stale / "old.parquet").write_bytes(b"x")
    (live / "live.parquet").write_bytes(b"x")
    # The sweep only removes directories nothing has touched for a while.
    two_hours_ago = time.time() - 2 * 3600
    for path in (stale / "old.parquet", stale):
        os.utime(path, (two_hours_ago, two_hours_ago))

    catalog = tmp_path / "catalog.ducklake"
    db_file = tmp_path / "main.duckdb"
    seed = duckdb.connect(str(db_file))
    seed.execute("INSTALL ducklake; LOAD ducklake;")
    seed.execute(f"ATTACH 'ducklake:{catalog}' AS ducklake_catalog (DATA_PATH '{data_path}/')")
    seed.execute("CREATE SCHEMA ducklake_catalog.main_silver")
    seed.execute("CREATE TABLE ducklake_catalog.main_silver.t AS SELECT 1 AS id")
    seed.close()

    env = {
        "SILVER_STORAGE_FORMAT": "ducklake",
        "DUCKLAKE_CATALOG_LOCATION": str(catalog),
        "DUCKLAKE_DATA_PATH": f"{data_path}/",
        "DUCKDB_DATABASE_LOCATION": str(db_file),
    }
    with patch.dict(_GVE, env), patch.dict(_PATH_UTILS, env):
        metadata = _materialize()

    assert metadata["orphan_dirs_removed"].value == 1
    assert not stale.exists()
    assert (live / "live.parquet").exists()  # live table data must never be deleted
