"""Tests for the DuckLake data-directory sweep (ddd_utils.ducklake_maintenance)."""

import os
import time

import duckdb
from ddd_python.ddd_utils.ducklake_maintenance import (
    SWEEP_FUNCTION_NAME,
    register_sweep_function,
    sweep_current_temp_dirs,
)


def _make_dir(parent, name, age_minutes):
    directory = parent / name
    directory.mkdir(parents=True)
    file = directory / "part.parquet"
    file.write_bytes(b"x")
    stamp = time.time() - age_minutes * 60
    for path in (file, directory):
        os.utime(path, (stamp, stamp))
    return directory


def test_removes_old_current_temp_dirs_only(tmp_path):
    schema = tmp_path / "main_silver"
    old = _make_dir(schema, "silver_a_current_temp", age_minutes=180)
    live_table = _make_dir(schema, "silver_a__dbt_tmp", age_minutes=180)
    keep = _make_dir(schema, "silver_a", age_minutes=180)

    assert sweep_current_temp_dirs(tmp_path, min_age_minutes=60) == 1

    assert not old.exists()
    assert live_table.exists()  # __dbt_tmp holds live table data
    assert keep.exists()


def test_recent_current_temp_dir_is_left_alone(tmp_path):
    """A dbt run may still be using it (dbt steps can run in parallel)."""
    recent = _make_dir(tmp_path / "main_silver", "silver_b_current_temp", age_minutes=5)
    assert sweep_current_temp_dirs(tmp_path, min_age_minutes=60) == 0
    assert recent.exists()


def test_one_fresh_file_keeps_the_whole_directory(tmp_path):
    directory = _make_dir(tmp_path / "main_silver", "silver_c_current_temp", age_minutes=300)
    (directory / "new.parquet").write_bytes(b"x")  # modified just now
    assert sweep_current_temp_dirs(tmp_path, min_age_minutes=60) == 0
    assert directory.exists()


def test_min_age_zero_removes_everything_matching(tmp_path):
    directory = _make_dir(tmp_path / "main_silver", "silver_d_current_temp", age_minutes=0)
    assert sweep_current_temp_dirs(tmp_path, min_age_minutes=0) == 1
    assert not directory.exists()


def test_missing_data_root_is_a_no_op(tmp_path):
    assert sweep_current_temp_dirs(tmp_path / "does_not_exist") == 0


def test_sweep_is_callable_as_a_sql_function(tmp_path):
    """The server registers it on its connection; clients call it over quack."""
    old = _make_dir(tmp_path / "main_silver", "silver_e_current_temp", age_minutes=180)
    fresh = _make_dir(tmp_path / "main_silver", "silver_f_current_temp", age_minutes=1)
    connection = duckdb.connect(":memory:")
    register_sweep_function(connection, str(tmp_path))
    row = connection.execute(f"SELECT {SWEEP_FUNCTION_NAME}(60)").fetchone()
    assert row is not None and row[0] == 1
    assert not old.exists()
    assert fresh.exists()
