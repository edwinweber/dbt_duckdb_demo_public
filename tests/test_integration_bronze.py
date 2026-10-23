"""Integration tests for the Bronze layer.

Creates temporary JSON fixture files on disk, then uses DuckDB
``read_json_auto`` to verify that the Bronze view pattern (as generated
by ``generate_model_bronze``) correctly:

* reads all rows from multiple JSON extractions,
* extracts the LKHS_filename from the path,
* excludes internal ``_dlt_`` columns and the raw ``filename`` column.
"""

import json
import os

import pytest

import duckdb


@pytest.fixture()
def bronze_fixture_dir(tmp_path):
    """Write two JSON extraction files for a fake 'widget' entity.

    File 1 (older): rows A and B
    File 2 (newer): rows B-changed and C-new, A is absent ⇒ deleted
    """
    entity_dir = tmp_path / "widget"
    entity_dir.mkdir()

    file1_name = "widget_20240101_120000.json"
    file2_name = "widget_20240201_120000.json"

    rows_file1 = [
        {"id": 1, "name": "Widget-A", "colour": "red", "_dlt_load_id": "x1"},
        {"id": 2, "name": "Widget-B", "colour": "blue", "_dlt_load_id": "x1"},
    ]
    rows_file2 = [
        {"id": 2, "name": "Widget-B", "colour": "green", "_dlt_load_id": "x2"},
        {"id": 3, "name": "Widget-C", "colour": "yellow", "_dlt_load_id": "x2"},
    ]

    (entity_dir / file1_name).write_text("\n".join(json.dumps(r) for r in rows_file1) + "\n")
    (entity_dir / file2_name).write_text("\n".join(json.dumps(r) for r in rows_file2) + "\n")

    return tmp_path


# ── Tests for the "all files" Bronze view pattern ────────────────────


def test_bronze_reads_all_rows_from_all_files(bronze_fixture_dir):
    """The Bronze view should return every row from every extraction file."""
    conn = duckdb.connect(":memory:")
    glob = os.path.join(bronze_fixture_dir, "widget", "widget_*.json*")

    result = conn.execute(
        f"""
        SELECT DISTINCT
               COLUMNS(lambda c: c != 'filename' AND NOT starts_with(c, '_dlt_'))
        ,      SUBSTRING(filename,
                   LENGTH(filename)
                   - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
        FROM   read_json_auto('{glob}', filename=True, union_by_name=true)
        """
    ).fetchall()

    assert len(result) == 4  # 2 rows × file1, 2 rows × file2


def test_bronze_extracts_lkhs_filename(bronze_fixture_dir):
    """LKHS_filename should contain just the file name, not the full path."""
    conn = duckdb.connect(":memory:")
    glob = os.path.join(bronze_fixture_dir, "widget", "widget_*.json*")

    filenames = (
        conn.execute(
            f"""
        SELECT DISTINCT
               SUBSTRING(filename,
                   LENGTH(filename)
                   - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
        FROM   read_json_auto('{glob}', filename=True, union_by_name=true)
        """
        )
        .fetchdf()["LKHS_filename"]
        .tolist()
    )

    assert set(filenames) == {
        "widget_20240101_120000.json",
        "widget_20240201_120000.json",
    }


def test_bronze_excludes_dlt_columns(bronze_fixture_dir):
    """_dlt_* columns should not appear in the output."""
    conn = duckdb.connect(":memory:")
    glob = os.path.join(bronze_fixture_dir, "widget", "widget_*.json*")

    columns = conn.execute(
        f"""
        SELECT DISTINCT COLUMNS(lambda c: c != 'filename' AND NOT starts_with(c, '_dlt_'))
        FROM   read_json_auto('{glob}', filename=True, union_by_name=true)
        LIMIT 1
        """
    ).description

    col_names = [desc[0] for desc in columns]
    assert not any(c.startswith("_dlt_") for c in col_names)
    assert "filename" not in col_names
