"""End-to-end pipeline integration test.

Exercises the full Bronze → Silver → Delta Lake export pipeline logic:

1. Creates temporary JSON fixture files on disk
2. Runs the Bronze read pattern (read_json_auto)
3. Runs the Silver CDC (full-extraction) SQL to detect I/U/D
4. Materialises the Silver table in DuckDB
5. Exports to a local Delta Lake table via ``write_deltalake``
6. Reads the Delta back and verifies row counts, PK values, and CDC ops
7. Runs a second incremental export and verifies no duplicates
"""

import json
import os

import duckdb
import pyarrow as pa
import pytest

# deltalake is a project dependency — these tests exercise the real write path.
from deltalake import DeltaTable
from deltalake.writer import write_deltalake


@pytest.fixture()
def e2e_fixture(tmp_path):
    """Create fixture JSON files + tmp directories for the pipeline."""
    # ── Source JSON files ──
    src_dir = tmp_path / "source" / "item"
    src_dir.mkdir(parents=True)

    file1 = [
        {"id": 1, "name": "Apple", "price": 1.50, "updated": "2024-01-01T00:00:00"},
        {"id": 2, "name": "Banana", "price": 0.75, "updated": "2024-01-01T00:00:00"},
    ]
    file2 = [
        {"id": 1, "name": "Apple", "price": 1.75, "updated": "2024-02-01T00:00:00"},  # changed
        {"id": 2, "name": "Banana", "price": 0.75, "updated": "2024-02-01T00:00:00"},  # same
        {"id": 3, "name": "Cherry", "price": 2.00, "updated": "2024-02-01T00:00:00"},  # new
    ]

    (src_dir / "item_20240101_120000.json").write_text(
        "\n".join(json.dumps(r) for r in file1) + "\n"
    )
    (src_dir / "item_20240201_120000.json").write_text(
        "\n".join(json.dumps(r) for r in file2) + "\n"
    )

    # ── Delta Lake output directory ──
    delta_dir = tmp_path / "delta" / "silver_item"

    return {
        "source_dir": str(tmp_path / "source"),
        "delta_dir": str(delta_dir),
        "tmp_path": tmp_path,
    }


def _run_cdc_sql(conn: duckdb.DuckDBPyConnection, source_dir: str) -> None:
    """Run the full-extraction CDC pipeline and materialise a Silver table."""
    glob_json = os.path.join(source_dir, "item", "item_*.json*")

    # Step 1: Create Bronze view
    conn.execute(f"""
        CREATE OR REPLACE VIEW bronze_item AS
        SELECT DISTINCT
               COLUMNS(c -> c != 'filename' AND NOT starts_with(c, '_dlt_'))
        ,      SUBSTRING(filename,
                   LENGTH(filename)
                   - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
        ,      'TEST' AS LKHS_source_system_code
        FROM   read_json_auto('{glob_json}', filename=True, union_by_name=true)
    """)

    # Step 2: Run Silver CDC (full-extraction pattern)
    cdc_sql = f"""
    WITH CTE_BRONZE AS (
        SELECT src.*
        ,      sha256(CONCAT(
                   COALESCE(src.name::VARCHAR, '<NULL>'), ']##[',
                   COALESCE(src.price::VARCHAR, '<NULL>'), ']##['
               )) AS LKHS_hash_value
        ,      CAST(MIN(src.updated) OVER (PARTITION BY src.id) AS DATETIME) AS LKHS_date_inserted_src
        FROM   bronze_item src
    )
    ,CTE_FILES AS (
        SELECT LKHS_filename
        ,      strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - POSITION('.' IN REVERSE(LKHS_filename)) - 14, 15), '%Y%m%d_%H%M%S') AS LKHS_date_valid_from
        ,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
        ,      LEAD(LKHS_filename) OVER (ORDER BY LKHS_filename) AS LKHS_filename_next
        ,      LEAD(strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - POSITION('.' IN REVERSE(LKHS_filename)) - 14, 15), '%Y%m%d_%H%M%S'))
               OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_next
        FROM   (SELECT SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
                FROM   read_text('{glob_json}')
               ) files
    )
    ,CTE_BRONZE_INCL_LAG AS (
        SELECT CTE_BRONZE.*
        ,      CTE_FILES.LKHS_date_valid_from
        ,      CTE_BRONZE_PREV.LKHS_hash_value AS LKHS_hash_value_previous
        ,      CTE_BRONZE_PREV.id AS LKHS_pk_prev
        FROM       CTE_BRONZE
        INNER JOIN CTE_FILES ON CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
        LEFT  JOIN CTE_BRONZE CTE_BRONZE_PREV
               ON  CTE_FILES.LKHS_filename_previous = CTE_BRONZE_PREV.LKHS_filename
               AND CTE_BRONZE.id = CTE_BRONZE_PREV.id
    )
    SELECT  b.id, b.name, b.price, b.updated
    ,       b.LKHS_filename, b.LKHS_source_system_code
    ,       b.LKHS_hash_value, b.LKHS_date_inserted_src
    ,       b.LKHS_date_valid_from
    ,       CURRENT_TIMESTAMP AS LKHS_date_inserted
    ,       CASE
                WHEN b.LKHS_pk_prev IS NULL THEN 'I'
                WHEN b.LKHS_pk_prev IS NOT NULL AND b.LKHS_hash_value != b.LKHS_hash_value_previous THEN 'U'
            END AS LKHS_cdc_operation
    FROM    CTE_BRONZE_INCL_LAG b
    WHERE   CASE
                WHEN b.LKHS_pk_prev IS NULL THEN 'I'
                WHEN b.LKHS_pk_prev IS NOT NULL AND b.LKHS_hash_value != b.LKHS_hash_value_previous THEN 'U'
            END IN ('I', 'U')
    """
    conn.execute(f"CREATE TABLE silver_item AS ({cdc_sql})")


# ── Tests ────────────────────────────────────────────────────────────


def test_e2e_bronze_to_silver_row_count(e2e_fixture):
    """CDC should produce 4 rows: 2 Inserts (file 1) + 1 Insert + 1 Update (file 2)."""
    conn = duckdb.connect(":memory:")
    _run_cdc_sql(conn, e2e_fixture["source_dir"])

    count = conn.execute("SELECT COUNT(*) FROM silver_item").fetchone()[0]
    assert count == 4

    ops = conn.execute(
        "SELECT LKHS_cdc_operation, COUNT(*) FROM silver_item GROUP BY 1 ORDER BY 1"
    ).fetchdf()
    op_dict = dict(zip(ops.iloc[:, 0], ops.iloc[:, 1]))
    assert op_dict["I"] == 3  # rows 1,2 from file1 + row 3 from file2
    assert op_dict["U"] == 1  # row 1 updated in file2
    conn.close()


def test_e2e_silver_to_delta_first_load(e2e_fixture):
    """First Delta export should write all Silver rows with mode=overwrite."""
    conn = duckdb.connect(":memory:")
    _run_cdc_sql(conn, e2e_fixture["source_dir"])

    arrow_table = conn.execute("SELECT * FROM silver_item").fetch_arrow_table()
    delta_path = e2e_fixture["delta_dir"]
    os.makedirs(delta_path, exist_ok=True)

    write_deltalake(delta_path, arrow_table, mode="overwrite")

    dt = DeltaTable(delta_path)
    assert dt.to_pyarrow_table().num_rows == 4
    conn.close()


def test_e2e_incremental_export_no_duplicates(e2e_fixture):
    """Second incremental export (same data) should append 0 rows."""
    conn = duckdb.connect(":memory:")
    _run_cdc_sql(conn, e2e_fixture["source_dir"])

    arrow_table = conn.execute("SELECT * FROM silver_item").fetch_arrow_table()
    delta_path = e2e_fixture["delta_dir"]
    os.makedirs(delta_path, exist_ok=True)

    # First load
    write_deltalake(delta_path, arrow_table, mode="overwrite")

    # Incremental: LEFT JOIN to find new rows
    dt = DeltaTable(delta_path)
    existing = dt.to_pyarrow_table()
    conn.register("target_table", existing)

    new_rows = conn.execute("""
        SELECT src.*
        FROM   silver_item src
        LEFT JOIN target_table tgt
               ON src.id = tgt.id
              AND src.LKHS_date_valid_from = tgt.LKHS_date_valid_from
        WHERE  tgt.id IS NULL
    """).fetch_arrow_table()

    assert new_rows.num_rows == 0
    conn.close()


def test_e2e_incremental_export_appends_new_rows(e2e_fixture):
    """Adding a third file with a new row should produce 1 appended row."""
    conn = duckdb.connect(":memory:")
    _run_cdc_sql(conn, e2e_fixture["source_dir"])

    arrow_table = conn.execute("SELECT * FROM silver_item").fetch_arrow_table()
    delta_path = e2e_fixture["delta_dir"]
    os.makedirs(delta_path, exist_ok=True)

    # First load
    write_deltalake(delta_path, arrow_table, mode="overwrite")

    # Add a third extraction file with a new row
    src_dir = os.path.join(e2e_fixture["source_dir"], "item")
    file3 = [
        {"id": 1, "name": "Apple", "price": 1.75, "updated": "2024-03-01T00:00:00"},
        {"id": 2, "name": "Banana", "price": 0.75, "updated": "2024-03-01T00:00:00"},
        {"id": 3, "name": "Cherry", "price": 2.00, "updated": "2024-03-01T00:00:00"},
        {"id": 4, "name": "Durian", "price": 5.00, "updated": "2024-03-01T00:00:00"},  # new!
    ]
    with open(os.path.join(src_dir, "item_20240301_120000.json"), "w") as f:
        f.write("\n".join(json.dumps(r) for r in file3) + "\n")

    # Re-run CDC with all 3 files
    conn2 = duckdb.connect(":memory:")
    _run_cdc_sql(conn2, e2e_fixture["source_dir"])

    # Incremental export
    dt = DeltaTable(delta_path)
    existing = dt.to_pyarrow_table()
    conn2.register("target_table", existing)

    new_rows = conn2.execute("""
        SELECT src.*
        FROM   silver_item src
        LEFT JOIN target_table tgt
               ON src.id = tgt.id
              AND src.LKHS_date_valid_from = tgt.LKHS_date_valid_from
        WHERE  tgt.id IS NULL
    """).fetch_arrow_table()

    # Row 4 is new (Insert in file 3)
    assert new_rows.num_rows >= 1
    ids = new_rows.column("id").to_pylist()
    assert 4 in ids

    # Append and verify total
    write_deltalake(delta_path, new_rows, mode="append", schema_mode="merge")
    final = DeltaTable(delta_path).to_pyarrow_table()
    assert final.num_rows > 4  # original 4 + new rows

    conn.close()
    conn2.close()
