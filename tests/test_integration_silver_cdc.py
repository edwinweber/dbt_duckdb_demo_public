"""Integration tests for the Silver CDC layer.

Reproduces the core SQL logic from ``generate_model_silver_full_extraction``
and ``generate_model_silver_incr_extraction`` macros entirely in DuckDB,
using temporary fixture JSON files, to verify:

* Insert (I) detection on first file load
* Update (U) detection via hash comparison across files
* Delete (D) detection when a row disappears from the next extraction
* Correct LKHS_date_valid_from derived from filename timestamps
* NOT EXISTS dedup guard against re-inserting existing rows
* Current-version (_cv) view returns only the latest row per PK
* Incremental delete path (--full-refresh): _current_temp anti-join
  against bronze_latest produces 'D' rows for absent PKs
"""

import json
import os

import pytest

import duckdb

# ── Fixture ──────────────────────────────────────────────────────────


@pytest.fixture()
def silver_fixture_dir(tmp_path):
    """Write three extraction files simulating an entity lifecycle.

    File 1 (2024-01-01): rows 1 and 2
    File 2 (2024-02-01): rows 1 (changed) and 2 (same) — triggers U for row 1
    File 3 (2024-03-01): row 2 only — row 1 is deleted, triggers D for row 1
    """
    entity_dir = tmp_path / "thing"
    entity_dir.mkdir()

    files = {
        "thing_20240101_120000.json": [
            {"id": 1, "name": "Alpha", "value": 10, "opdateringsdato": "2024-01-01T00:00:00"},
            {"id": 2, "name": "Beta", "value": 20, "opdateringsdato": "2024-01-01T00:00:00"},
        ],
        "thing_20240201_120000.json": [
            {"id": 1, "name": "Alpha-v2", "value": 11, "opdateringsdato": "2024-02-01T00:00:00"},
            {"id": 2, "name": "Beta", "value": 20, "opdateringsdato": "2024-02-01T00:00:00"},
        ],
        "thing_20240301_120000.json": [
            {"id": 2, "name": "Beta", "value": 20, "opdateringsdato": "2024-03-01T00:00:00"},
        ],
    }

    for fname, rows in files.items():
        (entity_dir / fname).write_text("\n".join(json.dumps(r) for r in rows) + "\n")

    return tmp_path


def _build_full_extract_cdc_sql(data_dir: str, file_name: str = "thing") -> str:
    """Return the Silver full-extraction CDC SQL, matching the dbt macro logic.

    This mirrors ``generate_model_silver_full_extraction`` but with
    hard-coded values instead of Jinja references.
    """
    glob_json = os.path.join(data_dir, file_name, f"{file_name}_*.json*")
    glob_text = glob_json  # same glob for read_text

    return f"""
    WITH CTE_BRONZE AS (
        SELECT src.*
        ,      SUBSTRING(src.filename,
                   LENGTH(src.filename)
                   - POSITION('/' IN REVERSE(src.filename)) + 2) AS LKHS_filename
        ,      sha256(
                   CONCAT(
                       COALESCE(src.name::VARCHAR, '<NULL>'), ']##[',
                       COALESCE(src.value::VARCHAR, '<NULL>'), ']##['
                   )
               ) AS LKHS_hash_value
        ,      CAST(MIN(src.opdateringsdato) OVER (PARTITION BY src.id) AS DATETIME) AS LKHS_date_inserted_src
        FROM   read_json_auto('{glob_json}', filename=True, union_by_name=true) src
    )
    ,CTE_FILES AS (
        SELECT LKHS_filename
        ,      strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - POSITION('.' IN REVERSE(LKHS_filename)) - 14, 15), '%Y%m%d_%H%M%S') AS LKHS_date_valid_from
        ,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
        ,      LEAD(LKHS_filename) OVER (ORDER BY LKHS_filename) AS LKHS_filename_next
        ,      LAG(strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - POSITION('.' IN REVERSE(LKHS_filename)) - 14, 15), '%Y%m%d_%H%M%S')) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_previous
        ,      LEAD(strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - POSITION('.' IN REVERSE(LKHS_filename)) - 14, 15), '%Y%m%d_%H%M%S')) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_next
        FROM   (SELECT SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
                FROM read_text('{glob_text}')
               ) files
    )
    ,CTE_BRONZE_INCL_LAG AS (
        SELECT CTE_BRONZE.* EXCLUDE (filename)
        ,      CTE_FILES.LKHS_date_valid_from
        ,      CTE_BRONZE_PREVIOUS.LKHS_hash_value AS LKHS_hash_value_previous
        ,      CTE_BRONZE_PREVIOUS.id AS LKHS_primary_key_previous
        FROM       CTE_BRONZE
        INNER JOIN CTE_FILES
        ON         CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
        LEFT  JOIN CTE_BRONZE CTE_BRONZE_PREVIOUS
        ON         CTE_FILES.LKHS_filename_previous = CTE_BRONZE_PREVIOUS.LKHS_filename
        AND        CTE_BRONZE.id = CTE_BRONZE_PREVIOUS.id
    )
    ,CTE_ALL_ROWS AS (
        -- Inserts and Updates
        SELECT  CTE_BRONZE_INCL_LAG.id
        ,       CTE_BRONZE_INCL_LAG.name
        ,       CTE_BRONZE_INCL_LAG.value
        ,       CTE_BRONZE_INCL_LAG.opdateringsdato
        ,       CTE_BRONZE_INCL_LAG.LKHS_filename
        ,       CTE_BRONZE_INCL_LAG.LKHS_hash_value
        ,       CTE_BRONZE_INCL_LAG.LKHS_date_inserted_src
        ,       CTE_BRONZE_INCL_LAG.LKHS_date_valid_from
        ,       CASE
                    WHEN LKHS_primary_key_previous IS NULL THEN 'I'
                    WHEN LKHS_primary_key_previous IS NOT NULL
                         AND LKHS_hash_value != LKHS_hash_value_previous THEN 'U'
                END AS LKHS_cdc_operation
        FROM    CTE_BRONZE_INCL_LAG
        WHERE   CASE
                    WHEN LKHS_primary_key_previous IS NULL THEN 'I'
                    WHEN LKHS_primary_key_previous IS NOT NULL
                         AND LKHS_hash_value != LKHS_hash_value_previous THEN 'U'
                END IN ('I', 'U')

        UNION ALL

        -- Deletes: rows present in file N but absent from file N+1
        SELECT  CTE_BRONZE_INCL_LAG.id
        ,       CTE_BRONZE_INCL_LAG.name
        ,       CTE_BRONZE_INCL_LAG.value
        ,       CTE_BRONZE_INCL_LAG.opdateringsdato
        ,       CTE_FILES.LKHS_filename_next AS LKHS_filename
        ,       CTE_BRONZE_INCL_LAG.LKHS_hash_value
        ,       CTE_BRONZE_INCL_LAG.LKHS_date_inserted_src
        ,       CTE_FILES.LKHS_date_valid_from_next AS LKHS_date_valid_from
        ,       'D' AS LKHS_cdc_operation
        FROM    CTE_BRONZE_INCL_LAG
        INNER JOIN CTE_FILES
        ON      CTE_BRONZE_INCL_LAG.LKHS_filename = CTE_FILES.LKHS_filename
        LEFT  JOIN CTE_BRONZE_INCL_LAG CTE_NEXT
        ON      CTE_FILES.LKHS_filename_next = CTE_NEXT.LKHS_filename
        AND     CTE_BRONZE_INCL_LAG.id = CTE_NEXT.id
        WHERE   CTE_NEXT.id IS NULL
        AND     CTE_FILES.LKHS_filename_next IS NOT NULL
    )
    SELECT * FROM CTE_ALL_ROWS
    ORDER BY id, LKHS_date_valid_from
    """


# ── Tests ────────────────────────────────────────────────────────────


class TestSilverFullExtractCDC:
    """Tests mirroring the full-extraction Silver macro logic."""

    def test_first_file_produces_inserts_only(self, silver_fixture_dir):
        """Rows in the very first file should all be CDC operation 'I'."""
        conn = duckdb.connect(":memory:")
        sql = _build_full_extract_cdc_sql(str(silver_fixture_dir))
        df = conn.execute(sql).fetchdf()

        first_file_rows = df[df["LKHS_filename"] == "thing_20240101_120000.json"]
        assert len(first_file_rows) == 2
        assert set(first_file_rows["LKHS_cdc_operation"]) == {"I"}

    def test_changed_row_produces_update(self, silver_fixture_dir):
        """Row 1 changed between file 1 and file 2 — should produce an 'U'."""
        conn = duckdb.connect(":memory:")
        sql = _build_full_extract_cdc_sql(str(silver_fixture_dir))
        df = conn.execute(sql).fetchdf()

        updates = df[(df["id"] == 1) & (df["LKHS_cdc_operation"] == "U")]
        assert len(updates) == 1
        assert updates.iloc[0]["name"] == "Alpha-v2"

    def test_unchanged_row_is_not_duplicated(self, silver_fixture_dir):
        """Row 2 is identical in file 1 and file 2 — no U record should appear."""
        conn = duckdb.connect(":memory:")
        sql = _build_full_extract_cdc_sql(str(silver_fixture_dir))
        df = conn.execute(sql).fetchdf()

        row2_ops = df[(df["id"] == 2)]["LKHS_cdc_operation"].tolist()
        assert "U" not in row2_ops

    def test_missing_row_produces_delete(self, silver_fixture_dir):
        """Row 1 is absent in file 3 — should produce a 'D' record."""
        conn = duckdb.connect(":memory:")
        sql = _build_full_extract_cdc_sql(str(silver_fixture_dir))
        df = conn.execute(sql).fetchdf()

        deletes = df[(df["id"] == 1) & (df["LKHS_cdc_operation"] == "D")]
        assert len(deletes) == 1
        # The delete is timestamped from file 3 (the file where the row is missing)
        assert "20240301" in str(deletes.iloc[0]["LKHS_filename"])

    def test_date_valid_from_derived_from_filename(self, silver_fixture_dir):
        """LKHS_date_valid_from should be parsed from the filename timestamp."""
        conn = duckdb.connect(":memory:")
        sql = _build_full_extract_cdc_sql(str(silver_fixture_dir))
        df = conn.execute(sql).fetchdf()

        inserts = df[df["LKHS_cdc_operation"] == "I"]
        # All inserts are from file 1 → 2024-01-01 12:00:00
        for _, row in inserts.iterrows():
            assert row["LKHS_date_valid_from"].year == 2024
            assert row["LKHS_date_valid_from"].month == 1
            assert row["LKHS_date_valid_from"].day == 1

    def test_total_cdc_operations(self, silver_fixture_dir):
        """3 files with known transitions should produce exactly:
        - 2 Inserts (file 1: rows 1, 2)
        - 1 Update  (file 2: row 1 changed)
        - 1 Delete  (file 3: row 1 absent)
        Total: 4 CDC rows
        """
        conn = duckdb.connect(":memory:")
        sql = _build_full_extract_cdc_sql(str(silver_fixture_dir))
        df = conn.execute(sql).fetchdf()

        op_counts = df["LKHS_cdc_operation"].value_counts().to_dict()
        assert op_counts.get("I", 0) == 2
        assert op_counts.get("U", 0) == 1
        assert op_counts.get("D", 0) == 1
        assert len(df) == 4

    def test_hash_detects_content_change(self, silver_fixture_dir):
        """The SHA256 hash should differ between the original and updated row 1."""
        conn = duckdb.connect(":memory:")
        sql = _build_full_extract_cdc_sql(str(silver_fixture_dir))
        df = conn.execute(sql).fetchdf()

        row1 = df[df["id"] == 1].sort_values("LKHS_date_valid_from")
        insert_hash = row1.iloc[0]["LKHS_hash_value"]
        update_hash = row1.iloc[1]["LKHS_hash_value"]
        assert insert_hash != update_hash


class TestSilverCurrentVersionView:
    """Tests for the _cv (current version) view pattern."""

    def test_cv_returns_one_row_per_pk(self, silver_fixture_dir):
        """The _cv view should return exactly one row per primary key."""
        conn = duckdb.connect(":memory:")
        sql = _build_full_extract_cdc_sql(str(silver_fixture_dir))

        conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")
        conn.execute(f"CREATE TABLE main_silver.silver_thing AS ({sql})")
        cv_df = conn.execute("""
            SELECT *
            FROM   main_silver.silver_thing src
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY src.id
                ORDER BY src.LKHS_date_valid_from DESC
            ) = 1
        """).fetchdf()

        assert len(cv_df) == 2  # two distinct PKs (1 and 2)

    def test_cv_returns_latest_version(self, silver_fixture_dir):
        """For row 1, the _cv view should return the Delete (latest operation)."""
        conn = duckdb.connect(":memory:")
        sql = _build_full_extract_cdc_sql(str(silver_fixture_dir))
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")
        conn.execute(f"CREATE TABLE main_silver.silver_thing AS ({sql})")

        cv_df = conn.execute("""
            SELECT *
            FROM   main_silver.silver_thing src
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY src.id
                ORDER BY src.LKHS_date_valid_from DESC
            ) = 1
        """).fetchdf()

        row1 = cv_df[cv_df["id"] == 1].iloc[0]
        assert row1["LKHS_cdc_operation"] == "D"

    def test_cv_excludes_deleted_for_active_rows(self, silver_fixture_dir):
        """The real Gold pattern: get _cv first (latest per PK), then
        filter out rows whose latest operation is 'D'.  Row 1 is deleted
        in file 3 so it should be excluded; row 2 should remain."""
        conn = duckdb.connect(":memory:")
        sql = _build_full_extract_cdc_sql(str(silver_fixture_dir))
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")
        conn.execute(f"CREATE TABLE main_silver.silver_thing AS ({sql})")

        active_df = conn.execute("""
            SELECT *
            FROM (
                SELECT *
                FROM   main_silver.silver_thing src
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY src.id
                    ORDER BY src.LKHS_date_valid_from DESC
                ) = 1
            )
            WHERE LKHS_cdc_operation != 'D'
        """).fetchdf()

        assert len(active_df) == 1
        assert active_df.iloc[0]["id"] == 2


class TestNotExistsDedup:
    """Tests for the NOT EXISTS dedup guard used in incremental mode."""

    def test_not_exists_prevents_duplicate_insert(self, silver_fixture_dir):
        """If a Silver table already has a row (PK, date_valid_from),
        NOT EXISTS should prevent re-inserting it."""
        conn = duckdb.connect(":memory:")
        sql = _build_full_extract_cdc_sql(str(silver_fixture_dir))

        # First load: materialise all CDC rows
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")
        conn.execute(f"CREATE TABLE main_silver.silver_thing AS ({sql})")
        initial_count = conn.execute("SELECT COUNT(*) FROM main_silver.silver_thing").fetchone()[0]

        # Second load: simulate incremental with NOT EXISTS
        deduped_count = conn.execute(f"""
            SELECT COUNT(*)
            FROM ({sql}) cdc
            WHERE NOT EXISTS (
                SELECT id FROM main_silver.silver_thing
                WHERE  id = cdc.id
                AND    LKHS_date_valid_from = cdc.LKHS_date_valid_from
            )
        """).fetchone()[0]

        assert deduped_count == 0  # nothing new to insert
        assert initial_count == 4  # sanity check


# ── Incremental delete path ───────────────────────────────────────────


@pytest.fixture()
def incr_delete_fixture(tmp_path):
    """Create two extraction rounds that simulate the incremental delete path.

    Round 1 (initial load, two files):
      File 1 (2024-01-01): rows 1, 2, 3
      File 2 (2024-02-01): rows 1 (changed), 2 (same), 3 (same)
    This produces a Silver table with rows 1I, 2I, 3I, 1U.

    Round 2 (--full-refresh Bronze snapshot, i.e. _latest):
      The _latest view contains only rows 2 and 3 — row 1 has been deleted
      by the upstream source.

    The fixture returns a dict with:
      - ``data_dir``: path for the two-file Bronze glob
      - ``latest_dir``: path for the single-file _latest glob
      - ``latest_filename``: just the filename, for timestamp assertions
    """
    entity = "thing"

    # Two-file Bronze history used to build the Silver table state
    bronze_dir = tmp_path / "bronze" / entity
    bronze_dir.mkdir(parents=True)

    # File 1: rows 1, 2, 3
    (bronze_dir / f"{entity}_20240101_120000.json").write_text(
        "\n".join(
            json.dumps(r)
            for r in [
                {"id": 1, "name": "Alpha", "value": 10, "opdateringsdato": "2024-01-01T00:00:00"},
                {"id": 2, "name": "Beta", "value": 20, "opdateringsdato": "2024-01-01T00:00:00"},
                {"id": 3, "name": "Gamma", "value": 30, "opdateringsdato": "2024-01-01T00:00:00"},
            ]
        )
        + "\n"
    )
    # File 2: row 1 updated, rows 2 and 3 unchanged
    (bronze_dir / f"{entity}_20240201_120000.json").write_text(
        "\n".join(
            json.dumps(r)
            for r in [
                {
                    "id": 1,
                    "name": "Alpha-v2",
                    "value": 11,
                    "opdateringsdato": "2024-02-01T00:00:00",
                },
                {"id": 2, "name": "Beta", "value": 20, "opdateringsdato": "2024-02-01T00:00:00"},
                {"id": 3, "name": "Gamma", "value": 30, "opdateringsdato": "2024-02-01T00:00:00"},
            ]
        )
        + "\n"
    )

    # Latest Bronze snapshot: row 1 is absent — only rows 2 and 3
    latest_filename = f"{entity}_20240301_120000.json"
    latest_dir = tmp_path / "latest" / entity
    latest_dir.mkdir(parents=True)
    (latest_dir / latest_filename).write_text(
        "\n".join(
            json.dumps(r)
            for r in [
                {"id": 2, "name": "Beta", "value": 20, "opdateringsdato": "2024-03-01T00:00:00"},
                {"id": 3, "name": "Gamma", "value": 30, "opdateringsdato": "2024-03-01T00:00:00"},
            ]
        )
        + "\n"
    )

    return {
        "data_dir": str(tmp_path / "bronze"),
        "latest_dir": str(latest_dir),
        "latest_filename": latest_filename,
    }


def _parse_filename_ts(col: str) -> str:
    """DuckDB expression that extracts the timestamp embedded in a filename.

    Matches the ``parse_filename_ts`` Jinja macro used by the dbt model:
    strips the extension then reads the trailing 15-char ``YYYYMMDD_HHMMSS``
    substring.
    """
    return (
        f"strptime(SUBSTRING({col},"
        f" LENGTH({col}) - POSITION('.' IN REVERSE({col})) - 14, 15),"
        f" '%Y%m%d_%H%M%S')"
    )


def _build_silver_initial_state_sql(data_dir: str, entity: str = "thing") -> str:
    """Return SQL that produces the Silver table rows from a two-file Bronze history.

    Mirrors the incremental Silver macro's I/U logic (without the delete branch)
    so we can materialise a realistic Silver starting state.
    """
    glob_json = os.path.join(data_dir, entity, f"{entity}_*.json*")
    ts_expr = _parse_filename_ts("LKHS_filename")
    return f"""
    WITH CTE_BRONZE AS (
        SELECT  src.*
        ,       SUBSTRING(src.filename,
                    LENGTH(src.filename)
                    - POSITION('/' IN REVERSE(src.filename)) + 2) AS LKHS_filename
        ,       sha256(
                    CONCAT(
                        COALESCE(src.name::VARCHAR,  '<NULL>'), ']##[',
                        COALESCE(src.value::VARCHAR, '<NULL>'), ']##['
                    )
                ) AS LKHS_hash_value
        ,       CAST(MIN(src.opdateringsdato)
                    OVER (PARTITION BY src.id) AS DATETIME) AS LKHS_date_inserted_src
        ,       'DDD' AS LKHS_source_system_code
        FROM    read_json_auto('{glob_json}', filename=True, union_by_name=true) src
    )
    ,CTE_FILES AS (
        SELECT  LKHS_filename
        ,       {ts_expr} AS LKHS_date_valid_from
        ,       LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
        FROM    (
            SELECT  SUBSTRING(filename,
                        LENGTH(filename)
                        - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
            FROM    read_text('{glob_json}')
        ) files
    )
    ,CTE_FILE_LATEST AS (
        SELECT  MAX(LKHS_filename)        AS LKHS_filename
        ,       MAX(LKHS_date_valid_from) AS LKHS_date_valid_from
        FROM    CTE_FILES
    )
    ,CTE_BRONZE_INCL_LAG AS (
        SELECT  CTE_BRONZE.*
        ,       CTE_FILES.LKHS_date_valid_from
        ,       LAG(CTE_BRONZE.LKHS_hash_value)
                    OVER (PARTITION BY CTE_BRONZE.id ORDER BY CTE_BRONZE.LKHS_filename)
                    AS LKHS_hash_value_previous
        FROM        CTE_BRONZE
        INNER JOIN  CTE_FILES
        ON          CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
    )
    SELECT  id, name, value, opdateringsdato
    ,       LKHS_filename, LKHS_source_system_code, LKHS_hash_value, LKHS_date_inserted_src
    ,       LKHS_date_valid_from
    ,       CAST('2024-02-01 12:00:00' AS DATETIME) AS LKHS_date_inserted
    ,       CASE
                WHEN LKHS_hash_value_previous IS NULL THEN 'I'
                WHEN LKHS_hash_value != LKHS_hash_value_previous THEN 'U'
            END AS LKHS_cdc_operation
    FROM    CTE_BRONZE_INCL_LAG
    WHERE   CASE
                WHEN LKHS_hash_value_previous IS NULL THEN 'I'
                WHEN LKHS_hash_value != LKHS_hash_value_previous THEN 'U'
            END IN ('I', 'U')
    """


def _build_current_temp_sql(silver_table: str) -> str:
    """Return the SQL the pre-hook uses to build _current_temp.

    Mirrors ``generate_pre_hook_silver_full_refresh``:
    latest row per (LKHS_source_system_code, id).
    """
    return f"""
    SELECT src.*
    FROM   {silver_table} src
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY src.LKHS_source_system_code, src.id
        ORDER BY src.LKHS_date_valid_from DESC
    ) = 1
    """


def _build_delete_branch_sql(
    current_temp_table: str,
    bronze_latest_table: str,
    latest_ts_expr: str,
) -> str:
    """Return the two UNION ALL arms from the macro's delete branch.

    Arm 1: re-emit existing 'D' rows from _current_temp (preserves prior deletes).
    Arm 2: anti-join _current_temp (non-D) against bronze_latest;
           absent PKs get a new 'D' row timestamped from the latest file.

    ``latest_ts_expr`` should be a scalar SQL expression that resolves to the
    LKHS_date_valid_from of the latest Bronze file — in tests we pass a literal
    TIMESTAMP.
    """
    return f"""
    -- Arm 1: carry forward any rows that were already 'D' in _current_temp
    SELECT  cv.id, cv.name, cv.value, cv.opdateringsdato
    ,       cv.LKHS_filename, cv.LKHS_source_system_code
    ,       cv.LKHS_hash_value, cv.LKHS_date_inserted_src
    ,       cv.LKHS_date_valid_from
    ,       cv.LKHS_date_inserted
    ,       cv.LKHS_cdc_operation
    FROM    {current_temp_table} cv
    WHERE   cv.LKHS_cdc_operation = 'D'

    UNION ALL

    -- Arm 2: new deletes — PK present in _current_temp (non-D) but absent from _latest
    SELECT  cv.id, cv.name, cv.value, cv.opdateringsdato
    ,       cv.LKHS_filename, cv.LKHS_source_system_code
    ,       cv.LKHS_hash_value, cv.LKHS_date_inserted_src
    ,       {latest_ts_expr} AS LKHS_date_valid_from
    ,       CAST('2024-03-01 12:00:00' AS DATETIME) AS LKHS_date_inserted
    ,       'D' AS LKHS_cdc_operation
    FROM    {current_temp_table} cv
    LEFT JOIN {bronze_latest_table} bronze_latest
    ON        cv.id = bronze_latest.id
    WHERE     cv.LKHS_cdc_operation != 'D'
    AND       bronze_latest.id IS NULL
    """


class TestIncrementalSilverCDCDelete:
    """Tests for the incremental Silver delete path (--full-refresh branch).

    The macro only emits delete rows during a ``--full-refresh`` run
    (``is_incremental() == False``).  The pre-hook snapshots the current _cv
    state into ``_current_temp``; the main model anti-joins that snapshot
    against ``bronze_<entity>_latest`` and emits 'D' rows for absent PKs.

    These tests reproduce that SQL directly in DuckDB — no dbt, no Dagster.
    """

    def _setup_conn(self, incr_delete_fixture) -> tuple[duckdb.DuckDBPyConnection, str, str]:
        """Build a DuckDB connection with:
        - ``main_silver.silver_thing``: initial Silver state (I + U rows)
        - ``silver_thing_current_temp``: pre-hook snapshot (_cv of silver_thing)
        - ``bronze_latest``: the latest Bronze view (rows 2 and 3 only)

        Returns ``(conn, current_temp_table_name, bronze_latest_table_name)``.
        """
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")

        # Materialise the initial Silver state from two Bronze files
        initial_sql = _build_silver_initial_state_sql(incr_delete_fixture["data_dir"])
        conn.execute(f"CREATE TABLE main_silver.silver_thing AS ({initial_sql})")

        # Build _current_temp as the pre-hook would: latest row per PK
        current_temp_sql = _build_current_temp_sql("main_silver.silver_thing")
        conn.execute(f"CREATE TABLE silver_thing_current_temp AS ({current_temp_sql})")

        # Build the bronze_latest view: only rows 2 and 3 (row 1 absent)
        latest_glob = os.path.join(
            incr_delete_fixture["latest_dir"],
            "thing_*.json*",
        )
        conn.execute(f"""
            CREATE VIEW bronze_thing_latest AS
            SELECT  id, name, value, opdateringsdato
            ,       SUBSTRING(filename,
                        LENGTH(filename)
                        - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
            ,       'DDD' AS LKHS_source_system_code
            ,       'N'   AS LKHS_deleted_ind
            FROM    read_json_auto('{latest_glob}', filename=True, union_by_name=true)
        """)

        return conn, "silver_thing_current_temp", "bronze_thing_latest"

    def test_deleted_pk_produces_d_row(self, incr_delete_fixture):
        """Row 1 is absent from bronze_latest — exactly one 'D' row should be emitted."""
        conn, current_temp, bronze_latest = self._setup_conn(incr_delete_fixture)

        latest_ts = "CAST('2024-03-01 12:00:00' AS TIMESTAMP)"
        delete_sql = _build_delete_branch_sql(current_temp, bronze_latest, latest_ts)
        df = conn.execute(delete_sql).fetchdf()

        deletes = df[df["LKHS_cdc_operation"] == "D"]
        assert len(deletes) == 1
        assert deletes.iloc[0]["id"] == 1

    def test_present_pk_not_marked_deleted(self, incr_delete_fixture):
        """Rows 2 and 3 are still in bronze_latest — neither should appear as 'D'."""
        conn, current_temp, bronze_latest = self._setup_conn(incr_delete_fixture)

        latest_ts = "CAST('2024-03-01 12:00:00' AS TIMESTAMP)"
        delete_sql = _build_delete_branch_sql(current_temp, bronze_latest, latest_ts)
        df = conn.execute(delete_sql).fetchdf()

        assert 2 not in df[df["LKHS_cdc_operation"] == "D"]["id"].tolist()
        assert 3 not in df[df["LKHS_cdc_operation"] == "D"]["id"].tolist()

    def test_d_row_timestamped_from_latest_file(self, incr_delete_fixture):
        """The 'D' row's LKHS_date_valid_from should match the latest Bronze file's timestamp."""
        conn, current_temp, bronze_latest = self._setup_conn(incr_delete_fixture)

        latest_ts = "CAST('2024-03-01 12:00:00' AS TIMESTAMP)"
        delete_sql = _build_delete_branch_sql(current_temp, bronze_latest, latest_ts)
        df = conn.execute(delete_sql).fetchdf()

        d_row = df[df["LKHS_cdc_operation"] == "D"].iloc[0]
        assert d_row["LKHS_date_valid_from"].year == 2024
        assert d_row["LKHS_date_valid_from"].month == 3
        assert d_row["LKHS_date_valid_from"].day == 1

    def test_multiple_deletes_in_one_run(self, tmp_path):
        """If both rows 1 and 3 are absent from bronze_latest, two 'D' rows appear."""
        entity = "thing"
        bronze_dir = tmp_path / "bronze" / entity
        bronze_dir.mkdir(parents=True)

        # Single Bronze file with rows 1, 2, 3
        (bronze_dir / f"{entity}_20240101_120000.json").write_text(
            "\n".join(
                json.dumps(r)
                for r in [
                    {
                        "id": 1,
                        "name": "Alpha",
                        "value": 10,
                        "opdateringsdato": "2024-01-01T00:00:00",
                    },
                    {
                        "id": 2,
                        "name": "Beta",
                        "value": 20,
                        "opdateringsdato": "2024-01-01T00:00:00",
                    },
                    {
                        "id": 3,
                        "name": "Gamma",
                        "value": 30,
                        "opdateringsdato": "2024-01-01T00:00:00",
                    },
                ]
            )
            + "\n"
        )

        # _latest contains only row 2
        latest_dir = tmp_path / "latest" / entity
        latest_dir.mkdir(parents=True)
        (latest_dir / f"{entity}_20240201_120000.json").write_text(
            json.dumps(
                {"id": 2, "name": "Beta", "value": 20, "opdateringsdato": "2024-02-01T00:00:00"}
            )
            + "\n"
        )

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")

        initial_sql = _build_silver_initial_state_sql(str(tmp_path / "bronze"))
        conn.execute(f"CREATE TABLE main_silver.silver_thing AS ({initial_sql})")

        current_temp_sql = _build_current_temp_sql("main_silver.silver_thing")
        conn.execute(f"CREATE TABLE silver_thing_current_temp AS ({current_temp_sql})")

        latest_glob = os.path.join(str(latest_dir), "thing_*.json*")
        conn.execute(f"""
            CREATE VIEW bronze_thing_latest AS
            SELECT  id, name, value, opdateringsdato
            ,       SUBSTRING(filename,
                        LENGTH(filename)
                        - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
            ,       'DDD' AS LKHS_source_system_code
            ,       'N'   AS LKHS_deleted_ind
            FROM    read_json_auto('{latest_glob}', filename=True, union_by_name=true)
        """)

        latest_ts = "CAST('2024-02-01 12:00:00' AS TIMESTAMP)"
        delete_sql = _build_delete_branch_sql(
            "silver_thing_current_temp", "bronze_thing_latest", latest_ts
        )
        df = conn.execute(delete_sql).fetchdf()

        d_rows = df[df["LKHS_cdc_operation"] == "D"]
        assert len(d_rows) == 2
        assert set(d_rows["id"].tolist()) == {1, 3}

    def test_prior_d_rows_carried_forward(self, incr_delete_fixture):
        """If _current_temp already contains a 'D' row (from a previous run),
        Arm 1 re-emits it unchanged.  It must not be re-treated as 'present'
        and generate a second 'D' in Arm 2."""
        conn, _, bronze_latest = self._setup_conn(incr_delete_fixture)

        # Inject an already-deleted row (id=99) directly into _current_temp
        conn.execute("""
            INSERT INTO silver_thing_current_temp
            SELECT  99   AS id
            ,       'Old' AS name
            ,       99   AS value
            ,       CAST('2023-12-01T00:00:00' AS DATETIME) AS opdateringsdato
            ,       'thing_20231201_120000.json'             AS LKHS_filename
            ,       'DDD'                                    AS LKHS_source_system_code
            ,       sha256('Old]##[99]##[')                  AS LKHS_hash_value
            ,       CAST('2023-12-01 00:00:00' AS DATETIME) AS LKHS_date_inserted_src
            ,       CAST('2023-12-01 12:00:00' AS DATETIME) AS LKHS_date_valid_from
            ,       CAST('2023-12-01 12:00:00' AS DATETIME) AS LKHS_date_inserted
            ,       'D'                                      AS LKHS_cdc_operation
        """)

        latest_ts = "CAST('2024-03-01 12:00:00' AS TIMESTAMP)"
        delete_sql = _build_delete_branch_sql("silver_thing_current_temp", bronze_latest, latest_ts)
        df = conn.execute(delete_sql).fetchdf()

        # id=99 should appear exactly once (Arm 1), not twice
        id99_rows = df[df["id"] == 99]
        assert len(id99_rows) == 1
        assert id99_rows.iloc[0]["LKHS_cdc_operation"] == "D"

        # id=99's LKHS_date_valid_from is preserved from the original 'D' row,
        # not overwritten with the latest file's timestamp
        assert id99_rows.iloc[0]["LKHS_date_valid_from"].year == 2023

    def test_idempotency_appending_to_silver(self, incr_delete_fixture):
        """Simulates appending the delete-branch output to Silver, then running
        the delete branch a second time.

        After the first run, the Silver table contains the 'D' row for id=1.
        On a second --full-refresh the _current_temp is rebuilt from the latest
        Silver state (which now includes the 'D' row), so the 'D' row for id=1
        is carried forward via Arm 1 and NOT re-generated by Arm 2.
        The final Silver table must contain exactly one 'D' row for id=1.
        """
        conn, current_temp, bronze_latest = self._setup_conn(incr_delete_fixture)

        latest_ts = "CAST('2024-03-01 12:00:00' AS TIMESTAMP)"
        delete_sql = _build_delete_branch_sql(current_temp, bronze_latest, latest_ts)

        # First run: append delete rows to Silver
        conn.execute(f"INSERT INTO main_silver.silver_thing ({delete_sql})")
        count_after_first = conn.execute(
            "SELECT COUNT(*) FROM main_silver.silver_thing WHERE LKHS_cdc_operation = 'D' AND id = 1"
        ).fetchone()[0]
        assert count_after_first == 1

        # Rebuild _current_temp from the updated Silver (simulates the pre-hook on next --full-refresh)
        conn.execute("DROP TABLE IF EXISTS silver_thing_current_temp")
        current_temp_sql = _build_current_temp_sql("main_silver.silver_thing")
        conn.execute(f"CREATE TABLE silver_thing_current_temp AS ({current_temp_sql})")

        # Second run of the delete branch
        df2 = conn.execute(delete_sql).fetchdf()
        d_rows_id1 = df2[(df2["id"] == 1) & (df2["LKHS_cdc_operation"] == "D")]

        # Arm 1 carries it forward once; Arm 2 must NOT produce a second 'D'
        # because the current_temp entry for id=1 already has LKHS_cdc_operation='D'
        assert len(d_rows_id1) == 1

    def test_current_temp_reflects_latest_cv_state(self, incr_delete_fixture):
        """_current_temp should contain at most one row per PK (the latest version).

        The fixture's Silver state has rows: 1I, 2I, 3I, 1U.
        _current_temp (the _cv snapshot) must therefore hold:
          id=1 → LKHS_cdc_operation='U'  (most recent for id=1)
          id=2 → LKHS_cdc_operation='I'
          id=3 → LKHS_cdc_operation='I'
        """
        conn, current_temp, _ = self._setup_conn(incr_delete_fixture)

        df = conn.execute(f"SELECT * FROM {current_temp}").fetchdf()

        assert len(df) == 3  # one row per PK
        assert set(df["id"].tolist()) == {1, 2, 3}

        id1_op = df[df["id"] == 1].iloc[0]["LKHS_cdc_operation"]
        assert id1_op == "U"  # most recent version of id=1

    def test_no_delete_rows_when_all_pks_present(self, tmp_path):
        """If every PK in _current_temp is still present in bronze_latest,
        no 'D' rows should be emitted by Arm 2."""
        entity = "thing"
        bronze_dir = tmp_path / "bronze" / entity
        bronze_dir.mkdir(parents=True)

        (bronze_dir / f"{entity}_20240101_120000.json").write_text(
            "\n".join(
                json.dumps(r)
                for r in [
                    {
                        "id": 1,
                        "name": "Alpha",
                        "value": 10,
                        "opdateringsdato": "2024-01-01T00:00:00",
                    },
                    {
                        "id": 2,
                        "name": "Beta",
                        "value": 20,
                        "opdateringsdato": "2024-01-01T00:00:00",
                    },
                ]
            )
            + "\n"
        )

        # _latest contains both rows — no deletions
        latest_dir = tmp_path / "latest" / entity
        latest_dir.mkdir(parents=True)
        (latest_dir / f"{entity}_20240201_120000.json").write_text(
            "\n".join(
                json.dumps(r)
                for r in [
                    {
                        "id": 1,
                        "name": "Alpha",
                        "value": 10,
                        "opdateringsdato": "2024-02-01T00:00:00",
                    },
                    {
                        "id": 2,
                        "name": "Beta",
                        "value": 20,
                        "opdateringsdato": "2024-02-01T00:00:00",
                    },
                ]
            )
            + "\n"
        )

        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")

        initial_sql = _build_silver_initial_state_sql(str(tmp_path / "bronze"))
        conn.execute(f"CREATE TABLE main_silver.silver_thing AS ({initial_sql})")

        current_temp_sql = _build_current_temp_sql("main_silver.silver_thing")
        conn.execute(f"CREATE TABLE silver_thing_current_temp AS ({current_temp_sql})")

        latest_glob = os.path.join(str(latest_dir), "thing_*.json*")
        conn.execute(f"""
            CREATE VIEW bronze_thing_latest AS
            SELECT  id, name, value, opdateringsdato
            ,       SUBSTRING(filename,
                        LENGTH(filename)
                        - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
            ,       'DDD' AS LKHS_source_system_code
            ,       'N'   AS LKHS_deleted_ind
            FROM    read_json_auto('{latest_glob}', filename=True, union_by_name=true)
        """)

        latest_ts = "CAST('2024-02-01 12:00:00' AS TIMESTAMP)"
        delete_sql = _build_delete_branch_sql(
            "silver_thing_current_temp", "bronze_thing_latest", latest_ts
        )
        df = conn.execute(delete_sql).fetchdf()

        assert len(df[df["LKHS_cdc_operation"] == "D"]) == 0
