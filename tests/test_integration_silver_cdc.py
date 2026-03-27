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
"""

import json
import os

import duckdb
import pytest


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
        (entity_dir / fname).write_text(
            "\n".join(json.dumps(r) for r in rows) + "\n"
        )

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

        # Materialise the Silver table, then apply the _cv pattern
        conn.execute(f"CREATE TABLE silver_thing AS ({sql})")
        cv_df = conn.execute("""
            SELECT *
            FROM   silver_thing src
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
        conn.execute(f"CREATE TABLE silver_thing AS ({sql})")

        cv_df = conn.execute("""
            SELECT *
            FROM   silver_thing src
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
        conn.execute(f"CREATE TABLE silver_thing AS ({sql})")

        # Step 1: get current version (latest per PK)
        # Step 2: exclude rows where the latest operation is D
        active_df = conn.execute("""
            SELECT *
            FROM (
                SELECT *
                FROM   silver_thing src
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
        conn.execute(f"CREATE TABLE silver_thing AS ({sql})")
        initial_count = conn.execute("SELECT COUNT(*) FROM silver_thing").fetchone()[0]

        # Second load: simulate incremental with NOT EXISTS
        deduped_count = conn.execute(f"""
            SELECT COUNT(*)
            FROM ({sql}) cdc
            WHERE NOT EXISTS (
                SELECT id FROM silver_thing
                WHERE  id = cdc.id
                AND    LKHS_date_valid_from = cdc.LKHS_date_valid_from
            )
        """).fetchone()[0]

        assert deduped_count == 0  # nothing new to insert
        assert initial_count == 4  # sanity check
