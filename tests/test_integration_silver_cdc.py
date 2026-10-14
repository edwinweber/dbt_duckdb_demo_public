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
* Incremental delete path (--full-refresh): _current_temp holds the newest version
  and every D row per key; keys whose newest version is missing from bronze_latest
  get a 'D' row named after the latest file; existing D rows are carried forward
* A key that Silver deleted and that reappears in Bronze is an insert ('I')

``test_dbt_silver.py`` runs the real macros with dbt.
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
        ,      strptime(regexp_extract(LKHS_filename, '([0-9]{{8}}_[0-9]{{6}})', 1), '%Y%m%d_%H%M%S') AS LKHS_date_valid_from
        ,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
        ,      LEAD(LKHS_filename) OVER (ORDER BY LKHS_filename) AS LKHS_filename_next
        ,      LAG(strptime(regexp_extract(LKHS_filename, '([0-9]{{8}}_[0-9]{{6}})', 1), '%Y%m%d_%H%M%S')) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_previous
        ,      LEAD(strptime(regexp_extract(LKHS_filename, '([0-9]{{8}}_[0-9]{{6}})', 1), '%Y%m%d_%H%M%S')) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_next
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
    picks the ``YYYYMMDD_HHMMSS`` part with a regex, so it works for ``.json``,
    ``.jsonl`` and the ``.json.jsonl`` names dlt >= 1.30 writes.
    """
    return f"strptime(regexp_extract({col}, '([0-9]{{8}}_[0-9]{{6}})', 1), '%Y%m%d_%H%M%S')"


@pytest.mark.parametrize(
    "file_name",
    [
        "thing_20240101_120000.json",
        "thing_20240101_120000.jsonl",
        "thing_20240101_120000.json.jsonl",
    ],
)
def test_parse_filename_ts_handles_all_dlt_extensions(file_name):
    """The timestamp must parse for every extension dlt has produced."""
    conn = duckdb.connect(":memory:")
    ts = conn.execute(f"SELECT {_parse_filename_ts('?')}", [file_name]).fetchone()[0]
    assert str(ts) == "2024-01-01 12:00:00"


def _build_silver_initial_state_sql(
    data_dir: str, entity: str = "thing", silver_deletes: str | None = None
) -> str:
    """Return SQL that produces the Silver I/U rows from a Bronze history.

    Mirrors the incremental Silver macro's I/U logic (``CTE_CHANGES``, without the
    delete branches).  ``silver_deletes`` names a table with the D rows already in
    Silver (``id``, ``LKHS_date_valid_from``): a Bronze version is an 'I' when one of
    them lies between the key's previous Bronze version and this one.
    """
    glob_json = os.path.join(data_dir, entity, f"{entity}_*.json*")
    ts_expr = _parse_filename_ts("LKHS_filename")
    if silver_deletes:
        deleted_since_previous = f"""EXISTS (
                    SELECT 1 FROM {silver_deletes} del
                    WHERE  del.id = lagged.id
                    AND    del.LKHS_date_valid_from > lagged.LKHS_date_valid_from_previous
                    AND    del.LKHS_date_valid_from < lagged.LKHS_date_valid_from)"""
    else:
        deleted_since_previous = "FALSE"
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
        ,       LAG(CTE_FILES.LKHS_date_valid_from)
                    OVER (PARTITION BY CTE_BRONZE.id ORDER BY CTE_BRONZE.LKHS_filename)
                    AS LKHS_date_valid_from_previous
        FROM        CTE_BRONZE
        INNER JOIN  CTE_FILES
        ON          CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
    )
    ,CTE_BRONZE_INCL_DELETES AS (
        SELECT  lagged.*
        ,       {deleted_since_previous} AS LKHS_deleted_since_previous
        FROM    CTE_BRONZE_INCL_LAG lagged
    )
    SELECT * FROM (
        SELECT  id, name, value, opdateringsdato
        ,       LKHS_filename, LKHS_source_system_code, LKHS_hash_value, LKHS_date_inserted_src
        ,       LKHS_date_valid_from
        ,       CAST('2024-02-01 12:00:00' AS DATETIME) AS LKHS_date_inserted
        ,       CASE
                    WHEN LKHS_hash_value_previous IS NULL THEN 'I'
                    WHEN LKHS_deleted_since_previous THEN 'I'
                    WHEN LKHS_hash_value != LKHS_hash_value_previous THEN 'U'
                END AS LKHS_cdc_operation
        FROM    CTE_BRONZE_INCL_DELETES
    )
    WHERE   LKHS_cdc_operation IN ('I', 'U')
    """


def _build_current_temp_sql(silver_table: str) -> str:
    """Return the SQL the pre-hook uses to build _current_temp.

    Mirrors ``generate_pre_hook_silver_full_refresh``: the newest row per
    (LKHS_source_system_code, id) plus every 'D' row.
    """
    return f"""
    SELECT src.*
    FROM   {silver_table} src
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY src.LKHS_source_system_code, src.id
        ORDER BY src.LKHS_date_valid_from DESC
    ) = 1
    OR src.LKHS_cdc_operation = 'D'
    """


_NO_FILES = (
    "(SELECT NULL::TIMESTAMP AS LKHS_date_valid_from, NULL::VARCHAR AS LKHS_filename WHERE FALSE)"
)


def _build_delete_branch_sql(
    current_temp_table: str,
    bronze_latest_table: str,
    latest_ts_expr: str,
    *,
    changes_table: str,
    latest_filename: str = "thing_20240301_120000.json",
    files_table: str = _NO_FILES,
) -> str:
    """Return the two delete branches the macro adds on a --full-refresh.

    Carried deletes: every 'D' row in _current_temp, with LKHS_filename set to the
    Bronze file of its timestamp (``files_table``: LKHS_date_valid_from,
    LKHS_filename) when that file exists.
    New deletes: per key, the newest version from the rebuilt I/U rows
    (``changes_table``) or _current_temp; when it is not a 'D' and the key is
    absent from bronze_latest, a 'D' row named after the latest file.

    ``latest_ts_expr`` is a scalar SQL expression for the LKHS_date_valid_from of
    the latest Bronze file; in tests a literal TIMESTAMP.
    """
    return f"""
    WITH carried AS (
        SELECT    cv.* EXCLUDE (LKHS_filename)
        ,         COALESCE(files.LKHS_filename, cv.LKHS_filename) AS LKHS_filename
        FROM      {current_temp_table} cv
        LEFT JOIN {files_table} files
        ON        files.LKHS_date_valid_from = cv.LKHS_date_valid_from
        WHERE     cv.LKHS_cdc_operation = 'D'
    )
    ,newest AS (
        SELECT versions.*
        FROM  (SELECT chg.*, 1 AS LKHS_version_rank FROM {changes_table} chg
               UNION ALL BY NAME
               SELECT cv.*, 2 AS LKHS_version_rank FROM {current_temp_table} cv
              ) versions
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY versions.id
            ORDER BY versions.LKHS_date_valid_from DESC, versions.LKHS_version_rank
        ) = 1
    )
    ,new_deletes AS (
        SELECT    nv.* EXCLUDE (LKHS_filename, LKHS_date_valid_from, LKHS_date_inserted,
                                LKHS_cdc_operation, LKHS_version_rank)
        ,         '{latest_filename}' AS LKHS_filename
        ,         {latest_ts_expr} AS LKHS_date_valid_from
        ,         CAST('2024-03-01 12:00:00' AS DATETIME) AS LKHS_date_inserted
        ,         'D' AS LKHS_cdc_operation
        FROM      newest nv
        LEFT JOIN {bronze_latest_table} bronze_latest
        ON        nv.id = bronze_latest.id
        WHERE     nv.LKHS_cdc_operation != 'D'
        AND       bronze_latest.id IS NULL
    )
    SELECT * FROM carried
    UNION ALL BY NAME
    SELECT * FROM new_deletes
    """


class TestIncrementalSilverCDCDelete:
    """Tests for the incremental Silver delete path (--full-refresh branch).

    The macro only emits delete rows during a ``--full-refresh`` run
    (``is_incremental() == False``).  The pre-hook copies the newest version and
    every 'D' row per key into ``_current_temp``; the main model carries those 'D'
    rows forward and emits a new 'D' for every key whose newest version is not a
    'D' and that is absent from ``bronze_<entity>_latest``.

    These tests reproduce that SQL directly in DuckDB — no dbt, no Dagster.
    """

    def _setup_conn(self, incr_delete_fixture) -> tuple[duckdb.DuckDBPyConnection, str, str]:
        """Build a DuckDB connection with:
        - ``main_silver.silver_thing``: initial Silver state (I + U rows)
        - ``thing_rebuilt``: the I/U rows a full refresh rebuilds from Bronze
        - ``silver_thing_current_temp``: pre-hook snapshot of silver_thing
        - ``bronze_latest``: the latest Bronze view (rows 2 and 3 only)

        Returns ``(conn, current_temp_table_name, bronze_latest_table_name)``.
        """
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")

        # Materialise the initial Silver state from two Bronze files
        initial_sql = _build_silver_initial_state_sql(incr_delete_fixture["data_dir"])
        conn.execute(f"CREATE TABLE main_silver.silver_thing AS ({initial_sql})")
        conn.execute(f"CREATE TABLE thing_rebuilt AS ({initial_sql})")

        # Build _current_temp as the pre-hook would: newest row per PK plus all D rows
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
        delete_sql = _build_delete_branch_sql(
            current_temp, bronze_latest, latest_ts, changes_table="thing_rebuilt"
        )
        df = conn.execute(delete_sql).fetchdf()

        deletes = df[df["LKHS_cdc_operation"] == "D"]
        assert len(deletes) == 1
        assert deletes.iloc[0]["id"] == 1

    def test_present_pk_not_marked_deleted(self, incr_delete_fixture):
        """Rows 2 and 3 are still in bronze_latest — neither should appear as 'D'."""
        conn, current_temp, bronze_latest = self._setup_conn(incr_delete_fixture)

        latest_ts = "CAST('2024-03-01 12:00:00' AS TIMESTAMP)"
        delete_sql = _build_delete_branch_sql(
            current_temp, bronze_latest, latest_ts, changes_table="thing_rebuilt"
        )
        df = conn.execute(delete_sql).fetchdf()

        assert 2 not in df[df["LKHS_cdc_operation"] == "D"]["id"].tolist()
        assert 3 not in df[df["LKHS_cdc_operation"] == "D"]["id"].tolist()

    def test_d_row_timestamped_from_latest_file(self, incr_delete_fixture):
        """The 'D' row's LKHS_date_valid_from should match the latest Bronze file's timestamp."""
        conn, current_temp, bronze_latest = self._setup_conn(incr_delete_fixture)

        latest_ts = "CAST('2024-03-01 12:00:00' AS TIMESTAMP)"
        delete_sql = _build_delete_branch_sql(
            current_temp, bronze_latest, latest_ts, changes_table="thing_rebuilt"
        )
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
        conn.execute(f"CREATE TABLE thing_rebuilt AS ({initial_sql})")

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
            "silver_thing_current_temp",
            "bronze_thing_latest",
            latest_ts,
            changes_table="thing_rebuilt",
            latest_filename="thing_20240201_120000.json",
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
        delete_sql = _build_delete_branch_sql(
            "silver_thing_current_temp", bronze_latest, latest_ts, changes_table="thing_rebuilt"
        )
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
        delete_sql = _build_delete_branch_sql(
            current_temp, bronze_latest, latest_ts, changes_table="thing_rebuilt"
        )

        # First run: append delete rows to Silver (by column name, as dbt does)
        conn.execute(f"INSERT INTO main_silver.silver_thing BY NAME ({delete_sql})")
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
        """Without 'D' rows, _current_temp holds one row per PK: the newest version.

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
        conn.execute(f"CREATE TABLE thing_rebuilt AS ({initial_sql})")

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
            "silver_thing_current_temp",
            "bronze_thing_latest",
            latest_ts,
            changes_table="thing_rebuilt",
            latest_filename="thing_20240201_120000.json",
        )
        df = conn.execute(delete_sql).fetchdf()

        assert len(df[df["LKHS_cdc_operation"] == "D"]) == 0


# ── Keys that come back after a delete, and multiple deletes per key ──────────


def _write_bronze(entity_dir, files: dict[str, list[dict]]) -> None:
    entity_dir.mkdir(parents=True, exist_ok=True)
    for name, rows in files.items():
        (entity_dir / name).write_text("".join(json.dumps(r) + "\n" for r in rows))


def _row(id_: int, name: str, value: int, day: str) -> dict:
    return {"id": id_, "name": name, "value": value, "opdateringsdato": f"{day}T00:00:00"}


class TestIncrementalSilverDeletedKeys:
    """Mirrors of the parts of the incremental macro that use Silver's 'D' rows.

    The real macros are exercised by ``test_dbt_silver.py``.
    """

    @staticmethod
    def _changes(tmp_path, deletes: list[tuple[int, str]] | None) -> list[tuple]:
        """I/U rows for a Bronze history in which key 1 is missing from no file
        but was deleted by an earlier full refresh (the given D rows)."""
        conn = duckdb.connect(":memory:")
        conn.execute("CREATE TABLE silver_deletes (id INTEGER, LKHS_date_valid_from TIMESTAMP)")
        for id_, ts in deletes or []:
            conn.execute("INSERT INTO silver_deletes VALUES (?, ?)", [id_, ts])
        sql = _build_silver_initial_state_sql(
            str(tmp_path / "bronze"), silver_deletes="silver_deletes" if deletes else None
        )
        return conn.execute(
            f"SELECT id, LKHS_cdc_operation, LKHS_date_valid_from::DATE::VARCHAR FROM ({sql}) "
            "ORDER BY id, LKHS_date_valid_from"
        ).fetchall()

    @pytest.fixture()
    def returning_key(self, tmp_path):
        """Key 1 in files 1, 3 and 4 (unchanged); key 2 in files 1, 3 and 4.
        File 2 is not part of the history: key 1 was deleted as of 2024-02-01."""
        _write_bronze(
            tmp_path / "bronze" / "thing",
            {
                "thing_20240101_120000.json": [
                    _row(1, "Alpha", 10, "2024-01-01"),
                    _row(2, "Beta", 20, "2024-01-01"),
                ],
                "thing_20240301_120000.json": [
                    _row(1, "Alpha", 10, "2024-01-01"),
                    _row(2, "Beta", 20, "2024-01-01"),
                ],
                "thing_20240401_120000.json": [
                    _row(1, "Alpha", 10, "2024-01-01"),
                    _row(2, "Beta", 20, "2024-01-01"),
                ],
            },
        )
        return tmp_path

    def test_unchanged_key_after_delete_is_insert(self, returning_key):
        rows = self._changes(returning_key, [(1, "2024-02-01 12:00:00")])
        assert rows == [
            (1, "I", "2024-01-01"),
            (1, "I", "2024-03-01"),  # back after the D; file 4 is unchanged again
            (2, "I", "2024-01-01"),
        ]

    def test_without_delete_unchanged_key_gives_no_row(self, returning_key):
        rows = self._changes(returning_key, None)
        assert rows == [(1, "I", "2024-01-01"), (2, "I", "2024-01-01")]

    def test_delete_before_previous_version_is_ignored(self, returning_key):
        """A D older than the key's previous Bronze version does not mark a return."""
        rows = self._changes(returning_key, [(1, "2023-12-01 12:00:00")])
        assert rows == [(1, "I", "2024-01-01"), (2, "I", "2024-01-01")]

    def test_changed_key_after_delete_is_insert_not_update(self, tmp_path):
        _write_bronze(
            tmp_path / "bronze" / "thing",
            {
                "thing_20240101_120000.json": [_row(1, "Alpha", 10, "2024-01-01")],
                "thing_20240301_120000.json": [_row(1, "Alpha-v2", 11, "2024-03-01")],
            },
        )
        rows = self._changes(tmp_path, [(1, "2024-02-01 12:00:00")])
        assert rows == [(1, "I", "2024-01-01"), (1, "I", "2024-03-01")]

    def test_current_temp_keeps_every_delete(self):
        """A key deleted and inserted twice keeps both D rows in _current_temp."""
        conn = duckdb.connect(":memory:")
        conn.execute("""
            CREATE TABLE silver AS SELECT * FROM (VALUES
                (7, 'DDD', TIMESTAMP '2024-01-01', 'I'), (7, 'DDD', TIMESTAMP '2024-02-01', 'D'),
                (7, 'DDD', TIMESTAMP '2024-03-01', 'I'), (7, 'DDD', TIMESTAMP '2024-04-01', 'D'),
                (7, 'DDD', TIMESTAMP '2024-05-01', 'I'), (8, 'DDD', TIMESTAMP '2024-01-01', 'I'),
                (8, 'DDD', TIMESTAMP '2024-02-01', 'U'))
            t(id, LKHS_source_system_code, LKHS_date_valid_from, LKHS_cdc_operation)
        """)
        rows = conn.execute(
            f"SELECT id, LKHS_cdc_operation, month(LKHS_date_valid_from) FROM ({_build_current_temp_sql('silver')}) "
            "ORDER BY id, LKHS_date_valid_from"
        ).fetchall()
        assert rows == [(7, "D", 2), (7, "D", 4), (7, "I", 5), (8, "U", 2)]


class TestIncrementalSilverDeleteBranchDetails:
    """File names and column alignment of the full-refresh delete branches."""

    @staticmethod
    def _conn() -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(":memory:")
        columns = """id INTEGER, name VARCHAR, value INTEGER, opdateringsdato TIMESTAMP,
            LKHS_filename VARCHAR, LKHS_source_system_code VARCHAR, LKHS_hash_value VARCHAR,
            LKHS_date_inserted_src TIMESTAMP, LKHS_date_valid_from TIMESTAMP,
            LKHS_date_inserted TIMESTAMP, LKHS_cdc_operation VARCHAR"""
        for table in ("current_temp", "rebuilt"):
            conn.execute(f"CREATE TABLE {table} ({columns})")
        conn.execute("CREATE TABLE bronze_latest (id INTEGER)")
        conn.execute("""CREATE TABLE files AS SELECT * FROM (VALUES
            (TIMESTAMP '2024-01-01 12:00:00', 'thing_20240101_120000.json'),
            (TIMESTAMP '2024-02-01 12:00:00', 'thing_20240201_120000.json'),
            (TIMESTAMP '2024-03-01 12:00:00', 'thing_20240301_120000.json'))
            t(LKHS_date_valid_from, LKHS_filename)""")
        return conn

    @staticmethod
    def _insert(conn, table: str, id_: int, filename: str, valid_from: str, op: str) -> None:
        conn.execute(
            f"INSERT INTO {table} VALUES (?, 'n', 1, NULL, ?, 'DDD', 'h', NULL, ?, NULL, ?)",
            [id_, filename, valid_from, op],
        )

    def _deletes(self, conn) -> list[tuple]:
        sql = _build_delete_branch_sql(
            "current_temp",
            "bronze_latest",
            "TIMESTAMP '2024-03-01 12:00:00'",
            changes_table="rebuilt",
            files_table="files",
        )
        return conn.execute(
            f"SELECT id, LKHS_date_valid_from::DATE::VARCHAR, LKHS_filename, LKHS_source_system_code "
            f"FROM ({sql}) ORDER BY id, LKHS_date_valid_from"
        ).fetchall()

    def test_new_delete_takes_the_latest_file_name(self):
        conn = self._conn()
        self._insert(conn, "rebuilt", 1, "thing_20240101_120000.json", "2024-01-01 12:00:00", "I")
        self._insert(
            conn, "current_temp", 1, "thing_20240101_120000.json", "2024-01-01 12:00:00", "I"
        )
        assert self._deletes(conn) == [(1, "2024-03-01", "thing_20240301_120000.json", "DDD")]

    def test_carried_delete_gets_the_file_name_of_its_timestamp(self):
        """Older macro versions stored the previous version's file name on D rows."""
        conn = self._conn()
        self._insert(conn, "rebuilt", 1, "thing_20240101_120000.json", "2024-01-01 12:00:00", "I")
        self._insert(
            conn, "current_temp", 1, "thing_20240101_120000.json", "2024-02-01 12:00:00", "D"
        )
        conn.execute("INSERT INTO bronze_latest VALUES (2)")
        assert self._deletes(conn) == [(1, "2024-02-01", "thing_20240201_120000.json", "DDD")]

    def test_carried_delete_keeps_its_name_when_the_file_is_gone(self):
        conn = self._conn()
        self._insert(
            conn, "current_temp", 1, "thing_20231101_120000.json", "2023-12-01 12:00:00", "D"
        )
        assert self._deletes(conn) == [(1, "2023-12-01", "thing_20231101_120000.json", "DDD")]

    def test_key_revived_by_the_rebuild_is_deleted_again_when_missing(self):
        """_current_temp's newest row is an old D, but the rebuild has a later I
        (a key the previous macro left deleted); missing from the latest file, it
        gets a new D on top of the carried one."""
        conn = self._conn()
        self._insert(conn, "rebuilt", 1, "thing_20240101_120000.json", "2024-01-01 12:00:00", "I")
        self._insert(conn, "rebuilt", 1, "thing_20240201_120000.json", "2024-02-01 12:00:00", "I")
        self._insert(
            conn, "current_temp", 1, "thing_20240101_120000.json", "2024-01-15 12:00:00", "D"
        )
        assert [r[:2] for r in self._deletes(conn)] == [(1, "2024-01-15"), (1, "2024-03-01")]

    def test_branches_are_combined_by_column_name(self):
        """After a new source column, Silver has it at the end, the rebuild in its
        Bronze position; D rows must still be aligned."""
        conn = self._conn()
        conn.execute("ALTER TABLE current_temp ADD COLUMN new_col VARCHAR")
        conn.execute("DROP TABLE rebuilt")
        conn.execute("""CREATE TABLE rebuilt (id INTEGER, name VARCHAR, new_col VARCHAR, value INTEGER,
            opdateringsdato TIMESTAMP, LKHS_filename VARCHAR, LKHS_source_system_code VARCHAR,
            LKHS_hash_value VARCHAR, LKHS_date_inserted_src TIMESTAMP, LKHS_date_valid_from TIMESTAMP,
            LKHS_date_inserted TIMESTAMP, LKHS_cdc_operation VARCHAR)""")
        conn.execute("""INSERT INTO current_temp BY NAME SELECT 1 AS id, 'n' AS name, 'x' AS new_col,
            'thing_20240101_120000.json' AS LKHS_filename, 'DDD' AS LKHS_source_system_code,
            TIMESTAMP '2024-02-01 12:00:00' AS LKHS_date_valid_from, 'D' AS LKHS_cdc_operation""")
        conn.execute("""INSERT INTO rebuilt BY NAME SELECT 2 AS id, 'n' AS name, 'y' AS new_col,
            'thing_20240101_120000.json' AS LKHS_filename, 'DDD' AS LKHS_source_system_code,
            TIMESTAMP '2024-01-01 12:00:00' AS LKHS_date_valid_from, 'I' AS LKHS_cdc_operation""")
        sql = _build_delete_branch_sql(
            "current_temp",
            "bronze_latest",
            "TIMESTAMP '2024-03-01 12:00:00'",
            changes_table="rebuilt",
            files_table="files",
        )
        rows = conn.execute(
            f"SELECT id, new_col, LKHS_source_system_code, LKHS_filename FROM ({sql}) ORDER BY id"
        ).fetchall()
        assert rows == [
            (1, "x", "DDD", "thing_20240201_120000.json"),  # carried
            (2, "y", "DDD", "thing_20240301_120000.json"),  # new
        ]
