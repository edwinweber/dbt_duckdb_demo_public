"""Integration tests for the Gold star-schema layer.

Uses an in-memory DuckDB with pre-populated Silver tables to verify:

* SCD Type 2 dimension logic (LEAD for date_valid_to, ROW_NUMBER for version)
* Surrogate key generation via the cast_hash_to_bigint pattern
* Business key generation (source_system_code + '-' + id)
* Fact table joins resolving surrogate keys from dimensions
* Unknown/default dimension rows (id = 0)
* Correct filtering of deleted rows in fact tables
"""

import duckdb
import pytest


@pytest.fixture()
def gold_duckdb():
    """Create an in-memory DuckDB with Silver-like tables for Gold tests.

    Populates:
    - silver_dimension: a simple SCD2 dimension (like actor or vote_type)
    - silver_fact: a simple fact reference (like stemme / individual vote)
    """
    conn = duckdb.connect(":memory:")

    # ── Silver dimension: two versions of row 1, one version of row 2 ──
    conn.execute("""
        CREATE TABLE silver_dimension AS
        SELECT * FROM (VALUES
            (1, 'Widget-A',   'DDD', '2024-01-01 12:00:00'::TIMESTAMP, 'I'),
            (1, 'Widget-A-v2','DDD', '2024-02-01 12:00:00'::TIMESTAMP, 'U'),
            (2, 'Widget-B',   'DDD', '2024-01-01 12:00:00'::TIMESTAMP, 'I'),
            (3, 'Widget-C',   'DDD', '2024-01-01 12:00:00'::TIMESTAMP, 'I'),
            (3, 'Widget-C',   'DDD', '2024-03-01 12:00:00'::TIMESTAMP, 'D')
        ) AS t(id, name, LKHS_source_system_code, LKHS_date_valid_from, LKHS_cdc_operation)
    """)

    # ── Silver fact table (like individual votes) ──
    conn.execute("""
        CREATE TABLE silver_fact AS
        SELECT * FROM (VALUES
            (101, 1, 'yes',   'DDD', '2024-01-15 12:00:00'::TIMESTAMP, 'I'),
            (102, 2, 'no',    'DDD', '2024-01-15 12:00:00'::TIMESTAMP, 'I'),
            (103, 3, 'yes',   'DDD', '2024-01-15 12:00:00'::TIMESTAMP, 'I'),
            (104, 1, 'maybe', 'DDD', '2024-02-15 12:00:00'::TIMESTAMP, 'D')
        ) AS t(id, dimension_id, vote_value, LKHS_source_system_code, LKHS_date_valid_from, LKHS_cdc_operation)
    """)

    yield conn
    conn.close()


# ── Surrogate key tests ──────────────────────────────────────────────


_CAST_HASH_TO_BIGINT = """
    CAST(
        CASE
            WHEN hash(cols) > 9223372036854775807
                THEN -1 * (hash(cols) - 9223372036854775807)
            ELSE hash(cols)
        END
        AS BIGINT
    )
"""


def test_surrogate_key_is_deterministic(gold_duckdb):
    """The same input should always produce the same surrogate key."""
    result = gold_duckdb.execute("""
        SELECT
            CAST(CASE WHEN hash('DDD', 1, '2024-01-01 12:00:00'::TIMESTAMP) > 9223372036854775807
                      THEN -1 * (hash('DDD', 1, '2024-01-01 12:00:00'::TIMESTAMP) - 9223372036854775807)
                      ELSE hash('DDD', 1, '2024-01-01 12:00:00'::TIMESTAMP)
                 END AS BIGINT) AS key1,
            CAST(CASE WHEN hash('DDD', 1, '2024-01-01 12:00:00'::TIMESTAMP) > 9223372036854775807
                      THEN -1 * (hash('DDD', 1, '2024-01-01 12:00:00'::TIMESTAMP) - 9223372036854775807)
                      ELSE hash('DDD', 1, '2024-01-01 12:00:00'::TIMESTAMP)
                 END AS BIGINT) AS key2
    """).fetchone()

    assert result[0] == result[1]
    assert isinstance(result[0], int)


def test_surrogate_key_differs_across_versions(gold_duckdb):
    """Different date_valid_from values should produce different surrogate keys."""
    result = gold_duckdb.execute("""
        SELECT
            CAST(CASE WHEN hash('DDD', 1, '2024-01-01 12:00:00'::TIMESTAMP) > 9223372036854775807
                      THEN -1 * (hash('DDD', 1, '2024-01-01 12:00:00'::TIMESTAMP) - 9223372036854775807)
                      ELSE hash('DDD', 1, '2024-01-01 12:00:00'::TIMESTAMP)
                 END AS BIGINT) AS key_v1,
            CAST(CASE WHEN hash('DDD', 1, '2024-02-01 12:00:00'::TIMESTAMP) > 9223372036854775807
                      THEN -1 * (hash('DDD', 1, '2024-02-01 12:00:00'::TIMESTAMP) - 9223372036854775807)
                      ELSE hash('DDD', 1, '2024-02-01 12:00:00'::TIMESTAMP)
                 END AS BIGINT) AS key_v2
    """).fetchone()

    assert result[0] != result[1]


# ── SCD Type 2 dimension tests ───────────────────────────────────────


_DIMENSION_SQL = """
    WITH CTE_SRC AS (
        SELECT
            CAST(CASE
                     WHEN hash(src.LKHS_source_system_code, src.id, src.LKHS_date_valid_from) > 9223372036854775807
                         THEN -1 * (hash(src.LKHS_source_system_code, src.id, src.LKHS_date_valid_from) - 9223372036854775807)
                     ELSE hash(src.LKHS_source_system_code, src.id, src.LKHS_date_valid_from)
                 END AS BIGINT) AS LKHS_dim_id
        ,   src.*
        ,   CONCAT(src.LKHS_source_system_code, '-', CAST(src.id AS VARCHAR)) AS dim_bk
        ,   LEAD(src.LKHS_date_valid_from, 1, CAST('9999-12-31' AS DATETIME))
                OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
        ,   ROW_NUMBER()
                OVER (PARTITION BY src.LKHS_source_system_code, src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
        FROM silver_dimension src
    )
    SELECT  CTE_SRC.LKHS_dim_id
    ,       CTE_SRC.dim_bk
    ,       CTE_SRC.id
    ,       CTE_SRC.name
    ,       CTE_SRC.LKHS_source_system_code
    ,       CTE_SRC.LKHS_cdc_operation
    ,       CASE
                WHEN CTE_SRC.LKHS_row_version = 1
                    THEN CAST('1900-01-01' AS DATETIME)
                ELSE CAST(CTE_SRC.LKHS_date_valid_from AS DATETIME)
            END AS LKHS_date_valid_from
    ,       CTE_SRC.LKHS_date_valid_to
    ,       CTE_SRC.LKHS_row_version
    FROM CTE_SRC

    UNION ALL

    SELECT 0                              AS LKHS_dim_id
    ,      'Unknown'                      AS dim_bk
    ,      0                              AS id
    ,      'Unknown'                      AS name
    ,      'LKHS'                         AS LKHS_source_system_code
    ,      'I'                            AS LKHS_cdc_operation
    ,      CAST('1900-01-01' AS DATETIME) AS LKHS_date_valid_from
    ,      CAST('9999-12-31' AS DATETIME) AS LKHS_date_valid_to
    ,      1                              AS LKHS_row_version
"""


def test_scd2_date_valid_to_chains_correctly(gold_duckdb):
    """For row 1 (2 versions), the first version's date_valid_to should
    equal the second version's date_valid_from."""
    df = gold_duckdb.execute(_DIMENSION_SQL).fetchdf()

    row1 = df[df["id"] == 1].sort_values("LKHS_row_version")
    assert len(row1) == 2
    # version 1's end date = version 2's date_valid_from (2024-02-01)
    assert row1.iloc[0]["LKHS_date_valid_to"] == row1.iloc[1]["LKHS_date_valid_from"]
    # latest version has the sentinel end date
    assert row1.iloc[1]["LKHS_date_valid_to"].year == 9999


def test_scd2_first_version_starts_at_1900(gold_duckdb):
    """The first version of each dimension row should have date_valid_from = 1900-01-01."""
    df = gold_duckdb.execute(_DIMENSION_SQL).fetchdf()

    version_1_rows = df[(df["LKHS_row_version"] == 1) & (df["id"] != 0)]
    for _, row in version_1_rows.iterrows():
        assert row["LKHS_date_valid_from"].year == 1900

def test_scd2_row_version_is_sequential(gold_duckdb):
    """Row version should be 1, 2, ... per PK."""
    df = gold_duckdb.execute(_DIMENSION_SQL).fetchdf()

    row1 = df[df["id"] == 1].sort_values("LKHS_row_version")
    assert list(row1["LKHS_row_version"]) == [1, 2]


def test_unknown_row_exists_with_id_zero(gold_duckdb):
    """The dimension should include an 'Unknown' default row with id 0."""
    df = gold_duckdb.execute(_DIMENSION_SQL).fetchdf()

    unknown = df[df["id"] == 0]
    assert len(unknown) == 1
    assert unknown.iloc[0]["dim_bk"] == "Unknown"
    assert unknown.iloc[0]["LKHS_dim_id"] == 0


def test_business_key_format(gold_duckdb):
    """Business keys should be formatted as '{source_system_code}-{id}'."""
    df = gold_duckdb.execute(_DIMENSION_SQL).fetchdf()

    row1 = df[df["id"] == 1].iloc[0]
    assert row1["dim_bk"] == "DDD-1"


# ── Current-version dimension view ───────────────────────────────────


_DIMENSION_CV_SQL = f"""
    WITH dim AS ({_DIMENSION_SQL})
    SELECT *
    FROM   dim
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dim.LKHS_source_system_code, dim.id
        ORDER BY dim.LKHS_date_valid_from DESC
    ) = 1
"""


def test_cv_dimension_returns_latest_per_pk(gold_duckdb):
    """The _cv view should return only the latest version of each dimension row."""
    df = gold_duckdb.execute(_DIMENSION_CV_SQL).fetchdf()

    # 3 PKs (1, 2, 3) + Unknown (0) = 4
    assert len(df) == 4

    row1 = df[df["id"] == 1].iloc[0]
    assert row1["name"] == "Widget-A-v2"
    assert row1["LKHS_row_version"] == 2


# ── Fact table join tests ────────────────────────────────────────────


_FACT_SQL = f"""
    WITH dim_cv AS ({_DIMENSION_CV_SQL})
    , fact AS (
        SELECT src.*
        FROM   silver_fact src
        WHERE  src.LKHS_cdc_operation != 'D'
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY src.LKHS_source_system_code, src.id
            ORDER BY src.LKHS_date_valid_from DESC
        ) = 1
    )
    SELECT
        COALESCE(dim_cv.LKHS_dim_id, 0) AS LKHS_dim_id
    ,   fact.id                          AS fact_id
    ,   fact.vote_value
    ,   dim_cv.dim_bk
    FROM      fact
    LEFT JOIN dim_cv
    ON        CONCAT(fact.LKHS_source_system_code, '-', CAST(fact.dimension_id AS VARCHAR)) = dim_cv.dim_bk
"""


def test_fact_excludes_deleted_rows(gold_duckdb):
    """Fact rows with LKHS_cdc_operation = 'D' should not appear."""
    df = gold_duckdb.execute(_FACT_SQL).fetchdf()

    # fact row 104 is D → excluded; rows 101, 102, 103 remain
    assert len(df) == 3
    assert 104 not in df["fact_id"].tolist()


def test_fact_resolves_dimension_surrogate_key(gold_duckdb):
    """Fact rows should have a non-zero dimension surrogate key when the dimension exists."""
    df = gold_duckdb.execute(_FACT_SQL).fetchdf()

    # All fact rows reference existing dimension PKs (1, 2, 3) → should resolve
    for _, row in df.iterrows():
        assert row["LKHS_dim_id"] != 0


def test_fact_coalesces_to_zero_for_missing_dimension(gold_duckdb):
    """If a fact row references a dimension that doesn't exist,
    COALESCE should produce the unknown key (0)."""
    # Add a fact row referencing dimension_id=999 (doesn't exist)
    gold_duckdb.execute("""
        INSERT INTO silver_fact VALUES
        (105, 999, 'unknown', 'DDD', '2024-05-01 12:00:00'::TIMESTAMP, 'I')
    """)

    df = gold_duckdb.execute(_FACT_SQL).fetchdf()
    orphan = df[df["fact_id"] == 105].iloc[0]
    assert orphan["LKHS_dim_id"] == 0
