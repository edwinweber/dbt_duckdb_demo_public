"""S3/MinIO integration tests for the Bronze → Silver → Delta Lake pipeline.

Exercises code paths that actually talk to S3-compatible storage:
1. DuckDB httpfs reading NDJSON from MinIO via ``read_json_auto()`` + ``read_text()``
2. Full-extraction CDC SQL over S3-hosted Bronze files
3. ``write_deltalake()`` writing a Delta table to MinIO
4. ``DeltaTable()`` reading the Delta table back from MinIO
5. Incremental dedup: second export of the same data appends 0 rows

All tests are skipped when ``S3_TEST_ENDPOINT`` is not set, so the standard
``pytest tests/`` run (which does not set that var) is unaffected.
In CI the ``test-s3`` job starts a MinIO container and sets the env var.
"""

import json
import os

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

import duckdb

# ---------------------------------------------------------------------------
# Skip the whole module when no S3 endpoint is available
# ---------------------------------------------------------------------------

pytestmark = [
    pytest.mark.s3_integration,
    pytest.mark.skipif(
        not os.getenv("S3_TEST_ENDPOINT"),
        reason="S3 integration tests require S3_TEST_ENDPOINT (e.g. http://localhost:9000)",
    ),
]

_ENDPOINT = os.getenv("S3_TEST_ENDPOINT", "http://localhost:9000")
_KEY = os.getenv("S3_TEST_ACCESS_KEY_ID", "minioadmin")
_SECRET = os.getenv("S3_TEST_SECRET_ACCESS_KEY", "minioadmin")
_REGION = os.getenv("S3_TEST_REGION", "us-east-1")

_BRONZE_BUCKET = "test-ddd-bronze"
_DELTA_BUCKET = "test-ddd-delta"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=_ENDPOINT,
        aws_access_key_id=_KEY,
        aws_secret_access_key=_SECRET,
        region_name=_REGION,
        config=Config(s3={"addressing_style": "path"}),
    )


@pytest.fixture(scope="module")
def s3_client():
    """Boto3 S3 client pointed at the MinIO test instance."""
    return _make_s3_client()


@pytest.fixture(scope="module", autouse=True)
def minio_buckets(s3_client):
    """Create test buckets before any test in the module; delete all objects and buckets after."""
    for bucket in (_BRONZE_BUCKET, _DELTA_BUCKET):
        try:
            s3_client.create_bucket(Bucket=bucket)
        except ClientError as e:
            if e.response["Error"]["Code"] not in (
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            ):
                raise
    yield
    for bucket in (_BRONZE_BUCKET, _DELTA_BUCKET):
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
        s3_client.delete_bucket(Bucket=bucket)


@pytest.fixture(scope="module")
def bronze_ndjson_in_s3(s3_client, minio_buckets):
    """Upload two-file NDJSON fixture to the MinIO Bronze bucket.

    File 1 (2024-01-01): rows 1, 2
    File 2 (2024-02-01): row 1 changed (price), row 2 same, row 3 new

    Returns the S3 glob URI that DuckDB should use to read both files.
    """
    file1 = [
        {"id": 1, "name": "Apple", "price": 1.50, "updated": "2024-01-01T00:00:00"},
        {"id": 2, "name": "Banana", "price": 0.75, "updated": "2024-01-01T00:00:00"},
    ]
    file2 = [
        {"id": 1, "name": "Apple", "price": 1.75, "updated": "2024-02-01T00:00:00"},
        {"id": 2, "name": "Banana", "price": 0.75, "updated": "2024-02-01T00:00:00"},
        {"id": 3, "name": "Cherry", "price": 2.00, "updated": "2024-02-01T00:00:00"},
    ]
    prefix = "Files/Bronze/DDD/item"
    for filename, rows in [
        ("item_20240101_120000.json", file1),
        ("item_20240201_120000.json", file2),
    ]:
        body = "\n".join(json.dumps(r) for r in rows) + "\n"
        s3_client.put_object(
            Bucket=_BRONZE_BUCKET,
            Key=f"{prefix}/{filename}",
            Body=body.encode(),
        )
    return f"s3://{_BRONZE_BUCKET}/{prefix}/item_*.json"


@pytest.fixture()
def duckdb_with_s3(minio_buckets):
    """Fresh in-memory DuckDB connection with httpfs loaded and a MinIO S3 secret configured.

    Function-scoped so each test gets an isolated in-memory database.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    # DuckDB ENDPOINT takes host:port only — strip the protocol prefix.
    endpoint_host = _ENDPOINT.split("://", 1)[-1]
    conn.execute(
        f"CREATE OR REPLACE SECRET minio_test ("
        f"TYPE s3, KEY_ID '{_KEY}', SECRET '{_SECRET}', "
        f"ENDPOINT '{endpoint_host}', URL_STYLE 'path', USE_SSL false, REGION '{_REGION}')"
    )
    yield conn
    conn.close()


def _delta_storage_opts() -> dict[str, str]:
    """Storage options for ``write_deltalake`` / ``DeltaTable`` against MinIO."""
    return {
        "AWS_ACCESS_KEY_ID": _KEY,
        "AWS_SECRET_ACCESS_KEY": _SECRET,
        "AWS_ENDPOINT_URL": _ENDPOINT,
        "AWS_REGION": _REGION,
        "AWS_S3_ADDRESSING_STYLE": "path",
        "AWS_ALLOW_HTTP": "true",
    }


def _run_cdc_sql_from_s3(conn: duckdb.DuckDBPyConnection, glob_path: str) -> None:
    """Bronze → Silver CDC in DuckDB, reading source NDJSON from S3.

    Mirrors the full-extraction CDC SQL generated by
    ``generate_model_silver_full_extraction.sql``.  The glob_path must be an
    S3 URI matching the two fixture files.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS main_bronze")
    conn.execute(f"""
        CREATE OR REPLACE VIEW main_bronze.bronze_item AS
        SELECT DISTINCT
               COLUMNS(c -> c != 'filename' AND NOT starts_with(c, '_dlt_'))
        ,      SUBSTRING(filename,
                   LENGTH(filename)
                   - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
        ,      'TEST' AS LKHS_source_system_code
        FROM   read_json_auto('{glob_path}', filename=True, union_by_name=true)
    """)

    cdc_sql = f"""
    WITH CTE_BRONZE AS (
        SELECT src.*
        ,      sha256(CONCAT(
                   COALESCE(src.name::VARCHAR, '<NULL>'), ']##[',
                   COALESCE(src.price::VARCHAR, '<NULL>'), ']##['
               )) AS LKHS_hash_value
        ,      CAST(MIN(src.updated) OVER (PARTITION BY src.id) AS DATETIME) AS LKHS_date_inserted_src
        FROM   main_bronze.bronze_item src
    )
    ,CTE_FILES AS (
        SELECT LKHS_filename
        ,      strptime(SUBSTRING(LKHS_filename,
                   LENGTH(LKHS_filename) - POSITION('.' IN REVERSE(LKHS_filename)) - 14, 15),
                   '%Y%m%d_%H%M%S') AS LKHS_date_valid_from
        ,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
        ,      LEAD(LKHS_filename) OVER (ORDER BY LKHS_filename) AS LKHS_filename_next
        ,      LEAD(strptime(SUBSTRING(LKHS_filename,
                   LENGTH(LKHS_filename) - POSITION('.' IN REVERSE(LKHS_filename)) - 14, 15),
                   '%Y%m%d_%H%M%S'))
               OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_next
        FROM   (SELECT SUBSTRING(filename,
                           LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
                FROM   read_text('{glob_path}')
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
                WHEN b.LKHS_pk_prev IS NOT NULL
                 AND b.LKHS_hash_value != b.LKHS_hash_value_previous THEN 'U'
            END AS LKHS_cdc_operation
    FROM    CTE_BRONZE_INCL_LAG b
    WHERE   CASE
                WHEN b.LKHS_pk_prev IS NULL THEN 'I'
                WHEN b.LKHS_pk_prev IS NOT NULL
                 AND b.LKHS_hash_value != b.LKHS_hash_value_previous THEN 'U'
            END IN ('I', 'U')
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS main_silver")
    conn.execute(f"CREATE TABLE main_silver.silver_item AS ({cdc_sql})")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_s3_duckdb_reads_json_from_s3(bronze_ndjson_in_s3, duckdb_with_s3):
    """DuckDB httpfs must read NDJSON files from MinIO via read_json_auto."""
    count = duckdb_with_s3.execute(
        f"SELECT COUNT(*) FROM read_json_auto('{bronze_ndjson_in_s3}', union_by_name=true)"
    ).fetchone()[0]
    # file1 has 2 rows, file2 has 3 rows
    assert count == 5


def test_s3_write_deltalake_and_read_back(duckdb_with_s3):
    """write_deltalake() must write to MinIO and DeltaTable() must read the result back."""
    arrow_table = duckdb_with_s3.execute(
        "SELECT 1 AS id, 'Alice' AS name UNION ALL SELECT 2, 'Bob'"
    ).to_arrow_table()
    delta_path = f"s3://{_DELTA_BUCKET}/Files/Silver/test_roundtrip/"
    opts = _delta_storage_opts()

    write_deltalake(delta_path, arrow_table, mode="overwrite", storage_options=opts)

    result = DeltaTable(delta_path, storage_options=opts).to_pyarrow_table()
    assert result.num_rows == 2
    assert set(result.column("name").to_pylist()) == {"Alice", "Bob"}


def test_s3_bronze_to_silver_cdc_row_count(bronze_ndjson_in_s3, duckdb_with_s3):
    """CDC reading S3-hosted Bronze JSON must produce 4 rows: I=3 (rows 1,2,3), U=1 (row 1 update)."""
    _run_cdc_sql_from_s3(duckdb_with_s3, bronze_ndjson_in_s3)

    count = duckdb_with_s3.execute("SELECT COUNT(*) FROM main_silver.silver_item").fetchone()[0]
    assert count == 4

    ops = duckdb_with_s3.execute(
        "SELECT LKHS_cdc_operation, COUNT(*) FROM main_silver.silver_item GROUP BY 1 ORDER BY 1"
    ).fetchdf()
    op_dict = dict(zip(ops.iloc[:, 0], ops.iloc[:, 1], strict=False))
    assert op_dict["I"] == 3
    assert op_dict["U"] == 1


def test_s3_full_pipeline_bronze_to_silver_to_delta(bronze_ndjson_in_s3, duckdb_with_s3):
    """Full pipeline: DuckDB reads Bronze from S3, CDC runs in-memory, Silver exported to
    a Delta Lake table on S3, and the Delta table reads back with the correct row count."""
    _run_cdc_sql_from_s3(duckdb_with_s3, bronze_ndjson_in_s3)

    arrow_table = duckdb_with_s3.execute("SELECT * FROM main_silver.silver_item").to_arrow_table()
    delta_path = f"s3://{_DELTA_BUCKET}/Files/Silver/silver_item/"
    opts = _delta_storage_opts()

    write_deltalake(delta_path, arrow_table, mode="overwrite", storage_options=opts)

    result = DeltaTable(delta_path, storage_options=opts).to_pyarrow_table()
    assert result.num_rows == 4


def test_s3_incremental_export_no_duplicates(bronze_ndjson_in_s3, duckdb_with_s3):
    """Second export of the same Silver data must find 0 new rows (pk + LKHS_date_valid_from dedup)."""
    _run_cdc_sql_from_s3(duckdb_with_s3, bronze_ndjson_in_s3)

    arrow_table = duckdb_with_s3.execute("SELECT * FROM main_silver.silver_item").to_arrow_table()
    delta_path = f"s3://{_DELTA_BUCKET}/Files/Silver/silver_item_dedup/"
    opts = _delta_storage_opts()

    # First load
    write_deltalake(delta_path, arrow_table, mode="overwrite", storage_options=opts)

    # Incremental: anti-join to find rows not already in the target
    existing = DeltaTable(delta_path, storage_options=opts).to_pyarrow_table()
    duckdb_with_s3.register("target_table", existing)

    new_rows = duckdb_with_s3.execute("""
        SELECT src.*
        FROM   main_silver.silver_item src
        LEFT JOIN target_table tgt
               ON src.id = tgt.id
              AND src.LKHS_date_valid_from = tgt.LKHS_date_valid_from
        WHERE  tgt.id IS NULL
    """).to_arrow_table()

    assert new_rows.num_rows == 0
