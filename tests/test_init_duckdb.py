"""Tests for init_duckdb.init_s3_secret — S3 secret DDL generation."""

from unittest.mock import MagicMock, patch

import pytest

_GVE = "ddd_python.ddd_dbt.init_duckdb.get_variables_from_env"


def _make_gve_s3(**overrides):
    defaults = dict(
        RAW_STORAGE_TARGET="s3",
        S3_ACCESS_KEY_ID="mykey",
        S3_SECRET_ACCESS_KEY="mysecret",
        S3_ENDPOINT="http://minio:9000",
        S3_URL_STYLE="path",
        S3_USE_SSL="false",
        S3_REGION="us-east-1",
    )
    return MagicMock(**{**defaults, **overrides})


# ---------------------------------------------------------------------------
# init_s3_secret: no-op when RAW_STORAGE_TARGET != 's3'
# ---------------------------------------------------------------------------


def test_init_s3_secret_no_op_in_local_mode():
    from ddd_python.ddd_dbt.init_duckdb import init_s3_secret

    mock_conn = MagicMock()
    env = MagicMock(RAW_STORAGE_TARGET="local", STORAGE_TARGET="local")
    with patch(_GVE, env):
        init_s3_secret(mock_conn)
    mock_conn.execute.assert_not_called()


def test_init_s3_secret_fires_when_only_storage_target_is_s3():
    """init_s3_secret must run when STORAGE_TARGET=s3 even with RAW_STORAGE_TARGET=local."""
    from ddd_python.ddd_dbt.init_duckdb import init_s3_secret

    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock(fetchall=lambda: [])
    env = _make_gve_s3(RAW_STORAGE_TARGET="local", STORAGE_TARGET="s3")
    with patch(_GVE, env):
        init_s3_secret(mock_conn)

    # Should have called execute (CREATE SECRET + SELECT verify)
    assert mock_conn.execute.call_count >= 1
    create_call = mock_conn.execute.call_args_list[0][0][0]
    assert "CREATE OR REPLACE PERSISTENT SECRET ddd_s3_secret" in create_call


# ---------------------------------------------------------------------------
# init_s3_secret: AWS S3 — empty S3_ENDPOINT
# ---------------------------------------------------------------------------


def test_init_s3_secret_aws_s3_omits_endpoint_clause():
    # S3_ENDPOINT is empty — must not appear in the DDL at all.
    from ddd_python.ddd_dbt.init_duckdb import init_s3_secret

    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock(fetchall=lambda: [])
    with patch(_GVE, _make_gve_s3(S3_ENDPOINT="")):
        init_s3_secret(mock_conn)

    create_call = mock_conn.execute.call_args_list[0][0][0]
    assert "ENDPOINT" not in create_call
    assert "URL_STYLE" not in create_call
    assert "USE_SSL" not in create_call


def test_init_s3_secret_aws_s3_includes_key_id_secret_region():
    # Core credentials must be present even when no endpoint is set.
    from ddd_python.ddd_dbt.init_duckdb import init_s3_secret

    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock(fetchall=lambda: [])
    with patch(_GVE, _make_gve_s3(S3_ENDPOINT="")):
        init_s3_secret(mock_conn)

    sql = mock_conn.execute.call_args_list[0][0][0]
    assert "KEY_ID 'mykey'" in sql
    assert "SECRET 'mysecret'" in sql
    assert "REGION 'us-east-1'" in sql


def test_init_s3_secret_aws_s3_executes_create_secret():
    from ddd_python.ddd_dbt.init_duckdb import init_s3_secret

    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock(fetchall=lambda: [])
    with patch(_GVE, _make_gve_s3(S3_ENDPOINT="")):
        init_s3_secret(mock_conn)

    sql = mock_conn.execute.call_args_list[0][0][0]
    assert "CREATE OR REPLACE PERSISTENT SECRET ddd_s3_secret" in sql
    assert "TYPE s3" in sql


# ---------------------------------------------------------------------------
# init_s3_secret: MinIO — non-empty S3_ENDPOINT
# ---------------------------------------------------------------------------


def test_init_s3_secret_minio_includes_endpoint():
    from ddd_python.ddd_dbt.init_duckdb import init_s3_secret

    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock(fetchall=lambda: [])
    with patch(_GVE, _make_gve_s3()):
        init_s3_secret(mock_conn)

    sql = mock_conn.execute.call_args_list[0][0][0]
    # Protocol prefix stripped — DuckDB ENDPOINT takes host:port only
    assert "ENDPOINT 'minio:9000'" in sql
    assert "URL_STYLE 'path'" in sql
    assert "USE_SSL false" in sql


def test_init_s3_secret_minio_use_ssl_true():
    from ddd_python.ddd_dbt.init_duckdb import init_s3_secret

    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock(fetchall=lambda: [])
    with patch(_GVE, _make_gve_s3(S3_USE_SSL="true")):
        init_s3_secret(mock_conn)

    sql = mock_conn.execute.call_args_list[0][0][0]
    assert "USE_SSL true" in sql


# ---------------------------------------------------------------------------
# init_s3_secret: single-quote escaping in credential values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field,value,expected_fragment",
    [
        ("S3_ACCESS_KEY_ID", "k'ey", "KEY_ID 'k''ey'"),
        ("S3_SECRET_ACCESS_KEY", "s'ec", "SECRET 's''ec'"),
        ("S3_REGION", "us-'e1", "REGION 'us-''e1'"),
    ],
)
def test_init_s3_secret_escapes_single_quotes(field, value, expected_fragment):
    from ddd_python.ddd_dbt.init_duckdb import init_s3_secret

    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock(fetchall=lambda: [])
    with patch(_GVE, _make_gve_s3(**{field: value})):
        init_s3_secret(mock_conn)

    sql = mock_conn.execute.call_args_list[0][0][0]
    assert expected_fragment in sql


def test_init_s3_secret_escapes_single_quote_in_endpoint():
    from ddd_python.ddd_dbt.init_duckdb import init_s3_secret

    mock_conn = MagicMock()
    mock_conn.execute.return_value = MagicMock(fetchall=lambda: [])
    with patch(_GVE, _make_gve_s3(S3_ENDPOINT="http://mini'o:9000")):
        init_s3_secret(mock_conn)

    sql = mock_conn.execute.call_args_list[0][0][0]
    # Protocol stripped first, then single-quote in host:port portion is escaped
    assert "ENDPOINT 'mini''o:9000'" in sql
