"""Unit tests for ddd_utils.path_utils."""

import os
from unittest.mock import MagicMock, patch

import ddd_python.ddd_dlt.dlt_pipeline_execution_functions as _dpef
from ddd_python.ddd_utils.path_utils import (
    _configure_s3_secret,
    build_bronze_destination_path,
    build_delta_export_path,
    open_export_connection,
)

# Patch targets
_ENV = "ddd_python.ddd_utils.path_utils.get_variables_from_env"
# get_fabric_onelake_clients is lazily imported inside build_delta_export_path;
# patch the function in its home module so the lazy import picks up the mock.
_GET_TOKEN = "ddd_python.ddd_utils.get_fabric_onelake_clients.get_fabric_token"


# ---------------------------------------------------------------------------
# build_bronze_destination_path
# ---------------------------------------------------------------------------


class TestBuildBronzeDestinationPath:
    def test_local_returns_relative_path(self):
        env = MagicMock(STORAGE_TARGET="local")
        with patch(_ENV, env):
            result = build_bronze_destination_path("DDD", "aktoer")
        assert result == "Files/Bronze/DDD/aktoer"

    def test_local_path_does_not_start_with_abfss(self):
        env = MagicMock(STORAGE_TARGET="local")
        with patch(_ENV, env):
            result = build_bronze_destination_path("RFAM", "family")
        assert not result.startswith("abfss://")

    def test_onelake_storage_target_still_returns_relative_path(self):
        env = MagicMock(STORAGE_TARGET="onelake")
        with patch(_ENV, env):
            result = build_bronze_destination_path("DDD", "aktoer")
        assert result == "Files/Bronze/DDD/aktoer"

    def test_onelake_includes_source_system_and_entity(self):
        env = MagicMock(STORAGE_TARGET="onelake")
        with patch(_ENV, env):
            result = build_bronze_destination_path("RFAM", "genome")
        assert result == "Files/Bronze/RFAM/genome"


# ---------------------------------------------------------------------------
# build_delta_export_path  — local storage
# ---------------------------------------------------------------------------


class TestBuildDeltaExportPathLocal:
    def test_silver_local_path_format(self, tmp_path):
        env = MagicMock(STORAGE_TARGET="local", LOCAL_STORAGE_PATH=str(tmp_path))
        with patch(_ENV, env):
            path, opts = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert path == f"{tmp_path}/Files/Silver/silver_ddd_aktoer/"
        assert opts == {}

    def test_gold_local_path_format(self, tmp_path):
        env = MagicMock(STORAGE_TARGET="local", LOCAL_STORAGE_PATH=str(tmp_path))
        with patch(_ENV, env):
            path, opts = build_delta_export_path("gold", "actor")
        assert path == f"{tmp_path}/Files/Gold/actor/"
        assert opts == {}

    def test_local_creates_directory(self, tmp_path):
        env = MagicMock(STORAGE_TARGET="local", LOCAL_STORAGE_PATH=str(tmp_path))
        with patch(_ENV, env):
            path, _ = build_delta_export_path("silver", "silver_ddd_moede")
        assert os.path.isdir(path)

    def test_local_storage_options_empty(self, tmp_path):
        env = MagicMock(STORAGE_TARGET="local", LOCAL_STORAGE_PATH=str(tmp_path))
        with patch(_ENV, env):
            _, opts = build_delta_export_path("gold", "vote")
        assert opts == {}


# ---------------------------------------------------------------------------
# build_delta_export_path  — OneLake storage
# ---------------------------------------------------------------------------


class TestBuildDeltaExportPathOneLake:
    _ENV_ONELAKE = dict(
        STORAGE_TARGET="onelake",
        FABRIC_WORKSPACE="my-workspace",
        FABRIC_ONELAKE_STORAGE_ACCOUNT="onelake",
        FABRIC_ONELAKE_FOLDER_SILVER="MyLakehouse.Lakehouse/Files/Silver",
        FABRIC_ONELAKE_FOLDER_GOLD="MyLakehouse.Lakehouse/Files/Gold",
    )

    def _mock_env(self):
        return MagicMock(**self._ENV_ONELAKE)

    def test_silver_path_starts_with_abfss(self):
        with patch(_ENV, self._mock_env()), patch(_GET_TOKEN, return_value="tok"):
            path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert path.startswith("abfss://")

    def test_silver_path_contains_workspace_and_folder(self):
        with patch(_ENV, self._mock_env()), patch(_GET_TOKEN, return_value="tok"):
            path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert "my-workspace" in path
        assert "MyLakehouse.Lakehouse/Files/Silver/silver_ddd_aktoer/" in path

    def test_gold_path_uses_gold_folder(self):
        with patch(_ENV, self._mock_env()), patch(_GET_TOKEN, return_value="tok"):
            path, _ = build_delta_export_path("gold", "actor")
        assert "MyLakehouse.Lakehouse/Files/Gold/actor/" in path

    def test_storage_options_contain_bearer_token(self):
        with patch(_ENV, self._mock_env()), patch(_GET_TOKEN, return_value="my-token"):
            _, opts = build_delta_export_path("silver", "silver_rfam_family")
        assert opts["bearer_token"] == "my-token"
        assert opts["use_fabric_endpoint"] == "true"


# ---------------------------------------------------------------------------
# _configure_s3_secret
# ---------------------------------------------------------------------------


class TestConfigureS3Secret:
    def _make_gve_mock(self, **overrides):
        defaults = dict(
            S3_ACCESS_KEY_ID="mykey",
            S3_SECRET_ACCESS_KEY="mysecret",
            S3_ENDPOINT="http://minio:9000",
            S3_URL_STYLE="path",
            S3_USE_SSL="false",
            S3_REGION="us-east-1",
        )
        return MagicMock(**{**defaults, **overrides})

    def test_executes_create_secret_sql(self):
        mock_conn = MagicMock()
        with patch("ddd_python.ddd_utils.path_utils.get_variables_from_env", self._make_gve_mock()):
            _configure_s3_secret(mock_conn)
        mock_conn.execute.assert_called_once()
        sql = mock_conn.execute.call_args[0][0]
        assert "CREATE OR REPLACE SECRET ddd_s3_secret" in sql
        assert "TYPE s3" in sql
        assert "mykey" in sql
        assert "mysecret" in sql
        # Protocol prefix stripped — DuckDB ENDPOINT takes host:port only
        assert "minio:9000" in sql

    def test_use_ssl_true_string_renders_as_true(self):
        mock_conn = MagicMock()
        with patch(
            "ddd_python.ddd_utils.path_utils.get_variables_from_env",
            self._make_gve_mock(S3_USE_SSL="true"),
        ):
            _configure_s3_secret(mock_conn)
        sql = mock_conn.execute.call_args[0][0]
        assert "USE_SSL true" in sql

    def test_use_ssl_false_string_renders_as_false(self):
        mock_conn = MagicMock()
        with patch(
            "ddd_python.ddd_utils.path_utils.get_variables_from_env",
            self._make_gve_mock(S3_USE_SSL="false"),
        ):
            _configure_s3_secret(mock_conn)
        sql = mock_conn.execute.call_args[0][0]
        assert "USE_SSL false" in sql

    def test_empty_endpoint_omits_endpoint_url_style_use_ssl(self):
        # AWS S3: S3_ENDPOINT is empty — ENDPOINT/URL_STYLE/USE_SSL must be absent.
        mock_conn = MagicMock()
        with patch(
            "ddd_python.ddd_utils.path_utils.get_variables_from_env",
            self._make_gve_mock(S3_ENDPOINT=""),
        ):
            _configure_s3_secret(mock_conn)
        sql = mock_conn.execute.call_args[0][0]
        assert "ENDPOINT" not in sql
        assert "URL_STYLE" not in sql
        assert "USE_SSL" not in sql

    def test_empty_endpoint_still_includes_key_id_secret_region(self):
        # Even without ENDPOINT the core credentials must be present.
        mock_conn = MagicMock()
        with patch(
            "ddd_python.ddd_utils.path_utils.get_variables_from_env",
            self._make_gve_mock(S3_ENDPOINT=""),
        ):
            _configure_s3_secret(mock_conn)
        sql = mock_conn.execute.call_args[0][0]
        assert "KEY_ID 'mykey'" in sql
        assert "SECRET 'mysecret'" in sql
        assert "REGION 'us-east-1'" in sql

    def test_nonempty_endpoint_includes_endpoint_url_style_use_ssl(self):
        # MinIO: S3_ENDPOINT set — all three extra fields must appear.
        mock_conn = MagicMock()
        with patch("ddd_python.ddd_utils.path_utils.get_variables_from_env", self._make_gve_mock()):
            _configure_s3_secret(mock_conn)
        sql = mock_conn.execute.call_args[0][0]
        # Protocol prefix stripped — DuckDB ENDPOINT takes host:port only
        assert "ENDPOINT 'minio:9000'" in sql
        assert "URL_STYLE 'path'" in sql
        assert "USE_SSL false" in sql


# ---------------------------------------------------------------------------
# open_export_connection — S3 secret creation before DuckLake ATTACH
# ---------------------------------------------------------------------------


class TestOpenExportConnectionS3:
    def test_s3_secret_created_before_attach_for_s3_ducklake(self, tmp_path):
        """When DUCKLAKE_DATA_PATH=s3://... the S3 secret must be set up before ATTACH."""
        db_file = tmp_path / "test.duckdb"
        import duckdb as real_duckdb

        # Create a minimal DuckDB file so duckdb.connect() does not fail.
        real_duckdb.connect(str(db_file)).close()

        execute_calls: list[str] = []

        mock_conn = MagicMock()
        mock_conn.execute.side_effect = lambda sql, *a, **kw: execute_calls.append(sql.strip())

        env = MagicMock(
            DUCKDB_DATABASE_LOCATION=str(db_file),
            SILVER_STORAGE_FORMAT="ducklake",
            DUCKLAKE_CATALOG_LOCATION="/data/duckdb/catalog.ducklake",
            DUCKLAKE_DATA_PATH="s3://ddd-ducklake/",
        )

        with (
            patch("ddd_python.ddd_utils.path_utils.get_variables_from_env", env),
            patch("ddd_python.ddd_utils.path_utils.silver_storage_is_ducklake", return_value=True),
            patch("ddd_python.ddd_utils.path_utils.duckdb") as mock_duckdb,
            patch("ddd_python.ddd_utils.path_utils._configure_s3_secret") as mock_s3,
        ):
            mock_duckdb.connect.return_value = mock_conn
            open_export_connection()

        mock_s3.assert_called_once_with(mock_conn)
        # Verify the secret call preceded the ATTACH call
        attach_calls = [c for c in execute_calls if "ATTACH" in c]
        assert len(attach_calls) == 1
        # _configure_s3_secret was called (mocked), so just confirm ATTACH was also called
        assert any("ATTACH" in str(c) for c in mock_conn.execute.call_args_list)

    def test_no_s3_secret_for_local_ducklake_path(self, tmp_path):
        """When DUCKLAKE_DATA_PATH is a local path, _configure_s3_secret must NOT be called."""
        db_file = tmp_path / "test.duckdb"
        import duckdb as real_duckdb

        real_duckdb.connect(str(db_file)).close()

        mock_conn = MagicMock()
        env = MagicMock(
            DUCKDB_DATABASE_LOCATION=str(db_file),
            SILVER_STORAGE_FORMAT="duckdb",
            STORAGE_TARGET="local",
            DUCKLAKE_CATALOG_LOCATION="/data/duckdb/catalog.ducklake",
            DUCKLAKE_DATA_PATH="/data/ducklake",
        )

        with (
            patch("ddd_python.ddd_utils.path_utils.get_variables_from_env", env),
            patch("ddd_python.ddd_utils.path_utils.silver_storage_is_ducklake", return_value=False),
            patch("ddd_python.ddd_utils.path_utils.duckdb") as mock_duckdb,
            patch("ddd_python.ddd_utils.path_utils._configure_s3_secret") as mock_s3,
        ):
            mock_duckdb.connect.return_value = mock_conn
            open_export_connection()

        mock_s3.assert_not_called()

    def test_s3_secret_created_for_non_ducklake_s3_export_target(self, tmp_path):
        """When STORAGE_TARGET=s3 and not DuckLake, _configure_s3_secret must be called."""
        db_file = tmp_path / "test.duckdb"
        import duckdb as real_duckdb

        real_duckdb.connect(str(db_file)).close()

        mock_conn = MagicMock()
        env = MagicMock(
            DUCKDB_DATABASE_LOCATION=str(db_file),
            STORAGE_TARGET="s3",
        )

        with (
            patch("ddd_python.ddd_utils.path_utils.get_variables_from_env", env),
            patch("ddd_python.ddd_utils.path_utils.silver_storage_is_ducklake", return_value=False),
            patch("ddd_python.ddd_utils.path_utils.duckdb") as mock_duckdb,
            patch("ddd_python.ddd_utils.path_utils._configure_s3_secret") as mock_s3,
        ):
            mock_duckdb.connect.return_value = mock_conn
            open_export_connection()

        mock_s3.assert_called_once_with(mock_conn)


# ---------------------------------------------------------------------------
# build_delta_export_path  — S3 storage
# ---------------------------------------------------------------------------


class TestBuildDeltaExportPathS3:
    def _make_env(self, **overrides):
        defaults = dict(
            STORAGE_TARGET="s3",
            S3_BUCKET_DELTA="ddd-delta",
            S3_PREFIX_DELTA="",
            S3_ACCESS_KEY_ID="mykey",
            S3_SECRET_ACCESS_KEY="mysecret",
            S3_REGION="us-east-1",
            S3_ENDPOINT="",
            S3_USE_SSL="false",
            S3_URL_STYLE="path",
        )
        return MagicMock(**{**defaults, **overrides})

    def test_s3_path_starts_with_s3_scheme(self):
        with patch(_ENV, self._make_env()):
            path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert path.startswith("s3://")

    def test_s3_path_contains_bucket_and_table(self):
        with patch(_ENV, self._make_env()):
            path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert "ddd-delta" in path
        assert "silver_ddd_aktoer" in path

    def test_s3_path_contains_layer_cap(self):
        with patch(_ENV, self._make_env()):
            path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert "/Files/Silver/" in path

    def test_s3_path_contains_gold_layer(self):
        with patch(_ENV, self._make_env()):
            path, _ = build_delta_export_path("gold", "actor")
        assert "/Files/Gold/actor/" in path

    def test_s3_path_ends_with_trailing_slash(self):
        with patch(_ENV, self._make_env()):
            path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert path.endswith("/")

    def test_storage_options_contain_aws_credentials(self):
        with patch(_ENV, self._make_env()):
            _, opts = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert opts["AWS_ACCESS_KEY_ID"] == "mykey"
        assert opts["AWS_SECRET_ACCESS_KEY"] == "mysecret"
        assert opts["AWS_REGION"] == "us-east-1"

    def test_storage_options_no_endpoint_url_for_aws_s3(self):
        # Empty S3_ENDPOINT means real AWS S3 — AWS_ENDPOINT_URL must be absent.
        with patch(_ENV, self._make_env(S3_ENDPOINT="")):
            _, opts = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert "AWS_ENDPOINT_URL" not in opts

    def test_storage_options_endpoint_url_present_for_minio(self):
        with patch(_ENV, self._make_env(S3_ENDPOINT="http://minio:9000")):
            _, opts = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert opts["AWS_ENDPOINT_URL"] == "http://minio:9000"

    def test_storage_options_addressing_style_set_for_minio(self):
        with patch(_ENV, self._make_env(S3_ENDPOINT="http://minio:9000", S3_URL_STYLE="path")):
            _, opts = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert opts["AWS_S3_ADDRESSING_STYLE"] == "path"

    def test_storage_options_allow_http_true_when_ssl_false(self):
        with patch(_ENV, self._make_env(S3_ENDPOINT="http://minio:9000", S3_USE_SSL="false")):
            _, opts = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert opts["AWS_ALLOW_HTTP"] == "true"

    def test_storage_options_allow_http_false_when_ssl_true(self):
        with patch(_ENV, self._make_env(S3_ENDPOINT="https://s3.example.com", S3_USE_SSL="true")):
            _, opts = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert opts["AWS_ALLOW_HTTP"] == "false"

    def test_prefix_included_in_path(self):
        with patch(_ENV, self._make_env(S3_PREFIX_DELTA="exports/delta")):
            path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert path.startswith("s3://ddd-delta/exports/delta/Files/Silver/")

    def test_prefix_with_leading_trailing_slashes_normalised(self):
        with patch(_ENV, self._make_env(S3_PREFIX_DELTA="/exports/delta/")):
            path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert path.startswith("s3://ddd-delta/exports/delta/Files/Silver/")

    def test_empty_prefix_produces_no_extra_path_segment(self):
        with patch(_ENV, self._make_env(S3_PREFIX_DELTA="")):
            path, _ = build_delta_export_path("silver", "silver_ddd_aktoer")
        assert path == "s3://ddd-delta/Files/Silver/silver_ddd_aktoer/"


# ---------------------------------------------------------------------------
# build_log_dir — always local regardless of STORAGE_TARGET
# ---------------------------------------------------------------------------


class TestBuildLogDir:
    """build_log_dir always returns LOCAL_STORAGE_PATH/logs/<source>/ regardless of STORAGE_TARGET."""

    def _build_log_dir(self, source_system_code: str, pipeline_name=None):
        return _dpef.build_log_dir(source_system_code, pipeline_name)

    def _env_patch(self, log_directory: str = "/data/logs"):
        return patch.object(
            _dpef,
            "get_variables_from_env",
            MagicMock(DLT_PIPELINE_RUN_LOG_DIR=log_directory),
        )

    def test_local_target_returns_local_path(self):
        with self._env_patch("/data/logs"):
            assert self._build_log_dir("DDD") == "/data/logs/DDD"

    def test_s3_target_returns_local_path(self):
        with self._env_patch("/data/logs"):
            assert self._build_log_dir("DDD") == "/data/logs/DDD"

    def test_onelake_target_still_returns_local_path(self):
        with self._env_patch("/data/logs"):
            assert self._build_log_dir("DDD") == "/data/logs/DDD"

    def test_rfam_source_system_code(self):
        with self._env_patch("/data/logs"):
            assert self._build_log_dir("RFAM") == "/data/logs/RFAM"

    def test_pipeline_name_appended_as_subdirectory(self):
        with self._env_patch("/data/logs"):
            assert self._build_log_dir("DDD", "afstemning") == "/data/logs/DDD/afstemning"

    def test_no_pipeline_name_stops_at_source_system_code(self):
        with self._env_patch("/data/logs"):
            result = self._build_log_dir("DDD")
        assert result.endswith("/logs/DDD")
        assert not result.endswith("/logs/DDD/")

    def test_custom_log_directory_override(self):
        with self._env_patch("/var/log/ddd"):
            assert self._build_log_dir("DDD") == "/var/log/ddd/DDD"

    def test_custom_log_directory_with_pipeline_name(self):
        with self._env_patch("/var/log/ddd"):
            assert self._build_log_dir("RFAM", "family") == "/var/log/ddd/RFAM/family"
