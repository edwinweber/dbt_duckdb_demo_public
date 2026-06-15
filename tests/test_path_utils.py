"""Unit tests for ddd_utils.path_utils."""

import os
from unittest.mock import MagicMock, patch

from ddd_python.ddd_utils.path_utils import build_bronze_destination_path, build_delta_export_path

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

    def test_onelake_returns_folder_path(self):
        env = MagicMock(
            STORAGE_TARGET="onelake",
            FABRIC_ONELAKE_FOLDER_BRONZE="MyLakehouse.Lakehouse/Files/Bronze",
        )
        with patch(_ENV, env):
            result = build_bronze_destination_path("DDD", "aktoer")
        assert result == "MyLakehouse.Lakehouse/Files/Bronze/DDD/aktoer"

    def test_onelake_includes_source_system_and_entity(self):
        env = MagicMock(
            STORAGE_TARGET="onelake",
            FABRIC_ONELAKE_FOLDER_BRONZE="MyLakehouse.Lakehouse/Files/Bronze",
        )
        with patch(_ENV, env):
            result = build_bronze_destination_path("RFAM", "genome")
        assert "/RFAM/genome" in result


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
