"""Tests for the _require helper and eager env-var validation in get_variables_from_env."""

import importlib
import sys

import pytest


def test_require_returns_value(monkeypatch):
    monkeypatch.setenv("TEST_REQUIRED_VAR", "hello")
    # Import after patching so dotenv doesn't interfere
    from ddd_python.ddd_utils.get_variables_from_env import _require

    assert _require("TEST_REQUIRED_VAR") == "hello"


def test_require_raises_on_missing(monkeypatch):
    monkeypatch.delenv("NONEXISTENT_VAR_12345", raising=False)
    from ddd_python.ddd_utils.get_variables_from_env import _require

    with pytest.raises(EnvironmentError, match="NONEXISTENT_VAR_12345"):
        _require("NONEXISTENT_VAR_12345")


def test_require_raises_on_empty(monkeypatch):
    monkeypatch.setenv("EMPTY_VAR_TEST", "")
    from ddd_python.ddd_utils.get_variables_from_env import _require

    with pytest.raises(EnvironmentError, match="EMPTY_VAR_TEST"):
        _require("EMPTY_VAR_TEST")


# ---------------------------------------------------------------------------
# RAW_STORAGE_TARGET validation
# ---------------------------------------------------------------------------


def _reload_env(monkeypatch, **env_overrides):
    """Re-import get_variables_from_env with patched env vars.

    The module executes validation logic at import time, so we must evict it
    from sys.modules and re-import to trigger the guards with different env values.
    """
    for key in (
        "STORAGE_TARGET",
        "RAW_STORAGE_TARGET",
        "SILVER_STORAGE_FORMAT",
        "S3_ENDPOINT",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
        "S3_BUCKET_BRONZE",
        "S3_BUCKET_DUCKLAKE",
        "S3_BUCKET_DELTA",
        "S3_PREFIX_DELTA",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in env_overrides.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    mod_name = "ddd_python.ddd_utils.get_variables_from_env"
    sys.modules.pop(mod_name, None)
    return importlib.import_module(mod_name)


def test_raw_storage_target_invalid_value_raises(monkeypatch):
    with pytest.raises(OSError, match="RAW_STORAGE_TARGET must be 'local' or 's3'"):
        _reload_env(monkeypatch, RAW_STORAGE_TARGET="banana")


def test_raw_storage_target_onelake_value_raises(monkeypatch):
    with pytest.raises(OSError, match="RAW_STORAGE_TARGET must be 'local' or 's3'"):
        _reload_env(monkeypatch, RAW_STORAGE_TARGET="onelake")


def test_raw_storage_target_s3_without_bucket_raises(monkeypatch):
    with pytest.raises(OSError, match="RAW_STORAGE_TARGET=s3 requires"):
        _reload_env(
            monkeypatch,
            RAW_STORAGE_TARGET="s3",
            S3_ENDPOINT="http://minio:9000",
            S3_ACCESS_KEY_ID="key",
            S3_SECRET_ACCESS_KEY="secret",
            # S3_BUCKET_BRONZE intentionally omitted
        )


def test_raw_storage_target_s3_without_endpoint_succeeds(monkeypatch):
    # S3_ENDPOINT is optional — empty means use the provider's default (AWS S3).
    mod = _reload_env(
        monkeypatch,
        RAW_STORAGE_TARGET="s3",
        SILVER_STORAGE_FORMAT="duckdb",
        # S3_ENDPOINT intentionally omitted — valid for real AWS S3
        S3_ACCESS_KEY_ID="key",
        S3_SECRET_ACCESS_KEY="secret",
        S3_BUCKET_BRONZE="ddd-bronze",
    )
    assert mod.S3_ENDPOINT == ""


def test_raw_storage_target_defaults_to_local_unconditionally(monkeypatch):
    monkeypatch.delenv("STORAGE_TARGET", raising=False)
    mod = _reload_env(monkeypatch)
    assert mod.RAW_STORAGE_TARGET == "local"


def test_raw_storage_target_independent_of_storage_target(monkeypatch):
    """RAW_STORAGE_TARGET does not inherit from STORAGE_TARGET."""
    # Even when STORAGE_TARGET=onelake, RAW_STORAGE_TARGET stays local by default.
    mod = _reload_env(monkeypatch, STORAGE_TARGET="onelake")
    assert mod.RAW_STORAGE_TARGET == "local"


def test_raw_storage_target_explicit_s3_not_overridden_by_storage_target(monkeypatch):
    mod = _reload_env(
        monkeypatch,
        STORAGE_TARGET="local",
        RAW_STORAGE_TARGET="s3",
        SILVER_STORAGE_FORMAT="duckdb",
        S3_ENDPOINT="http://minio:9000",
        S3_ACCESS_KEY_ID="k",
        S3_SECRET_ACCESS_KEY="s",
        S3_BUCKET_BRONZE="b",
    )
    assert mod.RAW_STORAGE_TARGET == "s3"


def test_s3_vars_not_set_in_local_mode(monkeypatch):
    # S3_* module attributes are only set when RAW_STORAGE_TARGET=s3.
    # In local mode they are absent — accessing them raises AttributeError.
    mod = _reload_env(monkeypatch)
    with pytest.raises(AttributeError):
        _ = mod.S3_ACCESS_KEY_ID
    with pytest.raises(AttributeError):
        _ = mod.S3_SECRET_ACCESS_KEY
    with pytest.raises(AttributeError):
        _ = mod.S3_BUCKET_BRONZE


def test_s3_vars_have_sensible_defaults_in_s3_mode(monkeypatch):
    # Defaults apply when RAW_STORAGE_TARGET=s3 but optional vars are unset.
    mod = _reload_env(
        monkeypatch,
        RAW_STORAGE_TARGET="s3",
        SILVER_STORAGE_FORMAT="duckdb",
        S3_ACCESS_KEY_ID="k",
        S3_SECRET_ACCESS_KEY="s",
        S3_BUCKET_BRONZE="b",
    )
    assert mod.S3_ENDPOINT == ""
    assert mod.S3_REGION == "us-east-1"
    assert mod.S3_USE_SSL == "false"
    assert mod.S3_URL_STYLE == "path"
    assert mod.S3_PREFIX_BRONZE == ""


def test_raw_storage_target_s3_all_required_vars_present_succeeds(monkeypatch):
    mod = _reload_env(
        monkeypatch,
        RAW_STORAGE_TARGET="s3",
        SILVER_STORAGE_FORMAT="duckdb",  # explicit — avoids ducklake guard in s3+ducklake path
        S3_ENDPOINT="http://minio:9000",
        S3_ACCESS_KEY_ID="mykey",
        S3_SECRET_ACCESS_KEY="mysecret",
        S3_BUCKET_BRONZE="ddd-bronze",
    )
    assert mod.RAW_STORAGE_TARGET == "s3"
    assert mod.S3_BUCKET_BRONZE == "ddd-bronze"


# ---------------------------------------------------------------------------
# DUCKLAKE_DATA_PATH auto-derivation in S3 + ducklake mode
# ---------------------------------------------------------------------------


def _reload_env_full(monkeypatch, **env_overrides):
    """Like _reload_env but clears all S3 + ducklake related vars first."""
    for key in (
        "RAW_STORAGE_TARGET",
        "SILVER_STORAGE_FORMAT",
        "S3_ENDPOINT",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
        "S3_BUCKET_BRONZE",
        "S3_PREFIX_BRONZE",
        "S3_BUCKET_DUCKLAKE",
        "S3_PREFIX_DUCKLAKE",
        "DUCKLAKE_DATA_PATH",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in env_overrides.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    mod_name = "ddd_python.ddd_utils.get_variables_from_env"
    sys.modules.pop(mod_name, None)
    return importlib.import_module(mod_name)


def test_ducklake_data_path_auto_derived_when_s3_and_ducklake(monkeypatch):
    mod = _reload_env_full(
        monkeypatch,
        RAW_STORAGE_TARGET="s3",
        SILVER_STORAGE_FORMAT="ducklake",
        S3_ENDPOINT="http://minio:9000",
        S3_ACCESS_KEY_ID="key",
        S3_SECRET_ACCESS_KEY="secret",
        S3_BUCKET_BRONZE="ddd-bronze",
        S3_BUCKET_DUCKLAKE="ddd-ducklake",
    )
    assert mod.DUCKLAKE_DATA_PATH == "s3://ddd-ducklake/"


def test_ducklake_data_path_auto_derived_with_prefix(monkeypatch):
    mod = _reload_env_full(
        monkeypatch,
        RAW_STORAGE_TARGET="s3",
        SILVER_STORAGE_FORMAT="ducklake",
        S3_ENDPOINT="http://minio:9000",
        S3_ACCESS_KEY_ID="key",
        S3_SECRET_ACCESS_KEY="secret",
        S3_BUCKET_BRONZE="ddd-bronze",
        S3_BUCKET_DUCKLAKE="ddd-ducklake",
        S3_PREFIX_DUCKLAKE="silver/parquet",
    )
    assert mod.DUCKLAKE_DATA_PATH == "s3://ddd-ducklake/silver/parquet/"


def test_ducklake_data_path_not_overridden_in_local_mode(monkeypatch):
    mod = _reload_env_full(
        monkeypatch,
        RAW_STORAGE_TARGET="local",
        SILVER_STORAGE_FORMAT="ducklake",
        DUCKLAKE_DATA_PATH="/data/ducklake",
    )
    assert mod.DUCKLAKE_DATA_PATH == "/data/ducklake"


def test_s3_ducklake_without_bucket_ducklake_raises(monkeypatch):
    with pytest.raises(OSError, match="requires S3_BUCKET_DUCKLAKE"):
        _reload_env_full(
            monkeypatch,
            RAW_STORAGE_TARGET="s3",
            SILVER_STORAGE_FORMAT="ducklake",
            S3_ENDPOINT="http://minio:9000",
            S3_ACCESS_KEY_ID="key",
            S3_SECRET_ACCESS_KEY="secret",
            S3_BUCKET_BRONZE="ddd-bronze",
            # S3_BUCKET_DUCKLAKE intentionally omitted
        )


# ---------------------------------------------------------------------------
# STORAGE_TARGET=s3 validation
# ---------------------------------------------------------------------------


def _reload_env_export_s3(monkeypatch, **env_overrides):
    """Like _reload_env_full but also clears Delta export S3 vars."""
    for key in (
        "STORAGE_TARGET",
        "RAW_STORAGE_TARGET",
        "SILVER_STORAGE_FORMAT",
        "S3_ENDPOINT",
        "S3_ACCESS_KEY_ID",
        "S3_SECRET_ACCESS_KEY",
        "S3_BUCKET_BRONZE",
        "S3_PREFIX_BRONZE",
        "S3_BUCKET_DUCKLAKE",
        "S3_PREFIX_DUCKLAKE",
        "S3_BUCKET_DELTA",
        "S3_PREFIX_DELTA",
        "DUCKLAKE_DATA_PATH",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in env_overrides.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    mod_name = "ddd_python.ddd_utils.get_variables_from_env"
    sys.modules.pop(mod_name, None)
    return importlib.import_module(mod_name)


def test_storage_target_s3_without_bucket_delta_raises(monkeypatch):
    with pytest.raises(OSError, match="STORAGE_TARGET=s3 requires S3_BUCKET_DELTA"):
        _reload_env_export_s3(
            monkeypatch,
            STORAGE_TARGET="s3",
            S3_ACCESS_KEY_ID="key",
            S3_SECRET_ACCESS_KEY="secret",
            # S3_BUCKET_DELTA intentionally omitted
        )


def test_storage_target_s3_without_access_key_raises(monkeypatch):
    with pytest.raises(OSError, match="S3 storage"):
        _reload_env_export_s3(
            monkeypatch,
            STORAGE_TARGET="s3",
            S3_SECRET_ACCESS_KEY="secret",
            S3_BUCKET_DELTA="ddd-delta",
            # S3_ACCESS_KEY_ID intentionally omitted
        )


def test_storage_target_s3_sets_bucket_delta_and_prefix(monkeypatch):
    mod = _reload_env_export_s3(
        monkeypatch,
        STORAGE_TARGET="s3",
        S3_ACCESS_KEY_ID="key",
        S3_SECRET_ACCESS_KEY="secret",
        S3_BUCKET_DELTA="ddd-delta",
        # S3_PREFIX_DELTA intentionally omitted — should default to ""
    )
    assert mod.S3_BUCKET_DELTA == "ddd-delta"
    assert mod.S3_PREFIX_DELTA == ""


def test_storage_target_s3_does_not_set_bucket_bronze(monkeypatch):
    # STORAGE_TARGET=s3 alone must not require or set RAW_STORAGE_TARGET=s3 vars.
    mod = _reload_env_export_s3(
        monkeypatch,
        STORAGE_TARGET="s3",
        S3_ACCESS_KEY_ID="key",
        S3_SECRET_ACCESS_KEY="secret",
        S3_BUCKET_DELTA="ddd-delta",
    )
    with pytest.raises(AttributeError):
        _ = mod.S3_BUCKET_BRONZE


def test_storage_target_s3_sets_shared_credentials(monkeypatch):
    # Shared S3 creds must be available when STORAGE_TARGET=s3.
    mod = _reload_env_export_s3(
        monkeypatch,
        STORAGE_TARGET="s3",
        S3_ACCESS_KEY_ID="mykey",
        S3_SECRET_ACCESS_KEY="mysecret",
        S3_BUCKET_DELTA="ddd-delta",
        S3_REGION="eu-west-1",
    )
    assert mod.S3_ACCESS_KEY_ID == "mykey"
    assert mod.S3_SECRET_ACCESS_KEY == "mysecret"
    assert mod.S3_REGION == "eu-west-1"


def test_storage_target_s3_is_valid_storage_target(monkeypatch):
    # Must not raise on STORAGE_TARGET=s3.
    mod = _reload_env_export_s3(
        monkeypatch,
        STORAGE_TARGET="s3",
        S3_ACCESS_KEY_ID="key",
        S3_SECRET_ACCESS_KEY="secret",
        S3_BUCKET_DELTA="ddd-delta",
    )
    assert mod.STORAGE_TARGET == "s3"
