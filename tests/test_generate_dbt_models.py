"""Integration tests for dbt model generation (generate_dbt_models.py).

Verifies that the generated SQL files contain the correct macro calls,
especially that incremental vs full-extraction macro selection is driven
by configuration_variables — not by a hardcoded list.
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from ddd_python.ddd_dbt.generate_dbt_models import (
    _INCREMENTAL_SILVER_MODELS_DDD,
    _INCREMENTAL_SILVER_MODELS_RFAM,
    generate_dbt_models_bronze,
    generate_dbt_models_silver,
    generate_dbt_models_gold_cv,
)
from ddd_python.ddd_utils import configuration_variables as cv

_PATCH_TARGET = "ddd_python.ddd_dbt.generate_dbt_models.get_variables_from_env"


@pytest.fixture
def dbt_models_tmpdir(tmp_path: Path):
    """Patch DBT_MODELS_DIRECTORY to a temporary directory for each test.

    Yields the ``pathlib.Path`` of the temporary directory so tests can
    construct expected file paths with ``/`` without calling ``os.path.join``.
    """
    with patch(_PATCH_TARGET) as mock_env:
        mock_env.DBT_MODELS_DIRECTORY = str(tmp_path)
        yield tmp_path


# ===========================================================================
# DDD — _INCREMENTAL_SILVER_MODELS derivation
# ===========================================================================


def test_incremental_silver_models_ddd_derived_from_config():
    """The DDD set should match the canonical incremental list, not a hardcoded one."""
    expected = {
        f"silver_ddd_{n.replace('ø', 'oe').replace('æ', 'ae').replace('å', 'aa').lower()}"
        for n in cv.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL
    }
    assert _INCREMENTAL_SILVER_MODELS_DDD == expected


def test_incremental_silver_models_ddd_has_6():
    assert len(_INCREMENTAL_SILVER_MODELS_DDD) == 6


# ===========================================================================
# DDD — Silver model generation — macro selection
# ===========================================================================


def test_silver_ddd_incremental_model_uses_incr_macro(dbt_models_tmpdir: Path):
    """An incremental DDD Silver model should use generate_model_silver_incr_extraction."""
    generate_dbt_models_silver(["silver_ddd_aktoer"])
    sql_path = dbt_models_tmpdir / "silver" / "silver_ddd_aktoer.sql"
    assert sql_path.exists()
    content = sql_path.read_text()
    assert "generate_model_silver_incr_extraction" in content
    assert "generate_model_silver_full_extraction" not in content


def test_silver_ddd_full_extract_model_uses_full_macro(dbt_models_tmpdir: Path):
    """A full-extract DDD Silver model should use generate_model_silver_full_extraction."""
    generate_dbt_models_silver(["silver_ddd_afstemning"])
    sql_path = dbt_models_tmpdir / "silver" / "silver_ddd_afstemning.sql"
    assert sql_path.exists()
    content = sql_path.read_text()
    assert "generate_model_silver_full_extraction" in content
    assert "generate_model_silver_incr_extraction" not in content


def test_silver_ddd_generates_cv_view(dbt_models_tmpdir: Path):
    """Each DDD Silver model should also produce a _cv (current-version) view."""
    generate_dbt_models_silver(["silver_ddd_moede"])
    cv_path = dbt_models_tmpdir / "silver" / "silver_ddd_moede_cv.sql"
    assert cv_path.exists()
    content = cv_path.read_text()
    assert "ROW_NUMBER()" in content
    assert "ref('silver_ddd_moede')" in content


def test_silver_ddd_cv_uses_correct_primary_key(dbt_models_tmpdir: Path):
    """The DDD _cv view should partition by the PK from DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS."""
    generate_dbt_models_silver(
        ["silver_ddd_afstemning"],
        primary_key_map=cv.DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS,
    )
    cv_path = dbt_models_tmpdir / "silver" / "silver_ddd_afstemning_cv.sql"
    assert "src.id" in cv_path.read_text()


def test_silver_ddd_all_models_get_correct_macro(dbt_models_tmpdir: Path):
    """Every DDD Silver model from the config list should get the right macro."""
    generate_dbt_models_silver(cv.DANISH_DEMOCRACY_MODELS_SILVER)
    silver_dir = dbt_models_tmpdir / "silver"
    for model_name in cv.DANISH_DEMOCRACY_MODELS_SILVER:
        sql_path = silver_dir / f"{model_name}.sql"
        assert sql_path.exists(), f"Missing {sql_path}"
        content = sql_path.read_text()
        if model_name in _INCREMENTAL_SILVER_MODELS_DDD:
            assert "generate_model_silver_incr_extraction" in content, (
                f"{model_name} should use incr macro"
            )
        else:
            assert "generate_model_silver_full_extraction" in content, (
                f"{model_name} should use full macro"
            )


# ===========================================================================
# DDD — Bronze model generation
# ===========================================================================


def test_bronze_ddd_generates_model_and_latest(dbt_models_tmpdir: Path):
    """DDD Bronze should generate both a main model and a _latest model."""
    generate_dbt_models_bronze(
        ["bronze_ddd_aktoer"], ["Aktør"], "DDD", "danish_parliament",
    )
    bronze_dir = dbt_models_tmpdir / "bronze"
    assert (bronze_dir / "bronze_ddd_aktoer.sql").exists()
    assert (bronze_dir / "bronze_ddd_aktoer_latest.sql").exists()
    main_content = (bronze_dir / "bronze_ddd_aktoer.sql").read_text()
    assert "generate_model_bronze" in main_content
    assert "'DDD'" in main_content
    latest_content = (bronze_dir / "bronze_ddd_aktoer_latest.sql").read_text()
    assert "generate_model_bronze_latest" in latest_content


# ===========================================================================
# DDD — Gold model generation
# ===========================================================================


def test_gold_generates_cv_and_skips_date(dbt_models_tmpdir: Path):
    """Gold should generate _cv files and skip the 'date' table."""
    generate_dbt_models_gold_cv(["actor", "date", "vote"])
    gold_dir = dbt_models_tmpdir / "gold"
    assert (gold_dir / "actor_cv.sql").exists()
    assert (gold_dir / "vote_cv.sql").exists()
    assert not (gold_dir / "date_cv.sql").exists()
    content = (gold_dir / "actor_cv.sql").read_text()
    assert "EXCLUDE" in content
    assert "ref('actor')" in content


# ===========================================================================
# RFAM — _INCREMENTAL_SILVER_MODELS derivation
# ===========================================================================


def test_incremental_silver_models_rfam_derived_from_config():
    """The RFAM set should match the canonical incremental list, not a hardcoded one."""
    expected = {
        f"silver_rfam_{name}"
        for name in cv.RFAM_TABLE_NAMES_INCREMENTAL
    }
    assert _INCREMENTAL_SILVER_MODELS_RFAM == expected


def test_incremental_silver_models_rfam_has_2():
    assert len(_INCREMENTAL_SILVER_MODELS_RFAM) == 2


# ===========================================================================
# RFAM — Silver model generation — macro selection
# ===========================================================================


def test_silver_rfam_incremental_model_uses_incr_macro(dbt_models_tmpdir: Path):
    """An incremental RFAM Silver model should use generate_model_silver_incr_extraction."""
    generate_dbt_models_silver(
        ["silver_rfam_family"],
        incremental_models=_INCREMENTAL_SILVER_MODELS_RFAM,
        date_column="updated",
        date_column_map=cv.RFAM_TABLE_DATE_COLUMNS,
        data_source_env_var="RFAM_DATA_SOURCE",
        bronze_prefix="silver_rfam_",
        primary_key_map=cv.RFAM_TABLE_PRIMARY_KEYS,
    )
    sql_path = dbt_models_tmpdir / "silver" / "silver_rfam_family.sql"
    assert sql_path.exists()
    content = sql_path.read_text()
    assert "generate_model_silver_incr_extraction" in content
    assert "generate_model_silver_full_extraction" not in content


def test_silver_rfam_full_extract_model_uses_full_macro(dbt_models_tmpdir: Path):
    """A full-extract RFAM Silver model should use generate_model_silver_full_extraction."""
    generate_dbt_models_silver(
        ["silver_rfam_clan"],
        incremental_models=_INCREMENTAL_SILVER_MODELS_RFAM,
        date_column="updated",
        date_column_map=cv.RFAM_TABLE_DATE_COLUMNS,
        data_source_env_var="RFAM_DATA_SOURCE",
        bronze_prefix="silver_rfam_",
        primary_key_map=cv.RFAM_TABLE_PRIMARY_KEYS,
    )
    sql_path = dbt_models_tmpdir / "silver" / "silver_rfam_clan.sql"
    assert sql_path.exists()
    content = sql_path.read_text()
    assert "generate_model_silver_full_extraction" in content
    assert "generate_model_silver_incr_extraction" not in content


def test_silver_rfam_generates_cv_view(dbt_models_tmpdir: Path):
    """Each RFAM Silver model should also produce a _cv (current-version) view."""
    generate_dbt_models_silver(
        ["silver_rfam_family"],
        incremental_models=_INCREMENTAL_SILVER_MODELS_RFAM,
        date_column="updated",
        date_column_map=cv.RFAM_TABLE_DATE_COLUMNS,
        data_source_env_var="RFAM_DATA_SOURCE",
        bronze_prefix="silver_rfam_",
        primary_key_map=cv.RFAM_TABLE_PRIMARY_KEYS,
    )
    cv_path = dbt_models_tmpdir / "silver" / "silver_rfam_family_cv.sql"
    assert cv_path.exists()
    content = cv_path.read_text()
    assert "ROW_NUMBER()" in content
    assert "ref('silver_rfam_family')" in content


def test_silver_rfam_cv_uses_correct_primary_key(dbt_models_tmpdir: Path):
    """The RFAM _cv view should partition by the PK from RFAM_TABLE_PRIMARY_KEYS."""
    generate_dbt_models_silver(
        ["silver_rfam_family"],
        incremental_models=_INCREMENTAL_SILVER_MODELS_RFAM,
        date_column="updated",
        date_column_map=cv.RFAM_TABLE_DATE_COLUMNS,
        data_source_env_var="RFAM_DATA_SOURCE",
        bronze_prefix="silver_rfam_",
        primary_key_map=cv.RFAM_TABLE_PRIMARY_KEYS,
    )
    cv_path = dbt_models_tmpdir / "silver" / "silver_rfam_family_cv.sql"
    # family PK is rfam_acc, not id
    assert "src.rfam_acc" in cv_path.read_text()


def test_silver_rfam_all_models_get_correct_macro(dbt_models_tmpdir: Path):
    """Every RFAM Silver model from the config list should get the right macro."""
    generate_dbt_models_silver(
        cv.RFAM_MODELS_SILVER,
        incremental_models=_INCREMENTAL_SILVER_MODELS_RFAM,
        date_column="updated",
        date_column_map=cv.RFAM_TABLE_DATE_COLUMNS,
        data_source_env_var="RFAM_DATA_SOURCE",
        bronze_prefix="silver_rfam_",
        primary_key_map=cv.RFAM_TABLE_PRIMARY_KEYS,
    )
    silver_dir = dbt_models_tmpdir / "silver"
    for model_name in cv.RFAM_MODELS_SILVER:
        sql_path = silver_dir / f"{model_name}.sql"
        assert sql_path.exists(), f"Missing {sql_path}"
        content = sql_path.read_text()
        if model_name in _INCREMENTAL_SILVER_MODELS_RFAM:
            assert "generate_model_silver_incr_extraction" in content, (
                f"{model_name} should use incr macro"
            )
        else:
            assert "generate_model_silver_full_extraction" in content, (
                f"{model_name} should use full macro"
            )


# ===========================================================================
# RFAM — Bronze model generation
# ===========================================================================


def test_bronze_rfam_generates_model_and_latest(dbt_models_tmpdir: Path):
    """RFAM Bronze should generate both a main model and a _latest model."""
    generate_dbt_models_bronze(
        ["bronze_rfam_family"], ["family"], "RFAM", "rfam",
        data_source_env_var="RFAM_DATA_SOURCE",
    )
    bronze_dir = dbt_models_tmpdir / "bronze"
    assert (bronze_dir / "bronze_rfam_family.sql").exists()
    assert (bronze_dir / "bronze_rfam_family_latest.sql").exists()
    main_content = (bronze_dir / "bronze_rfam_family.sql").read_text()
    assert "generate_model_bronze" in main_content
    assert "'RFAM'" in main_content
    latest_content = (bronze_dir / "bronze_rfam_family_latest.sql").read_text()
    assert "generate_model_bronze_latest" in latest_content
    assert "RFAM_DATA_SOURCE" in latest_content
