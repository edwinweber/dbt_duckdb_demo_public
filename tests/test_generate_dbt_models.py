"""Integration tests for dbt model generation (generate_dbt_models.py).

Verifies that the generated SQL files contain the correct macro calls,
especially that incremental vs full-extraction macro selection is driven
by configuration_variables — not by a hardcoded list.
"""

import os
import tempfile
from unittest.mock import patch

from ddd_python.ddd_dbt.generate_dbt_models import (
    _INCREMENTAL_SILVER_MODELS,
    generate_dbt_models_bronze,
    generate_dbt_models_silver,
    generate_dbt_models_gold_cv,
)
from ddd_python.ddd_utils import configuration_variables as cv


# ---------------------------------------------------------------------------
# _INCREMENTAL_SILVER_MODELS derivation
# ---------------------------------------------------------------------------


def test_incremental_silver_models_derived_from_config():
    """The set should match the canonical incremental list, not a hardcoded one."""
    expected = {
        f"silver_{n.replace('ø', 'oe').replace('æ', 'ae').replace('å', 'aa').lower()}"
        for n in cv.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL
    }
    assert _INCREMENTAL_SILVER_MODELS == expected


def test_incremental_silver_models_has_6():
    assert len(_INCREMENTAL_SILVER_MODELS) == 6


# ---------------------------------------------------------------------------
# Silver model generation — macro selection
# ---------------------------------------------------------------------------


def test_silver_incremental_model_uses_incr_macro():
    """An incremental Silver model should use generate_model_silver_incr_extraction."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("ddd_python.ddd_dbt.generate_dbt_models.get_variables_from_env") as mock_env:
            mock_env.DBT_MODELS_DIRECTORY = tmpdir
            generate_dbt_models_silver(["silver_aktoer"])

        sql_path = os.path.join(tmpdir, "silver", "silver_aktoer.sql")
        assert os.path.exists(sql_path)
        content = open(sql_path).read()
        assert "generate_model_silver_incr_extraction" in content
        assert "generate_model_silver_full_extraction" not in content


def test_silver_full_extract_model_uses_full_macro():
    """A full-extract Silver model should use generate_model_silver_full_extraction."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("ddd_python.ddd_dbt.generate_dbt_models.get_variables_from_env") as mock_env:
            mock_env.DBT_MODELS_DIRECTORY = tmpdir
            generate_dbt_models_silver(["silver_afstemning"])

        sql_path = os.path.join(tmpdir, "silver", "silver_afstemning.sql")
        assert os.path.exists(sql_path)
        content = open(sql_path).read()
        assert "generate_model_silver_full_extraction" in content
        assert "generate_model_silver_incr_extraction" not in content


def test_silver_generates_cv_view():
    """Each Silver model should also produce a _cv (current-version) view."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("ddd_python.ddd_dbt.generate_dbt_models.get_variables_from_env") as mock_env:
            mock_env.DBT_MODELS_DIRECTORY = tmpdir
            generate_dbt_models_silver(["silver_moede"])

        cv_path = os.path.join(tmpdir, "silver", "silver_moede_cv.sql")
        assert os.path.exists(cv_path)
        content = open(cv_path).read()
        assert "ROW_NUMBER()" in content
        assert "ref('silver_moede')" in content


def test_silver_all_models_get_correct_macro():
    """Every Silver model from the config list should get the right macro."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("ddd_python.ddd_dbt.generate_dbt_models.get_variables_from_env") as mock_env:
            mock_env.DBT_MODELS_DIRECTORY = tmpdir
            generate_dbt_models_silver(cv.DANISH_DEMOCRACY_MODELS_SILVER)

        silver_dir = os.path.join(tmpdir, "silver")
        for model_name in cv.DANISH_DEMOCRACY_MODELS_SILVER:
            sql_path = os.path.join(silver_dir, f"{model_name}.sql")
            assert os.path.exists(sql_path), f"Missing {sql_path}"
            content = open(sql_path).read()
            if model_name in _INCREMENTAL_SILVER_MODELS:
                assert "generate_model_silver_incr_extraction" in content, (
                    f"{model_name} should use incr macro"
                )
            else:
                assert "generate_model_silver_full_extraction" in content, (
                    f"{model_name} should use full macro"
                )


# ---------------------------------------------------------------------------
# Bronze model generation
# ---------------------------------------------------------------------------


def test_bronze_generates_model_and_latest():
    """Bronze should generate both a main model and a _latest model."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("ddd_python.ddd_dbt.generate_dbt_models.get_variables_from_env") as mock_env:
            mock_env.DBT_MODELS_DIRECTORY = tmpdir
            generate_dbt_models_bronze(
                ["bronze_aktoer"], ["Aktør"], "DDD", "danish_parliament",
            )

        bronze_dir = os.path.join(tmpdir, "bronze")
        assert os.path.exists(os.path.join(bronze_dir, "bronze_aktoer.sql"))
        assert os.path.exists(os.path.join(bronze_dir, "bronze_aktoer_latest.sql"))

        main_content = open(os.path.join(bronze_dir, "bronze_aktoer.sql")).read()
        assert "generate_model_bronze" in main_content
        assert "'DDD'" in main_content

        latest_content = open(os.path.join(bronze_dir, "bronze_aktoer_latest.sql")).read()
        assert "generate_model_bronze_latest" in latest_content


# ---------------------------------------------------------------------------
# Gold model generation
# ---------------------------------------------------------------------------


def test_gold_generates_cv_and_skips_date():
    """Gold should generate _cv files and skip the 'date' table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("ddd_python.ddd_dbt.generate_dbt_models.get_variables_from_env") as mock_env:
            mock_env.DBT_MODELS_DIRECTORY = tmpdir
            generate_dbt_models_gold_cv(["actor", "date", "vote"])

        gold_dir = os.path.join(tmpdir, "gold")
        assert os.path.exists(os.path.join(gold_dir, "actor_cv.sql"))
        assert os.path.exists(os.path.join(gold_dir, "vote_cv.sql"))
        assert not os.path.exists(os.path.join(gold_dir, "date_cv.sql"))

        content = open(os.path.join(gold_dir, "actor_cv.sql")).read()
        assert "EXCLUDE" in content
        assert "ref('actor')" in content
