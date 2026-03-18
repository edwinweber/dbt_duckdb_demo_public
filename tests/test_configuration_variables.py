"""Tests for configuration_variables — entity list consistency."""

from ddd_python.ddd_utils import configuration_variables as cv


def test_file_names_has_18_entities():
    assert len(cv.DANISH_DEMOCRACY_FILE_NAMES) == 18


def test_incremental_is_subset_of_all():
    all_lower = {n.lower() for n in cv.DANISH_DEMOCRACY_FILE_NAMES}
    incr_lower = {n.lower() for n in cv.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL}
    assert incr_lower.issubset(all_lower), f"Not in all: {incr_lower - all_lower}"


def test_incremental_has_6_entities():
    assert len(cv.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL) == 6


def test_bronze_models_match_file_names():
    """Each file name should have a corresponding bronze model."""
    expected = {f"bronze_{n.replace('ø','oe').replace('æ','ae').replace('å','aa').lower()}"
                for n in cv.DANISH_DEMOCRACY_FILE_NAMES}
    actual = set(cv.DANISH_DEMOCRACY_MODELS_BRONZE)
    assert actual == expected, f"Mismatch: {actual.symmetric_difference(expected)}"


def test_silver_models_match_bronze():
    """Each bronze model should have a corresponding silver model."""
    expected = {m.replace("bronze_", "silver_", 1) for m in cv.DANISH_DEMOCRACY_MODELS_BRONZE}
    actual = set(cv.DANISH_DEMOCRACY_MODELS_SILVER)
    assert actual == expected, f"Mismatch: {actual.symmetric_difference(expected)}"


def test_no_duplicates_in_lists():
    for name, lst in [
        ("FILE_NAMES", cv.DANISH_DEMOCRACY_FILE_NAMES),
        ("MODELS_BRONZE", cv.DANISH_DEMOCRACY_MODELS_BRONZE),
        ("MODELS_SILVER", cv.DANISH_DEMOCRACY_MODELS_SILVER),
        ("MODELS_GOLD", cv.DANISH_DEMOCRACY_MODELS_GOLD),
    ]:
        assert len(lst) == len(set(lst)), f"Duplicates in {name}"
