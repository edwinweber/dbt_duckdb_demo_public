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
    expected = {f"bronze_ddd_{n.replace('ø','oe').replace('æ','ae').replace('å','aa').lower()}"
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
        ("RFAM_TABLE_NAMES", cv.RFAM_TABLE_NAMES),
        ("RFAM_MODELS_BRONZE", cv.RFAM_MODELS_BRONZE),
        ("RFAM_MODELS_SILVER", cv.RFAM_MODELS_SILVER),
    ]:
        assert len(lst) == len(set(lst)), f"Duplicates in {name}"


# ── Rfam configuration consistency ───────────────────────────────────────────

def test_rfam_table_names_has_7_tables():
    assert len(cv.RFAM_TABLE_NAMES) == 7


def test_rfam_incremental_is_subset_of_all():
    assert set(cv.RFAM_TABLE_NAMES_INCREMENTAL).issubset(set(cv.RFAM_TABLE_NAMES)), \
        f"Not in all: {set(cv.RFAM_TABLE_NAMES_INCREMENTAL) - set(cv.RFAM_TABLE_NAMES)}"


def test_rfam_incremental_has_2_tables():
    assert len(cv.RFAM_TABLE_NAMES_INCREMENTAL) == 2


def test_rfam_bronze_models_match_table_names():
    """Each Rfam table name should have a corresponding bronze model."""
    expected = {f"bronze_rfam_{t}" for t in cv.RFAM_TABLE_NAMES}
    actual = set(cv.RFAM_MODELS_BRONZE)
    assert actual == expected, f"Mismatch: {actual.symmetric_difference(expected)}"


def test_rfam_silver_models_match_bronze():
    """Each bronze Rfam model should have a corresponding silver model."""
    expected = {m.replace("bronze_", "silver_", 1) for m in cv.RFAM_MODELS_BRONZE}
    actual = set(cv.RFAM_MODELS_SILVER)
    assert actual == expected, f"Mismatch: {actual.symmetric_difference(expected)}"


def test_rfam_primary_keys_cover_all_tables():
    """Every Rfam table must have a primary key defined."""
    assert set(cv.RFAM_TABLE_PRIMARY_KEYS.keys()) == set(cv.RFAM_TABLE_NAMES), \
        f"Missing PK: {set(cv.RFAM_TABLE_NAMES) - set(cv.RFAM_TABLE_PRIMARY_KEYS.keys())}"


def test_rfam_date_columns_cover_all_tables():
    """Every Rfam table must have a date column entry (may be empty string)."""
    assert set(cv.RFAM_TABLE_DATE_COLUMNS.keys()) == set(cv.RFAM_TABLE_NAMES), \
        f"Missing date col: {set(cv.RFAM_TABLE_NAMES) - set(cv.RFAM_TABLE_DATE_COLUMNS.keys())}"


def test_rfam_queries_cover_all_tables():
    """Every Rfam table must have a SQL query defined."""
    assert set(cv.RFAM_TABLE_QUERIES.keys()) == set(cv.RFAM_TABLE_NAMES), \
        f"Missing query: {set(cv.RFAM_TABLE_NAMES) - set(cv.RFAM_TABLE_QUERIES.keys())}"


# ── Cross-source consistency ─────────────────────────────────────────────────

def test_silver_table_primary_keys_covers_all_silver_models():
    """SILVER_TABLE_PRIMARY_KEYS must have an entry for every DDD + Rfam Silver model."""
    expected = set(cv.DANISH_DEMOCRACY_MODELS_SILVER) | set(cv.RFAM_MODELS_SILVER)
    actual = set(cv.SILVER_TABLE_PRIMARY_KEYS.keys())
    assert actual == expected, f"Mismatch: {actual.symmetric_difference(expected)}"
