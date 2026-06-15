"""Unit tests for ddd_utils.string_utils."""

from datetime import UTC, datetime

import pytest

from ddd_python.ddd_utils.string_utils import normalize_danish_name, resolve_date_to_load_from

# ---------------------------------------------------------------------------
# normalize_danish_name
# ---------------------------------------------------------------------------


class TestNormalizeDanishName:
    def test_replaces_oe_lowercase(self):
        assert normalize_danish_name("aktør") == "aktoer"

    def test_replaces_oe_uppercase(self):
        assert normalize_danish_name("Aktør") == "aktoer"

    def test_replaces_oe_all_caps(self):
        assert normalize_danish_name("AKTØR") == "aktoer"

    def test_replaces_ae_lowercase(self):
        assert normalize_danish_name("sæson") == "saeson"

    def test_replaces_ae_uppercase(self):
        assert normalize_danish_name("Sæson") == "saeson"

    def test_replaces_ae_all_caps(self):
        assert normalize_danish_name("SÆSON") == "saeson"

    def test_replaces_aa_lowercase(self):
        assert normalize_danish_name("årsdag") == "aarsdag"

    def test_replaces_aa_uppercase(self):
        assert normalize_danish_name("Årsdag") == "aarsdag"

    def test_replaces_aa_all_caps(self):
        assert normalize_danish_name("ÅRSDAG") == "aarsdag"

    def test_lowercases(self):
        assert normalize_danish_name("MOEDE") == "moede"

    def test_all_three_chars_lowercase(self):
        assert normalize_danish_name("æøå") == "aeoeaa"

    def test_all_three_chars_uppercase(self):
        assert normalize_danish_name("ÆØÅ") == "aeoeaa"

    def test_no_danish_chars_unchanged_except_case(self):
        assert normalize_danish_name("Afstemning") == "afstemning"

    def test_empty_string(self):
        assert normalize_danish_name("") == ""


# ---------------------------------------------------------------------------
# resolve_date_to_load_from
# ---------------------------------------------------------------------------


class TestResolveDateToLoadFrom:
    _REF = datetime(2025, 3, 15, 12, 0, 0, tzinfo=UTC)

    def test_none_returns_lookback_date(self):
        result = resolve_date_to_load_from(None, 31, self._REF)
        assert result == "2025-02-12"

    def test_none_with_one_day_lookback(self):
        result = resolve_date_to_load_from(None, 1, self._REF)
        assert result == "2025-03-14"

    def test_explicit_date_returned_unchanged(self):
        result = resolve_date_to_load_from("2024-06-01", 31, self._REF)
        assert result == "2024-06-01"

    def test_invalid_date_raises_value_error(self):
        with pytest.raises(ValueError, match="must be in 'YYYY-MM-DD' format"):
            resolve_date_to_load_from("01-06-2024", 31, self._REF)

    def test_invalid_date_message_includes_bad_value(self):
        with pytest.raises(ValueError, match="01-06-2024"):
            resolve_date_to_load_from("01-06-2024", 31, self._REF)

    def test_non_date_string_raises_value_error(self):
        with pytest.raises(ValueError):
            resolve_date_to_load_from("not-a-date", 31, self._REF)
