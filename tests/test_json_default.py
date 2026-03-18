"""Tests for the _json_default fallback serializer."""

from datetime import datetime, date, time

from ddd_python.ddd_dlt.dlt_pipeline_execution_functions import _json_default


def test_datetime_returns_isoformat():
    dt = datetime(2024, 3, 15, 10, 30, 0)
    assert _json_default(dt) == "2024-03-15T10:30:00"


def test_date_returns_isoformat():
    d = date(2024, 3, 15)
    assert _json_default(d) == "2024-03-15"


def test_time_returns_isoformat():
    t = time(10, 30, 0)
    assert _json_default(t) == "10:30:00"


def test_non_isoformat_object_returns_str():
    assert _json_default(42) == "42"
    assert _json_default(None) == "None"
    assert _json_default(["a", "b"]) == "['a', 'b']"


class FakeDateTime:
    """Mimics pendulum.DateTime or similar objects with isoformat()."""
    def isoformat(self):
        return "2024-01-01T00:00:00+00:00"


def test_custom_isoformat_object():
    assert _json_default(FakeDateTime()) == "2024-01-01T00:00:00+00:00"
