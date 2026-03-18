"""Tests for the _serialize_trace helper."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

from ddd_python.ddd_dlt.dlt_pipeline_execution_functions import _serialize_trace


def test_none_trace_returns_empty_dict():
    assert _serialize_trace(None) == {}


def _make_trace(has_load_info=True, has_failed_jobs=False):
    """Build a minimal mock that mimics dlt's trace object."""
    trace = MagicMock()
    trace.transaction_id = "tx-123"
    trace.pipeline_name = "test_pipeline"
    trace.started_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    trace.finished_at = datetime(2024, 1, 1, 0, 5, 0, tzinfo=timezone.utc)
    trace.engine_version = 1
    trace.execution_context = {"library": {"version": "1.17.1"}}

    step = MagicMock()
    step.step = "load"
    step.started_at = datetime(2024, 1, 1, 0, 1, 0, tzinfo=timezone.utc)
    step.finished_at = datetime(2024, 1, 1, 0, 4, 0, tzinfo=timezone.utc)
    step.step_exception = None
    trace.steps = [step]

    if has_load_info:
        load_info = MagicMock()
        load_info.destination_name = "filesystem"
        load_info.loads_ids = ["load-1"]
        load_info.has_failed_jobs = has_failed_jobs
        load_info.is_empty = False
        trace.last_load_info = load_info
    else:
        trace.last_load_info = None

    return trace


def test_full_trace_serialization():
    trace = _make_trace()
    result = _serialize_trace(trace)

    assert result["transaction_id"] == "tx-123"
    assert result["pipeline_name"] == "test_pipeline"
    assert result["dlt_version"] == "1.17.1"
    assert result["destination_name"] == "filesystem"
    assert result["has_failed_jobs"] is False
    assert len(result["steps"]) == 1
    assert result["steps"][0]["step"] == "load"
    assert result["steps"][0]["step_exception"] is None


def test_trace_without_load_info():
    trace = _make_trace(has_load_info=False)
    result = _serialize_trace(trace)

    assert result["destination_name"] is None
    assert result["loads_ids"] is None
    assert result["has_failed_jobs"] is None


def test_trace_with_failed_jobs():
    trace = _make_trace(has_failed_jobs=True)
    result = _serialize_trace(trace)
    assert result["has_failed_jobs"] is True
