"""Tests for dbt_build_with_unique_logfile._check_for_test_failures.

Verifies that test failures embedded in a dbt NDJSON log are detected and
re-raised as RuntimeError so that nominally-successful dbt runs (exit code 0)
still surface as Dagster run failures when data quality tests fail.
"""

import json
from pathlib import Path

import pytest

from ddd_python.ddd_dbt.dbt_build_with_unique_logfile import _check_for_test_failures


def _write_log(path: Path, events: list[dict]) -> Path:
    """Write a synthetic dbt NDJSON log to *path*."""
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    return path


def _node_finished_event(unique_id: str, node_status: str) -> dict:
    """Return a minimal dbt NodeFinished JSON log event."""
    return {
        "info": {"name": "NodeFinished", "level": "info"},
        "data": {
            "node_info": {
                "unique_id": unique_id,
                "node_status": node_status,
            }
        },
    }


def _other_event(name: str = "LogStartLine") -> dict:
    return {"info": {"name": name, "level": "info"}, "data": {}}


# ---------------------------------------------------------------------------
# Happy path — no failures
# ---------------------------------------------------------------------------


def test_check_passes_when_no_node_finished_events(tmp_path):
    log = _write_log(tmp_path / "dbt.json", [_other_event(), _other_event("LogCmdMsg")])
    _check_for_test_failures(str(log))  # must not raise


def test_check_passes_when_all_nodes_pass(tmp_path):
    log = _write_log(
        tmp_path / "dbt.json",
        [
            _node_finished_event("test.my_project.not_null_id", "pass"),
            _node_finished_event("model.my_project.silver_ddd_aktoer", "success"),
        ],
    )
    _check_for_test_failures(str(log))  # must not raise


# ---------------------------------------------------------------------------
# Failure cases — test nodes with "fail" or "error" status
# ---------------------------------------------------------------------------


def test_check_raises_on_failed_test_node(tmp_path):
    log = _write_log(
        tmp_path / "dbt.json",
        [
            _node_finished_event("test.my_project.not_null_aktoer_id", "fail"),
        ],
    )
    with pytest.raises(RuntimeError, match="not_null_aktoer_id"):
        _check_for_test_failures(str(log))


def test_check_raises_on_error_node(tmp_path):
    log = _write_log(
        tmp_path / "dbt.json",
        [
            _node_finished_event("model.my_project.silver_broken", "error"),
        ],
    )
    with pytest.raises(RuntimeError, match="silver_broken"):
        _check_for_test_failures(str(log))


def test_check_raises_listing_all_failed_nodes(tmp_path):
    log = _write_log(
        tmp_path / "dbt.json",
        [
            _node_finished_event("test.my_project.unique_id", "fail"),
            _node_finished_event("test.my_project.not_null_name", "fail"),
            _node_finished_event("model.my_project.silver_ok", "success"),
        ],
    )
    with pytest.raises(RuntimeError) as exc_info:
        _check_for_test_failures(str(log))
    message = str(exc_info.value)
    assert "unique_id" in message
    assert "not_null_name" in message
    assert "2 node(s)" in message


def test_check_ignores_passing_nodes_among_failures(tmp_path):
    """Only failing nodes are listed; passing nodes must not appear in the error."""
    log = _write_log(
        tmp_path / "dbt.json",
        [
            _node_finished_event("test.my_project.unique_id", "pass"),
            _node_finished_event("test.my_project.not_null_id", "fail"),
        ],
    )
    with pytest.raises(RuntimeError) as exc_info:
        _check_for_test_failures(str(log))
    message = str(exc_info.value)
    assert "not_null_id" in message
    assert "unique_id" not in message


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_check_silently_ignores_missing_log_file(tmp_path):
    """If the log file doesn't exist (dbt never started), no error is raised."""
    _check_for_test_failures(str(tmp_path / "nonexistent.json"))  # must not raise


def test_check_silently_ignores_malformed_json_lines(tmp_path):
    """Malformed lines are skipped; only valid NodeFinished events are checked."""
    log_path = tmp_path / "dbt.json"
    log_path.write_text(
        "not json at all\n"
        + json.dumps(_node_finished_event("test.my_project.ok_test", "pass"))
        + "\n",
        encoding="utf-8",
    )
    _check_for_test_failures(str(log_path))  # must not raise


def test_check_handles_empty_log_file(tmp_path):
    log = tmp_path / "dbt.json"
    log.write_text("", encoding="utf-8")
    _check_for_test_failures(str(log))  # must not raise
