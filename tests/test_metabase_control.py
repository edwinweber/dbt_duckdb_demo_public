"""Unit tests for the Metabase stop/start control helpers."""

import logging
import subprocess
from unittest.mock import MagicMock

import pytest

from ddd_python.ddd_dagster import metabase_control_assets as mca


def test_missing_script_is_skipped(tmp_path, monkeypatch):
    monkeypatch.setattr(mca, "_SCRIPTS_DIR", tmp_path)
    run = MagicMock()
    monkeypatch.setattr(mca.subprocess, "run", run)
    assert mca._run_metabase_script("nope.sh") is False
    run.assert_not_called()


def test_existing_script_is_run_with_args(tmp_path, monkeypatch):
    (tmp_path / "go.sh").write_text("#!/bin/sh\n")
    monkeypatch.setattr(mca, "_SCRIPTS_DIR", tmp_path)
    run = MagicMock()
    monkeypatch.setattr(mca.subprocess, "run", run)
    assert mca._run_metabase_script("go.sh", "0") is True
    run.assert_called_once_with([str(tmp_path / "go.sh"), "0"], check=True)


def test_script_failure_propagates(tmp_path, monkeypatch):
    (tmp_path / "go.sh").write_text("#!/bin/sh\n")
    monkeypatch.setattr(mca, "_SCRIPTS_DIR", tmp_path)
    monkeypatch.setattr(
        mca.subprocess, "run", MagicMock(side_effect=subprocess.CalledProcessError(1, "go.sh"))
    )
    with pytest.raises(subprocess.CalledProcessError):
        mca._run_metabase_script("go.sh")


def test_restart_after_failure_swallows_errors(monkeypatch, caplog):
    monkeypatch.setattr(
        mca,
        "_run_metabase_script",
        MagicMock(side_effect=subprocess.CalledProcessError(1, "start")),
    )
    with caplog.at_level(logging.WARNING):
        mca.restart_metabase_after_failure()  # must not raise
    assert "Could not restart Metabase" in caplog.text


def test_restart_after_failure_uses_no_wait_arg(monkeypatch):
    script = MagicMock(return_value=True)
    monkeypatch.setattr(mca, "_run_metabase_script", script)
    mca.restart_metabase_after_failure()
    assert script.call_args.args == ("start_metabase_and_wait.sh", "0")
