"""Tests for execute_pipeline and run_extraction_pool.

All tests use real temp directories (``tmp_path``) and real file I/O.
No live network, no dlt state directory, no real OneLake or S3.

The log-write path is exercised for real — patching it would defeat the purpose
of testing the finally-block guarantee.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from ddd_python.ddd_dlt.dlt_pipeline_execution_functions import (
    PipelineTask,
    execute_pipeline,
    run_extraction_pool,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _env_patch(tmp_path: Path):
    """Patch get_variables_from_env attributes used by file_to_file pipelines."""
    return patch.dict(
        "ddd_python.ddd_dlt.dlt_pipeline_execution_functions.get_variables_from_env.__dict__",
        {
            "RAW_STORAGE_TARGET": "local",
            "LOCAL_STORAGE_PATH": str(tmp_path / "storage"),
            "DLT_PIPELINE_RUN_LOG_DIR": str(tmp_path / "logs"),
            "DLT_PIPELINES_DIR": str(tmp_path / "pipelines"),
        },
        clear=False,
    )


def _make_source_file(tmp_path: Path, name: str = "source.txt", content: bytes = b"hello") -> Path:
    src = tmp_path / name
    src.write_bytes(content)
    return src


def _read_log_entries(log_dir: Path, source_system_code: str, pipeline_name: str) -> list[dict]:
    """Read all NDJSON log entries written for a pipeline."""
    log_file = log_dir / source_system_code / pipeline_name / f"{pipeline_name}_log.ndjson"
    if not log_file.exists():
        return []
    return [json.loads(line) for line in log_file.read_text().splitlines() if line.strip()]


def _read_pool_log_entries(log_dir: Path, source_system_code: str, script_name: str) -> list[dict]:
    """Read the pool-level summary NDJSON log."""
    log_file = log_dir / source_system_code / f"{script_name}_log.ndjson"
    if not log_file.exists():
        return []
    return [json.loads(line) for line in log_file.read_text().splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# execute_pipeline — happy path
# ---------------------------------------------------------------------------


def test_execute_pipeline_file_to_file_local(tmp_path):
    src = _make_source_file(tmp_path, "data.txt", b"test content")
    dest_dir = str(tmp_path / "storage" / "bronze" / "test")
    log_dir = tmp_path / "logs"

    with _env_patch(tmp_path):
        result = execute_pipeline(
            "file_to_file",
            source_system_code="TEST",
            pipeline_name="test_pipeline",
            source_file_path=str(src),
            destination_directory_path=dest_dir,
            destination_file_name="data.txt",
        )

    # (a) file was copied to the destination
    assert (Path(dest_dir) / "data.txt").read_bytes() == b"test content"

    # (b) log entry was written under DLT_PIPELINE_RUN_LOG_DIR
    entries = _read_log_entries(log_dir, "TEST", "test_pipeline")
    assert len(entries) == 1

    # (c) result dict has status == "success"
    assert result["status"] == "success"


def test_execute_pipeline_reports_bytes_written(tmp_path):
    content = b"abc" * 100
    src = _make_source_file(tmp_path, "payload.bin", content)
    dest_dir = str(tmp_path / "dest")

    with _env_patch(tmp_path):
        result = execute_pipeline(
            "file_to_file",
            source_system_code="TEST",
            pipeline_name="bytes_test",
            source_file_path=str(src),
            destination_directory_path=dest_dir,
            destination_file_name="payload.bin",
        )

    assert result["bytes_written"] == len(content)


# ---------------------------------------------------------------------------
# execute_pipeline — failure path
# ---------------------------------------------------------------------------


def test_execute_pipeline_writes_log_on_failure(tmp_path):
    log_dir = tmp_path / "logs"

    with _env_patch(tmp_path), pytest.raises(FileNotFoundError):
        execute_pipeline(
            "file_to_file",
            source_system_code="TEST",
            pipeline_name="fail_pipeline",
            source_file_path=str(tmp_path / "nonexistent_file.txt"),
            destination_directory_path=str(tmp_path / "dest"),
            destination_file_name="out.txt",
        )

    # (b) a log entry was still written (finally block guarantee)
    entries = _read_log_entries(log_dir, "TEST", "fail_pipeline")
    assert len(entries) == 1
    assert entries[0]["level"] == "ERROR"
    assert entries[0]["result"]["status"] == "failure"
    assert entries[0]["error"] is not None


def test_execute_pipeline_invalid_type_raises(tmp_path):
    with _env_patch(tmp_path), pytest.raises(ValueError, match="Unsupported pipeline type"):
        execute_pipeline(
            "bad_type",  # type: ignore[arg-type]
            source_system_code="TEST",
            pipeline_name="bad",
        )


# ---------------------------------------------------------------------------
# execute_pipeline — secret scrubbing
# ---------------------------------------------------------------------------


def test_execute_pipeline_secrets_scrubbed_in_log(tmp_path):
    """connection_string value must not appear in the log entry, even on failure.

    Passing an unexpected kwarg to file_to_file causes a TypeError (the handler
    rejects it), but the finally block still runs and the log must be written
    with the secret already replaced by '***'.
    """
    log_dir = tmp_path / "logs"
    secret_value = "mysql+pymysql://user:SUPER_SECRET_PWD@host/db"

    with _env_patch(tmp_path), pytest.raises((TypeError, FileNotFoundError)):
        execute_pipeline(
            "file_to_file",
            source_system_code="TEST",
            pipeline_name="secret_test",
            source_file_path=str(tmp_path / "nonexistent.txt"),
            destination_directory_path=str(tmp_path / "dest"),
            destination_file_name="src.txt",
            connection_string=secret_value,
        )

    # Log must have been written despite the exception
    entries = _read_log_entries(log_dir, "TEST", "secret_test")
    assert len(entries) == 1
    # The raw log line must not contain the actual secret value
    raw_log = (log_dir / "TEST" / "secret_test" / "secret_test_log.ndjson").read_text()
    assert secret_value not in raw_log
    # The scrubbed placeholder must be present
    assert "***" in raw_log


# ---------------------------------------------------------------------------
# run_extraction_pool — all succeed
# ---------------------------------------------------------------------------


def test_run_extraction_pool_all_succeed(tmp_path):
    src1 = _make_source_file(tmp_path, "a.txt", b"AAA")
    src2 = _make_source_file(tmp_path, "b.txt", b"BBB")
    dest1 = str(tmp_path / "storage" / "dest_a")
    dest2 = str(tmp_path / "storage" / "dest_b")
    log_dir = tmp_path / "logs"

    tasks: list[PipelineTask] = [
        {
            "name": "task_a",
            "source_system_code": "TEST",
            "pipeline_type": "file_to_file",
            "kwargs": {
                "pipeline_name": "task_a",
                "source_file_path": str(src1),
                "destination_directory_path": dest1,
                "destination_file_name": "a.txt",
            },
        },
        {
            "name": "task_b",
            "source_system_code": "TEST",
            "pipeline_type": "file_to_file",
            "kwargs": {
                "pipeline_name": "task_b",
                "source_file_path": str(src2),
                "destination_directory_path": dest2,
                "destination_file_name": "b.txt",
            },
        },
    ]

    with _env_patch(tmp_path):
        # (c) no exception raised
        run_extraction_pool(
            tasks=tasks,
            script_name="test_script",
            source_system_code="TEST",
            date_to_load_from="2024-01-01",
            start_time=datetime.now(UTC),
        )

    # (a) both files appear at destination
    assert (Path(dest1) / "a.txt").read_bytes() == b"AAA"
    assert (Path(dest2) / "b.txt").read_bytes() == b"BBB"

    # (b) pool-level summary log was written
    pool_entries = _read_pool_log_entries(log_dir, "TEST", "test_script")
    assert len(pool_entries) == 1
    assert pool_entries[0]["status"] == "success"
    assert pool_entries[0]["pipelines_succeeded"] == 2
    assert pool_entries[0]["pipelines_failed"] == 0


# ---------------------------------------------------------------------------
# run_extraction_pool — partial failure
# ---------------------------------------------------------------------------


def test_run_extraction_pool_partial_failure_raises(tmp_path):
    src_good = _make_source_file(tmp_path, "good.txt", b"OK")
    dest_good = str(tmp_path / "storage" / "dest_good")
    log_dir = tmp_path / "logs"

    tasks: list[PipelineTask] = [
        {
            "name": "good_task",
            "source_system_code": "TEST",
            "pipeline_type": "file_to_file",
            "kwargs": {
                "pipeline_name": "good_task",
                "source_file_path": str(src_good),
                "destination_directory_path": dest_good,
                "destination_file_name": "good.txt",
            },
        },
        {
            "name": "bad_task",
            "source_system_code": "TEST",
            "pipeline_type": "file_to_file",
            "kwargs": {
                "pipeline_name": "bad_task",
                "source_file_path": str(tmp_path / "missing.txt"),  # does not exist
                "destination_directory_path": str(tmp_path / "dest_bad"),
                "destination_file_name": "missing.txt",
            },
        },
    ]

    with _env_patch(tmp_path), pytest.raises(RuntimeError, match="bad_task"):
        # (a) RuntimeError is raised after all tasks are attempted
        run_extraction_pool(
            tasks=tasks,
            script_name="partial_script",
            source_system_code="TEST",
            date_to_load_from="2024-01-01",
            start_time=datetime.now(UTC),
        )

    # (b) both individual log entries exist (success and failure)
    good_entries = _read_log_entries(log_dir, "TEST", "good_task")
    bad_entries = _read_log_entries(log_dir, "TEST", "bad_task")
    assert len(good_entries) == 1
    assert good_entries[0]["level"] == "INFO"
    assert len(bad_entries) == 1
    assert bad_entries[0]["level"] == "ERROR"

    # (c) pool-level summary log was written
    pool_entries = _read_pool_log_entries(log_dir, "TEST", "partial_script")
    assert len(pool_entries) == 1
    assert pool_entries[0]["status"] == "failure"
    assert pool_entries[0]["pipelines_failed"] == 1
    assert pool_entries[0]["pipelines_succeeded"] == 1
