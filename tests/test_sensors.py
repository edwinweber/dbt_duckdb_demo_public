"""Unit tests for the run-status sensor helpers (ntfy.sh alerts, run summaries)."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests

from ddd_python.ddd_dagster import sensors


@pytest.fixture
def logger():
    return MagicMock()


class TestSendNtfyAlert:
    def test_skips_when_topic_unset(self, monkeypatch, logger):
        monkeypatch.delenv("NTFY_TOPIC", raising=False)
        post = MagicMock()
        monkeypatch.setattr(sensors.requests, "post", post)
        sensors._send_ntfy_alert("job", "abcdef123456", "DEV", "success", logger)
        post.assert_not_called()

    @pytest.mark.parametrize(
        ("status", "word", "priority"),
        [("success", "SUCCEEDED", "default"), ("failure", "FAILED", "high")],
    )
    def test_posts_expected_payload(self, monkeypatch, logger, status, word, priority):
        monkeypatch.setenv("NTFY_TOPIC", "my-topic")
        post = MagicMock()
        monkeypatch.setattr(sensors.requests, "post", post)
        sensors._send_ntfy_alert("my_job", "abcdef123456", "PROD", status, logger)
        args, kwargs = post.call_args
        assert args[0] == "https://ntfy.sh/my-topic"
        assert kwargs["headers"]["Title"] == f"Dagster run {word} - my_job"
        assert kwargs["headers"]["Priority"] == priority
        assert kwargs["data"] == b"Job: my_job\nRun ID: abcdef12\nEnvironment: PROD"
        # HTTP headers must stay ASCII-only
        assert kwargs["headers"]["Title"].isascii()

    def test_network_error_is_logged_not_raised(self, monkeypatch, logger):
        monkeypatch.setenv("NTFY_TOPIC", "t")
        monkeypatch.setattr(
            sensors.requests,
            "post",
            MagicMock(side_effect=requests.exceptions.ConnectionError("down")),
        )
        sensors._send_ntfy_alert("job", "abcdef123456", "DEV", "failure", logger)
        logger.warning.assert_called_once()


def _step(key, status, start, end):
    return SimpleNamespace(
        step_key=key,
        status=SimpleNamespace(value=status) if status else None,
        start_time=start,
        end_time=end,
    )


def _context(steps):
    instance = MagicMock()
    instance.get_run_stats.return_value = SimpleNamespace(start_time=100.0, end_time=130.0)
    instance.get_run_step_stats.return_value = steps
    run = SimpleNamespace(job_name="job", run_id="run-1", tags={"a": "b"})
    return SimpleNamespace(dagster_run=run, instance=instance, log=MagicMock())


class TestBuildAndWriteRunSummary:
    def test_counts_and_sorts_steps(self):
        steps = [
            _step("z_step", "FAILURE", 1.0, 3.5),
            _step("a_step", "SUCCESS", 0.0, 1.0),
            _step("skipped", "SKIPPED", None, None),
        ]
        ctx = _context(steps)
        resource = MagicMock()
        sensors._build_and_write_run_summary(ctx, "failure", resource)

        kwargs = resource.write_job_run_log.call_args.kwargs
        extra = kwargs["extra"]
        assert kwargs["status"] == "failure"
        assert extra["steps_total"] == 3
        assert extra["steps_succeeded"] == 1
        assert extra["steps_failed"] == 1  # SKIPPED does not count as failed
        assert [s["step_key"] for s in extra["steps"]] == ["a_step", "skipped", "z_step"]
        assert extra["steps"][2]["duration_seconds"] == 2.5
        assert extra["steps"][1]["duration_seconds"] is None
        assert extra["tags"] == {"a": "b"}

    def test_write_failure_is_logged_not_raised(self):
        ctx = _context([])
        resource = MagicMock()
        resource.write_job_run_log.side_effect = OSError("disk full")
        sensors._build_and_write_run_summary(ctx, "success", resource)
        ctx.log.warning.assert_called_once()
