"""Dagster run-status sensors that persist job-run summaries and send ntfy.sh alerts.

Two ``@run_status_sensor`` definitions cover all Dagster jobs:

``danish_parliament_run_success_sensor``
    Fires when any job transitions to **SUCCESS**. Writes an NDJSON record to
    the configured log destination and sends a push notification via ntfy.sh
    (``NTFY_TOPIC``).

``danish_parliament_run_failure_sensor``
    Fires when any job transitions to **FAILURE**. Writes an NDJSON record to
    the configured log destination and sends a high-priority push notification
    via ntfy.sh (``NTFY_TOPIC``).

Run summary records are written to::

    <DLT_PIPELINE_RUN_LOG_DIR>/DDD/<job_name>_run_log.ndjson   (STORAGE_TARGET=onelake)
    <DLT_PIPELINES_LOG_DIR>/DDD/<job_name>_run_log.ndjson      (STORAGE_TARGET=local)

The record contains job name, Dagster run ID, UTC start/end times, duration,
overall status, and a per-step summary (step key, status, duration) ranked by
step key.

Write failures
--------------
If the log write or the ntfy.sh POST fails (e.g. transient connectivity), the
exception is caught, logged as a warning, and the sensor tick is marked
successful. A notification failure must never block or delay the next Dagster run.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import requests
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunStatusSensorContext,
    run_status_sensor,
)

from ddd_python.ddd_dagster.resources import DltOneLakeResource

_NTFY_BASE_URL = "https://ntfy.sh"


_NTFY_STATUS_CONFIG = {
    "success": {"title_word": "SUCCEEDED", "priority": "default", "tags": "white_check_mark"},
    "failure": {"title_word": "FAILED", "priority": "high", "tags": "rotating_light"},
}


def _send_ntfy_alert(job_name: str, run_id: str, environment: str, status: str, logger) -> None:
    """POST a run-status alert to ntfy.sh.

    Reads NTFY_TOPIC from the environment at call time. Logs a warning and
    returns early when the variable is not set. Network errors are caught and
    logged as warnings so they never block a sensor tick.
    """
    ntfy_topic = os.getenv("NTFY_TOPIC")
    if not ntfy_topic:
        logger.warning("NTFY_TOPIC is not set — skipping ntfy.sh alert")
        return

    cfg = _NTFY_STATUS_CONFIG[status]
    short_run_id = run_id[:8]
    message = f"Job: {job_name}\nRun ID: {short_run_id}\nEnvironment: {environment}"
    try:
        response = requests.post(
            f"{_NTFY_BASE_URL}/{ntfy_topic}",
            data=message.encode("utf-8"),
            headers={
                "Title": f"Dagster run {cfg['title_word']} - {job_name}",
                "Priority": cfg["priority"],
                "Tags": cfg["tags"],
            },
            timeout=10,
        )
        response.raise_for_status()
        logger.info(
            "ntfy.sh alert sent — job=%s run_id=%s status=%s environment=%s",
            job_name,
            short_run_id,
            status,
            environment,
        )
    except Exception as exc:
        logger.warning(
            "Failed to send ntfy.sh alert — job=%s run_id=%s: %s",
            job_name,
            short_run_id,
            exc,
        )


# ---------------------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------------------


def _build_and_write_run_summary(
    context: RunStatusSensorContext,
    status: str,
    dlt_onelake: DltOneLakeResource,
) -> None:
    """Collect run stats from the Dagster instance and write an NDJSON record to the log destination.

    Args:
        context: The sensor evaluation context injected by Dagster.
        status: ``"success"`` or ``"failure"`` — passed in by the caller.
        dlt_onelake: Injected resource used to write the log record.
    """
    run = context.dagster_run
    logger = context.log

    # ------------------------------------------------------------------
    # Collect run-level timing from the Dagster event store.
    # ------------------------------------------------------------------
    run_stats = context.instance.get_run_stats(run.run_id)

    start_time = (
        datetime.fromtimestamp(run_stats.start_time, tz=UTC)
        if run_stats.start_time
        else datetime.now(UTC)
    )
    end_time = (
        datetime.fromtimestamp(run_stats.end_time, tz=UTC)
        if run_stats.end_time
        else datetime.now(UTC)
    )

    # ------------------------------------------------------------------
    # Collect per-step results for the detailed pipelines list.
    # ------------------------------------------------------------------
    step_stats = context.instance.get_run_step_stats(run.run_id)

    steps_summary = sorted(
        [
            {
                "step_key": s.step_key,
                "status": s.status.value if s.status else "unknown",
                "duration_seconds": (
                    round(s.end_time - s.start_time, 3)
                    if s.end_time is not None and s.start_time is not None
                    else None
                ),
            }
            for s in step_stats
        ],
        key=lambda s: s["step_key"] or "",
    )

    steps_succeeded = sum(1 for s in step_stats if s.status and s.status.value == "SUCCESS")
    steps_failed = sum(
        1 for s in step_stats if s.status and s.status.value not in ("SUCCESS", "SKIPPED")
    )

    # ------------------------------------------------------------------
    # Delegate serialisation and write to the log resource.
    # ------------------------------------------------------------------
    try:
        dlt_onelake.write_job_run_log(
            job_name=run.job_name,
            run_id=run.run_id,
            status=status,
            start_time=start_time,
            end_time=end_time,
            extra={
                "steps_total": len(steps_summary),
                "steps_succeeded": steps_succeeded,
                "steps_failed": steps_failed,
                "steps": steps_summary,
                "tags": dict(run.tags) if run.tags else {},
            },
        )
        logger.info(
            "Job run summary written — job=%s run_id=%s status=%s",
            run.job_name,
            run.run_id,
            status,
        )
    except Exception as exc:
        # A log-write failure must never block the next Dagster run.
        logger.warning(
            "Failed to write job run summary — job=%s run_id=%s: %s",
            run.job_name,
            run.run_id,
            exc,
        )


# ---------------------------------------------------------------------------
# Sensor: SUCCESS
# ---------------------------------------------------------------------------


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="danish_parliament_run_success_sensor",
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Appends a job-run SUCCESS summary to the configured log destination "
        "(DDD/<job_name>_run_log.ndjson) and sends a ntfy.sh push notification."
    ),
)
def danish_parliament_run_success_sensor(
    context: RunStatusSensorContext,
    dlt_onelake: DltOneLakeResource,
) -> None:
    """Write a success summary to the configured log destination and send a ntfy.sh push alert."""
    _build_and_write_run_summary(context, status="success", dlt_onelake=dlt_onelake)
    run = context.dagster_run
    _send_ntfy_alert(
        job_name=run.job_name,
        run_id=run.run_id,
        environment=os.getenv("ENVIRONMENT", "unknown"),
        status="success",
        logger=context.log,
    )


# ---------------------------------------------------------------------------
# Sensor: FAILURE
# ---------------------------------------------------------------------------


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    name="danish_parliament_run_failure_sensor",
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Appends a job-run FAILURE summary to the configured log destination "
        "(DDD/<job_name>_run_log.ndjson) and sends a high-priority ntfy.sh push notification."
    ),
)
def danish_parliament_run_failure_sensor(
    context: RunStatusSensorContext,
    dlt_onelake: DltOneLakeResource,
) -> None:
    """Write a failure summary to the configured log destination and send a ntfy.sh push alert."""
    _build_and_write_run_summary(context, status="failure", dlt_onelake=dlt_onelake)
    run = context.dagster_run
    _send_ntfy_alert(
        job_name=run.job_name,
        run_id=run.run_id,
        environment=os.getenv("ENVIRONMENT", "unknown"),
        status="failure",
        logger=context.log,
    )
