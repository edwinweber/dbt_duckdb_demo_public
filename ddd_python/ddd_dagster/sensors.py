"""Dagster run-status sensors that persist job-run summaries to OneLake.

Two ``@run_status_sensor`` definitions mirror the script-level NDJSON summary
log that the original ``dlt_run_extraction_pipelines_danish_parliament_data.py``
wrote at the end of every invocation:

``danish_parliament_run_success_sensor``
    Fires when a monitored job transitions to **SUCCESS**.

``danish_parliament_run_failure_sensor``
    Fires when a monitored job transitions to **FAILURE**.

Both sensors write one NDJSON record to OneLake at::

    <DLT_PIPELINE_RUN_LOG_DIR>/DDD/<job_name>_run_log.ndjson

The record contains job name, Dagster run ID, UTC start/end times, duration,
overall status, and a per-step summary (step key, status, duration) ranked by
step key.  This gives operations teams a queryable OneLake audit trail that
complements the detailed per-pipeline logs already written by
:func:`dlt_pipeline_execution_functions.execute_pipeline`.

Monitoring scope
----------------
All three extraction jobs are monitored:
* ``danish_parliament_incremental_job``
* ``danish_parliament_full_extract_job``
* ``danish_parliament_all_job``

Write failures
--------------
If the OneLake append fails (e.g. transient connectivity), the exception is
caught, logged as a warning, and the sensor tick is marked successful.  A
logging failure must never block or delay the next Dagster run.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dagster import (
    DagsterRunStatus,
    DefaultSensorStatus,
    RunStatusSensorContext,
    run_status_sensor,
)

from ddd_python.ddd_dagster.resources import DltOneLakeResource
from ddd_python.ddd_dagster.jobs import (
    danish_parliament_all_job,
    danish_parliament_full_extract_job,
    danish_parliament_incremental_job,
)

_MONITORED_JOBS = [
    danish_parliament_incremental_job,
    danish_parliament_full_extract_job,
    danish_parliament_all_job,
]


# ---------------------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------------------


def _build_and_write_run_summary(
    context: RunStatusSensorContext,
    status: str,
    dlt_onelake: DltOneLakeResource,
) -> None:
    """Collect run stats from the Dagster instance and write an NDJSON record to OneLake.

    Args:
        context: The sensor evaluation context injected by Dagster.
        status: ``"success"`` or ``"failure"`` — passed in by the caller.
        dlt_onelake: Injected OneLake resource used to write the log record.
    """
    run = context.dagster_run
    logger = context.log

    # ------------------------------------------------------------------
    # Collect run-level timing from the Dagster event store.
    # ------------------------------------------------------------------
    run_stats = context.instance.get_run_stats(run.run_id)

    start_time = (
        datetime.fromtimestamp(run_stats.start_time, tz=timezone.utc)
        if run_stats.start_time
        else datetime.now(timezone.utc)
    )
    end_time = (
        datetime.fromtimestamp(run_stats.end_time, tz=timezone.utc)
        if run_stats.end_time
        else datetime.now(timezone.utc)
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
        key=lambda s: s["step_key"],
    )

    steps_succeeded = sum(
        1 for s in step_stats if s.status and s.status.value == "SUCCESS"
    )
    steps_failed = sum(
        1
        for s in step_stats
        if s.status and s.status.value not in ("SUCCESS", "SKIPPED")
    )

    # ------------------------------------------------------------------
    # Delegate serialisation and write to the OneLake resource.
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
            "Job run summary written to OneLake — job=%s run_id=%s status=%s",
            run.job_name,
            run.run_id,
            status,
        )
    except Exception as exc:
        # A log-write failure must never block the next Dagster run.
        logger.warning(
            "Failed to write job run summary to OneLake — job=%s run_id=%s: %s",
            run.job_name,
            run.run_id,
            exc,
        )


# ---------------------------------------------------------------------------
# Sensor: SUCCESS
# ---------------------------------------------------------------------------


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=_MONITORED_JOBS,
    name="danish_parliament_run_success_sensor",
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Appends a job-run SUCCESS summary to "
        "<DLT_PIPELINE_RUN_LOG_DIR>/DDD/<job_name>_run_log.ndjson on OneLake."
    ),
)
def danish_parliament_run_success_sensor(
    context: RunStatusSensorContext,
    dlt_onelake: DltOneLakeResource,
) -> None:
    """Write a success summary record to OneLake when a monitored job completes."""
    _build_and_write_run_summary(context, status="success", dlt_onelake=dlt_onelake)


# ---------------------------------------------------------------------------
# Sensor: FAILURE
# ---------------------------------------------------------------------------


@run_status_sensor(
    run_status=DagsterRunStatus.FAILURE,
    monitored_jobs=_MONITORED_JOBS,
    name="danish_parliament_run_failure_sensor",
    default_status=DefaultSensorStatus.RUNNING,
    description=(
        "Appends a job-run FAILURE summary to "
        "<DLT_PIPELINE_RUN_LOG_DIR>/DDD/<job_name>_run_log.ndjson on OneLake."
    ),
)
def danish_parliament_run_failure_sensor(
    context: RunStatusSensorContext,
    dlt_onelake: DltOneLakeResource,
) -> None:
    """Write a failure summary record to OneLake when a monitored job fails."""
    _build_and_write_run_summary(context, status="failure", dlt_onelake=dlt_onelake)
