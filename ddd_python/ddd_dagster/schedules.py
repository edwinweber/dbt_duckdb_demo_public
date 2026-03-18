"""Dagster schedule definitions for the Danish Democracy Data (DDD) pipeline.

A single schedule triggers the full end-to-end pipeline job which runs
extraction → dbt Bronze → Silver → Gold → export Silver → export Gold.
Layer ordering is enforced by asset dependencies, not by time offsets.

The schedule defaults to ``STOPPED`` — enable it in the Dagster UI or via
``dagster schedule start`` when ready to run on a schedule.
"""

from dagster import (
    DefaultScheduleStatus,
    ScheduleDefinition,
)

from ddd_python.ddd_dagster.jobs import danish_parliament_full_pipeline_job

danish_parliament_full_pipeline_schedule = ScheduleDefinition(
    name="danish_parliament_full_pipeline_schedule",
    job=danish_parliament_full_pipeline_job,
    cron_schedule="0 6 * * *",
    execution_timezone="UTC",
    description=(
        "Daily full pipeline at 06:00 UTC — runs extraction → dbt Bronze → "
        "Silver → Gold → export Silver → export Gold.  Layer ordering is "
        "enforced by Dagster asset dependencies, not time offsets."
    ),
    default_status=DefaultScheduleStatus.STOPPED,
)
