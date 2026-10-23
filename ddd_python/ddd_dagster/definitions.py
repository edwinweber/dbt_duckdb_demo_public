"""Dagster Definitions entrypoint for the project."""

import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

from dagster import Definitions
from dagster_dbt import DbtCliResource

from ddd_python.ddd_dagster.assets import all_extraction_assets
from ddd_python.ddd_dagster.bronze_archive_assets import bronze_archive_asset
from ddd_python.ddd_dagster.dbt_assets import (
    _DBT_PROJECT_DIR,
    dbt_bronze_assets,
    dbt_data_engineering_assets,
    dbt_gold_assets,
    dbt_seeds_assets,
    dbt_silver_assets,
)
from ddd_python.ddd_dagster.ducklake_cleanup_assets import ducklake_cleanup_asset
from ddd_python.ddd_dagster.export_assets import all_export_assets
from ddd_python.ddd_dagster.jobs import (
    bronze_archive_job,
    danish_parliament_all_job,
    danish_parliament_full_extract_job,
    danish_parliament_incremental_job,
    dbt_bronze_ddd_job,
    dbt_bronze_job,
    dbt_bronze_rfam_job,
    dbt_data_engineering_job,
    dbt_gold_job,
    dbt_seeds_job,
    dbt_silver_ddd_job,
    dbt_silver_job,
    dbt_silver_rfam_job,
    ducklake_cleanup_job,
    export_gold_job,
    export_silver_job,
    full_pipeline_job,
    rfam_all_job,
    rfam_full_extract_job,
    rfam_incremental_job,
)
from ddd_python.ddd_dagster.resources import DltPipelineResource
from ddd_python.ddd_dagster.rfam_assets import all_rfam_extraction_assets
from ddd_python.ddd_dagster.schedules import (
    danish_parliament_full_pipeline_schedule,
    dbt_data_engineering_schedule,
)
from ddd_python.ddd_dagster.sensors import (
    danish_parliament_run_failure_sensor,
    danish_parliament_run_success_sensor,
)

defs = Definitions(
    assets=[
        *list(all_extraction_assets),
        *list(all_rfam_extraction_assets),
        dbt_seeds_assets,
        dbt_bronze_assets,
        dbt_silver_assets,
        dbt_gold_assets,
        dbt_data_engineering_assets,
        *list(all_export_assets),
        ducklake_cleanup_asset,
        bronze_archive_asset,
    ],
    jobs=[
        danish_parliament_incremental_job,
        danish_parliament_full_extract_job,
        danish_parliament_all_job,
        dbt_seeds_job,
        dbt_bronze_job,
        dbt_bronze_ddd_job,
        dbt_bronze_rfam_job,
        dbt_silver_job,
        dbt_silver_ddd_job,
        dbt_silver_rfam_job,
        dbt_gold_job,
        dbt_data_engineering_job,
        ducklake_cleanup_job,
        bronze_archive_job,
        export_silver_job,
        export_gold_job,
        full_pipeline_job,
        rfam_incremental_job,
        rfam_full_extract_job,
        rfam_all_job,
    ],
    schedules=[
        danish_parliament_full_pipeline_schedule,
        dbt_data_engineering_schedule,
    ],
    sensors=[
        danish_parliament_run_success_sensor,
        danish_parliament_run_failure_sensor,
    ],
    resources={
        "dlt_onelake": DltPipelineResource(),
        "dbt": DbtCliResource(
            project_dir=_DBT_PROJECT_DIR,
            profiles_dir=_DBT_PROJECT_DIR,
            dbt_executable=os.getenv("DBT_EXECUTABLE", "dbt"),
        ),
    },
)
