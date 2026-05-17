"""Dagster Definitions entrypoint for the project."""

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

from dagster import AssetKey, AssetsDefinition, Definitions
from dagster_dbt import DbtCliResource

from ddd_python.ddd_dagster.assets import all_extraction_assets
from ddd_python.ddd_dagster.dbt_assets import (
    _DBT_PROJECT_DIR,
    dbt_bronze_assets,
    dbt_data_engineering_assets,
    dbt_gold_assets,
    dbt_seeds_assets,
    dbt_silver_assets,
)
from ddd_python.ddd_dagster.export_assets import all_export_assets
from ddd_python.ddd_dagster.jobs import (
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
    export_gold_job,
    export_silver_job,
    full_pipeline_job,
    rfam_all_job,
    rfam_full_extract_job,
    rfam_incremental_job,
)
from ddd_python.ddd_dagster.metabase_control_assets import (
    build_start_metabase_asset,
    stop_metabase_asset,
)
from ddd_python.ddd_dagster.resources import DltOneLakeResource
from ddd_python.ddd_dagster.rfam_assets import all_rfam_extraction_assets
from ddd_python.ddd_dagster.schedules import (
    danish_parliament_full_pipeline_schedule,
    dbt_data_engineering_schedule,
)
from ddd_python.ddd_dagster.sensors import (
    danish_parliament_run_failure_sensor,
    danish_parliament_run_success_sensor,
)


def _asset_keys(asset_defs: list[AssetsDefinition]) -> list[AssetKey]:
    keys: list[AssetKey] = []
    for asset_def in asset_defs:
        keys.extend(sorted(asset_def.keys, key=lambda key: key.to_user_string()))
    return keys


_extraction_assets = list(all_extraction_assets)
_rfam_assets = list(all_rfam_extraction_assets)
_dbt_assets = [
    dbt_seeds_assets,
    dbt_bronze_assets,
    dbt_silver_assets,
    dbt_gold_assets,
    dbt_data_engineering_assets,
]
_export_assets = list(all_export_assets)
_materialization_assets = [
    *_extraction_assets,
    *_rfam_assets,
    *_dbt_assets,
    *_export_assets,
]
start_metabase_asset = build_start_metabase_asset(_asset_keys(_materialization_assets))


defs = Definitions(
    assets=[
        stop_metabase_asset,
        *_extraction_assets,
        *_rfam_assets,
        *_dbt_assets,
        *_export_assets,
        start_metabase_asset,
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
        "dlt_onelake": DltOneLakeResource(),
        "dbt": DbtCliResource(
            project_dir=_DBT_PROJECT_DIR,
            profiles_dir=_DBT_PROJECT_DIR,
        ),
    },
)
