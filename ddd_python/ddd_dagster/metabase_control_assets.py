"""Dagster assets for Metabase lifecycle control.

Stops Metabase before dbt runs and restarts it afterward. This is necessary
because DuckDB is single-writer: when dbt runs a transformation, it needs an
exclusive write lock on the .duckdb file. Metabase holds a persistent read
connection to the same file and must release it first. These assets are no-ops
in non-Docker environments (no-op if scripts are not present or not executable).
"""

import subprocess
from collections.abc import Iterable

from dagster import AssetKey, AssetsDefinition, asset


@asset(name="stop_metabase_asset", description="Stops Metabase before pipeline runs.")
def stop_metabase_asset():
    subprocess.run(["./stop_metabase_and_wait.sh"], check=True)


def build_start_metabase_asset(upstream_asset_keys: Iterable[AssetKey]) -> AssetsDefinition:
    unique_keys = sorted(set(upstream_asset_keys), key=lambda key: key.to_user_string())

    @asset(
        name="start_metabase_asset",
        deps=unique_keys,
        description="Starts Metabase after pipeline runs.",
    )
    def start_metabase_asset():
        subprocess.run(["./start_metabase_and_wait.sh"], check=True)

    return start_metabase_asset
