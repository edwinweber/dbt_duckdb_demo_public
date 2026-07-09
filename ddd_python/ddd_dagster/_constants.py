"""Shared constants for Dagster asset definitions.

Centralises values that were previously duplicated across assets.py,
rfam_assets.py, export_assets.py, and dlt_pipeline_execution_functions.py.
"""

from dagster import AssetKey, Backoff, RetryPolicy

# Two retries with exponential back-off (60 s → 120 s) for transient API /
# OneLake network failures.  Shared by extraction and export asset factories.
_RETRY_POLICY = RetryPolicy(
    max_retries=2,
    delay=60,
    backoff=Backoff.EXPONENTIAL,
)

STOP_METABASE_ASSET_KEY = AssetKey(["stop_metabase_asset"])
START_METABASE_ASSET_KEY = AssetKey(["start_metabase_asset"])
DUCKLAKE_CLEANUP_ASSET_KEY = AssetKey(["ducklake_cleanup_asset"])

# Maximum concurrent pipeline tasks / Dagster processes.
# Used by multiprocess_executor in jobs.py and ThreadPoolExecutor in
# dlt_pipeline_execution_functions.py — one constant, no comment dependency.
MAX_CONCURRENT_WORKERS: int = 4
