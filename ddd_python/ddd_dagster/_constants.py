"""Shared constants for Dagster asset definitions.

Centralises values shared by assets.py, rfam_assets.py, export_assets.py and
jobs.py.  The worker count shared with the extraction pool lives in
ddd_utils.configuration_variables, because lower layers must not import from
ddd_dagster.
"""

from dagster import AssetKey, Backoff, RetryPolicy

# Two retries with exponential back-off (60 s → 120 s) for transient source /
# export-destination network failures.  Shared by extraction and export asset factories.
_RETRY_POLICY = RetryPolicy(
    max_retries=2,
    delay=60,
    backoff=Backoff.EXPONENTIAL,
)

DUCKLAKE_CLEANUP_ASSET_KEY = AssetKey(["ducklake_cleanup_asset"])
BRONZE_ARCHIVE_ASSET_KEY = AssetKey(["bronze_archive_asset"])
