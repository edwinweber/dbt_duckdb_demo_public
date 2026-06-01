"""Shared constants for Dagster asset definitions.

Centralises values that were previously duplicated across assets.py,
rfam_assets.py, and export_assets.py.
"""

from dagster import Backoff, RetryPolicy

# Two retries with exponential back-off (60 s → 120 s) for transient API /
# OneLake network failures.  Shared by extraction and export asset factories.
_RETRY_POLICY = RetryPolicy(
    max_retries=2,
    delay=60,
    backoff=Backoff.EXPONENTIAL,
)
