"""Shared constants for Dagster asset definitions.

Centralises values that were previously duplicated across assets.py,
rfam_assets.py, and export_assets.py.
"""

from dagster import Backoff, RetryPolicy

from ddd_python.ddd_utils import get_variables_from_env

# Two retries with exponential back-off (60 s → 120 s) for transient API /
# OneLake network failures.  Shared by extraction and export asset factories.
_RETRY_POLICY = RetryPolicy(
    max_retries=2,
    delay=60,
    backoff=Backoff.EXPONENTIAL,
)


def build_bronze_destination_path(source_system_code: str, entity_name: str) -> str:
    """Build the Bronze directory path for a source system entity.

    Args:
        source_system_code: Short code for the source system (e.g. ``"DDD"``, ``"RFAM"``).
        entity_name: Normalised entity / table name (e.g. ``"aktoer"``, ``"family"``).

    Returns:
        A storage path string rooted at the Bronze layer.
    """
    if get_variables_from_env.STORAGE_TARGET == "local":
        return f"Files/Bronze/{source_system_code}/{entity_name}"
    return (
        f"{get_variables_from_env.FABRIC_ONELAKE_FOLDER_BRONZE}"
        f"/{source_system_code}/{entity_name}"
    )
