"""Storage path utilities for the Danish Democracy Data project.

Centralises all logic for constructing Bronze / Silver / Gold storage paths
so that each call site does not have to know whether the target is local
disk or Fabric OneLake.

Used by:
- ``ddd_dlt`` extraction scripts (Bronze destination directories for dlt)
- ``ddd_dlt`` export scripts (Delta Lake table paths for Silver / Gold)
- ``ddd_dagster._constants`` (re-exports ``build_bronze_destination_path``
  for the Dagster asset factories)
"""

import os

from ddd_python.ddd_utils import get_variables_from_env


def build_bronze_destination_path(source_system_code: str, entity_name: str) -> str:
    """Build the Bronze directory path for a source system entity.

    Args:
        source_system_code: Short code for the source system
            (e.g. ``"DDD"``, ``"RFAM"``).
        entity_name: Normalised entity / table name
            (e.g. ``"aktoer"``, ``"family"``).

    Returns:
        A storage path string rooted at the Bronze layer.  For local storage
        this is relative to ``LOCAL_STORAGE_PATH`` (dlt prepends it); for
        OneLake it is an ``abfss://`` URI fragment starting with the
        ``FABRIC_ONELAKE_FOLDER_BRONZE`` folder.
    """
    if get_variables_from_env.STORAGE_TARGET == "local":
        return f"Files/Bronze/{source_system_code}/{entity_name}"
    return (
        f"{get_variables_from_env.FABRIC_ONELAKE_FOLDER_BRONZE}"
        f"/{source_system_code}/{entity_name}"
    )


def build_delta_export_path(layer: str, table: str) -> tuple[str, dict]:
    """Build the Delta Lake export path and storage options for a given layer.

    Handles the local-vs-OneLake switch and, for local storage, creates the
    target directory as a side effect so callers do not need an ``os.makedirs``
    after calling this function.

    Args:
        layer: Medallion layer name, lowercase (``"silver"`` or ``"gold"``).
        table: Table name used as the leaf directory
            (e.g. ``"silver_ddd_aktoer"``).

    Returns:
        A ``(path, storage_options)`` tuple ready for use with
        ``deltalake.write_deltalake`` / ``DeltaTable``.  ``storage_options``
        is an empty dict for local storage and contains a bearer token for
        OneLake.
    """
    layer_cap = layer.capitalize()   # "Silver" / "Gold"
    layer_upper = layer.upper()      # "SILVER" / "GOLD"

    if get_variables_from_env.STORAGE_TARGET == "local":
        path = f"{get_variables_from_env.LOCAL_STORAGE_PATH}/Files/{layer_cap}/{table}/"
        os.makedirs(path, exist_ok=True)
        return path, {}

    from ddd_python.ddd_utils import get_fabric_onelake_clients  # lazy — avoids loading Azure SDK when not needed
    token = get_fabric_onelake_clients.get_fabric_token()
    folder = getattr(get_variables_from_env, f"FABRIC_ONELAKE_FOLDER_{layer_upper}")
    path = (
        f"abfss://{get_variables_from_env.FABRIC_WORKSPACE}"
        f"@{get_variables_from_env.FABRIC_ONELAKE_STORAGE_ACCOUNT}"
        f".dfs.fabric.microsoft.com/{folder}/{table}/"
    )
    return path, {"bearer_token": token, "use_fabric_endpoint": "true"}
