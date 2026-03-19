"""Dagster job definitions for the Danish Democracy Data (DDD) pipeline.

Extraction jobs
~~~~~~~~~~~~~~~
``danish_parliament_incremental_job``
    Runs all assets in the ``ingestion/DDD`` group (6 incremental entities).

``danish_parliament_full_extract_job``
    Runs all assets in the ``ingestion/DDD`` group (12 full-extract entities).

``danish_parliament_all_job``
    All 18 extraction assets in a single job.

dbt transformation jobs
~~~~~~~~~~~~~~~~~~~~~~~
``dbt_seeds_job``, ``dbt_bronze_job``, ``dbt_silver_job``, ``dbt_gold_job``

Export jobs
~~~~~~~~~~
``export_silver_job``
    Exports all Silver tables from DuckDB to OneLake as Delta Lake tables
    (incremental append).

``export_gold_job``
    Exports all Gold tables from DuckDB to OneLake as Delta Lake tables
    (full overwrite).

End-to-end pipeline
~~~~~~~~~~~~~~~~~~~
``danish_parliament_full_pipeline_job``
    Runs extraction → dbt Bronze → Silver → Gold → export Silver → export Gold.

Executor
--------
Extraction and export jobs use ``multiprocess_executor`` (``max_concurrent=4``).
dbt jobs use ``in_process_executor`` (DuckDB single-writer constraint).
"""

from dagster import (
    AssetSelection,
    define_asset_job,
    in_process_executor,
    multiprocess_executor,
)
from dagster_dbt import build_dbt_asset_selection

# ---------------------------------------------------------------------------
# Shared executor: 4-way concurrency, mirrors ThreadPoolExecutor(max_workers=4)
# ---------------------------------------------------------------------------

_concurrent_executor = multiprocess_executor.configured({"max_concurrent": 4})

# ---------------------------------------------------------------------------
# Job definitions
# ---------------------------------------------------------------------------

danish_parliament_incremental_job = define_asset_job(
    name="danish_parliament_incremental_job",
    selection=AssetSelection.groups("ingestion_DDD_incremental"),
    executor_def=_concurrent_executor,
    description=(
        "Incrementally extracts the 6 Danish Parliament resources that support "
        "OData date filtering (opdateringsdato).  Runs daily; date lower-bound "
        "defaults to today minus DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD days."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "load_mode": "incremental",
    },
)

danish_parliament_full_extract_job = define_asset_job(
    name="danish_parliament_full_extract_job",
    selection=AssetSelection.groups("ingestion_DDD_full_extract"),
    executor_def=_concurrent_executor,
    description=(
        "Full extraction of the 12 Danish Parliament resources that are always "
        "fetched in full on every run.  These tables support opdateringsdato "
        "filtering but are small enough that a full extract is simpler and "
        "makes delete detection straightforward.  Runs weekly on Mondays."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "load_mode": "full_extract",
    },
)

danish_parliament_all_job = define_asset_job(
    name="danish_parliament_all_job",
    selection=AssetSelection.groups("ingestion_DDD_incremental", "ingestion_DDD_full_extract"),
    executor_def=_concurrent_executor,
    description=(
        "Runs all 18 Danish Parliament extraction assets — incremental and "
        "full-extract — in a single job.  Intended for initial backfills and "
        "ad-hoc full reloads."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "load_mode": "all",
    },
)

# ---------------------------------------------------------------------------
# dbt transformation jobs — Bronze → Silver → Gold (sequential)
# ---------------------------------------------------------------------------
#
# Import here (deferred) to avoid a circular dependency: dbt_assets.py imports
# from jobs.py indirectly via definitions.py, so we resolve at call time.

def _seeds_selection():
    from ddd_python.ddd_dagster.dbt_assets import dbt_seeds_assets
    return build_dbt_asset_selection([dbt_seeds_assets])


def _bronze_selection():
    from ddd_python.ddd_dagster.dbt_assets import dbt_bronze_assets
    return build_dbt_asset_selection([dbt_bronze_assets])


def _silver_selection():
    from ddd_python.ddd_dagster.dbt_assets import dbt_silver_assets
    return build_dbt_asset_selection([dbt_silver_assets])


def _gold_selection():
    from ddd_python.ddd_dagster.dbt_assets import dbt_gold_assets
    return build_dbt_asset_selection([dbt_gold_assets])


dbt_seeds_job = define_asset_job(
    name="dbt_seeds_job",
    selection=_seeds_selection(),
    executor_def=in_process_executor,
    description=(
        "Loads all dbt seeds (static CSV reference data) into local DuckDB "
        "via ``dbt seed``.  Run this job on initial setup or whenever the "
        "seed CSV files change."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "layer": "seeds",
    },
)

dbt_bronze_job = define_asset_job(
    name="dbt_bronze_job",
    selection=_bronze_selection(),
    executor_def=in_process_executor,
    description=(
        "Runs all dbt Bronze models (views that read raw NDJSON from OneLake) "
        "via ``dbt build --select bronze``.  Bronze models are cheap views; "
        "this job wires the dlt extraction output into the dbt transformation "
        "layer.  Normally included implicitly in the Silver job schedule."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "layer": "bronze",
    },
)

dbt_silver_job = define_asset_job(
    name="dbt_silver_job",
    selection=_silver_selection(),
    executor_def=in_process_executor,
    description=(
        "Runs all dbt Silver models (incremental CDC tables and current-version "
        "views) via ``dbt build --select silver``.  "
        "Reads from Bronze views on DuckDB (local)."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "layer": "silver",
    },
)

dbt_gold_job = define_asset_job(
    name="dbt_gold_job",
    selection=_gold_selection(),
    executor_def=in_process_executor,
    description=(
        "Runs all dbt Gold models (dimensional views and the individual_votes "
        "fact table) via ``dbt build --select gold``.  "
        "Reads from the Silver layer.  Must run after dbt_silver_job."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "layer": "gold",
    },
)


# ---------------------------------------------------------------------------
# Export jobs — DuckDB → OneLake Delta Lake
# ---------------------------------------------------------------------------

export_silver_job = define_asset_job(
    name="export_silver_job",
    selection=AssetSelection.groups("export_silver"),
    executor_def=_concurrent_executor,
    description=(
        "Exports all Silver tables from DuckDB to OneLake as Delta "
        "Lake tables (incremental append).  Each table is exported as its own "
        "asset, running concurrently (max 4).  Must run after dbt_silver_job."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "layer": "export_silver",
    },
)

export_gold_job = define_asset_job(
    name="export_gold_job",
    selection=AssetSelection.groups("export_gold"),
    executor_def=_concurrent_executor,
    description=(
        "Exports all Gold tables from DuckDB to OneLake as Delta "
        "Lake tables (full overwrite).  Each table is exported as its own "
        "asset, running concurrently (max 4).  Must run after dbt_gold_job."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "layer": "export_gold",
    },
)


# ---------------------------------------------------------------------------
# Full pipeline job — extraction → dbt → export
# ---------------------------------------------------------------------------

def _full_pipeline_selection():
    from ddd_python.ddd_dagster.dbt_assets import dbt_bronze_assets, dbt_silver_assets, dbt_gold_assets
    return (
        AssetSelection.groups("ingestion_DDD_incremental", "ingestion_DDD_full_extract")
        | build_dbt_asset_selection([dbt_bronze_assets])
        | build_dbt_asset_selection([dbt_silver_assets])
        | build_dbt_asset_selection([dbt_gold_assets])
        | AssetSelection.groups("export_silver", "export_gold")
    )


danish_parliament_full_pipeline_job = define_asset_job(
    name="danish_parliament_full_pipeline_job",
    selection=_full_pipeline_selection(),
    executor_def=_concurrent_executor,
    description=(
        "End-to-end pipeline: extracts all 18 Danish Parliament resources via "
        "dlt, runs dbt Bronze → Silver → Gold, then exports Silver and Gold "
        "tables to OneLake as Delta Lake.  Extraction and export assets run "
        "concurrently (max 4); dbt assets run sequentially via asset "
        "dependencies and DuckDB's single-writer constraint (threads: 1)."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "load_mode": "full_pipeline",
    },
)
