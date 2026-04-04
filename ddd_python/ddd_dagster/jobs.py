"""Dagster job definitions for all source systems.

DDD extraction jobs
~~~~~~~~~~~~~~~~~~~
``danish_parliament_incremental_job``
    Runs all assets in the ``ingestion/DDD`` group (6 incremental entities).

``danish_parliament_full_extract_job``
    Runs all assets in the ``ingestion/DDD`` group (12 full-extract entities).

``danish_parliament_all_job``
    All 18 DDD extraction assets in a single job.

Rfam extraction jobs
~~~~~~~~~~~~~~~~~~~~
``rfam_incremental_job``
    Incrementally extracts Rfam tables with ``updated`` column (2 tables).

``rfam_full_extract_job``
    Full extraction of the 5 small Rfam lookup tables.

``rfam_all_job``
    All 7 Rfam extraction assets in a single job.

dbt transformation jobs
~~~~~~~~~~~~~~~~~~~~~~~
``dbt_seeds_job``

``dbt_bronze_job`` / ``dbt_bronze_ddd_job`` / ``dbt_bronze_rfam_job``
    All Bronze models, or filtered by source system.

``dbt_silver_job`` / ``dbt_silver_ddd_job`` / ``dbt_silver_rfam_job``
    All Silver models, or filtered by source system.

``dbt_gold_job``

``dbt_data_engineering_job``
    Refreshes the Dagster observability layer (reads from Dagster SQLite).

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
``full_pipeline_job``
    Runs all source system extractions (DDD + Rfam) → dbt Bronze → Silver →
    Gold → export Silver → export Gold → Data Engineering observability layer.

Executor
--------
Extraction and export jobs use ``multiprocess_executor`` (``max_concurrent=4``).
dbt jobs use ``in_process_executor`` (DuckDB single-writer constraint).
``full_pipeline_job`` uses ``in_process_executor`` to enforce strict sequential
ordering: ingestion → Bronze → Silver → Gold → export Silver → export Gold.
"""

from dagster import (
    AssetSelection,
    define_asset_job,
    in_process_executor,
    multiprocess_executor,
)
from dagster_dbt import build_dbt_asset_selection

# Default Launchpad configs for incremental jobs.
# date_to_load_from defaults to null so normal runs use today - lookback days
# (31 for DDD, 365 for RFAM). Set an explicit date only for backfills.
_DDD_INCREMENTAL_CONFIG = {
    "ops": {
        f"ingestion__DDD__{name}": {"config": {"date_to_load_from": None}}
        for name in ["aktoer", "moede", "sag", "sagstrin", "sagstrinaktoer", "stemme"]
    }
}

_RFAM_INCREMENTAL_CONFIG = {
    "ops": {
        f"ingestion__RFAM__{name}": {"config": {"date_to_load_from": None}}
        for name in ["family", "genome"]
    }
}

_DBT_SILVER_DEFAULT_CONFIG = {
    "ops": {
        "dbt_silver_assets": {"config": {"full_refresh": False}}
    }
}

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
    config=_DDD_INCREMENTAL_CONFIG,
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
    config=_DDD_INCREMENTAL_CONFIG,
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
# Rfam extraction jobs
# ---------------------------------------------------------------------------

rfam_incremental_job = define_asset_job(
    name="rfam_incremental_job",
    selection=AssetSelection.groups("ingestion_RFAM_incremental"),
    executor_def=_concurrent_executor,
    config=_RFAM_INCREMENTAL_CONFIG,
    description=(
        "Incrementally extracts Rfam tables that support date filtering via "
        "the ``updated`` timestamp column (family, genome)."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "RFAM",
        "load_mode": "incremental",
    },
)

rfam_full_extract_job = define_asset_job(
    name="rfam_full_extract_job",
    selection=AssetSelection.groups("ingestion_RFAM_full_extract"),
    executor_def=_concurrent_executor,
    description=(
        "Full extraction of the 5 small Rfam lookup tables that are always "
        "fetched in full on every run."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "RFAM",
        "load_mode": "full_extract",
    },
)

rfam_all_job = define_asset_job(
    name="rfam_all_job",
    selection=AssetSelection.groups("ingestion_RFAM_incremental", "ingestion_RFAM_full_extract"),
    executor_def=_concurrent_executor,
    config=_RFAM_INCREMENTAL_CONFIG,
    description=(
        "Runs all 7 Rfam extraction assets — incremental and full-extract — "
        "in a single job."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "RFAM",
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


def _dbt_select_with_latest(model_names: list[str]) -> str:
    """Build a dbt select string for a list of models and their ``_latest`` views."""
    all_names = model_names + [f"{m}_latest" for m in model_names]
    return " ".join(all_names)


def _dbt_select_with_cv(model_names: list[str]) -> str:
    """Build a dbt select string for a list of models and their ``_cv`` views."""
    all_names = model_names + [f"{m}_cv" for m in model_names]
    return " ".join(all_names)


def _bronze_ddd_selection():
    from ddd_python.ddd_dagster.dbt_assets import dbt_bronze_assets
    from ddd_python.ddd_utils import configuration_variables
    select = _dbt_select_with_latest(configuration_variables.DANISH_DEMOCRACY_MODELS_BRONZE)
    return build_dbt_asset_selection([dbt_bronze_assets], dbt_select=select)


def _bronze_rfam_selection():
    from ddd_python.ddd_dagster.dbt_assets import dbt_bronze_assets
    from ddd_python.ddd_utils import configuration_variables
    select = _dbt_select_with_latest(configuration_variables.RFAM_MODELS_BRONZE)
    return build_dbt_asset_selection([dbt_bronze_assets], dbt_select=select)


def _bronze_selection():
    return _bronze_ddd_selection() | _bronze_rfam_selection()


def _silver_ddd_selection():
    from ddd_python.ddd_dagster.dbt_assets import dbt_silver_assets
    from ddd_python.ddd_utils import configuration_variables
    select = _dbt_select_with_cv(configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER)
    return build_dbt_asset_selection([dbt_silver_assets], dbt_select=select)


def _silver_rfam_selection():
    from ddd_python.ddd_dagster.dbt_assets import dbt_silver_assets
    from ddd_python.ddd_utils import configuration_variables
    select = _dbt_select_with_cv(configuration_variables.RFAM_MODELS_SILVER)
    return build_dbt_asset_selection([dbt_silver_assets], dbt_select=select)


def _silver_selection():
    return _silver_ddd_selection() | _silver_rfam_selection()


def _gold_selection():
    from ddd_python.ddd_dagster.dbt_assets import dbt_gold_assets
    return build_dbt_asset_selection([dbt_gold_assets])


def _data_engineering_selection():
    from ddd_python.ddd_dagster.dbt_assets import dbt_data_engineering_assets
    return build_dbt_asset_selection([dbt_data_engineering_assets])


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
        "Runs all dbt Bronze models for all source systems (DDD + RFAM).  "
        "Bronze models are cheap views that read raw NDJSON via "
        "``read_json_auto``.  Use ``dbt_bronze_ddd_job`` or "
        "``dbt_bronze_rfam_job`` to run a single source system."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "all",
        "layer": "bronze",
    },
)

dbt_bronze_ddd_job = define_asset_job(
    name="dbt_bronze_ddd_job",
    selection=_bronze_ddd_selection(),
    executor_def=in_process_executor,
    description=(
        "Runs dbt Bronze models for the DDD (Danish Parliament) source system "
        "only.  Selects ``bronze_ddd_*`` models."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "layer": "bronze",
    },
)

dbt_bronze_rfam_job = define_asset_job(
    name="dbt_bronze_rfam_job",
    selection=_bronze_rfam_selection(),
    executor_def=in_process_executor,
    description=(
        "Runs dbt Bronze models for the RFAM source system only.  "
        "Selects ``bronze_rfam_*`` models."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "RFAM",
        "layer": "bronze",
    },
)

dbt_silver_job = define_asset_job(
    name="dbt_silver_job",
    selection=_silver_selection(),
    executor_def=in_process_executor,
    config=_DBT_SILVER_DEFAULT_CONFIG,
    description=(
        "Runs all dbt Silver models for all source systems (DDD + RFAM).  "
        "Set ``full_refresh: true`` in the Launchpad to rebuild all tables from scratch.  "
        "Use ``dbt_silver_ddd_job`` or ``dbt_silver_rfam_job`` to run a "
        "single source system."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "all",
        "layer": "silver",
    },
)

dbt_silver_ddd_job = define_asset_job(
    name="dbt_silver_ddd_job",
    selection=_silver_ddd_selection(),
    executor_def=in_process_executor,
    config=_DBT_SILVER_DEFAULT_CONFIG,
    description=(
        "Runs dbt Silver models for the DDD (Danish Parliament) source system "
        "only.  Selects ``silver_ddd_*`` models.  "
        "Set ``full_refresh: true`` in the Launchpad to rebuild from scratch."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "DDD",
        "layer": "silver",
    },
)

dbt_silver_rfam_job = define_asset_job(
    name="dbt_silver_rfam_job",
    selection=_silver_rfam_selection(),
    executor_def=in_process_executor,
    config=_DBT_SILVER_DEFAULT_CONFIG,
    description=(
        "Runs dbt Silver models for the RFAM source system only.  "
        "Selects ``silver_rfam_*`` models.  "
        "Set ``full_refresh: true`` in the Launchpad to rebuild from scratch."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "RFAM",
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


dbt_data_engineering_job = define_asset_job(
    name="dbt_data_engineering_job",
    selection=_data_engineering_selection(),
    executor_def=in_process_executor,
    description=(
        "Refreshes the Data Engineering observability layer in DuckDB.  "
        "Rebuilds all models that read from Dagster's own SQLite databases: "
        "staging (dagster_pipeline_runs, dagster_event_logs), "
        "dimensions (dagster_job, dagster_asset), "
        "facts (dagster_run, dagster_asset_materialization), and "
        "failure tracking (dagster_step_failures_raw, dagster_step_failure).  "
        "Run this job after any pipeline run to update the observability layer."
    ),
    tags={
        "team": "data-engineering",
        "layer": "data_engineering",
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
    return (
        AssetSelection.groups(
            "ingestion_DDD_incremental", "ingestion_DDD_full_extract",
            "ingestion_RFAM_incremental", "ingestion_RFAM_full_extract",
        )
        | _bronze_selection()
        | _silver_selection()
        | _gold_selection()
        | AssetSelection.groups("export_silver", "export_gold")
        | _data_engineering_selection()
    )


full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=_full_pipeline_selection(),
    executor_def=in_process_executor,
    config={"ops": {**_DDD_INCREMENTAL_CONFIG["ops"], **_RFAM_INCREMENTAL_CONFIG["ops"], **_DBT_SILVER_DEFAULT_CONFIG["ops"]}},
    description=(
        "End-to-end pipeline: extracts all 18 Danish Parliament resources and "
        "all 7 Rfam tables via dlt, runs dbt Bronze → Silver → Gold, exports "
        "Silver and Gold to OneLake as Delta Lake, then refreshes the Data "
        "Engineering observability layer.  All steps run sequentially "
        "(in_process_executor) to respect DuckDB's single-writer constraint: "
        "ingestion → Bronze → Silver → Gold → export Silver → export Gold → "
        "data_engineering."
    ),
    tags={
        "team": "data-engineering",
        "source_system": "all",
        "load_mode": "full_pipeline",
    },
)
