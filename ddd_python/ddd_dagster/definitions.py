"""Dagster top-level Definitions for the Danish Democracy Data (DDD) project.

This module is the single entry point recognised by the Dagster CLI and daemon.
It assembles every asset, job, schedule, sensor, and resource into one
:class:`dagster.Definitions` object (``defs``).

Usage
-----
Start the Dagster UI locally::

    dagster dev -f ddd_python/ddd_dagster/definitions.py

Or via the workspace file at the repository root::

    dagster dev -w workspace.yaml

The workspace.yaml approach is preferred for deployed environments so the
working directory and module path are pinned explicitly.

Architecture summary
--------------------

.. code-block:: text

    ┌─────────────────────────────────────────────────────────────┐
    │  Schedule (disabled by default)                             │
    │  └── 06:00 UTC daily → full_pipeline_job                    │
    │      Layer ordering enforced by asset dependencies          │
    └───────────────────────┬─────────────────────────────────────┘
                            │ triggers
    ┌───────────────────────▼─────────────────────────────────────┐
    │  dlt Extraction jobs  (multiprocess, max_concurrent=4)      │
    │  DDD:                                                       │
    │  ├── danish_parliament_incremental_job  (6 assets)          │
    │  ├── danish_parliament_full_extract_job (12 assets)         │
    │  └── danish_parliament_all_job          (18 assets)         │
    │  Rfam:                                                      │
    │  ├── rfam_incremental_job              (2 assets)           │
    │  ├── rfam_full_extract_job             (5 assets)           │
    │  └── rfam_all_job                      (7 assets)           │
    │  Output: NDJSON files on OneLake Bronze via DltOneLakeResource│
    └───────────────────────┬─────────────────────────────────────┘
                            │ lineage (DddDbtTranslator)
    ┌───────────────────────▼─────────────────────────────────────┐
    │  dbt Bronze models    (group: ddd_bronze)                   │
    │  Views that read the OneLake JSON via DuckDB read_json_auto  │
    └───────────────────────┬─────────────────────────────────────┘
                            │
    ┌───────────────────────▼─────────────────────────────────────┐
    │  dbt Silver models    (group: ddd_silver)                   │
    │  Incremental CDC tables + current-version views             │
    └──────────┬────────────────────────────────────┬─────────────┘
               │                                    │
    ┌──────────▼──────────────────────┐  ┌──────────▼─────────────┐
    │  export_silver_job (18 assets)  │  │  dbt Gold models       │
    │  DuckDB Silver → OneLake Delta  │  │  (group: ddd_gold)     │
    │  Incremental append             │  │  Star-schema views     │
    └─────────────────────────────────┘  └──────────┬─────────────┘
                                                    │
                                         ┌──────────▼─────────────┐
                                         │  export_gold_job       │
                                         │  (9 assets)            │
                                         │  DuckDB Gold → OneLake │
                                         │  Delta (full overwrite)│
                                         └────────────────────────┘
                            │ on run completion/failure
    ┌───────────────────────▼─────────────────────────────────────┐
    │  Sensors                                                    │
    │  ├── success sensor → writes run summary to OneLake         │
    │  └── failure sensor → writes run summary to OneLake         │
    └─────────────────────────────────────────────────────────────┘

Resources
---------
``dlt_onelake``
    :class:`~ddd_python.ddd_dagster.resources.DltOneLakeResource` — injected
    into every extraction asset.  Authentication against OneLake is handled
    transparently by the underlying ``ClientSecretCredential`` constructed from
    environment variables (``AZURE_TENANT_ID``,
    ``AZURE_CLIENT_ID``,
    ``AZURE_CLIENT_SECRET``).

``dbt``
    :class:`dagster_dbt.DbtCliResource` — injected into the dbt asset functions.
    Points at the ``dbt/`` subdirectory; ``profiles_dir`` is set to the same
    directory so that ``profiles.yml`` is found at runtime.  The local DuckDB
    path is read from the environment by dbt itself.
"""

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

from dagster import Definitions
from dagster_dbt import DbtCliResource

from ddd_python.ddd_dagster.assets import all_extraction_assets
from ddd_python.ddd_dagster.rfam_assets import all_rfam_extraction_assets
from ddd_python.ddd_dagster.dbt_assets import _DBT_PROJECT_DIR, dbt_bronze_assets, dbt_gold_assets, dbt_seeds_assets, dbt_silver_assets
from ddd_python.ddd_dagster.export_assets import all_export_assets
from ddd_python.ddd_dagster.jobs import (
    danish_parliament_all_job,
    danish_parliament_full_extract_job,
    full_pipeline_job,
    danish_parliament_incremental_job,
    dbt_bronze_ddd_job,
    dbt_bronze_job,
    dbt_bronze_rfam_job,
    dbt_gold_job,
    dbt_seeds_job,
    dbt_silver_ddd_job,
    dbt_silver_job,
    dbt_silver_rfam_job,
    export_gold_job,
    export_silver_job,
    rfam_all_job,
    rfam_full_extract_job,
    rfam_incremental_job,
)
from ddd_python.ddd_dagster.resources import DltOneLakeResource
from ddd_python.ddd_dagster.schedules import danish_parliament_full_pipeline_schedule
from ddd_python.ddd_dagster.sensors import (
    danish_parliament_run_failure_sensor,
    danish_parliament_run_success_sensor,
)

defs = Definitions(
    assets=[
        *all_extraction_assets,
        *all_rfam_extraction_assets,
        dbt_seeds_assets,
        dbt_bronze_assets,
        dbt_silver_assets,
        dbt_gold_assets,
        *all_export_assets,
    ],
    jobs=[
        # dlt extraction jobs
        danish_parliament_incremental_job,
        danish_parliament_full_extract_job,
        danish_parliament_all_job,
        # dbt transformation jobs (seeds + bronze → silver → gold)
        dbt_seeds_job,
        dbt_bronze_job,
        dbt_bronze_ddd_job,
        dbt_bronze_rfam_job,
        dbt_silver_job,
        dbt_silver_ddd_job,
        dbt_silver_rfam_job,
        dbt_gold_job,
        # Delta Lake export jobs (DuckDB → OneLake)
        export_silver_job,
        export_gold_job,
        # end-to-end pipeline (extraction → dbt → export)
        full_pipeline_job,
        # Rfam extraction jobs
        rfam_incremental_job,
        rfam_full_extract_job,
        rfam_all_job,
    ],
    schedules=[
        danish_parliament_full_pipeline_schedule,
    ],
    sensors=[
        danish_parliament_run_success_sensor,
        danish_parliament_run_failure_sensor,
    ],
    resources={
        # Injected into dlt extraction assets
        "dlt_onelake": DltOneLakeResource(),
        # Injected into dbt_silver_assets and dbt_gold_assets
        "dbt": DbtCliResource(
            project_dir=_DBT_PROJECT_DIR,
            profiles_dir=_DBT_PROJECT_DIR,
        ),
    },
)
