"""Dagster assets for exporting Silver and Gold tables as Delta Lake tables (local, S3, or OneLake).

Each Silver and Gold table in DuckDB is represented as a single
Dagster ``@asset`` that writes the corresponding Delta Lake table to the configured destination (local, S3, or OneLake).

Execution order enforced by two ordering barriers:

    dbt Gold (all models)
        ↓
    barrier_dbt_gold_complete   ← no-op; gates Silver exports
        ↓
    export Silver (all 25 tables)
        ↓
    barrier_all_silver_exported ← no-op; gates Gold exports
        ↓
    export Gold (all 10 tables)

* **export/silver** — incremental append to Silver Delta tables.
  Each asset depends on its corresponding dbt Silver model and on
  ``barrier_dbt_gold_complete``.

* **export/gold** — full overwrite of Gold Delta tables.
  Each asset depends on its corresponding dbt Gold model and on
  ``barrier_all_silver_exported``.

The factory pattern mirrors ``assets.py`` (extraction assets) to keep the
definitions DRY across 25 Silver tables (18 DDD + 7 Rfam) and 10 Gold tables.
"""

from datetime import UTC, datetime

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ddd_python.ddd_dagster._constants import _RETRY_POLICY
from ddd_python.ddd_dlt.export_gold import export_single_gold_table
from ddd_python.ddd_dlt.export_silver import export_single_silver_table
from ddd_python.ddd_utils import configuration_variables
from ddd_python.ddd_utils.path_utils import open_export_connection

_STOP_METABASE_KEY = AssetKey(["stop_metabase_asset"])

# All Silver table names (DDD + Rfam) — used to build barrier deps.
_ALL_SILVER_TABLES: list[str] = (
    configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER
    + configuration_variables.RFAM_MODELS_SILVER
)


# ---------------------------------------------------------------------------
# Barrier 1: wait for dbt Gold to finish before starting Silver exports
# ---------------------------------------------------------------------------


@asset(
    name="barrier_dbt_gold_complete",
    key_prefix=["export"],
    group_name="export_silver",
    deps=[_STOP_METABASE_KEY]
    + [AssetKey(["gold", name]) for name in configuration_variables.DANISH_DEMOCRACY_MODELS_GOLD],
    description=(
        "Ordering barrier: depends on every dbt Gold model.  "
        "Silver exports depend on this asset so that dbt Gold completes "
        "before any Silver export starts.  No data is written."
    ),
)
def barrier_dbt_gold_complete() -> None:
    pass


# ---------------------------------------------------------------------------
# Silver export asset factory
# ---------------------------------------------------------------------------


def _make_export_silver_asset(table_name: str) -> AssetsDefinition:
    """Return a Dagster asset that exports one Silver table as a Delta Lake table."""

    @asset(
        name=f"export_{table_name}",
        key_prefix=["export", "silver"],
        group_name="export_silver",
        deps=[
            _STOP_METABASE_KEY,
            AssetKey(["silver", table_name]),
            AssetKey(["export", "barrier_dbt_gold_complete"]),
        ],
        retry_policy=_RETRY_POLICY,
        description=(
            f"Export **{table_name}** from DuckDB Silver as a "
            "Delta Lake table (incremental append)."
        ),
        metadata={
            "table": MetadataValue.text(table_name),
            "layer": MetadataValue.text("silver"),
            "mode": MetadataValue.text("incremental_append"),
        },
    )
    def _export_silver(context: AssetExecutionContext) -> MaterializeResult:
        logger = context.log
        logger.info("START Silver export — table=%s", table_name)

        ts = datetime.now(UTC)
        # In DuckLake mode this attaches the DuckLake catalog (read-only) so the
        # Silver tables in ducklake_catalog.main_silver are readable.
        connection = open_export_connection()
        try:
            rows = export_single_silver_table(connection, table_name)
        finally:
            connection.close()

        duration = (datetime.now(UTC) - ts).total_seconds()
        logger.info(
            "END Silver export — table=%s  rows=%d  duration_s=%.1f",
            table_name,
            rows,
            duration,
        )

        return MaterializeResult(
            metadata={
                "rows_written": MetadataValue.int(rows),
                "duration_seconds": MetadataValue.float(round(duration, 2)),
                "table": MetadataValue.text(table_name),
            }
        )

    return _export_silver


# ---------------------------------------------------------------------------
# Barrier 2: wait for all Silver exports before starting Gold exports
# ---------------------------------------------------------------------------


@asset(
    name="barrier_all_silver_exported",
    key_prefix=["export"],
    group_name="export_silver",
    deps=[_STOP_METABASE_KEY]
    + [AssetKey(["export", "silver", f"export_{name}"]) for name in _ALL_SILVER_TABLES],
    description=(
        "Ordering barrier: depends on every Silver export asset.  "
        "Gold exports depend on this asset so that all Silver exports "
        "complete before any Gold export starts.  No data is written."
    ),
)
def barrier_all_silver_exported() -> None:
    pass


# ---------------------------------------------------------------------------
# Gold export asset factory
# ---------------------------------------------------------------------------


def _make_export_gold_asset(table_name: str) -> AssetsDefinition:
    """Return a Dagster asset that exports one Gold table as a Delta Lake table."""

    @asset(
        name=f"export_{table_name}",
        key_prefix=["export", "gold"],
        group_name="export_gold",
        deps=[
            _STOP_METABASE_KEY,
            AssetKey(["gold", table_name]),
            AssetKey(["export", "barrier_all_silver_exported"]),
        ],
        retry_policy=_RETRY_POLICY,
        description=(
            f"Export **{table_name}** from DuckDB Gold as a Delta Lake table (full overwrite)."
        ),
        metadata={
            "table": MetadataValue.text(table_name),
            "layer": MetadataValue.text("gold"),
            "mode": MetadataValue.text("overwrite"),
        },
    )
    def _export_gold(context: AssetExecutionContext) -> MaterializeResult:
        logger = context.log
        logger.info("START Gold export — table=%s", table_name)

        ts = datetime.now(UTC)
        # In DuckLake mode this attaches the DuckLake catalog (read-only) so the
        # Gold views — which reference ducklake_catalog.main_silver — resolve.
        connection = open_export_connection()
        try:
            rows = export_single_gold_table(connection, table_name)
        finally:
            connection.close()

        duration = (datetime.now(UTC) - ts).total_seconds()
        logger.info(
            "END Gold export — table=%s  rows=%d  duration_s=%.1f",
            table_name,
            rows,
            duration,
        )

        return MaterializeResult(
            metadata={
                "rows_written": MetadataValue.int(rows),
                "duration_seconds": MetadataValue.float(round(duration, 2)),
                "table": MetadataValue.text(table_name),
            }
        )

    return _export_gold


# ---------------------------------------------------------------------------
# Materialise asset lists from configuration
# ---------------------------------------------------------------------------

#: Silver export assets — one per table in DANISH_DEMOCRACY_MODELS_SILVER + RFAM_MODELS_SILVER.
export_silver_assets: list[AssetsDefinition] = [
    _make_export_silver_asset(name) for name in _ALL_SILVER_TABLES
]

#: Gold export assets — one per table in DANISH_DEMOCRACY_MODELS_GOLD.
export_gold_assets: list[AssetsDefinition] = [
    _make_export_gold_asset(name) for name in configuration_variables.DANISH_DEMOCRACY_MODELS_GOLD
]

# ---------------------------------------------------------------------------
# Barrier 3: wait for all Gold exports before starting Data Engineering refresh
# ---------------------------------------------------------------------------


@asset(
    name="barrier_all_gold_exported",
    key_prefix=["export"],
    group_name="export_gold",
    deps=[_STOP_METABASE_KEY]
    + [
        AssetKey(["export", "gold", f"export_{name}"])
        for name in configuration_variables.DANISH_DEMOCRACY_MODELS_GOLD
    ],
    description=(
        "Ordering barrier: depends on every Gold export asset.  "
        "The Data Engineering observability layer depends on this asset so that "
        "all Gold exports complete before the observability models are rebuilt.  "
        "No data is written."
    ),
)
def barrier_all_gold_exported() -> None:
    pass


#: Combined list passed to ``Definitions(assets=...)``.
all_export_assets: list[AssetsDefinition] = (
    [barrier_dbt_gold_complete]
    + export_silver_assets
    + [barrier_all_silver_exported]
    + export_gold_assets
    + [barrier_all_gold_exported]
)
