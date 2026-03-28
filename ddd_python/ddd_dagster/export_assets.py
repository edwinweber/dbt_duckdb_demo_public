"""Dagster assets for exporting Silver and Gold tables to Fabric OneLake as Delta Lake.

Each Silver and Gold table in DuckDB is represented as a single
Dagster ``@asset`` that writes the corresponding Delta Lake table to OneLake.

* **export/silver** — incremental append to OneLake Silver Delta tables.
  Each asset depends on its corresponding dbt Silver model, so Dagster ensures
  the dbt transformation completes before the export runs.

* **export/gold** — full overwrite of OneLake Gold Delta tables.
  Each asset depends on its corresponding dbt Gold model.

The factory pattern mirrors ``assets.py`` (extraction assets) to keep the
definitions DRY across 25 Silver tables (18 DDD + 7 Rfam) and 9 Gold tables.
"""

from datetime import datetime, timezone

import duckdb
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ddd_python.ddd_utils import configuration_variables, get_variables_from_env
from ddd_python.ddd_dagster._constants import _RETRY_POLICY
from ddd_python.ddd_dlt.export_main_silver_to_fabric_silver import export_single_silver_table
from ddd_python.ddd_dlt.export_main_gold_to_fabric_gold import export_single_gold_table


# ---------------------------------------------------------------------------
# Silver export asset factory
# ---------------------------------------------------------------------------


def _make_export_silver_asset(table_name: str) -> AssetsDefinition:
    """Return a Dagster asset that exports one Silver table to OneLake as Delta Lake.

    The asset declares a dependency on the corresponding dbt Silver model so
    that Dagster sequences the export after the transformation.
    """

    @asset(
        name=f"export_{table_name}",
        key_prefix=["export", "silver"],
        group_name="export_silver",
        deps=[AssetKey(["silver", table_name])],
        retry_policy=_RETRY_POLICY,
        description=(
            f"Export **{table_name}** from DuckDB Silver to OneLake as a "
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

        ts = datetime.now(timezone.utc)
        connection = duckdb.connect(
            get_variables_from_env.DUCKDB_DATABASE_LOCATION, read_only=True,
        )
        try:
            rows = export_single_silver_table(connection, table_name)
        finally:
            connection.close()

        duration = (datetime.now(timezone.utc) - ts).total_seconds()
        logger.info(
            "END Silver export — table=%s  rows=%d  duration_s=%.1f",
            table_name, rows, duration,
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
# Gold export asset factory
# ---------------------------------------------------------------------------


def _make_export_gold_asset(table_name: str) -> AssetsDefinition:
    """Return a Dagster asset that exports one Gold table to OneLake as Delta Lake.

    The asset declares a dependency on the corresponding dbt Gold model.
    """

    @asset(
        name=f"export_{table_name}",
        key_prefix=["export", "gold"],
        group_name="export_gold",
        deps=[AssetKey(["gold", table_name])],
        retry_policy=_RETRY_POLICY,
        description=(
            f"Export **{table_name}** from DuckDB Gold to OneLake as a "
            "Delta Lake table (full overwrite)."
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

        ts = datetime.now(timezone.utc)
        connection = duckdb.connect(
            get_variables_from_env.DUCKDB_DATABASE_LOCATION, read_only=True,
        )
        try:
            rows = export_single_gold_table(connection, table_name)
        finally:
            connection.close()

        duration = (datetime.now(timezone.utc) - ts).total_seconds()
        logger.info(
            "END Gold export — table=%s  rows=%d  duration_s=%.1f",
            table_name, rows, duration,
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
    _make_export_silver_asset(name)
    for name in (
        configuration_variables.DANISH_DEMOCRACY_MODELS_SILVER
        + configuration_variables.RFAM_MODELS_SILVER
    )
]

#: Gold export assets — one per table in DANISH_DEMOCRACY_MODELS_GOLD.
export_gold_assets: list[AssetsDefinition] = [
    _make_export_gold_asset(name)
    for name in configuration_variables.DANISH_DEMOCRACY_MODELS_GOLD
]

#: Combined list passed to ``Definitions(assets=...)``.
all_export_assets: list[AssetsDefinition] = export_silver_assets + export_gold_assets
