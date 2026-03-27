"""Dagster software-defined assets for Rfam MySQL database extraction.

Each of the 7 Rfam tables is represented as a single Dagster ``@asset``.
Assets are split into two groups:

* **ingestion_RFAM_incremental** — ``family`` and ``genome`` support date
  filtering via the ``updated`` timestamp column.
* **ingestion_RFAM_full_extract** — the remaining 5 small tables are fetched
  in full on every run.

The factory pattern mirrors ``assets.py`` (DDD) so the two source systems
are architecturally consistent.
"""

from datetime import datetime, timedelta, timezone
import re

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Config,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    Backoff,
    asset,
)

from ddd_python.ddd_utils import configuration_variables, get_variables_from_env
from ddd_python.ddd_dagster.resources import DltOneLakeResource


class RfamExtractionConfig(Config):
    """Per-run configuration for Rfam extraction assets.

    Attributes:
        date_to_load_from: Lower-bound date for incremental extraction in
            ``YYYY-MM-DD`` format.  Leave ``None`` (default) to use
            ``today - RFAM_DEFAULT_DAYS_TO_LOAD`` days.
    """

    date_to_load_from: str | None = None


_SOURCE_SYSTEM_CODE = "RFAM"
_PIPELINE_TYPE = "sql_to_file"

_INCREMENTAL_NAMES: frozenset[str] = frozenset(
    configuration_variables.RFAM_TABLE_NAMES_INCREMENTAL
)

_RETRY_POLICY = RetryPolicy(
    max_retries=2,
    delay=60,
    backoff=Backoff.EXPONENTIAL,
)


def _destination_path(table_name: str) -> str:
    """Build the Bronze directory path for a given Rfam table name."""
    if get_variables_from_env.STORAGE_TARGET == "local":
        return f"Files/Bronze/{_SOURCE_SYSTEM_CODE}/{table_name}"
    return (
        f"{get_variables_from_env.FABRIC_ONELAKE_FOLDER_BRONZE}"
        f"/{_SOURCE_SYSTEM_CODE}/{table_name}"
    )


def _make_incremental_asset(table_name: str) -> AssetsDefinition:
    """Return a ``@asset`` for an incrementally-extracted Rfam table."""
    query_template = configuration_variables.RFAM_TABLE_QUERIES[table_name]

    @asset(
        name=table_name,
        key_prefix=["ingestion", "RFAM"],
        group_name="ingestion_RFAM_incremental",
        retry_policy=_RETRY_POLICY,
        description=(
            f"Incremental extraction of **{table_name}** from the Rfam MySQL "
            "database into Bronze storage.  Filters by ``updated >= date``."
        ),
        metadata={
            "table_name": MetadataValue.text(table_name),
            "load_mode": MetadataValue.text("incremental"),
            "incremental_field": MetadataValue.text("updated"),
        },
    )
    def _incremental_asset(
        context: AssetExecutionContext,
        config: RfamExtractionConfig,
        dlt_onelake: DltOneLakeResource,
    ) -> MaterializeResult:
        logger = context.log

        if config.date_to_load_from is not None:
            date_from = config.date_to_load_from
        else:
            default_days = get_variables_from_env.RFAM_DEFAULT_DAYS_TO_LOAD
            date_from = f"{datetime.now(timezone.utc) - timedelta(days=default_days):%Y-%m-%d}"

        logger.info(
            "START incremental extraction — table=%s  date_from=%s",
            table_name, date_from,
        )

        ts = datetime.now(timezone.utc)
        destination_file = f"{table_name}_{ts:%Y%m%d_%H%M%S}.json"
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_from):
            raise ValueError(f"Unsafe date value: {date_from!r}")
        sql_query = query_template.format(
            where_clause=f" WHERE updated >= '{date_from}'"
        )

        result = dlt_onelake.execute_pipeline(
            pipeline_type=_PIPELINE_TYPE,
            source_system_code=_SOURCE_SYSTEM_CODE,
            pipeline_name=table_name,
            source_connection_string=get_variables_from_env.RFAM_CONNECTION_STRING,
            source_sql_query=sql_query,
            destination_directory_path=_destination_path(table_name),
            destination_file_name=destination_file,
            loader_file_format="jsonl",
        )

        duration = (datetime.now(timezone.utc) - ts).total_seconds()
        records = result.get("records_written") or 0

        logger.info(
            "END incremental extraction — table=%s  date_from=%s  records=%d  duration_s=%.1f",
            table_name, date_from, records, duration,
        )

        return MaterializeResult(
            metadata={
                "records_written": MetadataValue.int(records),
                "destination_file": MetadataValue.text(destination_file),
                "destination_path": MetadataValue.text(_destination_path(table_name)),
                "date_from": MetadataValue.text(date_from),
                "duration_seconds": MetadataValue.float(round(duration, 2)),
                "pipeline_status": MetadataValue.text(result.get("status", "unknown")),
            }
        )

    return _incremental_asset


def _make_full_extract_asset(table_name: str) -> AssetsDefinition:
    """Return a ``@asset`` for a full-extract Rfam table."""
    query_template = configuration_variables.RFAM_TABLE_QUERIES[table_name]

    @asset(
        name=table_name,
        key_prefix=["ingestion", "RFAM"],
        group_name="ingestion_RFAM_full_extract",
        retry_policy=_RETRY_POLICY,
        description=(
            f"Full extraction of **{table_name}** from the Rfam MySQL "
            "database into Bronze storage.  Fetches all records on every run."
        ),
        metadata={
            "table_name": MetadataValue.text(table_name),
            "load_mode": MetadataValue.text("full_extract"),
        },
    )
    def _full_extract_asset(
        context: AssetExecutionContext,
        dlt_onelake: DltOneLakeResource,
    ) -> MaterializeResult:
        logger = context.log
        logger.info("START full extraction — table=%s", table_name)

        ts = datetime.now(timezone.utc)
        destination_file = f"{table_name}_{ts:%Y%m%d_%H%M%S}.json"
        sql_query = query_template.format(where_clause="")

        result = dlt_onelake.execute_pipeline(
            pipeline_type=_PIPELINE_TYPE,
            source_system_code=_SOURCE_SYSTEM_CODE,
            pipeline_name=table_name,
            source_connection_string=get_variables_from_env.RFAM_CONNECTION_STRING,
            source_sql_query=sql_query,
            destination_directory_path=_destination_path(table_name),
            destination_file_name=destination_file,
            loader_file_format="jsonl",
        )

        duration = (datetime.now(timezone.utc) - ts).total_seconds()
        records = result.get("records_written") or 0

        logger.info(
            "END full extraction — table=%s  records=%d  duration_s=%.1f",
            table_name, records, duration,
        )

        return MaterializeResult(
            metadata={
                "records_written": MetadataValue.int(records),
                "destination_file": MetadataValue.text(destination_file),
                "destination_path": MetadataValue.text(_destination_path(table_name)),
                "duration_seconds": MetadataValue.float(round(duration, 2)),
                "pipeline_status": MetadataValue.text(result.get("status", "unknown")),
            }
        )

    return _full_extract_asset


# ---------------------------------------------------------------------------
# Materialise asset lists from configuration
# ---------------------------------------------------------------------------

incremental_assets: list[AssetsDefinition] = [
    _make_incremental_asset(name)
    for name in configuration_variables.RFAM_TABLE_NAMES
    if name in _INCREMENTAL_NAMES
]

full_extract_assets: list[AssetsDefinition] = [
    _make_full_extract_asset(name)
    for name in configuration_variables.RFAM_TABLE_NAMES
    if name not in _INCREMENTAL_NAMES
]

all_rfam_extraction_assets: list[AssetsDefinition] = incremental_assets + full_extract_assets
