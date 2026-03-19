"""Dagster software-defined assets for Danish Parliament data extraction.

Each of the 18 Danish Parliament OData API resources is represented as a
single Dagster ``@asset``.  Assets are split into two groups:

* **ingestion/DDD** — resources that support OData date filtering via
  ``opdateringsdato``.  One file is written per resource per run, exactly
  as the original script did.  The lower-bound date is controlled by the
  ``date_to_load_from`` run-config field (defaults to
  ``today - DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD`` days).

* **ingestion/DDD** — resources that are always fully extracted on every run.
  The API does support ``opdateringsdato`` filtering for these entities, but
  they are small tables and a full extraction on every run keeps delete
  detection simple — no date config is applied.

Design notes
------------
* No daily partitions — one job run produces exactly one file per resource,
  matching the behaviour of the original script.

* ``date_to_load_from`` is exposed as a Dagster ``Config`` field so a
  specific historical date can be supplied from the UI or CLI without any
  code changes (useful for ad-hoc backfills).

* Assets are created with a **factory pattern** so the 18 definitions stay
  DRY.  Each factory call closes over the ``api_resource`` name, producing
  a distinct :class:`dagster.AssetsDefinition` with the correct key.

* All extraction logic delegates to :class:`~.resources.DltOneLakeResource`,
  which wraps ``dlt_pipeline_execution_functions.execute_pipeline()``.
  Per-pipeline NDJSON run logs are written to OneLake **inside** that
  function; assets additionally emit structured metadata visible in the
  Dagster UI via ``context.add_output_metadata()``.

* A :class:`dagster.RetryPolicy` with exponential backoff retries transient
  API / network failures up to two times before marking an asset as failed.

* Concurrent execution is managed by the Dagster multiprocess executor
  configured in ``jobs.py`` (``max_concurrent=4``, mirroring the original
  ``ThreadPoolExecutor(max_workers=4)``).
"""

from datetime import datetime, timedelta, timezone

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


class ExtractionConfig(Config):
    """Per-run configuration for extraction assets.

    Attributes:
        date_to_load_from: Lower-bound date for incremental extraction in
            ``YYYY-MM-DD`` format.  Leave ``None`` (default) to use
            ``today - DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD`` days, which
            reproduces the behaviour of the original script.  Ignored by
            full-extract assets.
    """

    date_to_load_from: str | None = None

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

_SOURCE_SYSTEM_CODE = "DDD"
_PIPELINE_TYPE = "api_to_file"

_INCREMENTAL_NAMES: frozenset[str] = frozenset(
    configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL
)

# Two retries with exponential back-off (60 s → 120 s) for transient API /
# OneLake network failures.
_RETRY_POLICY = RetryPolicy(
    max_retries=2,
    delay=60,
    backoff=Backoff.EXPONENTIAL,
)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _base_name(api_resource: str) -> str:
    """Normalise an API resource name to a safe, lowercased file-system identifier.

    Replaces Danish characters that are unsupported by DuckDB / OneLake paths.
    """
    return (
        api_resource
        .replace("ø", "oe")
        .replace("æ", "ae")
        .replace("å", "aa")
        .lower()
    )


def _destination_path(base: str) -> str:
    """Build the Bronze directory path for a given normalised resource name."""
    if get_variables_from_env.STORAGE_TARGET == "local":
        return f"Files/Bronze/{_SOURCE_SYSTEM_CODE}/{base}"
    return (
        f"{get_variables_from_env.FABRIC_ONELAKE_FOLDER_BRONZE}"
        f"/{_SOURCE_SYSTEM_CODE}/{base}"
    )


# ---------------------------------------------------------------------------
# Asset factories
# ---------------------------------------------------------------------------


def _make_incremental_asset(api_resource: str) -> AssetsDefinition:
    """Return a non-partitioned ``@asset`` for an incremental OData resource.

    One file is written per resource per run, exactly matching the original
    script.  The extraction lower-bound date is taken from the
    ``date_to_load_from`` run-config field; when blank, it defaults to
    ``today - DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD`` days.

    For an ad-hoc backfill, supply ``date_to_load_from`` in the Dagster UI
    (Launchpad → config) or via the CLI
    ``--config '{"ops":{"<asset_name>":{"config":{"date_to_load_from":"2026-01-01"}}}}'``.

    Args:
        api_resource: Exact OData entity name as returned by the API
            (e.g. ``"Aktør"``, ``"Møde"``).  Danish characters are accepted.

    Returns:
        A configured :class:`dagster.AssetsDefinition`.
    """
    base = _base_name(api_resource)

    @asset(
        name=base,
        key_prefix=["ingestion", "DDD"],
        group_name="ingestion_DDD_incremental",
        retry_policy=_RETRY_POLICY,
        description=(
            f"Incremental extraction of **{api_resource}** from the Danish "
            "Parliament OData API into OneLake Bronze.  "
            "One file per run.  Date lower-bound set via run config "
            "(defaults to today minus DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD)."
        ),
        metadata={
            "api_resource": MetadataValue.text(api_resource),
            "load_mode": MetadataValue.text("incremental"),
            "odata_filter_field": MetadataValue.text("opdateringsdato"),
            "source_base_url": MetadataValue.url(
                get_variables_from_env.DANISH_DEMOCRACY_BASE_URL or ""
            ),
        },
    )
    def _incremental_asset(
        context: AssetExecutionContext,
        config: ExtractionConfig,
        dlt_onelake: DltOneLakeResource,
    ) -> MaterializeResult:
        logger = context.log

        # Resolve date_to_load_from — mirrors the original script's logic exactly.
        if config.date_to_load_from is not None:
            date_from = config.date_to_load_from
        else:
            default_days = get_variables_from_env.DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD
            date_from = f"{datetime.now(timezone.utc) - timedelta(days=default_days):%Y-%m-%d}"

        logger.info(
            "START incremental extraction — resource=%s  date_from=%s",
            api_resource,
            date_from,
        )

        ts = datetime.now(timezone.utc)
        destination_file = f"{base}_{ts:%Y%m%d_%H%M%S}.json"
        api_filter = f"$filter=opdateringsdato ge DateTime'{date_from}'&$orderby=id"

        result = dlt_onelake.execute_pipeline(
            pipeline_type=_PIPELINE_TYPE,
            pipeline_name=base,
            source_api_base_url=get_variables_from_env.DANISH_DEMOCRACY_BASE_URL,
            source_api_resource=api_resource,
            source_api_filter=api_filter,
            source_api_date_to_load_from=date_from,
            destination_directory_path=_destination_path(base),
            destination_file_name=destination_file,
        )

        duration = (datetime.now(timezone.utc) - ts).total_seconds()
        records = result.get("records_written") or 0

        logger.info(
            "END incremental extraction — resource=%s  date_from=%s  records=%d  duration_s=%.1f",
            api_resource,
            date_from,
            records,
            duration,
        )

        return MaterializeResult(
            metadata={
                "records_written": MetadataValue.int(records),
                "destination_file": MetadataValue.text(destination_file),
                "destination_path": MetadataValue.text(_destination_path(base)),
                "date_from": MetadataValue.text(date_from),
                "duration_seconds": MetadataValue.float(round(duration, 2)),
                "pipeline_status": MetadataValue.text(result.get("status", "unknown")),
            }
        )

    return _incremental_asset


def _make_full_extract_asset(api_resource: str) -> AssetsDefinition:
    """Return a non-partitioned ``@asset`` for a full-extract OData resource.

    Every run fetches all records (``$inlinecount=allpages``).  The API does
    support ``opdateringsdato`` filtering for these entities, but they are small
    tables so a full extraction is simpler and makes delete detection
    straightforward — no cursor state to manage.

    Args:
        api_resource: Exact OData entity name (e.g. ``"Afstemning"``).

    Returns:
        A configured :class:`dagster.AssetsDefinition`.
    """
    base = _base_name(api_resource)

    @asset(
        name=base,
        key_prefix=["ingestion", "DDD"],
        group_name="ingestion_DDD_full_extract",
        retry_policy=_RETRY_POLICY,
        description=(
            f"Full extraction of **{api_resource}** from the Danish Parliament "
            "OData API into Bronze storage.  "
            "Fetches all records on every run — the table is small and a full "
            "extract keeps delete detection simple."
        ),
        metadata={
            "api_resource": MetadataValue.text(api_resource),
            "load_mode": MetadataValue.text("full_extract"),
            "source_base_url": MetadataValue.url(
                get_variables_from_env.DANISH_DEMOCRACY_BASE_URL or ""
            ),
        },
    )
    def _full_extract_asset(
        context: AssetExecutionContext,
        dlt_onelake: DltOneLakeResource,
    ) -> MaterializeResult:
        logger = context.log
        logger.info("START full extraction — resource=%s", api_resource)

        ts = datetime.now(timezone.utc)
        destination_file = f"{base}_{ts:%Y%m%d_%H%M%S}.json"
        today = f"{ts:%Y-%m-%d}"

        result = dlt_onelake.execute_pipeline(
            pipeline_type=_PIPELINE_TYPE,
            pipeline_name=base,
            source_api_base_url=get_variables_from_env.DANISH_DEMOCRACY_BASE_URL,
            source_api_resource=api_resource,
            source_api_filter="$inlinecount=allpages&$orderby=id",
            source_api_date_to_load_from=today,
            destination_directory_path=_destination_path(base),
            destination_file_name=destination_file,
        )

        duration = (datetime.now(timezone.utc) - ts).total_seconds()
        records = result.get("records_written") or 0

        logger.info(
            "END full extraction — resource=%s  records=%d  duration_s=%.1f",
            api_resource,
            records,
            duration,
        )

        return MaterializeResult(
            metadata={
                "records_written": MetadataValue.int(records),
                "destination_file": MetadataValue.text(destination_file),
                "destination_path": MetadataValue.text(_destination_path(base)),
                "run_date": MetadataValue.text(today),
                "duration_seconds": MetadataValue.float(round(duration, 2)),
                "pipeline_status": MetadataValue.text(result.get("status", "unknown")),
            }
        )

    return _full_extract_asset


# ---------------------------------------------------------------------------
# Materialise asset lists from configuration
# ---------------------------------------------------------------------------

#: Incremental assets — one per resource in DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL.
incremental_assets: list[AssetsDefinition] = [
    _make_incremental_asset(name)
    for name in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES
    if name in _INCREMENTAL_NAMES
]

#: Full-extract assets — every resource NOT in the incremental set.
full_extract_assets: list[AssetsDefinition] = [
    _make_full_extract_asset(name)
    for name in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES
    if name not in _INCREMENTAL_NAMES
]

#: Combined list passed to ``Definitions(assets=...)``.
all_extraction_assets: list[AssetsDefinition] = incremental_assets + full_extract_assets
