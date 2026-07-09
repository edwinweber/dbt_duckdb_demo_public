"""
dlt Pipeline Execution Functions.

Unified execution layer for dlt (data load tool) pipelines that extract data
from various sources and write Bronze files to local disk or S3-compatible
storage.

Three pipeline types are supported:

* **api_to_file** — Fetches paginated JSON from an OData / REST API, streaming
  individual records through dlt page by page, and writes them as NDJSON.
  dlt handles batching, schema inference, and pipeline state.
* **sql_to_file** — Executes a SQL query via SQLAlchemy, streams individual rows
  through dlt in configurable chunks, and writes the result as a single Parquet
  or NDJSON file.
* **file_to_file** — Reads an arbitrary local file and uploads the raw bytes
  as-is to storage.  dlt is not used here; it adds no value for a plain copy.
  S3 mode (``RAW_STORAGE_TARGET=s3``) is not yet implemented for this type.

Every pipeline run — successful or failed — is logged as an NDJSON record via
:func:`write_log_entry`.

Storage destination
-------------------
Two independent env vars control where data lands:

* ``RAW_STORAGE_TARGET`` (``local`` | ``s3``) — governs where Bronze extraction
  files are written by ``api_to_file`` and ``sql_to_file``.
* Pipeline run logs are always written to the local filesystem under
  ``DLT_PIPELINE_RUN_LOG_DIR/<source>/`` (default: ``LOCAL_STORAGE_PATH/logs``),
  regardless of ``STORAGE_TARGET``.

Authentication
--------------
For ``api_to_file`` and ``sql_to_file`` with ``RAW_STORAGE_TARGET=s3``,
dlt's built-in ``filesystem`` destination writes directly to S3-compatible
storage via the configured S3 credentials.  For ``file_to_file``, a plain
local file write is performed; S3 is not yet implemented for this type.

Typical usage::

    from ddd_python.ddd_dlt.dlt_pipeline_execution_functions import execute_pipeline

    execute_pipeline(
        pipeline_type="api_to_file",
        source_system_code="DDD",
        pipeline_name="Stemmetype",
        source_api_base_url="https://oda.ft.dk/api",
        source_api_resource="Stemmetype",
        source_api_filter="$inlinecount=allpages",
        source_api_date_to_load_from="2024-01-01",
        destination_directory_path="Files/Bronze/test",
        destination_file_name="stemmetype.json",
    )
"""

import concurrent.futures
import json
import logging
import os
import re
import time
import traceback
import warnings
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, TypedDict

import requests
from dlt.destinations import filesystem as dlt_filesystem
from sqlalchemy import create_engine, text

import dlt

logger = logging.getLogger(__name__)

from ddd_python.ddd_dagster._constants import MAX_CONCURRENT_WORKERS
from ddd_python.ddd_utils import get_variables_from_env
from ddd_python.ddd_utils.string_utils import normalize_danish_name


class PipelineTask(TypedDict):
    name: str
    source_system_code: str
    pipeline_type: Literal["api_to_file", "sql_to_file", "file_to_file"]
    kwargs: dict[str, Any]


# Disable gzip compression so the filesystem destination writes plain JSONL files,
# not .jsonl.gz — downstream dbt models glob for *.json / *.jsonl.
os.environ.setdefault("NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION", "true")

# Prevent dlt from normalizing the dataset_name (which we use as a directory path
# containing dots and slashes, e.g. "<YOUR_LAKEHOUSE>.Lakehouse/Files/Bronze/DDD").
os.environ.setdefault("DESTINATION__FILESYSTEM__ENABLE_DATASET_NAME_NORMALIZATION", "false")

# Maximum concurrent pipeline tasks in ThreadPoolExecutor (extraction pool).
# Imported from ddd_dagster._constants — single source of truth shared with
# the Dagster multiprocess_executor in jobs.py.
_MAX_PIPELINE_WORKERS = MAX_CONCURRENT_WORKERS

# Keys whose values are redacted in run logs to prevent credential leakage.
_SENSITIVE_KEYS = frozenset({"connection_string", "secret", "password", "token"})

# Truncate fractional seconds to microsecond precision when API timestamps carry
# more than 6 decimal digits (e.g. 2025-01-01T12:34:56.1234567).
_TS_MICROSEC = re.compile(r"(\.\d{6})\d+")


def _scrub_secrets(params: dict) -> dict:
    """Return a copy of *params* with values of sensitive keys replaced by '***'."""
    return {
        k: "***" if any(s in k.lower() for s in _SENSITIVE_KEYS) else v for k, v in params.items()
    }


def _json_default(obj: Any) -> str:
    """JSON serialization fallback for types not handled by the standard encoder.

    dlt normalises source data using its own type system (e.g. ``pendulum.DateTime``
    for date/time fields).  These types are not natively JSON-serialisable, so
    this function is passed as the ``default=`` argument to every ``json.dumps``
    call in this module.

    Args:
        obj: The non-serialisable object encountered by ``json.dumps``.

    Returns:
        An ISO-8601 string if *obj* has an ``isoformat`` method (covers Python
        ``datetime``, ``date``, ``time``, and ``pendulum.DateTime``), otherwise
        ``str(obj)``.
    """
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _upload_to_local(data: bytes | str, directory_path: str, file_name: str) -> None:
    """Write data to a local file, overwriting any existing content."""
    Path(directory_path).mkdir(parents=True, exist_ok=True)
    file_path = Path(directory_path) / file_name
    mode = "wb" if isinstance(data, bytes) else "w"
    with open(file_path, mode) as f:
        f.write(data)


def _upload(data: bytes | str, directory_path: str, file_name: str) -> None:
    """Upload data to local or S3 storage (file_to_file; S3 not yet implemented)."""
    raw = get_variables_from_env.RAW_STORAGE_TARGET
    if raw == "local":
        _upload_to_local(data, directory_path, file_name)
    elif raw == "s3":
        raise NotImplementedError(
            "run_file_to_file_pipeline does not yet support RAW_STORAGE_TARGET=s3. "
            "Implement _upload_to_s3() using boto3 or the dlt filesystem destination."
        )
    else:
        raise AssertionError(
            f"Unreachable: RAW_STORAGE_TARGET={raw!r} should have been caught at import"
        )


def _make_destination(
    directory_path: str,
    file_name: str,
    data_table_name: str,
) -> tuple[Any, str]:
    """Build a dlt filesystem destination for either local or S3 storage.

    When ``RAW_STORAGE_TARGET=local``, *directory_path* is treated as a path
    relative to ``LOCAL_STORAGE_PATH`` (e.g. ``"bronze/DDD/afstemning"``).
    When ``RAW_STORAGE_TARGET=s3``, *directory_path* is part of the S3 key path
    under the configured bucket.

    Returns:
        A tuple of ``(destination, dataset_name)`` to pass to ``dlt.pipeline``.
    """
    parent_path = str(Path(directory_path).parent)

    def _resolve_path(
        schema_name: str,
        table_name: str,
        load_id: str,
        file_id: str,
        ext: str,
    ) -> str:
        if table_name == data_table_name:
            return file_name
        return f"{table_name}.{file_id}.{ext}"

    if get_variables_from_env.RAW_STORAGE_TARGET == "s3":
        bucket_url = f"s3://{get_variables_from_env.S3_BUCKET_BRONZE}"
        if get_variables_from_env.S3_PREFIX_BRONZE:
            bucket_url = f"{bucket_url}/{get_variables_from_env.S3_PREFIX_BRONZE.strip('/')}"
        destination = dlt_filesystem(
            bucket_url=bucket_url,
            layout="{table_name}/{_resolve_path}",
            extra_placeholders={"_resolve_path": _resolve_path},
            credentials={
                "aws_access_key_id": get_variables_from_env.S3_ACCESS_KEY_ID,
                "aws_secret_access_key": get_variables_from_env.S3_SECRET_ACCESS_KEY,
                "endpoint_url": get_variables_from_env.S3_ENDPOINT or None,
                "region_name": get_variables_from_env.S3_REGION,
            },
        )
    else:
        destination = dlt_filesystem(
            bucket_url=f"file://{get_variables_from_env.LOCAL_STORAGE_PATH}",
            layout="{table_name}/{_resolve_path}",
            extra_placeholders={"_resolve_path": _resolve_path},
        )
    return destination, parent_path


def _serialize_trace(trace: Any) -> dict[str, Any]:
    """Serialize a dlt pipeline trace to a plain, JSON-serializable dictionary.

    Args:
        trace: A ``dlt.Pipeline.last_trace`` object, or ``None`` if the pipeline
            has not run yet.

    Returns:
        A dictionary with the keys ``transaction_id``, ``pipeline_name``,
        ``started_at``, ``finished_at``, ``engine_version``, ``dlt_version``,
        ``steps``, ``destination_name``, ``loads_ids``, ``has_failed_jobs``,
        and ``is_empty``.  Returns an empty dict when *trace* is ``None``.
    """
    if trace is None:
        return {}
    load_info = trace.last_load_info
    return {
        "transaction_id": trace.transaction_id,
        "pipeline_name": trace.pipeline_name,
        "started_at": trace.started_at.isoformat() if trace.started_at else None,
        "finished_at": trace.finished_at.isoformat() if trace.finished_at else None,
        "engine_version": trace.engine_version,
        "dlt_version": trace.execution_context.get("library", {}).get("version"),
        "steps": [
            {
                "step": s.step,
                "started_at": s.started_at.isoformat() if s.started_at else None,
                "finished_at": s.finished_at.isoformat() if s.finished_at else None,
                "step_exception": s.step_exception,
            }
            for s in trace.steps
        ],
        "destination_name": load_info.destination_name if load_info else None,
        "loads_ids": load_info.loads_ids if load_info else None,
        "has_failed_jobs": load_info.has_failed_jobs if load_info else None,
        "is_empty": getattr(load_info, "is_empty", None) if load_info else None,
    }


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def write_log_entry(
    data: str,
    destination_directory_path: str,
    destination_file_name: str,
) -> None:
    """Append a log entry to an NDJSON log file on the local filesystem."""
    Path(destination_directory_path).mkdir(parents=True, exist_ok=True)
    file_path = Path(destination_directory_path) / destination_file_name
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(data)


# ---------------------------------------------------------------------------
# Pipeline handlers
# ---------------------------------------------------------------------------


def run_api_to_file_pipeline(
    pipeline_name: str,
    source_api_base_url: str,
    source_api_resource: str,
    source_api_filter: str,
    source_api_date_to_load_from: str,
    destination_directory_path: str,
    destination_file_name: str,
) -> dict[str, Any]:
    """Fetch data from a paginated OData / REST API and write it as NDJSON to local or S3 storage.

    Records are yielded **individually** as dlt resource items — one dict per
    API record — rather than as a single serialised blob.  This means dlt can
    infer and track the schema, manage pipeline state, and control memory usage
    through its normal batching mechanism.

    Pagination follows ``odata.nextLink`` automatically until exhausted.  The
    destination writes all records as a single NDJSON file in one call.

    Date filtering is always handled by the caller via *source_api_filter*
    (e.g. ``"$filter=opdateringsdato ge DateTime'2024-01-01'&$orderby=id"``).
    Silver CDC handles deduplication — dlt cursor state is not used for
    incremental logic.

    Args:
        pipeline_name: Unique dlt pipeline identifier used for state tracking
            and logging.  Must be a valid file-system name (no spaces or special
            characters).
        source_api_base_url: Base URL without a trailing slash
            (e.g. ``"https://oda.ft.dk/api"``).
        source_api_resource: OData entity set or path segment
            (e.g. ``"Afstemning"``).
        source_api_filter: OData query-string options appended after ``?``
            (e.g. ``"$filter=opdateringsdato ge DateTime'2024-01-01'&$orderby=id"``).
            The caller is responsible for embedding any date filter.
        source_api_date_to_load_from: ISO-8601 date string (``YYYY-MM-DD``).
            Recorded in the run log; not used to modify the request URL.
        destination_directory_path: Bronze directory for the output file
            (e.g. ``"Files/Bronze/DDD/afstemning"``).
        destination_file_name: Output file name including extension
            (e.g. ``"afstemning_20240101_120000.json"``).

    Returns:
        A dictionary with:

        * ``"status"`` — ``"success"``
        * ``"records_written"`` — total records yielded from the API
        * ``"trace"`` — serialised dlt trace (see :func:`_serialize_trace`)

    Raises:
        requests.HTTPError: If any API request returns a non-2xx HTTP status.
        RuntimeError: If the storage write fails.
    """
    num_rows = 0
    session = requests.Session()  # reuse TCP connection across pages

    def _iter_odata_pages(initial_url: str) -> Iterator[dict[str, Any]]:
        """Yield individual records from a paginated OData endpoint.

        Follows ``odata.nextLink`` until exhausted.  Each call to this
        generator is a single HTTP round-trip, keeping memory usage low for
        large entity sets.
        """
        nonlocal num_rows
        api_url: str | None = initial_url
        while api_url is not None:
            response = session.get(api_url, timeout=30)
            response.raise_for_status()
            body = response.json()
            if "value" not in body:
                raise ValueError(
                    f"API response missing 'value' key; got keys: {sorted(body.keys())}"
                )
            records: list = body["value"]
            num_rows += len(records)
            for record in records:
                yield {
                    normalize_danish_name(k): _TS_MICROSEC.sub(r"\1", v)
                    if isinstance(v, str)
                    else v
                    for k, v in record.items()
                }
            api_url = body.get("odata.nextLink")  # follow OData pagination

    @dlt.resource(name=pipeline_name, write_disposition="append", max_table_nesting=0)
    def get_api_data(api_url: str) -> Any:
        yield from _iter_odata_pages(api_url)

    destination, dataset_name = _make_destination(
        destination_directory_path,
        destination_file_name,
        data_table_name=pipeline_name,
    )

    pipeline = dlt.pipeline(  # type: ignore[call-overload]  # restore_from_destination absent from dlt stub
        pipeline_name=pipeline_name,
        destination=destination,
        pipelines_dir=get_variables_from_env.DLT_PIPELINES_DIR,
        dataset_name=dataset_name,
        restore_from_destination=True,
    )

    try:
        api_url = f"{source_api_base_url}/{source_api_resource}?{source_api_filter}"
        pipeline.run(get_api_data(api_url), loader_file_format="jsonl")
    finally:
        session.close()

    return {
        "status": "success",
        "records_written": num_rows,
        "trace": _serialize_trace(pipeline.last_trace),
    }


def run_sql_to_file_pipeline(
    pipeline_name: str,
    source_connection_string: str,
    source_sql_query: str,
    destination_directory_path: str,
    destination_file_name: str,
    chunk_size: int = 100_000,
    loader_file_format: str = "parquet",
    sql_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Execute a SQL query and write the result as a Parquet or NDJSON file to local or S3 storage.

    Rows are yielded **individually** from the SQL cursor in chunks of
    *chunk_size* — one dict per row — so dlt can infer the schema and control
    memory usage.

    Args:
        pipeline_name: Unique dlt pipeline identifier.
        source_connection_string: SQLAlchemy connection URL, e.g.:

            * ``"mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"``
            * ``"postgresql+psycopg2://user:pass@host/db"``

        source_sql_query: Full SQL ``SELECT`` statement to execute.
        destination_directory_path: Bronze directory for the output file.
        destination_file_name: Output file name including extension
            (e.g. ``"my_table.parquet"``).
        chunk_size: Rows fetched per database round-trip.  Defaults to
            ``100_000``.  Controls memory pressure on the database cursor side;
            the destination still receives all rows in one batch.
        loader_file_format: Output file format.  ``"parquet"`` (default)
            or ``"jsonl"``.  When ``"jsonl"`` the output is NDJSON, compatible
            with the Bronze layer's ``read_json_auto()`` views.
        sql_params: Optional dictionary of bound parameters for the SQL query,
            e.g. ``{"updated_from": "2024-01-01"}``.  Values are passed via
            SQLAlchemy's parameterised execution (``text()`` + named params),
            which prevents SQL injection and is preferred over string
            interpolation for any user-supplied values.

    Returns:
        A dictionary with:

        * ``"status"`` — ``"success"``
        * ``"records_written"`` — total row count
        * ``"trace"`` — serialised dlt trace (see :func:`_serialize_trace`)

    Raises:
        sqlalchemy.exc.SQLAlchemyError: On connection or query execution failure.
        RuntimeError: If the storage write fails.
    """
    num_rows = 0

    _bound_params: dict[str, Any] = sql_params or {}

    @dlt.resource(name=pipeline_name, write_disposition="append")
    def get_sql_data(connection_string: str, sql_query: str) -> Iterator[dict[str, Any]]:
        nonlocal num_rows
        engine = create_engine(
            connection_string,
            connect_args={"connect_timeout": 30},
        )
        try:
            with engine.connect() as conn:
                result = conn.execute(text(sql_query), _bound_params)
                columns = list(result.keys())
                while True:
                    rows = result.fetchmany(chunk_size)
                    if not rows:
                        break
                    for row in rows:
                        num_rows += 1
                        yield dict(
                            zip(columns, row, strict=False)
                        )  # individual rows — dlt sees real schema
        finally:
            engine.dispose()

    destination, dataset_name = _make_destination(
        destination_directory_path,
        destination_file_name,
        data_table_name=pipeline_name,
    )

    pipeline = dlt.pipeline(  # type: ignore[call-overload]  # restore_from_destination absent from dlt stub
        pipeline_name=pipeline_name,
        destination=destination,
        pipelines_dir=get_variables_from_env.DLT_PIPELINES_DIR,
        dataset_name=dataset_name,
        restore_from_destination=True,
    )

    pipeline.run(
        get_sql_data(source_connection_string, source_sql_query),
        loader_file_format=loader_file_format,
    )

    return {
        "status": "success",
        "records_written": num_rows,
        "trace": _serialize_trace(pipeline.last_trace),
    }


def run_file_to_file_pipeline(
    pipeline_name: str,
    source_file_path: str,
    destination_directory_path: str,
    destination_file_name: str,
) -> dict[str, Any]:
    """Read a local file and write it as-is to the configured local storage destination.

    dlt is **not used** here.  A plain file copy is all that is needed, and
    wrapping it in a dlt pipeline would add overhead with no benefit — dlt
    provides no value for a binary pass-through with no schema or state
    management requirements.

    S3 mode (``RAW_STORAGE_TARGET=s3``) is not yet implemented for this type.

    Args:
        pipeline_name: Used only for log messages and the run log entry.
        source_file_path: Absolute or relative path to the local source file.
        destination_directory_path: Local directory for the output file.
        destination_file_name: Output file name, typically matching the source
            file name.

    Returns:
        A dictionary with:

        * ``"status"`` — ``"success"``
        * ``"bytes_written"`` — number of bytes in the written file
        * ``"trace"`` — empty dict (no dlt pipeline)

    Raises:
        FileNotFoundError: If *source_file_path* does not exist.
        NotImplementedError: If ``RAW_STORAGE_TARGET=s3``.
    """
    with open(source_file_path, "rb") as f:
        file_bytes = f.read()

    _upload(file_bytes, destination_directory_path, destination_file_name)

    return {"status": "success", "bytes_written": len(file_bytes), "trace": {}}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_log_dir(source_system_code: str, pipeline_name: str | None = None) -> str:
    """Build the log directory path for a given source system and optional pipeline.

    Logs always go to the local filesystem under
    ``DLT_PIPELINE_RUN_LOG_DIR/<source_system_code>/`` (default: ``LOCAL_STORAGE_PATH/logs``).
    Override the root with the ``DLT_PIPELINE_RUN_LOG_DIR`` env var.

    Args:
        source_system_code: Short source-system identifier (e.g. ``"DDD"``,
            ``"RFAM"``).
        pipeline_name: Optional pipeline / resource name appended as a
            sub-directory.  When omitted the path stops at *source_system_code*.

    Returns:
        A directory path string, without a trailing slash.
    """
    base = f"{get_variables_from_env.DLT_PIPELINE_RUN_LOG_DIR}/{source_system_code}"
    return f"{base}/{pipeline_name}" if pipeline_name else base


def _ensure_pipelines_dir() -> None:
    Path(get_variables_from_env.DLT_PIPELINES_DIR).mkdir(parents=True, exist_ok=True)


_PIPELINE_HANDLERS: dict[str, Callable[..., dict[str, Any]]] = {
    "api_to_file": run_api_to_file_pipeline,
    "sql_to_file": run_sql_to_file_pipeline,
    "file_to_file": run_file_to_file_pipeline,
}


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def execute_pipeline(
    pipeline_type: Literal["api_to_file", "sql_to_file", "file_to_file"],
    source_system_code: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Execute a named pipeline type and write a structured log entry to local storage.

    This is the primary entry point for all pipeline runs.  It dispatches to the
    appropriate handler based on *pipeline_type*, measures wall-clock duration,
    and unconditionally writes an NDJSON log record to the local filesystem — even on failure.

    Args:
        pipeline_type: Selects the handler to invoke.  One of:

            * ``"api_to_file"`` — calls :func:`run_api_to_file_pipeline`
            * ``"sql_to_file"`` — calls :func:`run_sql_to_file_pipeline`
            * ``"file_to_file"`` — calls :func:`run_file_to_file_pipeline`

        source_system_code: Short source-system identifier used to build the
            log path ``<log_root>/<source_system_code>/<pipeline_name>/``
            (e.g. ``"DDD"``, ``"RFAM"``).

        **kwargs: Keyword arguments forwarded to the selected handler.

            Required for **all** pipeline types:

            * ``pipeline_name`` (*str*) — unique pipeline identifier.

            Additional keys by pipeline type:

            **api_to_file**:
            ``source_api_base_url``, ``source_api_resource``,
            ``source_api_filter``, ``source_api_date_to_load_from``,
            ``destination_directory_path``, ``destination_file_name``

            **sql_to_file**:
            ``source_connection_string``, ``source_sql_query``,
            ``destination_directory_path``, ``destination_file_name``;
            optional: ``chunk_size`` (default ``100_000``),
            ``loader_file_format`` (default ``"parquet"``)

            **file_to_file**:
            ``source_file_path``,
            ``destination_directory_path``, ``destination_file_name``

    Returns:
        The result dictionary returned by the underlying handler.  Shape varies
        by pipeline type — see the individual ``run_*`` functions.

    Raises:
        ValueError: If *pipeline_type* is not one of the supported values.
        Exception: Re-raises any exception thrown by the handler after the log
            entry has been written.
    """
    _ensure_pipelines_dir()
    start_timestamp = time.time()
    pipeline_name: str = kwargs["pipeline_name"]
    log_dir = build_log_dir(source_system_code, pipeline_name)
    log_file = f"{pipeline_name}_log.ndjson"

    result = {"status": "failure"}
    level, message, error = "ERROR", "Pipeline execution failed", None

    try:
        handler = _PIPELINE_HANDLERS.get(pipeline_type)
        if handler is None:
            raise ValueError(f"Unsupported pipeline type: {pipeline_type}")
        result = handler(**kwargs)

        level, message = "INFO", "Pipeline execution completed successfully"
        return result

    except Exception:
        error = traceback.format_exc()
        raise

    finally:
        end_timestamp = time.time()
        log_params = {"source_system_code": source_system_code, **kwargs}
        try:
            write_log_entry(
                json.dumps(
                    {
                        "level": level,
                        "message": message,
                        "pipeline_type": pipeline_type,
                        "start_time": datetime.fromtimestamp(start_timestamp, tz=UTC).isoformat(),
                        "end_time": datetime.fromtimestamp(end_timestamp, tz=UTC).isoformat(),
                        "duration_seconds": round(end_timestamp - start_timestamp, 3),
                        "parameters": _scrub_secrets(log_params),
                        "result": result,
                        "error": error,
                    },
                    default=_json_default,
                )
                + "\n",
                log_dir,
                log_file,
            )
        except Exception as log_exc:
            # Log write must never mask the original pipeline result / exception.
            warnings.warn(
                f"Failed to write pipeline run log to local filesystem: {log_exc}",
                RuntimeWarning,
                stacklevel=2,
            )


# ---------------------------------------------------------------------------
# Shared orchestration helper
# ---------------------------------------------------------------------------


def build_rfam_sql(
    query_template: str,
    is_incremental: bool,
    date_to_load_from: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Format an Rfam SQL query template and return the query + bound parameters.

    Args:
        query_template: SQL template with a ``{where_clause}`` placeholder.
        is_incremental: When ``True``, injects a ``WHERE updated >= :updated_from``
            clause and binds ``date_to_load_from`` as a named parameter.
        date_to_load_from: Required when *is_incremental* is ``True``; the lower-
            bound date for the ``updated`` column filter (``YYYY-MM-DD``).

    Returns:
        ``(sql_query, sql_params)`` where *sql_params* is a dict suitable for
        SQLAlchemy parameterised execution (empty for full-extract tables).
    """
    if is_incremental:
        return (
            query_template.format(where_clause=" WHERE updated >= :updated_from"),
            {"updated_from": date_to_load_from},
        )
    return query_template.format(where_clause=""), {}


def run_extraction_pool(
    tasks: list[PipelineTask],
    script_name: str,
    source_system_code: str,
    date_to_load_from: str,
    start_time: datetime,
    resource_label: str = "resource",
    max_workers: int = _MAX_PIPELINE_WORKERS,
) -> None:
    """Run a set of pipeline tasks concurrently and write a script-level summary log.

    Each item in *tasks* must be a ``PipelineTask`` with:
      - ``"name"`` (str): human-readable resource/table name for logging.
      - ``"source_system_code"`` (str): source system identifier forwarded to
        :func:`execute_pipeline` as an explicit argument.
      - ``"pipeline_type"`` (str): one of ``"api_to_file"``, ``"sql_to_file"``,
        ``"file_to_file"``.
      - ``"kwargs"`` (dict): remaining keyword arguments forwarded to
        :func:`execute_pipeline`.

    Raises:
        RuntimeError: If one or more pipeline tasks fail.
    """
    pipeline_results: list[dict] = []
    failed: list[str] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_name: dict[concurrent.futures.Future, str] = {}
        for task in tasks:
            task_source_system_code: str = task["source_system_code"]
            task_pipeline_type: Literal["api_to_file", "sql_to_file", "file_to_file"] = task[
                "pipeline_type"
            ]
            future = executor.submit(
                execute_pipeline,
                task_pipeline_type,
                task_source_system_code,
                **task["kwargs"],
            )
            future_to_name[future] = task["name"]

        for future in concurrent.futures.as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result()
                pipeline_results.append(
                    {
                        resource_label: name,
                        "status": "success",
                        "records_written": result.get("records_written"),
                    }
                )
            except Exception as exc:
                pipeline_results.append(
                    {
                        resource_label: name,
                        "status": "failure",
                        "error": traceback.format_exc(),
                    }
                )
                failed.append(name)
                logger.error("Pipeline failed for %s %s: %s", resource_label, name, exc)

    end_time = datetime.now(UTC)
    duration_seconds = (end_time - start_time).total_seconds()
    overall_status = "failure" if failed else "success"

    log_record = (
        json.dumps(
            {
                "script_name": script_name,
                "source_system_code": source_system_code,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": round(duration_seconds, 3),
                "date_to_load_from": date_to_load_from,
                "status": overall_status,
                "pipelines_total": len(pipeline_results),
                "pipelines_succeeded": sum(1 for p in pipeline_results if p["status"] == "success"),
                "pipelines_failed": len(failed),
                "pipelines": sorted(pipeline_results, key=lambda p: p.get(resource_label, "")),
            },
            ensure_ascii=False,
        )
        + "\n"
    )

    log_dir = build_log_dir(source_system_code)
    log_file = f"{script_name}_log.ndjson"
    try:
        write_log_entry(log_record, log_dir, log_file)
    except Exception as log_exc:
        warnings.warn(
            f"Failed to write script-level run log: {log_exc}",
            RuntimeWarning,
            stacklevel=2,
        )

    if failed:
        raise RuntimeError(f"The following pipelines failed: {', '.join(failed)}")
