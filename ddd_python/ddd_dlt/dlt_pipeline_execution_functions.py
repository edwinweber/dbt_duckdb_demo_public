"""
dlt Pipeline Execution Functions for Microsoft Fabric OneLake.

This module provides a unified execution layer for dlt (data load tool) pipelines
that extract data from various sources and land it in Microsoft Fabric OneLake.

Three pipeline types are supported:

* **api_to_file** — Fetches paginated JSON from an OData / REST API, streaming
  individual records through dlt page by page, and writes them as NDJSON to
  OneLake.  dlt handles batching, schema inference, and pipeline state.
* **sql_to_file** — Executes a SQL query via SQLAlchemy, streams individual rows
  through dlt in configurable chunks, and writes the result as a single Parquet
  file to OneLake.
* **file_to_file** — Reads an arbitrary local file and uploads the raw bytes
  as-is to OneLake.  dlt is not used here; it adds no value for a plain copy.

Every pipeline run — successful or failed — is logged as an NDJSON record in
OneLake via :func:`write_log_to_onelake`.

Authentication
--------------
All OneLake I/O is authenticated via an Azure AD service principal using
``azure-identity`` ``ClientSecretCredential``.  The three required values are
read from environment variables (typically a ``.env`` file) by
``ddd_python.ddd_utils.get_variables_from_env``:

* ``AZURE_TENANT_ID``
* ``AZURE_CLIENT_ID``
* ``AZURE_CLIENT_SECRET``

For ``api_to_file`` and ``sql_to_file``, dlt's built-in ``filesystem``
destination writes directly to OneLake via ``adlfs`` (ADLS Gen2), using the
same service-principal credentials.  For ``file_to_file`` and log writes, the
``azure-storage-file-datalake`` SDK is used directly via
``ddd_python.ddd_utils.get_fabric_onelake_clients``.

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
        destination_directory_path="<YOUR_LAKEHOUSE>.Lakehouse/Files/Bronze/test",
        destination_file_name="stemmetype.json",
    )
"""

from datetime import datetime, timezone
from typing import Any
import logging
import requests
import time
import json
import os
import traceback
import warnings
import dlt
from dlt.destinations import filesystem as dlt_filesystem
import io
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

from ddd_python.ddd_utils import get_variables_from_env

# Ensure the dlt state directory exists — critical when running in Docker with a
# freshly-mounted volume where the path does not yet exist in the container FS.
os.makedirs(get_variables_from_env.DLT_PIPELINES_DIR, exist_ok=True)

# Disable gzip compression so the filesystem destination writes plain JSONL files,
# not .jsonl.gz — downstream dbt models glob for *.json / *.jsonl.
os.environ.setdefault("NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION", "true")

# Prevent dlt from normalizing the dataset_name (which we use as a directory path
# containing dots and slashes, e.g. "<YOUR_LAKEHOUSE>.Lakehouse/Files/Bronze/DDD").
os.environ.setdefault("DESTINATION__FILESYSTEM__ENABLE_DATASET_NAME_NORMALIZATION", "false")

# Keys whose values are redacted in run logs to prevent credential leakage.
_SENSITIVE_KEYS = frozenset({"connection_string", "secret", "password", "token"})


def _scrub_secrets(params: dict) -> dict:
    """Return a copy of *params* with values of sensitive keys replaced by '***'."""
    return {
        k: "***" if any(s in k.lower() for s in _SENSITIVE_KEYS) else v
        for k, v in params.items()
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


def _upload_to_onelake(data: bytes | str, directory_path: str, file_name: str) -> None:
    """Upload data to a Fabric OneLake file, overwriting any existing content."""
    from ddd_python.ddd_utils import get_fabric_onelake_clients
    file_client = get_fabric_onelake_clients.get_fabric_file_client_default_workspace(
        directory_path, file_name
    )
    file_client.upload_data(data, overwrite=True, timeout=300)


def _upload_to_local(data: bytes | str, directory_path: str, file_name: str) -> None:
    """Write data to a local file, overwriting any existing content."""
    os.makedirs(directory_path, exist_ok=True)
    file_path = os.path.join(directory_path, file_name)
    mode = "wb" if isinstance(data, bytes) else "w"
    with open(file_path, mode) as f:
        f.write(data)


def _upload(data: bytes | str, directory_path: str, file_name: str) -> None:
    """Upload data to local or OneLake storage, based on STORAGE_TARGET."""
    if get_variables_from_env.STORAGE_TARGET == "local":
        _upload_to_local(data, directory_path, file_name)
    else:
        _upload_to_onelake(data, directory_path, file_name)



def _make_destination(
    directory_path: str,
    file_name: str,
    data_table_name: str,
) -> tuple[dlt_filesystem, str]:
    """Build a dlt filesystem destination for either OneLake or local storage.

    When ``STORAGE_TARGET=local``, *directory_path* is treated as a path
    relative to ``LOCAL_STORAGE_PATH`` (e.g. ``"bronze/DDD/afstemning"``).
    When ``STORAGE_TARGET=onelake`` (default), *directory_path* is the full
    OneLake path (e.g. ``"<LAKEHOUSE>.Lakehouse/Files/Bronze/DDD/afstemning"``).

    Returns:
        A tuple of ``(destination, dataset_name)`` to pass to ``dlt.pipeline``.
    """
    parent_path = directory_path.rsplit("/", 1)[0]

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

    if get_variables_from_env.STORAGE_TARGET == "local":
        destination = dlt_filesystem(
            bucket_url=f"file://{get_variables_from_env.LOCAL_STORAGE_PATH}",
            layout="{table_name}/{_resolve_path}",
            extra_placeholders={"_resolve_path": _resolve_path},
        )
    else:
        destination = dlt_filesystem(
            bucket_url=f"az://{get_variables_from_env.FABRIC_WORKSPACE}",
            layout="{table_name}/{_resolve_path}",
            extra_placeholders={"_resolve_path": _resolve_path},
            credentials={
                "azure_storage_account_name": get_variables_from_env.FABRIC_ONELAKE_STORAGE_ACCOUNT,
                "azure_account_host": "onelake.blob.fabric.microsoft.com",
                "azure_client_id": get_variables_from_env.AZURE_CLIENT_ID,
                "azure_client_secret": get_variables_from_env.AZURE_CLIENT_SECRET,
                "azure_tenant_id": get_variables_from_env.AZURE_TENANT_ID,
            },
        )
    return destination, parent_path


def _serialize_trace(trace: Any) -> dict:
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
        "is_empty": load_info.is_empty if load_info else None,
    }


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def write_log_to_onelake(
    data: str,
    destination_directory_path: str,
    destination_file_name: str,
) -> None:
    """Append a log entry to an NDJSON log file in Fabric OneLake or local storage.

    When ``STORAGE_TARGET=local``, the log is written to a local file under
    *destination_directory_path*.  Otherwise it is appended to OneLake.
    """
    if get_variables_from_env.STORAGE_TARGET == "local":
        os.makedirs(destination_directory_path, exist_ok=True)
        file_path = os.path.join(destination_directory_path, destination_file_name)
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(data)
        return

    from ddd_python.ddd_utils import get_fabric_onelake_clients
    file_client = get_fabric_onelake_clients.get_fabric_file_client_default_workspace(
        destination_directory_path, destination_file_name
    )
    encoded = data.encode("utf-8")
    try:
        offset = file_client.get_file_properties().size
    except Exception:  # file does not exist yet
        file_client.create_file()
        offset = 0
    file_client.append_data(io.BytesIO(encoded), offset=offset, length=len(encoded))
    file_client.flush_data(offset + len(encoded))


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
    source_api_incremental_field: str | None = None,
    full_refresh: bool = False,
) -> dict:
    """Fetch data from a paginated OData / REST API and write it as NDJSON to OneLake.

    Records are yielded **individually** as dlt resource items — one dict per
    API record — rather than as a single serialised blob.  This means dlt can
    infer and track the schema, manage pipeline state, and control memory usage
    through its normal batching mechanism (``batch_size=1_000_000_000`` in
    :func:`_make_onelake_ndjson_destination`).

    Pagination follows ``odata.nextLink`` automatically until exhausted.  The
    destination writes all records as a single NDJSON file in one call.

    **Incremental mode** (``source_api_incremental_field`` is set):

    dlt manages the date cursor via :class:`dlt.sources.incremental`.  On the
    first run — or after ``full_refresh=True`` — the lower-bound date is
    *source_api_date_to_load_from*.  On subsequent runs dlt automatically
    advances the cursor to the maximum value of *source_api_incremental_field*
    seen across all yielded records, so only new or updated records are fetched.
    The date filter is appended to *source_api_filter* automatically; do
    **not** embed it manually in *source_api_filter* when using this mode.

    **Full-extract mode** (``source_api_incremental_field`` is ``None``):

    *source_api_filter* is used as-is (the caller is responsible for any date
    filter).  Every run fetches all matching records.  *full_refresh* is ignored.

    Args:
        pipeline_name: Unique dlt pipeline identifier used for state tracking
            and logging.  Must be a valid file-system name (no spaces or special
            characters).
        source_api_base_url: Base URL without a trailing slash
            (e.g. ``"https://oda.ft.dk/api"``).
        source_api_resource: OData entity set or path segment
            (e.g. ``"Afstemning"``).
        source_api_filter: Base OData query-string options appended after ``?``
            (e.g. ``"$inlinecount=allpages"``).  In incremental mode the date
            filter is appended automatically — do **not** include it here.
        source_api_date_to_load_from: ISO-8601 date string (``YYYY-MM-DD``)
            used as the initial incremental lower-bound and as the reload
            start when ``full_refresh=True``.
        destination_directory_path: OneLake directory for the output file
            (e.g. ``"<YOUR_LAKEHOUSE>.Lakehouse/Files/Bronze/DDD/afstemning"``).
        destination_file_name: Output file name including extension
            (e.g. ``"afstemning_20240101_120000.json"``).
        source_api_incremental_field: OData field name to use as the
            incremental cursor (e.g. ``"opdateringsdato"``).  When ``None``
            the function runs in full-extract mode.
        full_refresh: When ``True`` and *source_api_incremental_field* is set,
            the stored dlt cursor state is dropped before running so the load
            starts fresh from *source_api_date_to_load_from*.  Has no effect
            in full-extract mode.

    Returns:
        A dictionary with:

        * ``"status"`` — ``"success"``
        * ``"records_written"`` — total records yielded from the API
        * ``"trace"`` — serialised dlt trace (see :func:`_serialize_trace`)

    Raises:
        requests.HTTPError: If any API request returns a non-2xx HTTP status.
        RuntimeError: If the OneLake write fails.
    """
    num_rows = 0
    session = requests.Session()  # reuse TCP connection across pages

    if source_api_incremental_field:
        @dlt.resource(name=pipeline_name, write_disposition="append", max_table_nesting=0)
        def get_api_data(
            api_url_base: str,
            updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
                source_api_incremental_field,
                initial_value=source_api_date_to_load_from,
            ),
        ):
            nonlocal num_rows
            # Truncate to YYYY-MM-DD for the OData DateTime'...' literal syntax
            last_date = str(updated_at.last_value)[:10]
            date_filter = f"$filter={source_api_incremental_field} ge DateTime'{last_date}'"
            combined_filter = f"{source_api_filter}&{date_filter}" if source_api_filter else date_filter
            api_url = f"{api_url_base}?{combined_filter}"
            while api_url is not None:
                response = session.get(api_url, timeout=30)
                response.raise_for_status()
                body = response.json()
                records = body.get("value", [])
                num_rows += len(records)
                yield from records                      # individual records — dlt sees real schema
                api_url = body.get("odata.nextLink")    # follow OData pagination
    else:
        @dlt.resource(name=pipeline_name, write_disposition="append", max_table_nesting=0)
        def get_api_data(api_url: str):  # type: ignore[no-redef]
            nonlocal num_rows
            while api_url is not None:
                response = session.get(api_url, timeout=30)
                response.raise_for_status()
                body = response.json()
                records = body.get("value", [])
                num_rows += len(records)
                yield from records                      # individual records — dlt sees real schema
                api_url = body.get("odata.nextLink")    # follow OData pagination

    destination, dataset_name = _make_destination(
        destination_directory_path, destination_file_name, data_table_name=pipeline_name,
    )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        pipelines_dir=get_variables_from_env.DLT_PIPELINES_DIR,
        dataset_name=dataset_name,
        restore_from_destination=True,
    )

    if full_refresh and source_api_incremental_field:
        pipeline.drop()  # wipes stored cursor so next run starts from source_api_date_to_load_from

    try:
        if source_api_incremental_field:
            url_base = f"{source_api_base_url}/{source_api_resource}"
            pipeline.run(get_api_data(url_base), loader_file_format="jsonl")
        else:
            api_url = f"{source_api_base_url}/{source_api_resource}?{source_api_filter}"
            pipeline.run(get_api_data(api_url), loader_file_format="jsonl")
    finally:
        session.close()

    return {"status": "success", "records_written": num_rows, "trace": _serialize_trace(pipeline.last_trace)}


def run_sql_to_file_pipeline(
    pipeline_name: str,
    source_connection_string: str,
    source_sql_query: str,
    destination_directory_path: str,
    destination_file_name: str,
    chunk_size: int = 100_000,
    parquet_compression: str = "snappy",
) -> dict:
    """Execute a SQL query and write the result as a Parquet file to OneLake.

    Rows are yielded **individually** from the SQL cursor in chunks of
    *chunk_size* — one dict per row — so dlt can infer the schema and control
    memory usage.  The destination (:func:`_make_onelake_parquet_destination`)
    collects all rows via a large ``batch_size`` and writes a single Parquet
    file, preserving the original column names and types as inferred by PyArrow.

    Args:
        pipeline_name: Unique dlt pipeline identifier.
        source_connection_string: SQLAlchemy connection URL, e.g.:

            * ``"mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server"``
            * ``"postgresql+psycopg2://user:pass@host/db"``

        source_sql_query: Full SQL ``SELECT`` statement to execute.
        destination_directory_path: OneLake directory for the output file.
        destination_file_name: Output file name including extension
            (e.g. ``"my_table.parquet"``).
        chunk_size: Rows fetched per database round-trip.  Defaults to
            ``100_000``.  Controls memory pressure on the database cursor side;
            the destination still receives all rows in one batch.
        parquet_compression: Parquet codec.  Accepted values: ``"snappy"``
            (default), ``"gzip"``, ``"brotli"``, ``"zstd"``, ``"none"``.

    Returns:
        A dictionary with:

        * ``"status"`` — ``"success"``
        * ``"records_written"`` — total row count
        * ``"trace"`` — serialised dlt trace (see :func:`_serialize_trace`)

    Raises:
        sqlalchemy.exc.SQLAlchemyError: On connection or query execution failure.
        RuntimeError: If the OneLake write fails.
    """
    num_rows = 0

    @dlt.resource(name=pipeline_name, write_disposition="append")
    def get_sql_data(connection_string: str, sql_query: str):
        nonlocal num_rows
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            result = conn.execute(text(sql_query))
            columns = list(result.keys())
            while True:
                rows = result.fetchmany(chunk_size)
                if not rows:
                    break
                for row in rows:
                    num_rows += 1
                    yield dict(zip(columns, row))   # individual rows — dlt sees real schema

    destination, dataset_name = _make_destination(
        destination_directory_path, destination_file_name, data_table_name=pipeline_name,
    )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        pipelines_dir=get_variables_from_env.DLT_PIPELINES_DIR,
        dataset_name=dataset_name,
        restore_from_destination=True,
    )

    pipeline.run(get_sql_data(source_connection_string, source_sql_query), loader_file_format="parquet")

    return {"status": "success", "records_written": num_rows, "trace": _serialize_trace(pipeline.last_trace)}


def run_file_to_file_pipeline(
    pipeline_name: str,
    source_file_path: str,
    destination_directory_path: str,
    destination_file_name: str,
) -> dict:
    """Read a local file and upload it as-is to Fabric OneLake.

    dlt is **not used** here.  A plain file copy via the ADLS Gen2 SDK is all
    that is needed, and wrapping it in a dlt pipeline would add overhead with no
    benefit — dlt provides no value for a binary pass-through with no schema or
    state management requirements.

    Authentication uses the same ``ClientSecretCredential`` as all other
    OneLake I/O in this module (see module docstring for details).

    Args:
        pipeline_name: Used only for log messages and the run log entry.
        source_file_path: Absolute or relative path to the local source file.
        destination_directory_path: OneLake directory for the output file.
        destination_file_name: Output file name, typically matching the source
            file name.

    Returns:
        A dictionary with:

        * ``"status"`` — ``"success"``
        * ``"bytes_written"`` — number of bytes in the uploaded file
        * ``"trace"`` — empty dict (no dlt pipeline)

    Raises:
        FileNotFoundError: If *source_file_path* does not exist.
        RuntimeError: If the OneLake upload fails.
    """
    with open(source_file_path, "rb") as f:
        file_bytes = f.read()

    _upload(file_bytes, destination_directory_path, destination_file_name)

    return {"status": "success", "bytes_written": len(file_bytes), "trace": {}}


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def execute_pipeline(pipeline_type: str, **kwargs) -> dict:
    """Execute a named pipeline type and write a structured log entry to OneLake.

    This is the primary entry point for all pipeline runs.  It dispatches to the
    appropriate handler based on *pipeline_type*, measures wall-clock duration,
    and unconditionally writes an NDJSON log record to OneLake — even on failure.

    Args:
        pipeline_type: Selects the handler to invoke.  One of:

            * ``"api_to_file"`` — calls :func:`run_api_to_file_pipeline`
            * ``"sql_to_file"`` — calls :func:`run_sql_to_file_pipeline`
            * ``"file_to_file"`` — calls :func:`run_file_to_file_pipeline`

        **kwargs: Keyword arguments forwarded to the selected handler.

            Keys required by **all** pipeline types:

            * ``source_system_code`` (*str*) — used to build the log path
              ``<log_root>/<source_system_code>/<pipeline_name>/``.
            * ``pipeline_name`` (*str*) — unique pipeline identifier.

            Additional keys by pipeline type:

            **api_to_file**:
            ``source_api_base_url``, ``source_api_resource``,
            ``source_api_filter``, ``source_api_date_to_load_from``,
            ``destination_directory_path``, ``destination_file_name``;
            optional: ``source_api_incremental_field`` (default ``None`` —
            full-extract mode), ``full_refresh`` (default ``False``)

            **sql_to_file**:
            ``source_connection_string``, ``source_sql_query``,
            ``destination_directory_path``, ``destination_file_name``;
            optional: ``chunk_size`` (default ``100_000``),
            ``parquet_compression`` (default ``"snappy"``)

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
    start_timestamp = time.time()
    if get_variables_from_env.STORAGE_TARGET == "local":
        log_dir = f"{get_variables_from_env.LOCAL_STORAGE_PATH}/logs/{kwargs['source_system_code']}/{kwargs['pipeline_name']}"
    else:
        log_dir = f"{get_variables_from_env.DLT_PIPELINE_RUN_LOG_DIR}/{kwargs['source_system_code']}/{kwargs['pipeline_name']}"
    log_file = f"{kwargs['pipeline_name']}_log.ndjson"

    result = {"status": "failure"}
    level, message, error = "ERROR", "Pipeline execution failed", None

    try:
        if pipeline_type == "api_to_file":
            result = run_api_to_file_pipeline(
                pipeline_name=kwargs["pipeline_name"],
                source_api_base_url=kwargs["source_api_base_url"],
                source_api_resource=kwargs["source_api_resource"],
                source_api_filter=kwargs.get("source_api_filter", ""),
                source_api_date_to_load_from=kwargs["source_api_date_to_load_from"],
                destination_directory_path=kwargs["destination_directory_path"],
                destination_file_name=kwargs["destination_file_name"],
                source_api_incremental_field=kwargs.get("source_api_incremental_field"),
                full_refresh=kwargs.get("full_refresh", False),
            )
        elif pipeline_type == "sql_to_file":
            result = run_sql_to_file_pipeline(
                pipeline_name=kwargs["pipeline_name"],
                source_connection_string=kwargs["source_connection_string"],
                source_sql_query=kwargs["source_sql_query"],
                destination_directory_path=kwargs["destination_directory_path"],
                destination_file_name=kwargs["destination_file_name"],
                chunk_size=kwargs.get("chunk_size", 100_000),
                parquet_compression=kwargs.get("parquet_compression", "snappy"),
            )
        elif pipeline_type == "file_to_file":
            result = run_file_to_file_pipeline(
                pipeline_name=kwargs["pipeline_name"],
                source_file_path=kwargs["source_file_path"],
                destination_directory_path=kwargs["destination_directory_path"],
                destination_file_name=kwargs["destination_file_name"],
            )
        else:
            raise ValueError(f"Unsupported pipeline type: {pipeline_type}")

        level, message = "INFO", "Pipeline execution completed successfully"
        return result

    except Exception as e:
        error = traceback.format_exc()
        raise

    finally:
        end_timestamp = time.time()
        try:
            write_log_to_onelake(
                json.dumps({
                    "level": level,
                    "message": message,
                    "pipeline_type": pipeline_type,
                    "start_time": datetime.fromtimestamp(start_timestamp, tz=timezone.utc).isoformat(),
                    "end_time": datetime.fromtimestamp(end_timestamp, tz=timezone.utc).isoformat(),
                    "duration_seconds": round(end_timestamp - start_timestamp, 3),
                    "parameters": _scrub_secrets(kwargs),
                    "result": result,
                    "error": error,
                }, default=_json_default) + "\n",
                log_dir,
                log_file,
            )
        except Exception as log_exc:
            # Log write must never mask the original pipeline result / exception.
            warnings.warn(
                f"Failed to write pipeline run log to OneLake: {log_exc}",
                RuntimeWarning,
                stacklevel=2,
            )
