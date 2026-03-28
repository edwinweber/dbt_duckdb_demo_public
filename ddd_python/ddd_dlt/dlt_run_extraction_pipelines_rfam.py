"""Extraction pipelines for the Rfam public MySQL database.

Connects to ``mysql-rfam-public.ebi.ac.uk:4497`` (read-only, user ``rfamro``,
no password) and extracts 7 tables into Bronze storage as NDJSON files — the
same format used by the Danish Parliament API extraction so the existing Bronze
and Silver dbt macros work unchanged.

Two extraction modes mirror the DDD pattern:

* **Incremental** — ``family`` and ``genome`` have an ``updated`` timestamp
  column.  Only rows with ``updated >= date_to_load_from`` are fetched.
* **Full extract** — the remaining 5 small tables are fetched in full on every
  run, keeping delete detection simple.

Usage::

    python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam
    python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam --date_to_load_from 2024-01-01
    python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam --table_names_to_retrieve family genome
"""

import argparse
import concurrent.futures
import json
import logging
import sys
import time
import traceback
import warnings
from datetime import datetime, timedelta, timezone

from ddd_python.ddd_utils import configuration_variables, get_variables_from_env
from ddd_python.ddd_dlt import dlt_pipeline_execution_functions as dpef

logger = logging.getLogger(__name__)

SOURCE_SYSTEM_CODE = "RFAM"
PIPELINE_TYPE = "sql_to_file"
SCRIPT_NAME = "dlt_run_extraction_pipelines_rfam"


def run_extraction_pipelines_rfam(
    date_to_load_from: str | None = None,
    table_names_to_retrieve: list[str] | None = None,
) -> None:
    """Execute extraction pipelines for Rfam MySQL tables.

    Args:
        date_to_load_from: Starting date for incremental extraction in
            ``YYYY-MM-DD`` format.  Defaults to today minus
            ``RFAM_DEFAULT_DAYS_TO_LOAD`` days (env var, default 365).
        table_names_to_retrieve: Table names to extract.  Defaults to
            ``configuration_variables.RFAM_TABLE_NAMES``.

    Raises:
        ValueError: If ``date_to_load_from`` does not match ``YYYY-MM-DD``.
        RuntimeError: If one or more pipeline tasks fail during execution.
    """
    script_start = time.monotonic()
    start_time = datetime.now(timezone.utc)

    # Resolve date_to_load_from
    if date_to_load_from is None:
        default_days = get_variables_from_env.RFAM_DEFAULT_DAYS_TO_LOAD
        date_to_load_from = f"{start_time - timedelta(days=default_days):%Y-%m-%d}"
    else:
        try:
            datetime.strptime(date_to_load_from, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"date_to_load_from '{date_to_load_from}' must be in 'YYYY-MM-DD' format."
            )

    # Resolve table_names_to_retrieve
    if table_names_to_retrieve is None:
        table_names_to_retrieve = configuration_variables.RFAM_TABLE_NAMES

    incremental_set = set(configuration_variables.RFAM_TABLE_NAMES_INCREMENTAL)

    pipeline_results: list[dict] = []
    failed: list[str] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_name: dict[concurrent.futures.Future, str] = {}

        for table_name in table_names_to_retrieve:
            query_template = configuration_variables.RFAM_TABLE_QUERIES[table_name]

            # Build the SQL query with optional date filter.
            # Incremental tables use a SQLAlchemy named parameter (:updated_from)
            # rather than string interpolation — this prevents SQL injection even
            # if the date value ever bypasses the format check above.
            if table_name in incremental_set:
                sql_query = query_template.format(where_clause=" WHERE updated >= :updated_from")
                sql_params: dict = {"updated_from": date_to_load_from}
            else:
                sql_query = query_template.format(where_clause="")
                sql_params = {}

            destination_file_name = f"{table_name}_{start_time:%Y%m%d_%H%M%S}.json"

            if get_variables_from_env.STORAGE_TARGET == "local":
                dest_dir = f"Files/Bronze/{SOURCE_SYSTEM_CODE}/{table_name}"
            else:
                dest_dir = (
                    f"{get_variables_from_env.FABRIC_ONELAKE_FOLDER_BRONZE}"
                    f"/{SOURCE_SYSTEM_CODE}/{table_name}"
                )

            future = executor.submit(
                dpef.execute_pipeline,
                pipeline_type=PIPELINE_TYPE,
                source_system_code=SOURCE_SYSTEM_CODE,
                pipeline_name=table_name,
                source_connection_string=get_variables_from_env.RFAM_CONNECTION_STRING,
                source_sql_query=sql_query,
                sql_params=sql_params,
                destination_directory_path=dest_dir,
                destination_file_name=destination_file_name,
                loader_file_format="jsonl",
            )
            future_to_name[future] = table_name

        for future in concurrent.futures.as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result()
                pipeline_results.append({
                    "resource": name,
                    "status": "success",
                    "records_written": result.get("records_written"),
                })
            except Exception as exc:
                pipeline_results.append({
                    "resource": name,
                    "status": "failure",
                    "error": traceback.format_exc(),
                })
                failed.append(name)
                logger.error("Pipeline failed for table %s: %s", name, exc)

    end_time = datetime.now(timezone.utc)
    duration_seconds = time.monotonic() - script_start
    overall_status = "failure" if failed else "success"

    log_record = json.dumps(
        {
            "script_name": SCRIPT_NAME,
            "source_system_code": SOURCE_SYSTEM_CODE,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": round(duration_seconds, 3),
            "date_to_load_from": date_to_load_from,
            "status": overall_status,
            "pipelines_total": len(pipeline_results),
            "pipelines_succeeded": sum(1 for p in pipeline_results if p["status"] == "success"),
            "pipelines_failed": len(failed),
            "pipelines": sorted(pipeline_results, key=lambda p: p["resource"]),
        },
        ensure_ascii=False,
    ) + "\n"

    log_dir = dpef.build_log_dir(SOURCE_SYSTEM_CODE)
    log_file = f"{SCRIPT_NAME}_log.ndjson"
    try:
        dpef.write_log_to_onelake(log_record, log_dir, log_file)
    except Exception as log_exc:
        warnings.warn(
            f"Failed to write script-level run log: {log_exc}",
            RuntimeWarning,
            stacklevel=2,
        )

    if failed:
        raise RuntimeError(f"The following pipelines failed: {', '.join(failed)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Run extraction pipelines for Rfam MySQL database."
    )
    parser.add_argument(
        "--date_to_load_from",
        help="Starting date for incremental extraction (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--table_names_to_retrieve",
        nargs="+",
        metavar="TABLE",
        help="One or more Rfam table names to retrieve (e.g. family genome clan).",
    )
    args = parser.parse_args()

    try:
        run_extraction_pipelines_rfam(
            args.date_to_load_from,
            args.table_names_to_retrieve,
        )
    except Exception as exc:
        logger.error("ERROR: %s", exc)
        sys.exit(1)
