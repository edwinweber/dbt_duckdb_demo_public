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
    python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam \
        --table_names_to_retrieve family genome
"""

import argparse
import logging
import sys
from datetime import UTC, datetime
from typing import Literal

from ddd_python.ddd_dlt import dlt_pipeline_execution_functions as dpef
from ddd_python.ddd_dlt.dlt_pipeline_execution_functions import PipelineTask
from ddd_python.ddd_utils import configuration_variables, get_variables_from_env
from ddd_python.ddd_utils.path_utils import build_bronze_destination_path
from ddd_python.ddd_utils.string_utils import resolve_date_to_load_from

logger = logging.getLogger(__name__)

SOURCE_SYSTEM_CODE = "RFAM"
PIPELINE_TYPE: Literal["sql_to_file"] = "sql_to_file"
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
    start_time = datetime.now(UTC)

    date_to_load_from = resolve_date_to_load_from(
        date_to_load_from,
        get_variables_from_env.RFAM_DEFAULT_DAYS_TO_LOAD,
        start_time,
    )

    if table_names_to_retrieve is None:
        table_names_to_retrieve = configuration_variables.RFAM_TABLE_NAMES

    incremental_set = set(configuration_variables.RFAM_TABLE_NAMES_INCREMENTAL)

    tasks: list[PipelineTask] = []
    for table_name in table_names_to_retrieve:
        query_template = configuration_variables.RFAM_TABLE_QUERIES[table_name]

        # build_rfam_sql injects a WHERE clause and binds :updated_from for
        # incremental tables; full-extract tables get an empty clause and no params.
        sql_query, sql_params = dpef.build_rfam_sql(
            query_template,
            is_incremental=table_name in incremental_set,
            date_to_load_from=date_to_load_from,
        )

        destination_file_name = f"{table_name}_{start_time:%Y%m%d_%H%M%S}.json"
        dest_dir = build_bronze_destination_path(SOURCE_SYSTEM_CODE, table_name)

        tasks.append(
            {
                "name": table_name,
                "source_system_code": SOURCE_SYSTEM_CODE,
                "pipeline_type": PIPELINE_TYPE,
                "kwargs": {
                    "pipeline_name": table_name,
                    "source_connection_string": get_variables_from_env.RFAM_CONNECTION_STRING,
                    "source_sql_query": sql_query,
                    "sql_params": sql_params,
                    "destination_directory_path": dest_dir,
                    "destination_file_name": destination_file_name,
                    "loader_file_format": "jsonl",
                },
            }
        )

    dpef.run_extraction_pool(
        tasks=tasks,
        script_name=SCRIPT_NAME,
        source_system_code=SOURCE_SYSTEM_CODE,
        date_to_load_from=date_to_load_from,
        start_time=start_time,
        resource_label="resource",
    )


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
