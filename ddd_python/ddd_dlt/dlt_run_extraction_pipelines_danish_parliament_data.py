import argparse
import logging
import sys
from datetime import UTC, datetime
from typing import Literal

from ddd_python.ddd_dlt import dlt_pipeline_execution_functions as dpef
from ddd_python.ddd_dlt.dlt_pipeline_execution_functions import PipelineTask
from ddd_python.ddd_utils import configuration_variables, get_variables_from_env
from ddd_python.ddd_utils.path_utils import build_bronze_destination_path
from ddd_python.ddd_utils.string_utils import normalize_danish_name, resolve_date_to_load_from

logger = logging.getLogger(__name__)

SOURCE_SYSTEM_CODE = "DDD"
PIPELINE_TYPE: Literal["api_to_file"] = "api_to_file"
SCRIPT_NAME = "dlt_run_extraction_pipelines_danish_parliament_data"


def run_extraction_pipelines_danish_parliament_data(
    date_to_load_from: str | None = None,
    file_names_to_retrieve: list[str] | None = None,
) -> None:
    """
    Executes extraction pipelines for Danish parliament data.

    Retrieves data from the Danish parliament API and processes it using
    concurrent threads. Supports both incremental and full data loads based
    on the provided file names.

    A script-level run summary is always written as an NDJSON record
    (one record per invocation) to the local filesystem under
    ``LOCAL_STORAGE_PATH/logs/<SOURCE_SYSTEM_CODE>/``.
    The record includes script name, start/end time, duration, date loaded from,
    and one result row per pipeline resource.

    Args:
        date_to_load_from: Starting date for data extraction in 'YYYY-MM-DD'
            format. Defaults to today minus
            ``DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD`` days (env var, default 31).
        file_names_to_retrieve: API resource names to retrieve. Defaults to
            ``configuration_variables.DANISH_DEMOCRACY_FILE_NAMES``.

    Raises:
        ValueError: If ``date_to_load_from`` does not match 'YYYY-MM-DD'.
        RuntimeError: If one or more pipeline tasks fail during execution.
    """
    start_time = datetime.now(UTC)

    date_to_load_from = resolve_date_to_load_from(
        date_to_load_from,
        get_variables_from_env.DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD,
        start_time,
    )

    if file_names_to_retrieve is None:
        file_names_to_retrieve = configuration_variables.DANISH_DEMOCRACY_FILE_NAMES

    # Use a set for O(1) incremental membership checks
    incremental_set = set(configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL)

    tasks: list[PipelineTask] = []
    for file_name in file_names_to_retrieve:
        # Incremental entities use an opdateringsdato date filter so only
        # new/changed records are fetched.  The remaining entities also
        # support opdateringsdato, but they are small tables and a full
        # extract on every run keeps delete detection simple.
        api_filter = (
            f"$filter=opdateringsdato ge DateTime'{date_to_load_from}'&$orderby=id"
            if file_name in incremental_set
            else "$inlinecount=allpages&$orderby=id"
        )

        base_file_name_lower = normalize_danish_name(file_name)
        destination_file_name = f"{base_file_name_lower}_{start_time:%Y%m%d_%H%M%S}.json"
        dest_dir = build_bronze_destination_path(SOURCE_SYSTEM_CODE, base_file_name_lower)

        tasks.append(
            {
                "name": file_name,
                "source_system_code": SOURCE_SYSTEM_CODE,
                "pipeline_type": PIPELINE_TYPE,
                "kwargs": {
                    "pipeline_name": base_file_name_lower,
                    "source_api_base_url": get_variables_from_env.DANISH_DEMOCRACY_BASE_URL,
                    "source_api_resource": file_name,
                    "source_api_filter": api_filter,
                    "source_api_date_to_load_from": date_to_load_from,
                    "destination_directory_path": dest_dir,
                    "destination_file_name": destination_file_name,
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
        description="Run extraction pipelines for Danish parliament data."
    )
    parser.add_argument(
        "--date_to_load_from",
        help="Starting date for data extraction (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--file_names_to_retrieve",
        nargs="+",
        metavar="RESOURCE",
        help="One or more API resource names to retrieve (e.g. Afstemning Møde).",
    )
    args = parser.parse_args()

    try:
        run_extraction_pipelines_danish_parliament_data(
            args.date_to_load_from,
            args.file_names_to_retrieve,
        )
    except Exception as exc:
        logger.error("ERROR: %s", exc)
        sys.exit(1)
