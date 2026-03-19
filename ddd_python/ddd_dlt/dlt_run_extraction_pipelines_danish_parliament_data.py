import argparse
import concurrent.futures
import json
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone

from ddd_python.ddd_utils import configuration_variables, get_variables_from_env
from ddd_python.ddd_dlt import dlt_pipeline_execution_functions as dpef

SOURCE_SYSTEM_CODE = "DDD"
PIPELINE_TYPE = "api_to_file"
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

    A script-level run summary is always written to OneLake as an NDJSON record
    (one record per invocation) under
    ``DLT_PIPELINE_RUN_LOG_DIR/<SOURCE_SYSTEM_CODE>/``.
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
    script_start = time.monotonic()
    start_time = datetime.now(timezone.utc)

    # Resolve date_to_load_from
    if date_to_load_from is None:
        default_days = get_variables_from_env.DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD
        date_to_load_from = f"{start_time - timedelta(days=default_days):%Y-%m-%d}"
    else:
        try:
            datetime.strptime(date_to_load_from, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"date_to_load_from '{date_to_load_from}' must be in 'YYYY-MM-DD' format."
            )

    # Resolve file_names_to_retrieve
    if file_names_to_retrieve is None:
        file_names_to_retrieve = configuration_variables.DANISH_DEMOCRACY_FILE_NAMES

    # Use a set for O(1) incremental membership checks
    incremental_set = set(configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL)

    # Per-pipeline outcome accumulated for the script-level log
    pipeline_results: list[dict] = []
    failed: list[str] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_name: dict[concurrent.futures.Future, str] = {}

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

            # Replace Danish characters; DuckDB / OneLake does not support them in paths.
            base_file_name_lower = (
                file_name.replace("ø", "oe").replace("æ", "ae").replace("å", "aa").lower()
            )
            destination_file_name = f"{base_file_name_lower}_{start_time:%Y%m%d_%H%M%S}.json"

            if get_variables_from_env.STORAGE_TARGET == "local":
                # Relative to LOCAL_STORAGE_PATH; dlt filesystem prepends it.
                dest_dir = f"Files/Bronze/{SOURCE_SYSTEM_CODE}/{base_file_name_lower}"
            else:
                dest_dir = (
                    f"{get_variables_from_env.FABRIC_ONELAKE_FOLDER_BRONZE}"
                    f"/{SOURCE_SYSTEM_CODE}/{base_file_name_lower}"
                )

            future = executor.submit(
                dpef.execute_pipeline,
                pipeline_type=PIPELINE_TYPE,
                source_system_code=SOURCE_SYSTEM_CODE,
                pipeline_name=base_file_name_lower,
                source_api_base_url=get_variables_from_env.DANISH_DEMOCRACY_BASE_URL,
                source_api_resource=file_name,
                source_api_filter=api_filter,
                source_api_date_to_load_from=date_to_load_from,
                destination_directory_path=dest_dir,
                destination_file_name=destination_file_name,
            )
            future_to_name[future] = file_name

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

    if get_variables_from_env.STORAGE_TARGET == "local":
        log_dir = f"{get_variables_from_env.LOCAL_STORAGE_PATH}/logs/{SOURCE_SYSTEM_CODE}"
    else:
        log_dir = f"{get_variables_from_env.DLT_PIPELINE_RUN_LOG_DIR}/{SOURCE_SYSTEM_CODE}"
    log_file = f"{SCRIPT_NAME}_log.ndjson"
    try:
        dpef.write_log_to_onelake(log_record, log_dir, log_file)
    except Exception:
        pass  # log write failure must not mask pipeline errors

    if failed:
        raise RuntimeError(f"The following pipelines failed: {', '.join(failed)}")


if __name__ == "__main__":
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
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)