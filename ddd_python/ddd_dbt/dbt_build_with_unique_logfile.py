"""Run ``dbt build`` with a timestamped JSON log file uploaded to Fabric OneLake.

Usage::

    python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile
    python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile --models_to_select silver
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone

from ddd_python.ddd_utils import get_variables_from_env

logger = logging.getLogger(__name__)


def generate_log_filename() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"dbt_build_log_{timestamp}.json"


def run_dbt_build(log_file_local: str, models_to_select: str | None = None) -> int:
    """Run ``dbt build`` and capture output to *log_file_local*.

    Returns:
        The subprocess return code.
    """
    dbt_command = ["dbt", "build", "--log-format", "json", "--no-use-colors"]

    if models_to_select:
        dbt_command.extend(["--select", models_to_select])

    with open(log_file_local, "w") as log_output:
        result = subprocess.run(
            dbt_command,
            cwd=get_variables_from_env.DBT_PROJECT_DIRECTORY,
            stdout=log_output,
            stderr=subprocess.STDOUT,
        )
    return result.returncode


def upload_log_to_azure(log_file_local: str, log_file_name: str) -> None:
    # Deferred import: only called in onelake mode; keeps local-mode imports Azure-free.
    from ddd_python.ddd_utils import get_fabric_onelake_clients
    file_client = get_fabric_onelake_clients.get_fabric_file_client_default_workspace(
        get_variables_from_env.DBT_LOGS_DIRECTORY_FABRIC, log_file_name,
    )
    file_client.create_file()
    with open(log_file_local, "rb") as local_log:
        file_client.upload_data(local_log, overwrite=True)


def main(models_to_select: str | None = None) -> None:
    log_dir = get_variables_from_env.DBT_LOGS_DIRECTORY
    os.makedirs(log_dir, exist_ok=True)

    log_file_name = generate_log_filename()
    log_file_local = os.path.join(log_dir, log_file_name)

    return_code = run_dbt_build(log_file_local, models_to_select)
    logger.info("dbt build logs saved locally at: %s", log_file_local)

    if get_variables_from_env.STORAGE_TARGET != "local":
        upload_log_to_azure(log_file_local, log_file_name)
        logger.info(
            "dbt build logs uploaded to Azure at %s/%s",
            get_variables_from_env.DBT_LOGS_DIRECTORY_FABRIC,
            log_file_name,
        )

    if return_code != 0:
        raise RuntimeError(f"dbt build failed with exit code {return_code} — see {log_file_local}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Run dbt build with timestamped log upload to OneLake.")
    parser.add_argument("--models_to_select", required=False, help="The dbt-models to build, separated by spaces")
    args = parser.parse_args()

    try:
        main(args.models_to_select)
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)
