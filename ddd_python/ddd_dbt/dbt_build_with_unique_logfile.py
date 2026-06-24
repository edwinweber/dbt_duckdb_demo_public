"""Run ``dbt build`` with a timestamped JSON log file uploaded to Fabric OneLake.

Usage::

    python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile
    python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile --models_to_select silver
"""

import argparse
import logging
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

from ddd_python.ddd_utils import get_variables_from_env

load_dotenv(find_dotenv())

logger = logging.getLogger(__name__)


def _generate_log_filename() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    return f"dbt_build_log_{timestamp}.json"


def run_dbt_build(log_file_local: str, models_to_select: str | None = None) -> None:
    """Run ``dbt build`` and capture output to *log_file_local*.

    Raises:
        RuntimeError: If dbt exits with a non-zero return code or times out.
    """
    dbt_command = ["dbt", "build", "--log-format", "json", "--no-use-colors"]

    if models_to_select:
        dbt_command.extend(["--select", models_to_select])

    with open(log_file_local, "w") as log_output:
        try:
            subprocess.run(
                dbt_command,
                cwd=get_variables_from_env.DBT_PROJECT_DIRECTORY,
                stdout=log_output,
                stderr=subprocess.STDOUT,
                timeout=3600,
                check=True,
            )
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("dbt process timed out after 3600 seconds") from exc
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(f"dbt build failed with return code {exc.returncode}") from exc


def upload_log_to_azure(log_file_local: str, log_file_name: str) -> None:
    # Deferred import: only called in onelake mode; keeps local-mode imports Azure-free.
    from ddd_python.ddd_utils import get_fabric_onelake_clients

    log_dir_fabric = get_variables_from_env.DBT_LOGS_DIRECTORY_FABRIC
    if log_dir_fabric is None:
        raise OSError("DBT_LOGS_DIRECTORY_FABRIC must be set")
    file_client = get_fabric_onelake_clients.get_fabric_file_client_default_workspace(
        log_dir_fabric,
        log_file_name,
    )
    file_client.create_file()
    with open(log_file_local, "rb") as local_log:
        file_client.upload_data(local_log, overwrite=True)


def main(models_to_select: str | None = None) -> None:
    log_dir = get_variables_from_env.DBT_LOGS_DIRECTORY
    if log_dir is None:
        raise OSError("DBT_LOGS_DIRECTORY must be set")
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    log_file_name = _generate_log_filename()
    log_file_local = str(Path(log_dir) / log_file_name)

    run_dbt_build(log_file_local, models_to_select)
    logger.info("dbt build logs saved locally at: %s", log_file_local)

    if get_variables_from_env.STORAGE_TARGET != "local":
        upload_log_to_azure(log_file_local, log_file_name)
        logger.info(
            "dbt build logs uploaded to Azure at %s/%s",
            get_variables_from_env.DBT_LOGS_DIRECTORY_FABRIC,
            log_file_name,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        description="Run dbt build with timestamped log upload to OneLake."
    )
    parser.add_argument(
        "--models_to_select", required=False, help="The dbt-models to build, separated by spaces"
    )
    args = parser.parse_args()

    try:
        main(args.models_to_select)
    except RuntimeError as exc:
        logger.error("%s", exc)
        sys.exit(1)
