"""Run ``dbt build`` with a timestamped JSON log file saved locally.

dbt test failures and errors found in the JSON log are re-raised as
``RuntimeError`` even when ``dbt build`` exits with code 0 (e.g. when
``--no-fail-fast`` allows all models to run despite test failures).  This
ensures the Dagster asset always fails on bad data quality, which triggers
the existing failure sensor.

Usage::

    python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile
    python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile --models_to_select silver
"""

import argparse
import json
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


def _check_for_test_failures(log_path: str) -> None:
    """Raise if the dbt JSON log contains any test failures or errors.

    dbt exits 0 in some configurations even when tests fail (e.g. when
    ``--no-fail-fast`` is set and all models have run).  Parsing the NDJSON
    log lets us catch those cases and surface them as a ``RuntimeError`` so
    the calling Dagster asset fails and the failure sensor fires.

    Looks for ``NodeFinished`` events where the node result status is
    ``"fail"`` or ``"error"``.

    Raises:
        RuntimeError: If any test nodes finished with status ``"fail"`` or
            ``"error"``.  The message lists the failing node names.
    """
    failed_nodes: list[str] = []
    try:
        with open(log_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                # dbt JSON log structure: {"info": {"name": "NodeFinished", ...}, "data": {...}}
                info = record.get("info", {})
                if info.get("name") != "NodeFinished":
                    continue
                data = record.get("data", {})
                node_info = data.get("node_info", {})
                node_status = node_info.get("node_status", "")
                if node_status in ("fail", "error"):
                    unique_id = node_info.get("unique_id", "<unknown>")
                    failed_nodes.append(unique_id)
    except OSError:
        # If the log file is missing (e.g. dbt never started), skip the check —
        # the subprocess call will already have raised RuntimeError.
        return

    if failed_nodes:
        raise RuntimeError(
            f"dbt build completed but {len(failed_nodes)} node(s) failed: "
            + ", ".join(failed_nodes)
        )


def run_dbt_build(log_file_local: str, models_to_select: str | None = None) -> None:
    """Run ``dbt build`` and capture output to *log_file_local*.

    After the subprocess completes (including exit code 0), the JSON log is
    parsed for ``NodeFinished`` events with status ``"fail"`` or ``"error"``.
    Any such findings are re-raised as ``RuntimeError`` so that dbt test
    failures always surface as Dagster run failures, which in turn triggers
    the failure sensor.

    Raises:
        RuntimeError: If dbt exits with a non-zero return code, times out, or
            the log contains test failures / errors after a nominally-successful run.
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

    _check_for_test_failures(log_file_local)


def main(models_to_select: str | None = None) -> None:
    log_dir = get_variables_from_env.DBT_LOGS_DIRECTORY
    if log_dir is None:
        raise OSError("DBT_LOGS_DIRECTORY must be set")
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    log_file_name = _generate_log_filename()
    log_file_local = str(Path(log_dir) / log_file_name)

    run_dbt_build(log_file_local, models_to_select)
    logger.info("dbt build logs saved locally at: %s", log_file_local)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        description="Run dbt build, saving a timestamped JSON log to the local filesystem."
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
