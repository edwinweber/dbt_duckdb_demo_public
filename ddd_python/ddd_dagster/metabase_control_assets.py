"""Dagster assets for Metabase lifecycle control.

Stops Metabase before dbt runs and restarts it afterward. This is necessary
because DuckDB is single-writer: when dbt runs a transformation, it needs an
exclusive write lock on the .duckdb file. Metabase holds a persistent read
connection to the same file and must release it first. These assets are no-ops
in non-Docker environments (no-op if scripts are not present).

``start_metabase_asset`` only runs when every upstream step succeeded. When a
run fails halfway, ``restart_metabase_after_failure`` (called from the run-failure
sensor) brings Metabase back so a failed dbt step never leaves BI down.
"""

import logging
import subprocess
from collections.abc import Iterable
from pathlib import Path

from dagster import AssetKey, AssetsDefinition, asset

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
_module_logger = logging.getLogger(__name__)


def _run_metabase_script(
    script_name: str, *args: str, logger: logging.Logger | None = None
) -> bool:
    """Run a Metabase control script; return False (and skip) when it is absent.

    The path is resolved from the repository root rather than the working
    directory, so the assets behave the same regardless of where Dagster starts.
    A script that exists but exits non-zero still raises ``CalledProcessError``.
    """
    log = logger or _module_logger
    script = _SCRIPTS_DIR / script_name
    if not script.is_file():
        log.warning("Metabase control script %s not found - skipping", script)
        return False
    subprocess.run([str(script), *args], check=True)
    return True


def restart_metabase_after_failure(logger: logging.Logger | None = None) -> None:
    """Best-effort Metabase start for failed runs (no initialisation wait).

    ``docker start`` on a running container is a no-op, so this is safe to call
    even when the run failed before Metabase was stopped. Errors are logged and
    swallowed so they never mask the original run failure.
    """
    log = logger or _module_logger
    try:
        _run_metabase_script("start_metabase_and_wait.sh", "0", logger=log)
    except (subprocess.CalledProcessError, OSError) as exc:
        log.warning("Could not restart Metabase after failed run: %s", exc)


@asset(name="stop_metabase_asset", description="Stops Metabase before pipeline runs.")
def stop_metabase_asset():
    _run_metabase_script("stop_metabase_and_wait.sh")


def build_start_metabase_asset(upstream_asset_keys: Iterable[AssetKey]) -> AssetsDefinition:
    unique_keys = sorted(set(upstream_asset_keys), key=lambda key: key.to_user_string())

    @asset(
        name="start_metabase_asset",
        deps=unique_keys,
        description="Starts Metabase after pipeline runs.",
    )
    def start_metabase_asset():
        _run_metabase_script("start_metabase_and_wait.sh")

    return start_metabase_asset
