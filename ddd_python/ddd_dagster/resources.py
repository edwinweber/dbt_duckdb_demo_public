"""Dagster resource definitions for Danish Democracy Data (DDD) extraction.

The sole resource, :class:`DltOneLakeResource`, wraps
``dlt_pipeline_execution_functions.execute_pipeline()`` and
``write_log_to_onelake()`` as a Dagster ``ConfigurableResource``.

Injecting the resource rather than importing the functions directly in assets
enables:

* Dependency injection — assets declare what they need; Dagster wires it up.
* Testability — a mock resource can be swapped in during unit tests.
* Auditability — resource configuration is visible in the Dagster UI.

Authentication / credentials are handled transparently by
``get_fabric_onelake_clients``, which reads the three Azure AD service-principal
variables from the environment:  ``AZURE_TENANT_ID``,
``AZURE_CLIENT_ID``, and
``AZURE_CLIENT_SECRET``.  No secrets are stored in or
passed through this resource.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from dagster import ConfigurableResource

from ddd_python.ddd_dlt import dlt_pipeline_execution_functions as dpef
from ddd_python.ddd_utils import get_variables_from_env


class DltOneLakeResource(ConfigurableResource):
    """Dagster resource that dispatches DLT extraction pipelines to OneLake.

    This resource is declared once in the top-level :class:`dagster.Definitions`
    object and injected into every extraction asset.  All actual extraction and
    upload logic lives in :mod:`dlt_pipeline_execution_functions`.

    Attributes:
        source_system_code: Short identifier appended to every OneLake log path
            (default ``"DDD"``).
    """

    source_system_code: str = "DDD"

    # ------------------------------------------------------------------
    # Pipeline execution
    # ------------------------------------------------------------------

    def execute_pipeline(self, pipeline_type: str, **kwargs: Any) -> dict:
        """Dispatch a single DLT pipeline run and return the result dict.

        This is a thin pass-through to
        :func:`dlt_pipeline_execution_functions.execute_pipeline`.  Per-pipeline
        NDJSON run logs are written to OneLake **inside** that function, so
        callers do not need to handle logging themselves.

        Args:
            pipeline_type: One of ``"api_to_file"``, ``"sql_to_file"``, or
                ``"file_to_file"``.
            **kwargs: Forwarded verbatim to ``execute_pipeline()``; see that
                function's docstring for the full parameter list per type.

        Returns:
            Result dict from the underlying pipeline handler, e.g.::

                {"status": "success", "records_written": 1234, "trace": {...}}

        Raises:
            ValueError: If *pipeline_type* is not recognised.
            Exception: Any exception raised by the underlying pipeline.
        """
        return dpef.execute_pipeline(
            pipeline_type=pipeline_type,
            source_system_code=kwargs.pop("source_system_code", self.source_system_code),
            **kwargs,
        )

    # ------------------------------------------------------------------
    # Job-level run summary logging to OneLake
    # ------------------------------------------------------------------

    def write_job_run_log(
        self,
        job_name: str,
        run_id: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """Append a job-run summary NDJSON record to OneLake.

        One record per Dagster job run, stored at::

            <DLT_PIPELINE_RUN_LOG_DIR>/<source_system_code>/<job_name>_run_log.ndjson

        Args:
            job_name: Dagster job name (used in the log file name).
            run_id: Dagster run ID for cross-referencing with the Dagster UI.
            status: ``"success"`` or ``"failure"``.
            start_time: UTC datetime when the Dagster run started.
            end_time: UTC datetime when the Dagster run ended.
            extra: Optional additional fields merged into the log record
                (e.g. step summaries, failure reasons).

        Raises:
            Exception: Propagates any error from the OneLake file client.
        """
        record = (
            json.dumps(
                {
                    "job_name": job_name,
                    "run_id": run_id,
                    "source_system_code": self.source_system_code,
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "duration_seconds": round(
                        (end_time - start_time).total_seconds(), 3
                    ),
                    "status": status,
                    **(extra or {}),
                },
                ensure_ascii=False,
                default=str,
            )
            + "\n"
        )

        if get_variables_from_env.STORAGE_TARGET == "local":
            log_dir = f"{get_variables_from_env.LOCAL_STORAGE_PATH}/logs/{self.source_system_code}"
        else:
            log_dir = f"{get_variables_from_env.DLT_PIPELINE_RUN_LOG_DIR}/{self.source_system_code}"
        log_file = f"{job_name}_run_log.ndjson"

        dpef.write_log_to_onelake(record, log_dir, log_file)
