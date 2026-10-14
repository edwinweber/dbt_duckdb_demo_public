"""One-off backfill: export historic STEP_FAILURE events to NDJSON.

Before the run-failure sensor exported step failures, they existed only in the
per-run SQLite files ($DAGSTER_HOME/history/runs/<run_id>.db). This writes the
same NDJSON files the sensor now produces, so dagster_step_failures_raw also
covers older runs. Safe to re-run (one file per run, overwritten). Runs that
are no longer in $DAGSTER_HOME/history/runs.db are skipped: they would be
orphans that fail the dbt foreign-key tests on dagster_step_failure.

Usage: python scripts/backfill_step_failures.py
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from ddd_python.ddd_dagster.sensors import STEP_FAILURES_SOURCE_SYSTEM, STEP_FAILURES_SUBDIR
from ddd_python.ddd_dlt.dlt_pipeline_execution_functions import build_log_dir


def _known_run_ids(runs_db: Path) -> set[str]:
    with sqlite3.connect(f"file:{runs_db}?mode=ro", uri=True) as con:
        return {row[0] for row in con.execute("SELECT run_id FROM runs")}


def main() -> None:
    history_dir = Path(os.environ.get("DAGSTER_HOME", ".dagster")) / "history"
    runs_dir = history_dir / "runs"
    known = _known_run_ids(history_dir / "runs.db")
    out_dir = Path(build_log_dir(STEP_FAILURES_SOURCE_SYSTEM, STEP_FAILURES_SUBDIR))
    out_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for db in sorted(runs_dir.glob("*.db")):
        if db.name == "index.db" or db.stem not in known:
            continue
        with sqlite3.connect(f"file:{db}?mode=ro", uri=True) as con:
            rows = con.execute(
                "SELECT run_id, step_key, timestamp, event FROM event_logs "
                "WHERE dagster_event_type = 'STEP_FAILURE'"
            ).fetchall()
        lines = []
        for run_id, step_key, ts, event in rows:
            error = (
                json.loads(event).get("dagster_event", {}).get("event_specific_data", {}) or {}
            ).get("error") or {}
            failed_at = (
                ts if isinstance(ts, str) else datetime.fromtimestamp(ts, tz=UTC).isoformat()
            )
            lines.append(
                json.dumps(
                    {
                        "run_id": run_id,
                        "step_key": step_key,
                        "failed_at": failed_at,
                        "error_class": error.get("cls_name"),
                        "error_message": error.get("message"),
                    },
                    ensure_ascii=False,
                )
            )
        if lines:
            (out_dir / f"{db.stem}.ndjson").write_text("\n".join(lines) + "\n", encoding="utf-8")
            written += 1
    print(f"Wrote step-failure files for {written} runs to {out_dir}")


if __name__ == "__main__":
    main()
