"""
dagster_step_failures_raw: one row per failed asset per run.

Reads STEP_FAILURE events from individual per-run SQLite files
($DAGSTER_HOME/history/runs/{run_id}.db) — these are NOT in the consolidated
index.db, hence the Python model that globs across all run files.

Maps step_key → asset_key(s) via ASSET_MATERIALIZATION_PLANNED events in
the consolidated index.db, expanding to one row per planned asset so that
the downstream SQL model can join to dagster_asset via asset_key.
"""
import json
import os
import sqlite3
from glob import glob as file_glob

import pyarrow as pa


def model(dbt, session):
    dbt.config(materialized="table")

    dagster_home = os.environ.get("DAGSTER_HOME", ".dagster")
    index_db = os.path.join(dagster_home, "history", "runs", "index.db")
    run_db_pattern = os.path.join(dagster_home, "history", "runs", "*.db")

    # ── STEP_FAILURE events from individual per-run SQLite files ─────────────
    failures = []
    for db_path in sorted(file_glob(run_db_pattern)):
        if db_path == index_db:
            continue
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT run_id, step_key, timestamp, event
                FROM   event_logs
                WHERE  dagster_event_type = 'STEP_FAILURE'
                """
            )
            for run_id, step_key, ts, event_json in cur.fetchall():
                e = json.loads(event_json)
                esd = (e.get("dagster_event") or {}).get("event_specific_data") or {}
                error = esd.get("error") or {}
                failures.append(
                    {
                        "run_id": run_id,
                        "step_key": step_key,
                        "failed_at": ts,
                        "error_class": error.get("cls_name"),
                        "error_message": error.get("message"),
                    }
                )
            conn.close()
        except Exception:
            pass

    def _to_relation(tbl):
        # Register the PyArrow table with DuckDB and return a relation so that
        # dbt's generated materialize() does not hit the pyarrow.Table branch,
        # which triggers `import pyarrow.dataset` — a module that requires pandas.
        _reg = "_dagster_step_failures_raw_build"
        session.register(_reg, tbl)
        return session.query(f"SELECT * FROM {_reg}")

    empty = pa.table(
        {
            "run_id":        pa.array([], type=pa.string()),
            "step_key":      pa.array([], type=pa.string()),
            "asset_key":     pa.array([], type=pa.string()),
            "failed_at":     pa.array([], type=pa.timestamp("us")),
            "error_class":   pa.array([], type=pa.string()),
            "error_message": pa.array([], type=pa.large_utf8()),
        }
    )

    if not failures:
        return _to_relation(empty)

    # ── Map (run_id, step_key) → [asset_key] from consolidated index.db ──────
    # A single step_key can cover multiple assets (e.g. dbt_silver_assets),
    # so we expand to one row per planned asset.
    asset_map: dict[tuple, list] = {}
    try:
        conn = sqlite3.connect(f"file:{index_db}?mode=ro", uri=True)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT run_id, step_key, asset_key
            FROM   event_logs
            WHERE  dagster_event_type = 'ASSET_MATERIALIZATION_PLANNED'
              AND  asset_key IS NOT NULL
            """
        )
        for run_id, step_key, asset_key in cur.fetchall():
            key = (run_id, step_key)
            if asset_key not in asset_map.get(key, []):
                asset_map.setdefault(key, []).append(asset_key)
        conn.close()
    except Exception:
        pass

    # ── Expand failures to one row per planned asset ──────────────────────────
    run_ids, step_keys, asset_keys, failed_ats, error_classes, error_messages = (
        [], [], [], [], [], []
    )
    for f in failures:
        assets = asset_map.get((f["run_id"], f["step_key"]), [None])
        for asset_key in assets:
            run_ids.append(f["run_id"])
            step_keys.append(f["step_key"])
            asset_keys.append(asset_key)
            failed_ats.append(f["failed_at"])
            error_classes.append(f["error_class"])
            error_messages.append(f["error_message"])

    return _to_relation(pa.table(
        {
            "run_id":        pa.array(run_ids,        type=pa.string()),
            "step_key":      pa.array(step_keys,      type=pa.string()),
            "asset_key":     pa.array(asset_keys,     type=pa.string()),
            "failed_at":     pa.array(pa.array(failed_ats).cast(pa.timestamp("us"))),
            "error_class":   pa.array(error_classes,  type=pa.string()),
            "error_message": pa.array(error_messages, type=pa.large_utf8()),
        }
    ))
