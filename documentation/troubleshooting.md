# Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `CapacityNotActive` in export / Bronze models | Fabric capacity is paused | Resume the capacity in the Azure portal and re-run |
| `Parser Error: syntax error at or near "DBT_..."` in Bronze | Stale dbt partial-parse cache | Run `dbt run --no-partial-parse` or delete `dbt/target/partial_parse.msgpack` |
| Asset shows as **unsynced** (yellow) in Dagster | Upstream materialized without running downstream | Materialize the downstream asset, or run the relevant job |
| `INTERNAL Error: Failed to bind column reference "id"` | DuckDB QUALIFY + UNION ALL bug (affects 1.5.0; fixed in tests for 1.5.1) | Ensure `duckdb>=1.5.1` is installed: `pip install -e ".[dagster,dev]"` |
| `write_deltalake() unexpected keyword argument` | `deltalake` version mismatch | `pip install -e ".[dagster,dev]"` to restore pinned versions |
| `FileNotFoundError` on DuckDB path | `DUCKDB_DATABASE_LOCATION` not set | Check `.env` and ensure the directory exists |
| Bronze models return no rows (local mode) | `DANISH_DEMOCRACY_DATA_SOURCE` or `RFAM_DATA_SOURCE` points to empty or wrong directory | Verify files exist under `LOCAL_STORAGE_PATH/Files/Bronze/DDD/{entity}/` or `.../RFAM/{table}/` |
| Bronze models return no rows (OneLake mode) | `DANISH_DEMOCRACY_DATA_SOURCE` or `RFAM_DATA_SOURCE` missing or wrong `abfss://` path | Set the correct path pointing to the Bronze NDJSON root on OneLake |
| dbt uses wrong output profile | `STORAGE_TARGET` mismatch | `dbt/profiles.yml` selects the `local` or `onelake` output based on `STORAGE_TARGET` — ensure `.env` is set correctly |
| Azure credential errors in local mode | `STORAGE_TARGET=onelake` set accidentally | Set `STORAGE_TARGET=local` in `.env`; no Azure vars are needed |
| dbt models missing (empty `models/` dir) | Model generation not run | Run `python -m ddd_python.ddd_dbt.generate_dbt_models` |
| Metabase not reachable after a pipeline run | `start_metabase_asset` failed or container not started | Run `docker compose up metabase` or check the `start_metabase_asset` log in the Dagster UI |
| DuckDB write error while Metabase is running | Metabase holds an open connection; `stop_metabase_asset` was not triggered | Stop Metabase manually (`docker stop ddd-metabase`) before writing, or run the job from the Dagster UI which manages this automatically |
| `Permission denied` on `/var/run/docker.sock` | `DOCKER_GID` in `.env` does not match the socket's group on the host | Run `stat -c '%g' /var/run/docker.sock` on the host and update `DOCKER_GID` in `.env`, then restart the affected container |
| `Catalog Error: Cannot write to two databases in the same transaction` | Running DuckDB with `SILVER_STORAGE_FORMAT=ducklake` while also writing Bronze/Gold views (main DB) in the same transaction | Ensure dbt uses `local_ducklake` target; Silver helper tables must use `{{ this.database }}` qualification in macros — already done, so this usually means an env var mismatch |
| `Column not found` or wrong schema after switching `SILVER_STORAGE_FORMAT` | Silver tables rebuilt in wrong storage mode | Run `dbt build --select tag:silver --full-refresh` after changing `SILVER_STORAGE_FORMAT`; switching modes does not migrate data |
| DuckLake catalog `.ducklake` file locked | Another process holds the catalog write lock | Stop Metabase and any open DBeaver/DuckDB CLI connections; the `.ducklake` catalog is single-writer like the main `.duckdb` file |
| Silver Parquet files missing from `DUCKLAKE_DATA_PATH` | Small tables stored inline in the catalog (DuckLake feature) | Run `CALL ducklake_flush_inlined_data('main_silver', '<table>')` to force Parquet materialisation; inline storage is correct behaviour, not corruption |
| Full-refresh fails mid-run, Silver in inconsistent state | dbt run interrupted after pre-hook but before post-hook | Re-run `dbt build --select tag:silver --full-refresh`; the pre-hook drops `_last_file` so a retry replays from scratch cleanly |
