# Codebase Index — dbt_duckdb_demo
# Generated 2026-05-17. Update when the repo structure changes significantly.
# Purpose: give Claude a complete map of all source files and their APIs so
# individual files don't need to be re-read just to navigate the codebase.
# Note: CLAUDE.md (always loaded) already covers architecture, naming conventions,
# data sources, and design patterns — this file adds exact file paths + signatures.

## Python source files

### ddd_python/ddd_utils/configuration_variables.py
Single source of truth for all entity lists. Key exports:
- `normalize_danish_name(name: str) -> str` — ø→oe, æ→ae, å→aa + lowercase
- `DANISH_DEMOCRACY_FILE_NAMES` — 18 API entities (Afstemning … Stemmetype)
- `DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL` — 6 incremental: Aktør, Møde, Sagstrin, Sag, SagstrinAktør, Stemme
- `DANISH_DEMOCRACY_MODELS_BRONZE` — 18 bronze model names (bronze_ddd_*)
- `DANISH_DEMOCRACY_MODELS_SILVER` — 18 silver model names (silver_ddd_*)
- `DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS: dict[str,str]` — all 18 entities → "id"
- `DANISH_DEMOCRACY_MODELS_GOLD` — 10 gold models: actor, actor_type, case, date, meeting, meeting_status, meeting_type, individual_votes, vote, vote_type
- `RFAM_TABLE_NAMES` — 7 tables: family, genome, clan, clan_membership, author, literature_reference, dead_family
- `RFAM_TABLE_NAMES_INCREMENTAL` — 2: family, genome
- `RFAM_MODELS_BRONZE` / `RFAM_MODELS_SILVER` — 7 each (bronze_rfam_* / silver_rfam_*)
- `RFAM_TABLE_PRIMARY_KEYS: dict[str,str]` — family→rfam_acc, genome→upid, clan→clan_acc, clan_membership→rfam_acc, author→author_id, literature_reference→pmid, dead_family→rfam_acc
- `RFAM_TABLE_DATE_COLUMNS: dict[str,str]` — family/genome/clan→"updated", others→""
- `RFAM_TABLE_QUERIES: dict[str,str]` — SELECT * with optional {where_clause}
- `SILVER_TABLE_PRIMARY_KEYS: dict[str,str]` — combined DDD + Rfam silver_* → pk

### ddd_python/ddd_utils/get_variables_from_env.py
Lazy env var loading; import never fails without credentials.
- `_require(name: str) -> str` :10 — raises on missing/empty
- `_int_env(name: str, default: int) -> int` :18
- `class _LazyEnv(types.ModuleType)` :55 — module-level __getattr__ proxy
  - `__getattr__(self, name: str) -> str` :69 — defers credential access

### ddd_python/ddd_utils/get_fabric_onelake_clients.py
Azure ADLS Gen2 clients for OneLake.
- `_get_credential() -> ClientSecretCredential` :17
- `_get_service_client() -> DataLakeServiceClient` :34
- `get_fabric_token() -> str` :47
- `get_fabric_file_system_client(file_system_name: str) -> FileSystemClient` :53
- `get_fabric_directory_client(...)` :58
- `get_fabric_file_client(...)` :68
- `get_fabric_file_client_default_workspace(...)` :76

### ddd_python/ddd_utils/fabric_capacity_pause_resume.py
Fabric capacity management.
- `_capacity_url(suffix: str = "") -> str` :21
- `get_access_token() -> str` :32
- `get_capacity_status(access_token: str) -> str | None` :37
- `wait_for_status(target_status, access_token, poll_interval=10, timeout=300)` :52
- `change_capacity_state(action: str) -> None` :73

### ddd_python/ddd_utils/backup_common.py  [untracked — new]
Shared config for backup and restore scripts.
- Constants: `BACKUP_LOCAL_DIR`, `METABASE_DATA_DIR`, `DAGSTER_HOME`, `DUCKDB_DIR`
- `BACKUP_TARGETS: list[tuple[str, Path]]` — ordered: metabase, dagster, duckdb
- `REPO_ROOT`, `CONTAINERS = ("dagster", "metabase")`
- `stop_containers() -> None` :39 — docker compose stop
- `start_containers() -> None` :44 — docker compose start
- `available_timestamps(local_dir: Path) -> list[str]` :51 — sorted list of backup run timestamps

### ddd_python/ddd_utils/backup_platform.py  [untracked — new]
CLI backup script. Stops containers → waits 5 min → archives → uploads to Hetzner → prunes → restarts.
Entry point: `python -m ddd_python.ddd_utils.backup_platform`
Env vars required: BACKUP_LOCAL_DIR, ENVIRONMENT (DEV|PROD), HETZNER_STORAGEBOX_HOST, HETZNER_STORAGEBOX_USER, HETZNER_STORAGEBOX_REMOTE_DIR
- `_create_archive(name, source, timestamp, local_dir) -> Path` :60
- `_verify_archive(archive_path: Path) -> None` :75
- `_upload_to_hetzner(archive_path: Path, environment: str) -> bool` :87
- `_write_log_record(log_file: Path, record: dict) -> None` :111 — appends NDJSON
- `_purge_old_local_files(local_dir: Path) -> None` :117 — deletes files older than 62 days
- `main() -> None` :137

### ddd_python/ddd_utils/restore_platform.py  [untracked — new]
CLI restore script. Stops containers → extracts archives → rolls back on failure → restarts.
Entry point: `python -m ddd_python.ddd_utils.restore_platform --latest`
             `python -m ddd_python.ddd_utils.restore_platform --timestamp YYYYMMDD_HHMMSS [--targets metabase duckdb]`
Env vars required: BACKUP_LOCAL_DIR
- `_remove_path(p: Path) -> None` :40
- `_resolve_timestamp(local_dir: Path, requested: str | None) -> str` :45 — None → latest
- `_restore_one(name, source, archive_path) -> None` :58 — atomic with .pre_restore_bak rollback
- `main() -> None` :92 — argparse, interactive "yes" confirmation

---

### ddd_python/ddd_dlt/dlt_pipeline_execution_functions.py
Core extraction engine.
- `_scrub_secrets(params: dict) -> dict` :91
- `_json_default(obj: Any) -> str` :99
- `_upload_to_onelake(data, directory_path, file_name)` :125
- `_upload_to_local(data, directory_path, file_name)` :137
- `_upload(data, directory_path, file_name)` :146 — routes to local or onelake
- `_make_destination(...)` :155
- `_resolve_path(...)` :172 (inner method)
- `_serialize_trace(trace: Any) -> dict[str, Any]` :205
- `write_log_to_onelake(...)` :249
- `run_api_to_file_pipeline(...)` :287 — OData extraction with pagination
  - `_iter_odata_pages(initial_url: str)` :364 (inner generator)
- `run_sql_to_file_pipeline(...)` :438 — MySQL extraction
  - `get_sql_data(connection_string, sql_query)` :499 (inner generator)
- `run_file_to_file_pipeline(...)` :536 — OneLake→local copy
- `build_log_dir(source_system_code, pipeline_name=None) -> str` :583
- `execute_pipeline(pipeline_type: str, **kwargs) -> dict[str, Any]` :611 — dispatcher

### ddd_python/ddd_dlt/dlt_run_extraction_pipelines_danish_parliament_data.py
- `run_extraction_pipelines_danish_parliament_data(date_to_load_from, file_names=None)` :22 — ThreadPoolExecutor max_workers=4

### ddd_python/ddd_dlt/dlt_run_extraction_pipelines_rfam.py
- `run_extraction_pipelines_rfam(date_to_load_from)` :42

### ddd_python/ddd_dlt/export_main_silver_to_fabric_silver.py
- `_get_primary_key(table: str) -> str` :27
- `export_single_silver_table(connection, table) -> int` :32 — incremental LEFT JOIN
- `write_tables_to_onelake_silver(connection, tables: list[str]) -> None` :91
- `main() -> None` :113

### ddd_python/ddd_dlt/export_main_gold_to_fabric_gold.py
- `export_single_gold_table(connection, table) -> int` :25 — full overwrite
- `write_tables_to_onelake_gold(connection, tables: list[str]) -> None` :58
- `main() -> None` :80

---

### ddd_python/ddd_dbt/generate_dbt_models.py
- `generate_dbt_models_bronze(...)` :36 — generates bronze + _latest SQL files
- `generate_dbt_models_silver(...)` :90 — generates silver CDC + _cv SQL files; selects incr vs full macro from config
- `generate_dbt_models_gold_cv(table_names: list[str]) -> None` :193 — generates _cv views only

### ddd_python/ddd_dbt/dbt_build_with_unique_logfile.py
- `generate_log_filename() -> str` :25
- `run_dbt_build(log_file_local, models_to_select=None) -> int` :30
- `upload_log_to_azure(log_file_local, log_file_name) -> None` :51
- `main(models_to_select=None) -> None` :62

### ddd_python/ddd_dbt/init_duckdb.py
- `init_duckdb() -> None` :20 — runs duckdb/init_duckdb.sql

---

### ddd_python/ddd_dagster/hooks.py  [new]
Dagster hooks that restart Metabase around job execution.
- `run_shell_script(script_path)` :4 — subprocess.run wrapper
- `@success_hook start_metabase_after_job(context)` :8 — runs `./start_metabase_and_wait.sh`
- `@failure_hook start_metabase_on_failure(context)` :12 — same restart on failure
- `stop_metabase_before_job()` :15 — bare function, not a hook decorator

### ddd_python/ddd_dagster/metabase_control_assets.py  [new]
Dagster assets that stop/start the Metabase container as pipeline bookends.
- `stop_metabase_asset` :6 — `@asset`; runs `./stop_metabase_and_wait.sh`; placed first in every job selection via `_with_metabase_control()`
- `build_start_metabase_asset(upstream_asset_keys) -> AssetsDefinition` :14 — factory; `start_metabase_asset` depends on ALL materialization asset keys so it runs last

### ddd_python/ddd_dagster/assets.py
DDD extraction asset factory.
- `class ExtractionConfig(Config)` :62 — date_to_load_from field
- `_STOP_METABASE_KEY = AssetKey(["stop_metabase_asset"])` :81 — all extraction assets declare this dep
- `_base_name(api_resource) -> str` :93
- `_destination_path(base) -> str` :98
- `_make_incremental_asset(api_resource) -> AssetsDefinition` :108 — asset depends on `_STOP_METABASE_KEY`
- `_make_full_extract_asset(api_resource) -> AssetsDefinition` :207 — asset depends on `_STOP_METABASE_KEY`

### ddd_python/ddd_dagster/rfam_assets.py
- `class RfamExtractionConfig(Config)` :31
- `_destination_path(table_name) -> str` :51
- `_make_incremental_asset(table_name) -> AssetsDefinition` :56
- `_make_full_extract_asset(table_name) -> AssetsDefinition` :133

### ddd_python/ddd_dagster/dbt_assets.py
- `class DbtSilverConfig(Config)` :71
- `class DddDbtTranslator(DagsterDbtTranslator)` :95
  - `get_asset_key(self, dbt_resource_props) -> AssetKey` :118
  - `get_group_name(self, dbt_resource_props) -> str | None` :140
- `dbt_seeds_assets(context, dbt)` :165 — @multi_asset
- `dbt_bronze_assets(context, dbt)` :187 — @multi_asset
- `dbt_silver_assets(context, dbt, config)` :212 — @multi_asset
- `dbt_gold_assets(context, dbt)` :236 — @multi_asset
- `dbt_data_engineering_assets(context, dbt)` :263 — @multi_asset

### ddd_python/ddd_dagster/export_assets.py
- `_STOP_METABASE_KEY = AssetKey(["stop_metabase_asset"])` :47 — declared as dep on barrier + all export assets
- `barrier_dbt_gold_complete() -> None` :60 — @asset barrier; deps include `_STOP_METABASE_KEY`
- `_make_export_silver_asset(table_name) -> AssetsDefinition` :84 — deps include `_STOP_METABASE_KEY`
- `barrier_all_silver_exported() -> None` — @asset barrier
- `_make_export_gold_asset(table_name) -> AssetsDefinition` — deps include `_STOP_METABASE_KEY`
- `barrier_all_gold_exported() -> None` — @asset barrier

### ddd_python/ddd_dagster/jobs.py
Selection helpers + job definitions. All 18 jobs wrap their asset selection with `_with_metabase_control()`.
- `_with_metabase_control(selection) -> AssetSelection` :96 — prepends `stop_metabase_asset`, appends `start_metabase_asset`
- `_seeds_selection()` :223
- `_dbt_select_with_latest(model_names) -> str` :228
- `_dbt_select_with_cv(model_names) -> str` :234
- `_bronze_ddd_selection()` :240 / `_bronze_rfam_selection()` :247 / `_bronze_selection()` :254
- `_silver_ddd_selection()` :258 / `_silver_rfam_selection()` :265 / `_silver_selection()` :272
- `_gold_selection()` :276 / `_data_engineering_selection()` :281
- `_full_pipeline_selection()` :479

### ddd_python/ddd_dagster/definitions.py
Dagster `Definitions` entrypoint. Builds `start_metabase_asset` by passing all materialization asset keys so it runs after every other asset.
- `_asset_keys(asset_defs) -> list[AssetKey]` :56 — flattens AssetsDefinition list to keys
- `start_metabase_asset = build_start_metabase_asset(_asset_keys(_materialization_assets))` :79
- `defs = Definitions(assets=[stop_metabase_asset, ..., start_metabase_asset], ...)` :82

### ddd_python/ddd_dagster/sensors.py
- `_build_and_write_run_summary(...)` :67
- `danish_parliament_run_success_sensor(...)` :177 — @sensor
- `danish_parliament_run_failure_sensor(...)` :200 — @sensor

### ddd_python/ddd_dagster/resources.py
- `class DltOneLakeResource(ConfigurableResource)` :33
  - `execute_pipeline(self, pipeline_type, **kwargs) -> dict` :51
  - `write_job_run_log(self, ...)` :84

### ddd_python/ddd_dagster/_constants.py
- `build_bronze_destination_path(source_system_code, entity_name) -> str` :20

---

## dbt Models

### Bronze (dbt/models/bronze/) — 53 files
Generated by `generate_dbt_models_bronze()`. Pattern: one main view + one `_latest` view per entity.
- DDD (18 entities × 2): bronze_ddd_{afstemning,afstemningstype,aktoer,aktoertype,moede,moedestatus,moedetype,periode,sag,sagskategori,sagsstatus,sagstrin,sagstrinaktoer,sagstrinsstatus,sagstrinstype,sagstype,stemmetype,stemme}.sql + _latest.sql
- Rfam (7 entities × 2): bronze_rfam_{family,genome,clan,clan_membership,author,literature_reference,dead_family}.sql + _latest.sql
- Utility: bronze_dates.sql, bronze_dates_holidays.sql, bronze_source_systems.sql

### Silver (dbt/models/silver/) — 50 files
Generated by `generate_dbt_models_silver()`. Pattern: one CDC table + one `_cv` current-version view.
- DDD (18 × 2): silver_ddd_{entity}.sql + silver_ddd_{entity}_cv.sql
  - Incremental macro: aktoer, moede, sagstrin, sag, sagstrinaktoer, stemme
  - Full-extract macro: remaining 12
- Rfam (7 × 2): silver_rfam_{entity}.sql + silver_rfam_{entity}_cv.sql
  - Incremental macro: family, genome
  - Full-extract macro: remaining 5

### Gold (dbt/models/gold/) — 19 files
- Handcrafted: individual_votes.sql, date.sql, time.sql
- Generated dimensions (10 tables × _cv): actor.sql, actor_cv.sql, actor_type.sql, actor_type_cv.sql, case.sql, case_cv.sql, meeting.sql, meeting_cv.sql, meeting_status.sql, meeting_status_cv.sql, meeting_type.sql, meeting_type_cv.sql, vote.sql, vote_cv.sql, vote_type.sql, vote_type_cv.sql

### Data Engineering (dbt/models/data_engineering/) — 8 files
Dagster SQLite observability: dagster_asset.sql, dagster_asset_materialization.sql, dagster_event_logs.sql, dagster_job.sql, dagster_pipeline_runs.sql, dagster_run.sql, dagster_step_failure.sql
Python model: dagster_step_failures_raw.py

### dbt Macros (dbt/macros/) — 9 files
- cast_hash_to_bigint.sql — UBIGINT→BIGINT
- generate_base_for_hash.sql — build column list for SHA256
- generate_model_bronze.sql — bronze view factory
- generate_model_bronze_latest.sql — latest snapshot view
- generate_model_silver_incr_extraction.sql — CDC for incremental tables
- generate_model_silver_full_extraction.sql — CDC for full-extract tables
- generate_pre_hook_silver.sql — temp table for delete detection
- generate_pre_hook_silver_full_refresh.sql — pre-hook for full refresh
- generate_post_hook_silver.sql — drop temp table

---

## Tests (tests/) — 93 tests, 12 modules

| File | Key test functions |
|------|--------------------|
| conftest.py | `mock_fabric_clients` fixture |
| test_configuration_variables.py | entity counts, subset checks, no-dupes, PK coverage |
| test_generate_dbt_models.py | macro selection (incr vs full), _cv view generation |
| test_export_silver.py | incremental append, first load, skip-on-no-rows, Rfam PKs |
| test_export_gold.py | overwrite, row count, target path |
| test_integration_bronze.py | JSON read, filename extraction, _latest view |
| test_integration_silver_cdc.py | I/U/D detection, _cv view, NOT EXISTS dedup |
| test_integration_gold.py | surrogate keys, SCD2 chaining, fact joins, unknown row |
| test_integration_e2e_pipeline.py | Bronze→Silver→Delta Lake round-trip |
| test_json_default.py | datetime/date/time serialization |
| test_require_env.py | lazy env var loading |
| test_scrub_secrets.py | sensitive key redaction |
| test_serialize_trace.py | dlt trace serialization |

---

## Environment Variables (.env.example)

| Variable | Purpose |
|----------|---------|
| STORAGE_TARGET | local \| onelake |
| LOCAL_STORAGE_PATH | root for local file storage |
| DANISH_DEMOCRACY_DATA_SOURCE | path to DDD Bronze files |
| DANISH_DEMOCRACY_BASE_URL | https://oda.ft.dk/api |
| DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD | default 31 |
| RFAM_CONNECTION_STRING | mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam |
| RFAM_DATA_SOURCE | path to Rfam Bronze files |
| RFAM_DEFAULT_DAYS_TO_LOAD | default 365 |
| DAGSTER_HOME | path to .dagster/ directory |
| DBT_PROJECT_DIRECTORY | path to dbt/ folder |
| DBT_MODELS_DIRECTORY | path to dbt/models/ |
| DBT_LOGS_DIRECTORY | local dbt log path |
| DBT_FRESHNESS_WARN_AFTER_DAYS / DBT_FRESHNESS_ERROR_AFTER_DAYS | 2 / 7 |
| DLT_PIPELINES_DIR | dlt incremental state |
| DLT_PIPELINE_RUN_LOG_DIR | OneLake log path |
| DUCKDB_DATABASE_LOCATION | path to .duckdb file |
| DUCKDB_DATABASE | database name |
| AZURE_TENANT_ID / AZURE_CLIENT_ID / AZURE_CLIENT_SECRET | service principal |
| AZURE_SUBSCRIPTION_ID / AZURE_RESOURCE_GROUP | for capacity management |
| FABRIC_CAPACITY_NAME | Fabric capacity name |
| FABRIC_WORKSPACE | Fabric workspace name |
| FABRIC_ONELAKE_STORAGE_ACCOUNT | onelake |
| FABRIC_ONELAKE_FOLDER_BRONZE/SILVER/GOLD | OneLake folder paths |
| ENVIRONMENT | DEV \| PROD (backup script) |
| BACKUP_LOCAL_DIR | local archive directory |
| METABASE_DATA_DIR | Metabase data volume path |
| HETZNER_STORAGEBOX_HOST/USER/PORT/REMOTE_DIR | Hetzner SSH upload target |
| HETZNER_STORAGEBOX_SSH_KEY | optional SSH key path |

---

## Key non-Python files

| File | Purpose |
|------|---------|
| pyproject.toml | deps: dlt≥1.24, dbt-core≥1.10<1.12, dbt-duckdb≥1.10, duckdb≥1.5.1<1.6, dagster≥1.12<2, deltalake≥1.5, pyarrow≥17 |
| dbt/dbt_project.yml | dbt config, schema assignments, materialization defaults |
| dbt/profiles.yml | local (DuckDB file) + onelake (DuckDB + Azure) targets |
| dbt/packages.yml | dbt-utils 1.3.0, dbt-expectations 0.10.4 |
| dbt/seeds/LKHS_source_systems.csv | source system registry |
| dbt/seeds/publicholiday_dk.csv | Danish public holidays |
| dbt/tests/silver_no_duplicate_id_date_valid_from.sql | custom dbt test |
| duckdb/init_duckdb.sql | installs extensions, creates Azure secret |
| docker-compose.yml | services: run (one-off), dagster (UI + mounts `/var/run/docker.sock`), metabase (port 3001, UID 2000, 4 GB limit) |
| start_metabase_and_wait.sh | `docker start ddd-metabase` + wait 120 s; falls back to `sudo docker` if needed |
| stop_metabase_and_wait.sh | `docker stop ddd-metabase` + wait 120 s; same sudo fallback |
| workspace.yaml | Dagster workspace — loads ddd_dagster.definitions |
| .github/workflows/ci.yml | CI pipeline |
| documentation/silver_model_logic.md | Silver CDC logic documentation |
| documentation/dbt_macros.md | macro documentation |
