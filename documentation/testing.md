# Testing

This document describes the project's automated test strategy — **what** is
tested, **how** it is tested, **why** those choices were made, and **what is
deliberately left out of scope**.

The project has two complementary layers of automated tests:

| Layer | Tool | Scope |
| --- | --- | --- |
| **Python tests** | `pytest` | The Python codebase + the SQL *logic* of the dbt models, run against an ephemeral DuckDB |
| **Data-quality tests** | `dbt test` | The *materialised data* in DuckDB after a pipeline run |

The two layers answer different questions. The pytest layer answers *"is the
code and the transformation logic correct?"* and runs with no external
dependencies. The dbt layer answers *"is the data that landed in the warehouse
valid?"* and runs against a populated database.

---

## 1. Python tests (`pytest`)

### 1.1 Design principles

The Python suite is built around four deliberate constraints:

1. **No credentials, no network, no cloud.** Every test runs offline. There is
   no call to the Danish Parliament API, the Rfam MySQL server, Azure, or
   OneLake. Tests that touch those boundaries replace them with mocks. This
   means the whole suite runs in seconds on any laptop or in any sandbox, and
   never fails because an external service is down.

2. **Real DuckDB, mocks for the rest.** The transformation logic is SQL, so it
   is tested by running SQL. The integration tests use an **in-memory DuckDB**
   (`duckdb.connect(":memory:")`) and run SQL that mirrors the dbt macros. Only
   external systems (Delta Lake target storage, Fabric token issuer,
   environment variables) are mocked.

3. **Fixtures on disk, not in fixtures-as-strings.** Bronze/Silver/E2E tests
   write real `.json` extraction files into pytest's `tmp_path`, then point
   DuckDB's `read_json_auto()` / `read_text()` at them — exactly as the real
   Bronze layer does. This exercises the filename-parsing and globbing logic
   that string-based fixtures would bypass.

4. **The config file is the contract.** Because the entire codebase derives
   from `configuration_variables.py` (entity lists, primary keys, model names),
   a dedicated test module asserts that file's internal consistency. If someone
   adds an entity to one list but forgets the parallel list, these tests fail
   before anything else does.

### 1.2 How the integration tests work

The Bronze, Silver-CDC, Gold, and end-to-end modules do **not** invoke dbt.
Instead they reproduce the SQL emitted by the dbt macros directly in DuckDB.
This is a conscious trade-off:

- **Pro:** tests run in milliseconds, need no dbt profile, no `dbt parse`, no
  warehouse, and pinpoint logic bugs in isolation.
- **Con:** the reproduced SQL must be kept in step with the macros it mirrors.
  The test files name the macro they mirror at the top (e.g.
  `generate_model_silver_full_extraction`) so the relationship is explicit.

A typical Silver-CDC test:

1. The `silver_fixture_dir` fixture writes **three** extraction files for a fake
   `thing` entity that simulate a full lifecycle — row inserted, row changed,
   row deleted — across three dated snapshots.
2. `_build_full_extract_cdc_sql()` returns the CDC SQL (hash + `LAG`/`LEAD`
   window functions, NOT-EXISTS dedup, delete detection) with hard-coded values
   in place of Jinja.
3. The test runs that SQL in in-memory DuckDB and asserts the resulting
   `LKHS_cdc_operation` values, row counts, `_cv` view contents, etc.

### 1.3 What each module covers

#### Unit tests (pure Python, no DuckDB)

| Module | What / How / Why |
| --- | --- |
| `test_configuration_variables.py` | **What:** the single-source-of-truth config lists. **How:** asserts subset relationships, parallel Bronze↔Silver derivation, primary-key coverage, and the absence of duplicates. **Why:** adding an entity touches several lists; this catches a half-finished edit immediately. |
| `test_generate_dbt_models.py` | **What:** the dbt model-generator (`generate_dbt_models.py`). **How:** generates SQL into a temp dir and asserts the right macro is called per model, that **incremental vs full-extraction selection is derived from config**, not hardcoded, and that only a key of two or more columns adds `LKHS_primary_key_1_column` (also checked against the committed models). **Why:** a wrong macro choice would silently corrupt CDC for an entity. |
| `test_string_utils.py` | **What:** `normalize_danish_name` (ø→oe, æ→ae, å→aa, lowercasing) and `resolve_date_to_load_from` (incremental load-date validation/derivation). **How:** parametrised input→output assertions plus error cases. **Why:** these functions map API names to filesystem/schema identifiers everywhere — a regression would misroute every entity. |
| `test_path_utils.py` | **What:** `build_bronze_destination_path` and `build_delta_export_path`. **How:** patches env vars + the Fabric token, then asserts local, S3, and OneLake path construction and `storage_options` (including S3 endpoint, URL style, and credential handling). **Why:** every export and Bronze write depends on these paths; all branches must produce correct paths and storage options. |
| `test_require_env.py` | **What:** the `_require` lazy env-var helper. **How:** sets/unsets env vars via `monkeypatch` and asserts a clear `OSError` naming the variable on missing/empty values. **Why:** confirms importing modules for codegen/testing does not fail when credentials are absent (the `__getattr__` lazy-loading pattern). |
| `test_scrub_secrets.py` | **What:** `_scrub_secrets`, which redacts secrets before logging dlt run params. **How:** asserts case-insensitive redaction of `secret`/`password`/`token`/`connection_string` keys while preserving non-sensitive values. **Why:** prevents credentials leaking into logs/traces. |
| `test_serialize_trace.py` | **What:** `_serialize_trace`, which turns a dlt trace object into a JSON-safe dict. **How:** feeds `None` and mocked trace objects and checks the serialised shape (timestamps, steps, failed jobs). **Why:** trace logging must never crash the pipeline. |
| `test_json_default.py` | **What:** the custom JSON `default` serializer. **How:** round-trips `datetime`/`date`/`time` and other edge-case types. **Why:** extraction writes NDJSON; non-serialisable types would abort a run. |
| `test_sensors.py` | **What:** the run-status sensors' helpers. **How:** stubs `requests.post` and the Dagster instance; asserts ntfy.sh headers/payload, step counting and that alert or log failures never raise. **Why:** a sensor that crashes hides the very failure it should report. |
| `test_dbt_assets.py` | **What:** the dbt command of the Gold asset. **How:** a `DbtCliResource` that records its arguments; the Gold build must exclude `data_engineering`. **Why:** dbt selects every test that references a selected model, so the `data_engineering` relationship tests to `date` and `time` would otherwise run in the Gold step, before their models exist (the first production run with `silver_load_history` failed on it). |
| `test_silver_row_counts.py` | **What:** `stream_with_silver_row_counts` and `silver_row_count_metadata` in `dbt_assets.py`. **How:** a fake dbt invocation streams the post-hook's `silver_row_counts {…}` line and result events; the counts must land on the materialization of the model that logged them and on no other, and a pure-JSON log line (dagster-dbt's column metadata) is ignored. **Why:** keeps the Dagster side covered in the default run; the real path runs in `test_dbt_silver.py`. |
| `test_dbt_quack.py` | **What:** the `dbt-quack` wrapper (`dbt_quack.py`). **How:** stubs the patch function and dbt's runner; asserts that the patches are applied only with `QUACK_SERVER_HOST` (with a warning without it), that a dbt failure gives a non-zero exit code, and that `QUACK_SKIP_PATCHES` is parsed. **Why:** in Docker every dbt run goes through these patches. |
| `test_duckdb_quack_server.py` | **What:** the quack server start-up (`duckdb_quack_server.py`). **How:** unit tests with a mocked DuckDB connection, no server started; assert the token check and its SQL escaping, the DuckLake attach (and the errors for missing catalog or data path) and the signal handling. **Why:** every container depends on this one process. |
| `test_dbt_build.py` | **What:** the dbt JSON-log check in `dbt_build_with_unique_logfile`. **How:** writes NDJSON log fixtures and asserts that `fail`/`error` nodes raise. **Why:** dbt can exit 0 with failing tests; the run must still fail so the failure sensor fires. |
| `test_init_duckdb.py` | **What:** S3 secret DDL generation. **How:** mocked connection; asserts endpoint, URL style, SSL and quoting. **Why:** a wrong secret only shows up later as a confusing S3 403. |
| `test_backup_targets.py` | **What:** backup target assembly and the DuckLake ordering invariant. **How:** asserts that DuckLake data files are backed up before the catalog, that retention ages match, and that the catalog lives in the DuckDB target directory. **Why:** in DuckLake mode the catalog references Parquet files; files-first ordering ensures the catalog snapshot never points at an un-backed-up file. |

#### Integration tests (real in-memory DuckDB)

| Module | What / How / Why |
| --- | --- |
| `test_integration_bronze.py` | **What:** the Bronze view pattern. **How:** writes two dated JSON files, then runs `read_json_auto(..., filename=True)` to verify it reads all rows from all files, extracts `LKHS_filename` from the path, and excludes `_dlt_*` and raw `filename` columns. **Why:** every downstream layer reads Bronze, so wrong file or column handling affects all of them. |
| `test_integration_silver_cdc.py` | **What:** the hash-based CDC / SCD Type 2 engine of the full-extract macro. **How:** a 3-file lifecycle fixture drives assertions on Insert/Update/Delete detection, `LKHS_date_valid_from` derived from the filename timestamp, the NOT-EXISTS guard against re-inserting unchanged rows, and the `_cv` current-version view (one row per PK, latest version, delete handling). The incremental macro, including its deletes from `_full` files, is covered by `test_dbt_silver.py`. **Why:** a fast check of the CDC rules without dbt. |
| `test_integration_gold.py` | **What:** the Gold star-schema transformations. **How:** pre-populates Silver-like tables, then verifies surrogate-key generation (`cast_hash_to_bigint` — deterministic, unsigned→signed `BIGINT`), business keys (`source_system_code + '-' + id`), SCD2 `date_valid_to` via `LEAD`, version numbering via `ROW_NUMBER`, fact→dimension joins, the unknown/default row (`id = 0`), and filtering of deleted rows in facts. **Why:** Gold is what BI tools and Power BI consume; surrogate-key or join bugs surface as wrong dashboards. |
| `test_integration_e2e_pipeline.py` | **What:** the full Bronze→Silver→Delta Lake path end-to-end. **How:** writes JSON fixtures, runs the Bronze read + Silver CDC, materialises a Silver table, exports it to a **real local Delta Lake table** via `write_deltalake`, reads it back, and verifies row counts/PKs/CDC ops — then runs a **second incremental export** and asserts no duplicates. **Why:** checks that the layers work together and that incremental Delta appends are idempotent. |
| `test_export_silver.py` | **What:** the Silver→Delta incremental export across local, S3, and OneLake targets. **How:** in-memory DuckDB Silver table + patched `DeltaTable`/`write_deltalake`/env vars; asserts that only new rows are appended (LEFT JOIN on PK + `LKHS_date_valid_from`), first-load overwrite behaviour, DDD vs Rfam primary-key handling (also a key of several columns), and S3/OneLake path and storage-option routing. **Why:** an incorrect append predicate would either duplicate history or drop rows; S3/OneLake path handling bugs would target the wrong storage backend. |
| `test_export_gold.py` | **What:** the Gold→Delta full-overwrite export across local, S3, and OneLake targets. **How:** patched `write_deltalake` + mocked env vars; asserts `mode='overwrite'`, the returned row count, correct target paths (local filesystem, s3://, or abfss://), and storage options (empty for local, AWS credentials for S3, bearer token for OneLake). **Why:** Gold is rebuilt every run; it must overwrite, not append. Storage backend routing bugs would send data to the wrong destination. |
| `test_pipeline_execution.py` | **What:** `execute_pipeline` and `run_extraction_pool`. **How:** real temp directories and file I/O; asserts one NDJSON log record per run (success and failure) and that the pool attempts every task before raising. **Why:** the log record is the audit trail when extraction fails. |
| `test_full_extract_naming.py` | **What:** which Bronze files get the `_full` suffix. **How:** builds the tasks of both extraction CLIs and runs the Dagster extraction assets with a stubbed pipeline; asserts only incremental entities extracted from `1900-01-01` or earlier are named `_full`. **Why:** Silver deletes a key of an incremental entity only when it is missing from a `_full` file. |
| `test_dlt_file_naming.py` | **What:** the Bronze file name dlt writes. **How:** runs a real dlt filesystem load into `tmp_path`. **Why:** dlt ≥ 1.30 changed how it appends extensions; a wrong name breaks the `*.json*` globs in Bronze and Silver. |
| `test_dbt_silver.py` | **What:** the Silver layer with the real macros: both Silver macros, their hooks and the `_cv` views. **How:** runs `dbt` (dbt-core) on copies of the CI sample and checks every case of [dbt_transformations.md §3.10](dbt_transformations.md#310-all-cases-at-a-glance) and the building blocks of §3.1: the hash (column order, NULL token, delimiter), file-name variants, the `_last_file` bookmark and the `_files` log (also converted from an older version), `LKHS_` columns, insert/update/delete histories including keys deleted and back twice, the comparison with Silver's state, deletes of incremental entities from a `_full` file processed on its own (and the error when it is not), ignored full refreshes, first builds, normal runs, the row counts the last post-hook logs and Dagster attaches to the materialization, several files in one run (same result as one run per file), the delete guard and its switch, late files that stop the run until they are skipped, column types Bronze infers differently, archived Bronze files, added and dropped source fields, a key twice in one file, and primary keys of several columns, including the migration of an existing table to one (no source has one, so the tests give `stemme` and `periode` one on a copy of the dbt project with their models regenerated). The lifecycles also assert the Silver invariants (no duplicate versions, file name matches `LKHS_date_valid_from`, `D` rows carry the deleted version, `_cv` holds the newest version). DuckDB and DuckLake mode; DDD and Rfam entities. Skipped unless `DBT_INTEGRATION_TESTS=1` and `dbt` is on `PATH`; about 25–30 minutes. **Why:** the Silver logic is the core of the project, and a mirror of the SQL cannot show that the macros themselves are right. |
| `test_dbt_data_engineering.py` | **What:** the `data_engineering` models over Dagster events. **How:** materializes an extraction, a Silver table, a Gold view and an export asset in a temporary Dagster instance with the storage layout of the Docker entrypoint, builds the models with dbt and checks `rows_processed` and the Silver row counts in `dagster_event_logs`, `dagster_asset_materialization` and `dagster_run`; a dbt model's `Execution Duration` must not count as rows. Also builds all 25 Silver tables on the sample and checks `silver_load_history` over a first build, a run with a change and a delete and a run whose file changes nothing, per table against the table's row count, in DuckDB and DuckLake mode. Skipped unless `DBT_INTEGRATION_TESTS=1` and `dbt` is on `PATH`; about 2–3 minutes. **Why:** CI's sample build excludes these models, because they need a Dagster history. |
| `test_integration_s3_pipeline.py` | **What:** the S3 code paths end to end. **How:** Bronze → Silver → Delta round-trip against a live S3-compatible server; skipped unless `S3_TEST_ENDPOINT` is set (CI runs it against RustFS). **Why:** mocks cannot check that path style, secrets and URIs line up. |
| `test_mark_full_extracts.py` | **What:** which Bronze files are full extractions and the rename that marks them. **How:** temporary Bronze directories with files of chosen sizes and run-log records; asserts the log decides first (a large partial extraction is not full), size decides only without a log record, and the rename never overwrites. |
| `test_archive_bronze.py` | **What:** the Bronze archive script and `bronze_archive_job`. **How:** temporary Bronze directories and an in-memory Silver bookmark table; asserts that everything processed goes except the newest processed file and files younger than `--keep-days`, that late files (not in the `_files` log) stay, that archive, restore and re-archive are checksum-verified and never overwrite different content, and that the Dagster asset passes its config on. |
| `test_extraction_assets.py` | **What:** the DDD and Rfam extraction assets. **How:** materialises the real Dagster assets with only `DltPipelineResource.execute_pipeline` stubbed; asserts OData filters, SQL params, destination paths and that a malformed date never reaches the API. |
| `test_export_assets.py` | **What:** the Delta export assets, `write_tables_*` and the export CLIs. **How:** stubbed connection/export functions; asserts every table is attempted before a failure is raised and connections are always closed. |
| `test_ducklake_cleanup.py` | **What:** the DuckLake vacuum asset. **How:** a real local DuckLake catalog in `tmp_path`; asserts `*_current_temp` directories are removed and live `*__dbt_tmp` data is never touched. |
| `test_ducklake_maintenance.py` | **What:** the `*_current_temp` sweep and its registration as the SQL function `ducklake_sweep_current_temp`. **How:** real directories in `tmp_path` and an in-memory DuckDB; asserts the age limit and that `__dbt_tmp` stays. |
| `test_dagster_asset_graph.py` | **What:** the Dagster asset graph structure. **How:** loads the Definitions object with minimal env vars; asserts extraction assets, dbt Bronze/Silver/Gold assets, export assets, schedules, sensors, and key jobs are registered. **Why:** catches graph regressions during refactoring. Tests skip gracefully when `dbt/target/manifest.json` is absent. |

### 1.4 Shared fixtures

`conftest.py` provides `mock_fabric_clients`, which intercepts the
lazily-imported `get_fabric_onelake_clients` module via `patch.dict` on
`sys.modules` and returns a fake token. This lets the export tests run the real
code path up to the storage boundary without any Azure credentials.

The export tests also use a `_patch_env()` helper that patches env vars on the
module's `get_variables_from_env.__dict__` directly — bypassing the `__getattr__`
lazy-loader that would otherwise raise for required-but-absent vars in a
credential-free environment.

### 1.5 What the Python suite does not cover

These are left out of scope on purpose:

- **Live source systems.** No test calls the real OData API or Rfam MySQL.
  Network behaviour, auth, and pagination are out of scope (and would make the
  suite flaky).
- **Real OneLake / Azure.** Delta writes are tested against the **local**
  filesystem; the OneLake branch is verified only at the path-construction level
  (`test_path_utils.py`) and via mocks. No test authenticates to Fabric.
- **Dagster runtime behaviour.** The asset graph, job/schedule/sensor
  registration, the extraction/export assets and the sensor helpers are tested, but no test runs a whole job through the
  Dagster daemon (schedule firing, executor behaviour, sensor ticks). Those are
  validated by running the pipeline.
- **A full `dbt build`.** The integration tests mirror the macro SQL rather than
  invoking dbt, so a mismatch between a macro and its mirror is caught by the CI
  `dbt build` on the committed sample and by the dbt-layer tests below. The
  exceptions are `test_dbt_silver.py` and `test_dbt_data_engineering.py`, which
  run dbt for the Silver layer and the `data_engineering` models, only when
  `DBT_INTEGRATION_TESTS=1`.

---

## 2. Data-quality tests (`dbt test`)

Where the Python suite validates *logic*, the dbt tests validate the *data* that
exists in DuckDB after the pipeline has run. They are defined in the schema YAML
files next to the models (`dbt/models/*/__*.yml`) plus one custom singular test.

### 2.1 Generic tests

Defined declaratively in YAML. Counts across the project (all 322 data tests,
including the `data_engineering` layer; `dbt ls --resource-type test`):

| Test type | Count | Purpose |
| --- | --- | --- |
| `not_null` | 184 | Required columns (primary keys, `LKHS_` tracking columns, key business fields) are never null. |
| `unique` | 52 | Keys that must be unique (e.g. surrogate keys in Gold dimensions, PKs in `_cv` views). |
| `accepted_values` | 53 | Enumerations stay within their allowed set — notably `LKHS_cdc_operation ∈ {I, U, D}`. |
| `relationships` | 24 | Foreign keys in Gold and the observability facts resolve to their dimension. |
| `dbt_utils.unique_combination_of_columns` | 7 | Composite grains, e.g. `(id, LKHS_date_valid_from)` on Silver tables. |
| `dbt_utils.expression_is_true` | 1 | Row-level business rules (vote counts). |

### 2.2 Custom singular test

`dbt/tests/silver_no_duplicate_id_date_valid_from.sql` enforces the **SCD Type 2
grain** across all 18 DDD Silver tables: there must be no duplicate
`(key, LKHS_date_valid_from)` pair, the key being the column Silver keys on (`id`,
or `LKHS_primary_key_1_column` for a key of several columns). The test is
generated by `generate_dbt_models` as a Jinja loop that
`UNION ALL`s a `GROUP BY ... HAVING COUNT(*) > 1` over every Silver table and
passes when the result set is empty. It checks that the CDC logic never writes
two versions of the same entity with the same validity start, which is the main
invariant of the Silver layer.

### 2.3 Running the dbt tests

```bash
cd dbt
dbt test --profiles-dir .                  # all data-quality tests
dbt test --select silver --profiles-dir .  # Silver tests only
```

dbt tests require a populated database, so run them **after** a pipeline build.
In Docker run them with `dbt-quack` instead of `dbt` (for example
`docker compose run --rm run bash -c "cd /app/dbt && dbt-quack test --profiles-dir ."`).
To browse lineage and model/column documentation, generate the static dbt
docs site on demand (it is build output and is git-ignored):

```bash
dbt docs generate --project-dir dbt --profiles-dir dbt
```

dbt-core writes `index.html`, `manifest.json` and `catalog.json` to `dbt/target/`;
serve that folder with any static file server (CI publishes the same files to
GitHub Pages).

---

## 3. Running the Python suite

No cloud credentials are required.

```bash
pip install -e ".[dev]"     # installs pytest

pytest tests/                                  # run all tests
pytest tests/ -v                               # verbose
pytest tests/test_integration_silver_cdc.py    # one module
pytest -k "incremental"                        # keyword filter
pytest -k "cv"                                  # only current-version view tests

# real dbt runs of the Silver layer and the data_engineering models (dbt on PATH, about 30 minutes)
DBT_INTEGRATION_TESTS=1 pytest tests/test_dbt_silver.py tests/test_dbt_data_engineering.py
```

Tests are collected from `tests/` (configured via `[tool.pytest.ini_options]` in
`pyproject.toml`).

---

## 4. Continuous integration

Every push and pull request to `main` runs `.github/workflows/ci.yml`:

| Job | What it checks |
| --- | --- |
| `pytest` | The Python suite on a clean checkout, with an 80% coverage floor. |
| `pytest (S3-compatible)` | The S3 code paths against a real S3-compatible server (RustFS). |
| `ruff + mypy` | Lint, formatting and type checks. |
| `dbt parse` | Every SQL/YAML file parses on all three storage targets (`local`, `local_ducklake`, `local_ducklake_s3`). |
| `dbt build (sample data)` | A real Bronze → Silver → Gold `dbt build` including **all dbt tests** except those of `data_engineering` (excluded, because they need a Dagster history) on a committed 1.6 MB sample ([`tests/fixtures/bronze_sample`](../tests/fixtures/bronze_sample)), followed by a non-empty check on key tables (`scripts/ci_check_sample_build.py`) and the real-dbt tests of the Silver layer and the `data_engineering` models (`test_dbt_silver.py`, `test_dbt_data_engineering.py`). |
| `docker build` | The three images build (`ddd-dagster`, `ddd-duckdb-server`, `ddd-backup`). In `ddd-dagster` the `dbt` and `dbt-quack` commands and the Dagster definitions load; in `ddd-duckdb-server` the quack server module and numpy import; in `ddd-backup` the backup and restore modules import and the Docker CLI, rsync and ssh are present. |
| `publish dbt docs` | On `main` only, and only when repo variable `ENABLE_GITHUB_PAGES_PUBLISH=true`: the dbt docs site is generated from the sample build and published to GitHub Pages. |

The sample is cut from a full extraction by `scripts/build_ci_sample.py` (a few
dozen votes plus every row they reference, lookups in full). Refresh it only when
the source schema changes.

Run everything locally before pushing: `ruff check . && ruff format --check . &&
mypy --package ddd_python && pytest tests/`, plus the `dbt build` command shown in
the README.

## 5. When to run tests

Pushes and pull requests are gated by CI (above). Production deploys are still
performed manually with [`scripts/deploy.sh`](../scripts/deploy.sh), so before a
deploy:

1. Confirm CI is green on the commit being deployed.
2. After a pipeline build on a populated database, `dbt test` (`dbt-quack test`
   in Docker) checks the real data. The CI sample tests the logic, not today's
   data.

CI checks the code and the transformation logic; `dbt test` on production checks
the data that was loaded. A deploy needs both to pass.
