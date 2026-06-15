# Testing

This document describes the project's automated test strategy — **what** is
tested, **how** it is tested, **why** those choices were made, and **what is
deliberately left out of scope**.

The project has two complementary layers of automated tests:

| Layer | Tool | Count | Scope |
| --- | --- | --- | --- |
| **Python tests** | `pytest` | 132 tests across 15 modules | The Python codebase + the SQL *logic* of the dbt models, run against an ephemeral DuckDB |
| **Data-quality tests** | `dbt test` | ~263 tests (262 generic + 1 custom) | The *materialised data* in DuckDB after a pipeline run |

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

2. **Real DuckDB, fake everything else.** The transformation logic *is* SQL, so
   testing it against a mock would prove nothing. Instead, the integration
   tests spin up a genuine **in-memory DuckDB** (`duckdb.connect(":memory:")`)
   and run SQL that mirrors the dbt macros. Only the truly external systems
   (Delta Lake target storage, Fabric token issuer, environment variables) are
   mocked.

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

| Module | Tests | What / How / Why |
| --- | --- | --- |
| `test_configuration_variables.py` | 15 | **What:** the single-source-of-truth config lists. **How:** asserts counts (18 DDD entities, 6 incremental, 7 Rfam tables), subset relationships, parallel Bronze↔Silver derivation, primary-key coverage, and the absence of duplicates. **Why:** adding an entity touches several lists; this catches a half-finished edit immediately. |
| `test_generate_dbt_models.py` | 17 | **What:** the dbt model-generator (`generate_dbt_models.py`). **How:** generates SQL into a temp dir and asserts the right macro is called per model — crucially that **incremental vs full-extraction selection is derived from config**, not hardcoded. **Why:** a wrong macro choice would silently corrupt CDC for an entity. |
| `test_string_utils.py` | 20 | **What:** `normalize_danish_name` (ø→oe, æ→ae, å→aa, lowercasing) and `resolve_date_to_load_from` (incremental load-date validation/derivation). **How:** parametrised input→output assertions plus error cases. **Why:** these functions map API names to filesystem/schema identifiers everywhere — a regression would misroute every entity. |
| `test_path_utils.py` | 12 | **What:** `build_bronze_destination_path` and `build_delta_export_path`. **How:** patches env vars + the Fabric token, then asserts local vs OneLake path construction and `storage_options`. **Why:** the local/OneLake switch is the project's central abstraction; both branches must produce correct paths. |
| `test_require_env.py` | 3 | **What:** the `_require` lazy env-var helper. **How:** sets/unsets env vars via `monkeypatch` and asserts a clear `EnvironmentError` on missing/empty values. **Why:** confirms importing modules for codegen/testing does not fail when credentials are absent (the `__getattr__` lazy-loading pattern). |
| `test_scrub_secrets.py` | 8 | **What:** `_scrub_secrets`, which redacts secrets before logging dlt run params. **How:** asserts case-insensitive redaction of `secret`/`password`/`token`/`connection_string` keys while preserving non-sensitive values. **Why:** prevents credentials leaking into logs/traces. |
| `test_serialize_trace.py` | 4 | **What:** `_serialize_trace`, which turns a dlt trace object into a JSON-safe dict. **How:** feeds `None` and mocked trace objects and checks the serialised shape (timestamps, steps, failed jobs). **Why:** trace logging must never crash the pipeline. |
| `test_json_default.py` | 5 | **What:** the custom JSON `default` serializer. **How:** round-trips `datetime`/`date`/`time` and other edge-case types. **Why:** extraction writes NDJSON; non-serialisable types would abort a run. |

#### Integration tests (real in-memory DuckDB)

| Module | Tests | What / How / Why |
| --- | --- | --- |
| `test_integration_bronze.py` | 5 | **What:** the Bronze view pattern. **How:** writes two dated JSON files, then runs `read_json_auto(..., filename=True)` to verify it reads all rows from all files, extracts `LKHS_filename` from the path, selects only the newest file in the `_latest` variant, and excludes `_dlt_*` and raw `filename` columns. **Why:** Bronze is the foundation; wrong file/column handling poisons every downstream layer. |
| `test_integration_silver_cdc.py` | 11 | **What:** the hash-based CDC / SCD Type 2 engine. **How:** a 3-file lifecycle fixture drives assertions on Insert/Update/Delete detection, `LKHS_date_valid_from` derived from the filename timestamp, the NOT-EXISTS guard against re-inserting unchanged rows, and the `_cv` current-version view (one row per PK, latest version, delete handling). **Why:** this is the most intricate logic in the project and the easiest to break subtly. |
| `test_integration_gold.py` | 11 | **What:** the Gold star-schema transformations. **How:** pre-populates Silver-like tables, then verifies surrogate-key generation (`cast_hash_to_bigint` — deterministic, unsigned→signed `BIGINT`), business keys (`source_system_code + '-' + id`), SCD2 `date_valid_to` via `LEAD`, version numbering via `ROW_NUMBER`, fact→dimension joins, the unknown/default row (`id = 0`), and filtering of deleted rows in facts. **Why:** Gold is what BI tools and Power BI consume; surrogate-key or join bugs surface as wrong dashboards. |
| `test_integration_e2e_pipeline.py` | 4 | **What:** the full Bronze→Silver→Delta Lake path end-to-end. **How:** writes JSON fixtures, runs the Bronze read + Silver CDC, materialises a Silver table, exports it to a **real local Delta Lake table** via `write_deltalake`, reads it back, and verifies row counts/PKs/CDC ops — then runs a **second incremental export** and asserts no duplicates. **Why:** proves the layers compose correctly and that incremental Delta appends are idempotent. |
| `test_export_silver.py` | 7 | **What:** the Silver→Delta incremental export. **How:** in-memory DuckDB Silver table + patched `DeltaTable`/`write_deltalake`/Fabric token/env vars; asserts that only genuinely new rows are appended (LEFT JOIN on PK + `LKHS_date_valid_from`), first-load overwrite behaviour, and that DDD vs Rfam primary keys are honoured. **Why:** an incorrect append predicate would either duplicate history or drop rows. |
| `test_export_gold.py` | 3 | **What:** the Gold→Delta full-overwrite export. **How:** patched `write_deltalake`; asserts `mode='overwrite'`, the returned row count, and the `abfss://.../Gold/<table>/` target path. **Why:** Gold is rebuilt every run; it must overwrite, not append. |

### 1.4 Shared fixtures

`conftest.py` provides `mock_fabric_clients`, which intercepts the
lazily-imported `get_fabric_onelake_clients` module via `patch.dict` on
`sys.modules` and returns a fake token. This lets the export tests run the real
code path up to the storage boundary without any Azure credentials.

The export tests also use a `_patch_env()` helper that patches env vars on the
module's `get_variables_from_env.__dict__` directly — bypassing the `__getattr__`
lazy-loader that would otherwise raise for required-but-absent vars in a
credential-free environment.

### 1.5 What the Python suite deliberately does **not** cover

These are intentional scope boundaries, not gaps to be silently fixed:

- **Live source systems.** No test calls the real OData API or Rfam MySQL.
  Network behaviour, auth, and pagination are out of scope (and would make the
  suite flaky).
- **Real OneLake / Azure.** Delta writes are tested against the **local**
  filesystem; the OneLake branch is verified only at the path-construction level
  (`test_path_utils.py`) and via mocks. No test authenticates to Fabric.
- **Dagster orchestration wiring.** Job composition, schedules, sensors, and the
  Metabase start/stop assets are not unit-tested; they are validated by running
  the pipeline.
- **A full `dbt build`.** The integration tests mirror the macro SQL rather than
  invoking dbt, so a mismatch between a macro and its mirror is caught by code
  review and by the dbt-layer tests below — not by pytest.

---

## 2. Data-quality tests (`dbt test`)

Where the Python suite validates *logic*, the dbt tests validate the *data* that
exists in DuckDB after the pipeline has run. They are defined in the schema YAML
files next to the models (`dbt/models/*/__*.yml`) plus one custom singular test.

### 2.1 Generic tests

Defined declaratively per column. Approximate counts across the project:

| Test type | Count | Purpose |
| --- | --- | --- |
| `not_null` | 161 | Required columns (primary keys, `LKHS_` tracking columns, key business fields) are never null. |
| `unique` | 49 | Keys that must be unique (e.g. surrogate keys in Gold dimensions, PKs in `_cv` views). |
| `accepted_values` | 52 | Enumerations stay within their allowed set — notably `LKHS_cdc_operation ∈ {I, U, D}`. |

### 2.2 Custom singular test

`dbt/tests/silver_no_duplicate_id_date_valid_from.sql` enforces the **SCD Type 2
grain** across all 18 DDD Silver tables: there must be no duplicate
`(id, LKHS_date_valid_from)` pair. The test is written as a Jinja loop that
`UNION ALL`s a `GROUP BY ... HAVING COUNT(*) > 1` over every Silver table and
passes when the result set is empty. This guarantees the CDC engine never writes
two versions of the same entity with the same validity start — the single most
important invariant of the Silver layer.

### 2.3 Running the dbt tests

```bash
cd dbt
dbt test --profiles-dir .                  # all data-quality tests
dbt test --select silver --profiles-dir .  # Silver tests only
```

dbt tests require a populated database, so run them **after** a pipeline build.
The full catalogue (with column-level detail) is browsable in the committed dbt
docs at [`documentation/dbt-docs/`](dbt-docs/).

---

## 3. Running the Python suite

No cloud credentials are required.

```bash
pip install -e ".[dev]"     # installs pytest

pytest tests/                                  # all 132 tests
pytest tests/ -v                               # verbose
pytest tests/test_integration_silver_cdc.py    # one module
pytest -k "incremental"                        # keyword filter
pytest -k "cv"                                  # only current-version view tests
```

Tests are collected from `tests/` (configured via `[tool.pytest.ini_options]` in
`pyproject.toml`).

---

## 4. When to run tests

Because there is **no CI service** and production deploys are performed manually
with [`scripts/deploy.sh`](../scripts/deploy.sh), running the test suite is a
manual pre-deploy step:

1. `pytest tests/` — must be green before deploying. Runs anywhere, no setup.
2. After a pipeline build on a populated database, `dbt test --profiles-dir .`
   confirms the data itself is valid.

A green `pytest` run proves the code and transformation logic are sound; a green
`dbt test` run proves the data that landed in the warehouse is valid. Both
together are the project's definition of "safe to ship."
```
