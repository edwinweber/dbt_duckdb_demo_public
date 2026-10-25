# The Python Code Explained — A Guide for the Data Engineer

This document explains **every Python module** in this repository: what it does,
how it fits into the pipeline, the design choices behind it, and worked examples.
It is written for a data engineer who knows SQL, dbt and data modelling well and
wants to understand the Python "glue" well enough to change, extend and debug it.

You do **not** need to read this top to bottom. Use the map below to jump to the
part you care about. The SQL side is in [dbt_transformations.md](dbt_transformations.md);
the reasons behind the design are in [design_decisions.md](design_decisions.md).

---

## Table of contents

1. [The 30-second mental model](#1-the-30-second-mental-model)
2. [How the code is organised](#2-how-the-code-is-organised)
3. [Recurring Python patterns](#3-recurring-python-patterns)
4. [`ddd_utils` — the foundation layer](#4-ddd_utils--the-foundation-layer)
5. [`ddd_dlt` — extraction and export](#5-ddd_dlt--extraction-and-export)
6. [`ddd_dbt` — dbt tooling and code generation](#6-ddd_dbt--dbt-tooling-and-code-generation)
7. [`ddd_dagster` — orchestration](#7-ddd_dagster--orchestration)
8. [The platform-operations scripts (backup, restore, capacity)](#8-the-platform-operations-scripts)
9. [The test suite](#9-the-test-suite)
10. [Recurring design choices, summarised](#10-recurring-design-choices-summarised)
11. [How to make common changes](#11-how-to-make-common-changes)

---

## 1. The 30-second mental model

The pipeline moves data through five stages. Python owns the **bookends** and the
**orchestration**; dbt + DuckDB own the **SQL transformations** in the middle.

```text
      ┌──────────────────┐         ┌────────────────────────┐         ┌──────────────────┐
      │     SOURCES      │         │      DuckDB (dbt)      │         │      OUTPUT      │
      │                  │         │                        │         │                  │
      │  DK Parliament   │         │    Bronze   (views)    │         │    Delta Lake    │
      │    OData API     │ ──────> │  Silver   (CDC/SCD2)   │ ──────> │  local / S3 /    │
      │    Rfam MySQL    │         │    Gold     (star)     │         │     OneLake      │
      └──────────────────┘         └────────────────────────┘         └──────────────────┘
                ^                               ^                               ^
                │                               │                               │
             ddd_dlt                         ddd_dbt                         ddd_dlt
          (extraction)                (generate + run dbt)                  (export)
                │                               │                               │
                └───────────────────────────────┴───────────────────────────────┘
                                                │
                                    ddd_dagster orchestrates
                                  everything above as "assets"
```

* **`ddd_dlt`** pulls data out of the API and the MySQL database with the dlt
  library and lands it as NDJSON files (this is "extraction"). It also holds the
  Delta Lake export, which does not use dlt: DuckDB reads the tables and the
  `deltalake` library writes them (this is "export"). And it holds the quack
  server that owns the database in Docker.
* **`ddd_dbt`** writes the dbt SQL model files for you (code generation) and runs
  `dbt build`.
* **`ddd_dagster`** orchestrates: it turns every step above into a Dagster
  *asset*, draws the dependency graph, runs them in the right order, retries
  failures, and logs run summaries.
* **`ddd_utils`** is the shared foundation everything else imports: configuration
  lists, environment variables, path building, string helpers, and the
  cloud-storage client.

The single most important file is
[ddd_python/ddd_utils/configuration_variables.py](../ddd_python/ddd_utils/configuration_variables.py).
It is the *source of truth*: it lists every entity and primary key.
Almost everything else in the codebase is **derived** from it.

---

## 2. How the code is organised

All Python lives under [ddd_python/](../ddd_python/), split into four sub-packages.
The dependency direction always flows **downward** — higher layers import from
lower ones, never the reverse:

```text
                ddd_dagster   (orchestration)
                      │  imports
        ┌─────────────┴─────────────┐
        ▼                           ▼
    ddd_dlt                      ddd_dbt
(extract / export)        (generate / run dbt)
        │                           │
        └─────────────┬─────────────┘
                      │  both import
                      ▼
                ddd_utils   (foundation)
```

| Package | Role | Entry points you run |
|---------|------|----------------------|
| `ddd_utils` | Config, env vars, paths, strings, cloud clients, backup and Bronze-archive tools | mostly imported; CLI tools in §8 |
| `ddd_dlt` | Extract from sources (dlt); export to Delta Lake (DuckDB + `deltalake`); the quack server | `dlt_run_extraction_pipelines_*`, `export_silver`, `export_gold`, `duckdb_quack_server` |
| `ddd_dbt` | Generate dbt SQL; run `dbt build`; init DuckDB | `generate_dbt_models`, `dbt_build_with_unique_logfile` |
| `ddd_dagster` | Orchestrate everything as assets/jobs/schedules | `dagster dev -w workspace.yaml` |

The `__init__.py` files are almost all empty — they exist only to mark each folder
as a Python package so `import ddd_python.ddd_dlt...` works.

---

## 3. Recurring Python patterns

Five patterns recur throughout the code. They are described once here, before
the module-by-module tour.

### 3.1 The factory function pattern

Instead of writing nearly-identical asset definitions by hand for every entity, the code writes a
**function that returns a function** (or a Dagster asset). You call it once per
entity in a loop. This is "DRY" (Don't Repeat Yourself).

```python
def _make_incremental_asset(api_resource: str) -> AssetsDefinition:
    base = normalize_danish_name(api_resource)

    @asset(name=base, ...)                 # the inner asset "closes over" `base`
    def _incremental_asset(context, config, dlt_onelake):
        ...                                 # uses `base` and `api_resource`
    return _incremental_asset

# Build one asset per entity:
incremental_assets = [_make_incremental_asset(n) for n in INCREMENTAL_NAMES]
```

The inner function "remembers" the `api_resource`/`base` from the call that built
it — that captured variable is called a **closure**. This is how multiple distinct assets
are produced from a single factory.

### 3.2 Configuration-driven, not hardcoded

Lists of entities and their primary keys live in
`configuration_variables.py`. Everything else loops over those lists. Adding a new
table is meant to be a *one-file* change. You will see this comment style
throughout:

```python
# Derived from DANISH_DEMOCRACY_FILE_NAMES — one Bronze model per entity.
DANISH_DEMOCRACY_MODELS_BRONZE = [
    f"bronze_ddd_{normalize_danish_name(n)}" for n in DANISH_DEMOCRACY_FILE_NAMES
]
```

### 3.3 Lazy loading of credentials and heavy libraries

Azure SDKs and credentials are only needed when you actually talk to OneLake. So
the code **defers** importing them and reading the env vars until first use. This
lets you run tests, generate dbt models, and use local storage **without any Azure
credentials present**. Two mechanisms achieve this:

* Module-level `__getattr__` in `get_variables_from_env.py` (see §4.3).
* Function-local `import` statements (`from ... import get_fabric_onelake_clients`
  *inside* a function body, not at the top of the file).

### 3.4 The `STORAGE_TARGET` switch (Delta Lake export destination)

A single environment variable, `STORAGE_TARGET`, flips the Delta Lake **export**
between three options:

* `local` — write to your local disk under `LOCAL_STORAGE_PATH` (`data/...` by default). Zero cloud setup.
* `s3` — write to S3-compatible storage (RustFS locally, AWS S3 in production). Requires `S3_BUCKET_DELTA` and S3 credentials.
* `onelake` — write to Microsoft Fabric OneLake (`abfss://...`). Requires Azure service principal.

Every place that touches Delta Lake storage checks this flag. Note: this is independent
of `RAW_STORAGE_TARGET` (which controls Bronze files and DuckLake location) and
`SILVER_STORAGE_FORMAT` (which controls Silver table storage).

### 3.5 Structured run logging as NDJSON

Almost every operation writes a one-line JSON record (NDJSON = newline-delimited
JSON) to a log file on the local filesystem. Because each line is valid JSON, you
can later query the logs with DuckDB itself:

```sql
SELECT * FROM read_json_auto('data/logs/DDD/*/*_log.ndjson');
```

A logging failure is **never** allowed to break the actual pipeline — log writes
are wrapped in `try/except` that downgrade errors to warnings.

---

## 4. `ddd_utils` — the foundation layer

Location: [ddd_python/ddd_utils/](../ddd_python/ddd_utils/). Everything else
imports from here.

### 4.1 `configuration_variables.py` — the source of truth

[ddd_python/ddd_utils/configuration_variables.py](../ddd_python/ddd_utils/configuration_variables.py)

This file contains only **data** (lists, dictionaries and the two concurrency
settings `MAX_CONCURRENT_WORKERS` and `FULL_PIPELINE_MAX_CONCURRENT`) plus
import-time checks that every entity has a primary key of one or more column
names; no functions. The rest
of the project reads its configuration from it. It declares, for both
source systems:

* The full list of entities/tables (`DANISH_DEMOCRACY_FILE_NAMES`,
  `RFAM_TABLE_NAMES`).
* Which ones are extracted *incrementally* vs *fully*
  (`..._INCREMENTAL` subsets).
* The derived dbt model names for Bronze and Silver (built with list
  comprehensions, so they can never drift out of sync with the entity list).
* Primary keys per table (`..._TABLE_PRIMARY_KEYS`): a column name, or a list
  of column names for a key of several columns.
* For Rfam: the SQL query templates (`RFAM_TABLE_QUERIES`).

**Primary keys of several columns.** Silver keys on one column. The helpers in
[ddd_utils/primary_keys.py](../ddd_python/ddd_utils/primary_keys.py) turn a
configured key into that column:

```python
key_columns("id")  # -> ["id"]
key_columns(["afstemningid", "aktoerid"])  # -> ["afstemningid", "aktoerid"]
stored_key_column("id")  # -> "id"
stored_key_column(["id"])  # -> "id"  (a list of one is a key of one column)
stored_key_column(["afstemningid", "aktoerid"])  # -> "LKHS_primary_key_1_column"
```

The model generator and the Delta export use `stored_key_column`. Only a list of two or more columns makes the Bronze view
compute `LKHS_primary_key_1_column`; every key that is configured now is one
column, so the models are the same as without this feature
([dbt_transformations.md §3.1](dbt_transformations.md#31-shared-building-blocks) has the SQL).

**Worked example — how a name becomes three model names.** Take the API entity
`"Aktør"`:

```python
normalize_danish_name("Aktør")  # -> "aktoer"   (see §4.2)
# bronze model:  f"bronze_ddd_{...}"       -> "bronze_ddd_aktoer"
# silver model:  bronze -> silver          -> "silver_ddd_aktoer"
```

**Design choice — derived, not duplicated.** Look at how Silver names are built:

```python
DANISH_DEMOCRACY_MODELS_SILVER = [
    m.replace("bronze_", "silver_", 1) for m in DANISH_DEMOCRACY_MODELS_BRONZE
]
```

The Silver list is *computed* from the Bronze list, so adding a Bronze entity
also adds its Silver counterpart. The test file
`test_configuration_variables.py` checks the counts (18, 6, 7, 2) and the subset
relationships, so an inconsistent edit fails in CI.

**The Rfam query templates:**

```python
RFAM_TABLE_QUERIES = {
    "family": "SELECT * FROM family{where_clause}",
    "clan":   "SELECT * FROM clan",          # no placeholder — never filtered
    ...
}
```

The `{where_clause}` placeholder is filled in at runtime — incremental tables get
`" WHERE updated >= :updated_from"`, full-extract tables get `""`. Note `:updated_from`
is a **bound parameter**, not string interpolation — that's the SQL-injection
defence (see §5.2).

`SILVER_TABLE_PRIMARY_KEYS` at the bottom merges DDD + Rfam PKs into one dict using
the `{**a, **b}` dict-merge syntax. The export script uses it to build the correct
join key per table.

### 4.2 `string_utils.py` — Danish names and dates

[ddd_python/ddd_utils/string_utils.py](../ddd_python/ddd_utils/string_utils.py)

Three small, heavily-used functions:

**`normalize_danish_name(name)`** converts Danish characters to ASCII and
lowercases, so every layer that maps an API name to an identifier calls this:

```python
normalize_danish_name("Møde")  # -> "moede"
normalize_danish_name("SagstrinAktør")  # -> "sagstrinaktoer"
```

This is not a DuckDB limitation: DuckDB accepts `ø/æ/å` in schema, table and
column names, quoted and unquoted, and `read_json_auto` keeps the original
Danish column names. The normalisation is a **portability/convention** choice: dbt model names become `.sql` filenames, identifiers must round-trip
cleanly through Delta Lake / Parquet and downstream tools (Power BI / Fabric), and
staying ASCII avoids having to quote identifiers everywhere and sidesteps
case-folding differences between engines.

The implementation lowercases *first*, then does three `.replace()` calls — so it
only needs to handle the lowercase forms (`ø æ å`) rather than all six. It is the
single implementation; every caller imports it.

**`resolve_date_to_load_from(date, default_days, reference_time)`** decides the
lower-bound date for incremental loads. If you pass `None`, it returns
`reference_time - default_days` formatted as `YYYY-MM-DD`. If you pass a string, it
validates the format and returns it unchanged (raising `ValueError` on a bad
format). Passing `reference_time` in (rather than calling `datetime.now()` inside)
makes the function **deterministic and testable** — the tests can feed a fixed
clock.

**`is_full_extract(date_to_load_from)`** is true when the start date is on or
before `FULL_EXTRACT_DATE` (`1900-01-01`). The extraction then names the file
`<entity>_<timestamp>_full.json`, the marker the Silver layer needs to detect
deletes of incremental entities.

### 4.3 `get_variables_from_env.py` — lazy environment loading

[ddd_python/ddd_utils/get_variables_from_env.py](../ddd_python/ddd_utils/get_variables_from_env.py)

The problem it solves: some env vars are **required** (Azure credentials), others
are **optional** (with sensible defaults). If the module read all required vars at
import time, you couldn't even `import` it to generate dbt models or run unit tests
without Azure credentials in your shell.

The solution: **PEP 562 module-level `__getattr__`**.

* `load_dotenv()` fires at import time to populate `os.environ` from `.env`.
* **Optional** vars are read eagerly with `os.getenv(...)` and stored as module-level
  globals (e.g. `STORAGE_TARGET`, `LOCAL_STORAGE_PATH`, `DUCKDB_DATABASE_LOCATION`).
  These never raise. This includes `SILVER_STORAGE_FORMAT` (default `duckdb`,
  validated against `{duckdb, ducklake}` — an invalid value raises immediately),
  `RAW_STORAGE_TARGET` (default `local`, validated against `{local, s3}`),
  the DuckLake paths (`DUCKLAKE_CATALOG_LOCATION`, `DUCKLAKE_DATA_PATH`), and the
  S3 credentials and bucket vars (`S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, etc.). When
  `RAW_STORAGE_TARGET=s3` and `SILVER_STORAGE_FORMAT=ducklake`, `DUCKLAKE_DATA_PATH`
  is **automatically derived** from `S3_BUCKET_DUCKLAKE` and `S3_PREFIX_DUCKLAKE`
  so users don't manually construct S3 URIs.
* **Required** vars are listed in `_LAZY_REQUIRED` and resolved only on **first
  access**, via module-level `__getattr__`. When you access
  `get_variables_from_env.AZURE_CLIENT_SECRET`, Python automatically calls
  `__getattr__` only if the name is not already in the module's `__dict__`. Since
  optional vars are plain globals in `__dict__`, they return immediately; required
  vars go through `__getattr__`, which calls `_require()` and raises `OSError`
  naming the variable if it is missing.

Because optional vars are already in the module's `__dict__`, the `__getattr__`
function is never called for them, so they cost nothing. Names that only exist
through `__getattr__` are invisible to a type checker, which is why the module
ships a `.pyi` stub (`get_variables_from_env.pyi`) declaring them for mypy.

**S3 credentials are shared between raw and export S3 storage.** When either
`RAW_STORAGE_TARGET=s3` or `STORAGE_TARGET=s3`, the shared credentials
(`S3_ENDPOINT`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`,
`S3_USE_SSL`, `S3_URL_STYLE`) are loaded and validated. Bucket variables are
loaded only for the relevant switch (`S3_BUCKET_BRONZE` for raw,
`S3_BUCKET_DELTA` for export), so Bronze and the export can use different
buckets.

Helper functions:
* `_require(name)` — fetch or raise with a clear message.
* `_int_env(name, default)` — parse an int env var, raising a *useful* error if
  it's set but not a number (rather than a cryptic `ValueError` later).
* `STORAGE_TARGET` is validated against `{"local", "s3", "onelake"}` at import — a typo
  fails fast.
* `RAW_STORAGE_TARGET` is validated against `{"local", "s3"}` at import.
* `SILVER_STORAGE_FORMAT` is validated against `{"duckdb", "ducklake"}` at import.

### 4.4 `path_utils.py` — building storage paths

[ddd_python/ddd_utils/path_utils.py](../ddd_python/ddd_utils/path_utils.py)

Centralises the "local vs S3 vs OneLake" path logic so no call site has to repeat it.

* **`build_bronze_destination_path(source_system_code, entity_name)`** returns
  the relative Bronze directory, always `Files/Bronze/<source>/<entity>` (for
  example `Files/Bronze/DDD/aktoer`). It does not branch: the dlt destination
  built by `_make_destination` (§5.1) puts that path under `LOCAL_STORAGE_PATH`
  or under the S3 bucket, depending on `RAW_STORAGE_TARGET`.

* **`build_bronze_file_name(entity_name, timestamp, full=False)`** returns the
  Bronze file name, `<entity>_<YYYYMMDD_HHMMSS>.json`, with `_full` before the
  extension for a full extraction of an incremental entity.

* **`build_delta_export_path(layer, table)`** returns a `(path, storage_options)`
  tuple ready for `deltalake.write_deltalake`. Branches on `STORAGE_TARGET`:
  * Local: returns path under `LOCAL_STORAGE_PATH/Files/{Layer}/{table}/` and
    creates the directory as a side effect (`os.makedirs(..., exist_ok=True)`).
    `storage_options` is empty dict.
  * S3: returns `s3://S3_BUCKET_DELTA/Files/{Layer}/{table}/` URI and
    `storage_options` dict containing `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
    `AWS_REGION`. If `S3_ENDPOINT` is set (RustFS), also includes
    `AWS_ENDPOINT_URL` (full URL with protocol), `AWS_S3_ADDRESSING_STYLE`, and
    `AWS_ALLOW_HTTP` (based on `S3_USE_SSL`).
  * OneLake: returns `abfss://workspace@account.dfs.fabric.microsoft.com/...` URI,
    fetches a bearer token via lazy import of `get_fabric_onelake_clients`, and
    returns `storage_options` with token + `use_fabric_endpoint=true`.

* **`open_export_connection()`** opens the DuckDB connection for the Silver/Gold
  exports and the other readers (archive script, maintenance). When
  `QUACK_SERVER_HOST` is set (Docker) it is an in-memory connection that
  `ATTACH`es the quack server (`_open_quack_connection`); Silver is then
  `<DUCKDB_DATABASE>.ducklake_catalog.main_silver` in DuckLake mode. Otherwise it
  opens the DuckDB file read-only and, when `SILVER_STORAGE_FORMAT=ducklake`,
  attaches the DuckLake catalog read-only. If `DUCKLAKE_DATA_PATH` starts with
  `s3://` OR `STORAGE_TARGET=s3`, it calls `_configure_s3_secret` to create a
  session-scoped S3 secret on the connection so DuckLake/delta_scan can reach S3.

* **`open_server_session()`** is the same quack connection plus `CONNECT`, so
  every statement runs on the server with the server's own catalogs; the DuckLake
  cleanup uses it. Without a quack server it falls back to `open_export_connection()`.

* **`_configure_s3_secret(conn)`** creates a DuckDB S3 secret using `S3_ENDPOINT`,
  `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`, `S3_USE_SSL`, and
  `S3_URL_STYLE` from the environment. **Important:** `S3_ENDPOINT` is stored as
  a full URL (e.g. `http://rustfs:9000`) for dlt/boto3, but DuckDB's `CREATE SECRET`
  clause takes `host:port` only — the function strips the protocol prefix automatically
  (e.g. `rustfs:9000`). Called before any operation that needs to reach S3 (DuckLake
  ATTACH when data is on S3, or delta_scan when export target is S3).

Notice the **function-local import** of `get_fabric_onelake_clients` inside the
OneLake branch — the Azure SDK is only imported when you're actually exporting to
OneLake (the lazy-loading pattern from §3.3).

### 4.5 `get_fabric_onelake_clients.py` — the Azure client

[ddd_python/ddd_utils/get_fabric_onelake_clients.py](../ddd_python/ddd_utils/get_fabric_onelake_clients.py)

Wraps the Azure `ClientSecretCredential` (service-principal auth) and the ADLS
Gen2 `DataLakeServiceClient`. Both are created **once and cached** in module-level
globals (`_credential`, `_service_client`) — creating them is expensive and they're
reusable. `get_fabric_token()` returns an OAuth2 bearer token for Delta Lake writes;
`get_fabric_file_client_default_workspace(dir, file)` returns a file handle for a
given path in the configured workspace, creating the directory if needed.

This is the one place that knows how to authenticate to Fabric. Everything else
asks it for a client or a token.

### 4.6 `backup_common.py`, `backup_platform.py`, `restore_platform.py`, `fabric_capacity_pause_resume.py`

These are **operational** utilities, not part of the data pipeline itself. They're
covered in [§8](#8-the-platform-operations-scripts). Note: `backup_common.py`
defines a helper function `_ducklake_data_is_local()` that checks whether DuckLake
Parquet data lives on the local filesystem or on S3. When `RAW_STORAGE_TARGET=s3`
and DuckLake is active, the local DuckLake backup target is skipped; the bucket's
versioning provides durability instead.

---

## 5. `ddd_dlt` — extraction and export

Location: [ddd_python/ddd_dlt/](../ddd_python/ddd_dlt/). "dlt" = the
[data load tool](https://dlthub.com) library. This package gets data **in** (from
sources to Bronze files, with dlt) and **out** (from DuckDB to Delta Lake, with
DuckDB and the `deltalake` library; dlt is not involved). It also contains the
quack server (§5.6).

### 5.1 `dlt_pipeline_execution_functions.py` — the extraction engine

[ddd_python/ddd_dlt/dlt_pipeline_execution_functions.py](../ddd_python/ddd_dlt/dlt_pipeline_execution_functions.py)

This module contains the extraction logic. It exposes **two public entry points**:

* **`execute_pipeline(pipeline_type, source_system_code, **kwargs)`** — the main
  dispatcher. Selects a handler based on `pipeline_type`, measures wall-clock
  duration, and unconditionally writes an NDJSON log record (success or failure).
  Uses an internal `_PIPELINE_HANDLERS` dict to look up and call the right handler,
  forwarding `**kwargs` directly to it.

  | `pipeline_type` | Handler | Use |
  | --- | --- | --- |
  | `"api_to_file"` | `run_api_to_file_pipeline` | Paginated OData/REST → NDJSON |
  | `"sql_to_file"` | `run_sql_to_file_pipeline` | SQL query → Parquet or NDJSON |
  | `"file_to_file"` | `run_file_to_file_pipeline` | Raw byte copy (no dlt) |

* **`run_extraction_pool(tasks, script_name, source_system_code, date_to_load_from, start_time, resource_label="resource", max_workers=4)`** — shared
  orchestration helper that runs multiple `PipelineTask` items concurrently via
  `ThreadPoolExecutor`, collects outcomes, and writes a script-level NDJSON
  summary record (per-task status, total duration, success/failure counts).
  Raises `RuntimeError` on any task failure after attempting all tasks.
  Used by both extraction orchestrators (DDD and Rfam) to eliminate duplication.

**The key idea: yield records one at a time.** Both the API and SQL handlers define
an inner `@dlt.resource` generator that `yield`s **individual dicts** — one per API
record or per database row — rather than handing dlt one big blob. This lets dlt
infer the schema and keeps memory flat. (dlt's own incremental cursor is not
used; the date filter comes from the caller, see §5.2.)

```python
@dlt.resource(name=pipeline_name, write_disposition="append", max_table_nesting=0)
def get_api_data(...):
    yield from _iter_odata_pages(url)   # generator → one dict per record
```

**OData pagination** is handled by `_iter_odata_pages`: it follows the
`odata.nextLink` field page by page until it's gone, validating each response has a
`"value"` key (fail-fast on a malformed response), and normalising Danish keys on
the way through. It also truncates over-long fractional seconds in timestamps via
the `_TS_MICROSEC` regex (DuckDB/Arrow only want microsecond precision).

**The destination** is built by `_make_destination`, which branches on
`RAW_STORAGE_TARGET` (which controls where Bronze files land):
- `local` uses a `file://` bucket (local filesystem under `LOCAL_STORAGE_PATH`)
- `s3` uses an `s3://` bucket with credentials from `S3_ENDPOINT`, `S3_ACCESS_KEY_ID`,
  `S3_SECRET_ACCESS_KEY`, etc.

`STORAGE_TARGET` (the Delta Lake *export* destination) is separate and does not
affect where Bronze files go.

A custom `layout` (`{table_name}/{_resolve_stem}.{ext}`) with the placeholder
functions `_resolve_stem` and `_resolve_ext` forces dlt to write the exact
timestamped file name (`build_bronze_file_name`) instead of its default naming.

**Module-level setup** at the top of the file configures dlt:

```python
os.makedirs(get_variables_from_env.DLT_PIPELINES_DIR, exist_ok=True)
os.environ.setdefault(
    "NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION", "true"
)  # plain .jsonl, not .jsonl.gz
os.environ.setdefault("DESTINATION__FILESYSTEM__ENABLE_DATASET_NAME_NORMALIZATION", "false")
```

These configure dlt's behaviour so the output files are plain NDJSON that the dbt
Bronze `read_json_auto()` views can glob.

**Logging and secret scrubbing.** `execute_pipeline` wraps every run in
`try/except/finally`: it measures wall-clock time, and in the `finally` block writes
an NDJSON log record — *whether or not the run succeeded* — to
`<DLT_PIPELINE_RUN_LOG_DIR>/<source>/<pipeline>/<pipeline>_log.ndjson` on the
local filesystem (default root `LOCAL_STORAGE_PATH/logs`). Before logging the call parameters
it runs them through `_scrub_secrets`, which replaces the value of any key
containing `connection_string`/`secret`/`password`/`token` with `"***"`. The log
write itself is wrapped so a logging failure only emits a `warnings.warn`, never
masks the real result.

**`_serialize_trace` / `_json_default`** turn dlt's rich trace object and any exotic
types (like `pendulum.DateTime`) into plain JSON-serialisable values. These are
small but have dedicated unit tests (`test_serialize_trace.py`, `test_json_default.py`).

**`PipelineTask`**, the item type `run_extraction_pool` takes, is a `TypedDict`
with `name`, `source_system_code`, `pipeline_type` and `kwargs`; the first three
are passed to `execute_pipeline` explicitly, `kwargs` as `**kwargs`.

### 5.2 `dlt_run_extraction_pipelines_danish_parliament_data.py` — DDD orchestrator

[ddd_python/ddd_dlt/dlt_run_extraction_pipelines_danish_parliament_data.py](../ddd_python/ddd_dlt/dlt_run_extraction_pipelines_danish_parliament_data.py)

A runnable script (`python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data`).
It builds a task list by looping over the DDD entities, deciding the OData filter
for each:

```python
api_filter = (
    f"$filter=opdateringsdato ge DateTime'{date_to_load_from}'&$orderby=id"
    if file_name in incremental_set
    else "$inlinecount=allpages&$orderby=id"
)
```

Each task includes the entity name and a kwargs dict with all parameters for
`execute_pipeline`. The `incremental_set` is a `set(...)` for O(1) membership tests.

Once all tasks are built, the script calls `run_extraction_pool()` (see §5.1), which
runs them concurrently via `ThreadPoolExecutor`, collects results, writes a
script-level NDJSON summary, and raises `RuntimeError` if any pipeline failed.

The `if __name__ == "__main__":` block wires up `argparse` for
`--date_to_load_from` and `--file_names_to_retrieve`, so you can run an ad-hoc
backfill of just a couple of entities from a specific date.

### 5.3 `dlt_run_extraction_pipelines_rfam.py` — Rfam orchestrator

[ddd_python/ddd_dlt/dlt_run_extraction_pipelines_rfam.py](../ddd_python/ddd_dlt/dlt_run_extraction_pipelines_rfam.py)

Parallel to the DDD orchestrator but for the MySQL source. Like DDD, it builds a
task list by looping over the Rfam tables. The difference from DDD is how it
builds the query and **prevents SQL injection**. Both this script and the Dagster
Rfam assets call `build_rfam_sql` in `dlt_pipeline_execution_functions.py`:

```python
sql_query, sql_params = dpef.build_rfam_sql(
    query_template,
    is_incremental=table_name in incremental_set,
    date_to_load_from=date_to_load_from,
)
# incremental: "... WHERE updated >= :updated_from", {"updated_from": date_to_load_from}
# full extract: "...",                               {}
```

The date is passed as a **named bind parameter** (`:updated_from`), not interpolated
into the SQL string. SQLAlchemy substitutes it safely. Even though
`resolve_date_to_load_from` already validates the date format, this is defence in
depth.

Once all tasks are built, the script calls `run_extraction_pool()` — the same
shared orchestration helper as DDD — which runs them concurrently, collects
results, writes a script-level NDJSON summary, and raises on any failure.

### 5.4 `export_silver.py` — Silver → Delta Lake

[ddd_python/ddd_dlt/export_silver.py](../ddd_python/ddd_dlt/export_silver.py)

Reads each Silver table from DuckDB and writes it to a Delta Lake table, as an
**incremental append**:

* If the Delta table **already exists**, the dedup read happens *inside DuckDB*
  via the `delta` extension's `delta_scan`: the live Silver table is
  `LEFT JOIN`ed directly against `delta_scan('<target_path>')` on
  `primary_key + LKHS_date_valid_from`, keeping only rows where the target side is
  `NULL` (i.e. not yet exported). Those new rows are appended
  (`mode="append", schema_mode="merge"`). Reading the target with `delta_scan`
  (rather than `DeltaTable(...).to_pyarrow_table()`) means the existing Delta
  table is **never fully materialised into Python/PyArrow memory** — DuckDB reads
  only the join keys it needs, with projection pushdown.
* If the Delta table **does not exist** (first load), it writes everything with
  `mode="overwrite"`.

```python
query = (
    f"SELECT src.* FROM ...main_silver.{table} src "
    f"LEFT JOIN delta_scan('{target_table_path}') tgt "
    f"  ON src.{pk} = tgt.{pk} AND src.LKHS_date_valid_from = tgt.LKHS_date_valid_from "
    f"WHERE tgt.{pk} IS NULL"
)
```

**Read/write split.** DuckDB's `delta` extension can read and write at ≥2.0, but
the DuckDB delta writer has an Azure/OneLake regression. So the *existence check*
stays on
`deltalake.DeltaTable.is_deltatable` and the *write* stays on
`deltalake.write_deltalake`; only the dedup read moved into DuckDB. A
`_prepare_delta_read` helper `LOAD`s the extension (and, for OneLake, the Azure
stack + the persistent `azure_sp` secret) on the connection before `delta_scan`
runs. Moving the write into DuckDB waits for a writer-capable DuckDB version
without its Azure/OneLake regression (ADR-7 in
[design_decisions.md](design_decisions.md#adr-7-delta-export-reads-with-duckdb-writes-with-deltalake)).

**Storage-format aware (DuckDB vs DuckLake).** The connection comes from the
shared `open_export_connection()` (`ddd_utils/path_utils.py`): in Docker a quack
client attached to `duckdb_server`; otherwise a read-only connection to the main
DuckDB file, which in `ducklake` mode also attaches the DuckLake catalog
read-only. `_silver_source_database()` then resolves the source to
`ducklake_catalog.main_silver.<table>` (DuckLake),
`<DUCKDB_DATABASE>.ducklake_catalog.main_silver.<table>` (DuckLake through quack)
or `<DUCKDB_DATABASE>.main_silver.<table>` (DuckDB), so the export reads the
Silver tables from wherever dbt actually wrote them.  The **Gold** export uses the same
`open_export_connection()`, so its views (which reference
`ducklake_catalog.main_silver`) resolve in DuckLake mode too.

Per-table failures are collected and re-raised as one `RuntimeError` after *all*
tables are attempted, so one bad table doesn't silently skip the rest. The key comes
from `SILVER_TABLE_PRIMARY_KEYS` through `stored_key_column` (DDD entities use `id`,
Rfam tables vary, a key of several columns is `LKHS_primary_key_1_column`). Outside
Docker the connection is read-only; in Docker it is a quack client of the server.

### 5.5 `export_gold.py` — Gold → Delta Lake

[ddd_python/ddd_dlt/export_gold.py](../ddd_python/ddd_dlt/export_gold.py)

Simpler than Silver: Gold tables are dimensional **views** that are quick to rebuild,
so every export is a full `mode="overwrite"`. Same read-only DuckDB connection, same
collect-failures-then-raise pattern.

Each overwrite leaves a full copy of the table behind, so after every export
`_prune_history` limits time travel to 7 days (`GOLD_RETENTION_DAYS`): it sets the
Delta retention properties, vacuums unreferenced files older than 168 hours, and
removes expired `_delta_log` entries. A prune failure is logged as a warning and does
not fail the export; the next run prunes again.

Both export scripts expose `export_single_*_table(connection, table)` functions that
the Dagster export assets import and call directly (see §7.4) — the same code path
works both as a CLI script and inside an orchestrated asset.

### 5.6 `duckdb_quack_server.py` — the single DuckDB writer in Docker

[ddd_python/ddd_dlt/duckdb_quack_server.py](../ddd_python/ddd_dlt/duckdb_quack_server.py)

The process behind the `duckdb_server` container (ADR-12 in
[design_decisions.md](design_decisions.md)). It opens the `.duckdb` file
read-write, sets `memory_limit` from `QUACK_SERVER_MEMORY_LIMIT` (default `12GB`),
loads the extensions, attaches the DuckLake catalog in `ducklake` mode, creates
the persistent secrets (`init_secrets_on_connection`, §6.3), registers the SQL
function `ducklake_sweep_current_temp` (§7.7), and then serves the database over
quack RPC on `QUACK_SERVER_PORT` (default 9494). `QUACK_SERVER_TOKEN` is required
(at least 4 characters). `SIGTERM`/`SIGINT` stop the server cleanly. Every other
container (Dagster, dbt, Metabase, the `run` service) connects as a client and
mounts no database files.

---

## 6. `ddd_dbt` — dbt tooling and code generation

Location: [ddd_python/ddd_dbt/](../ddd_python/ddd_dbt/).

### 6.1 `generate_dbt_models.py` — writing SQL with Python

[ddd_python/ddd_dbt/generate_dbt_models.py](../ddd_python/ddd_dbt/generate_dbt_models.py)

This is the bridge between the config file and the dbt project. It **writes `.sql`
files** into `dbt/models/{bronze,silver,gold}/` by stamping out dbt model files that
just call the dbt macros. Run it whenever you change the entity lists:

```bash
python -m ddd_python.ddd_dbt.generate_dbt_models
```

Three generators:

* **`generate_dbt_models_bronze`** writes one model per entity that calls the
  `generate_model_bronze` macro.
* **`generate_dbt_models_silver`** writes the incremental CDC model **and** its
  `_cv` (current-version) view per entity. It chooses which macro to stamp
  (`generate_model_silver_incr_extraction` vs `..._full_extraction`) based on
  whether the model name is in the derived incremental set — *not* a hardcoded list:

  ```python
  _INCREMENTAL_SILVER_MODELS_DDD = frozenset(
      f"silver_ddd_{normalize_danish_name(n)}"
      for n in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL
  )
  ```

* **`generate_dbt_models_gold_cv`** writes the `_cv` views for Gold, skipping the two
  handcrafted models without versions (`date`, `individual_votes`).

It also writes the singular dbt test that checks the Silver grain (no duplicate
key + `LKHS_date_valid_from`, the key being `id` or `LKHS_primary_key_1_column`)
over the DDD Silver tables.

The string-building uses `PREFIX = '{{'` / `SUFFIX = '}}'` constants so the
generated text contains literal Jinja `{{ ... }}` and `{%- ... -%}` blocks without
fighting Python's own `.format()`. The output is intentionally thin — the real
logic lives in the dbt macros (documented in
[dbt_transformations.md](dbt_transformations.md)); Python just decides *which* macro each model calls
and with *what* parameters (primary key, source system).

The `if __name__ == "__main__":` block at the bottom is the actual recipe: for DDD
it calls the Bronze, Silver and Gold `_cv` generators and writes the grain test;
for Rfam it calls the Bronze and Silver generators with Rfam's config (different
prefix, different env var, different incremental set). Every call passes the
primary-key map, so a key of several columns reaches the Bronze view, the Silver
model and the grain test (§4.1).

### 6.2 `dbt_build_with_unique_logfile.py` — running dbt

[ddd_python/ddd_dbt/dbt_build_with_unique_logfile.py](../ddd_python/ddd_dbt/dbt_build_with_unique_logfile.py)

A thin wrapper that runs `dbt build --log-format json` as a subprocess (one-hour
timeout), capturing stdout to `dbt_build_log_<timestamp>.json` under
`DBT_LOGS_DIRECTORY`. It fails on a non-zero exit code **and** when the JSON log
contains failed or errored nodes, because dbt can exit 0 with failing tests. The
binary is `DBT_EXECUTABLE` (default `dbt`; `dbt-quack` in Docker, §6.4). The
`--models_to_select` CLI arg passes through to `dbt --select`;
`--no_silver_delete_guard` and `--skip_late_files` set the Silver switches for one
run (as `--vars`, built by `silver_vars`).

### 6.3 `init_duckdb.py` — bootstrapping the database

[ddd_python/ddd_dbt/init_duckdb.py](../ddd_python/ddd_dbt/init_duckdb.py)

The Python counterpart of the shell initialisation (`docker/docker-entrypoint.sh`
and `duckdb/init_duckdb.sh`), run with `python -m ddd_python.ddd_dbt.init_duckdb`.
It opens the database, installs/loads the `httpfs`, `azure`, and `delta`
extensions, sets the Azure transport option, and creates persistent **secrets**
for cloud storage.

Three functions:

* **`init_s3_secret(con)`** creates a persistent S3 secret when either
  `RAW_STORAGE_TARGET=s3` or `STORAGE_TARGET=s3`. The secret includes
  `KEY_ID`, `SECRET`, and `REGION` (always), plus `ENDPOINT`, `URL_STYLE`,
  and `USE_SSL` (only when `S3_ENDPOINT` is non-empty, i.e. RustFS). For real
  AWS S3, omitting these lets DuckDB fall back to AWS defaults. The secret is
  **persistent** so non-dbt sessions (e.g. ad-hoc DBeaver queries or
  `delta_scan` in export scripts) can reach S3-backed Bronze, DuckLake,
  or Delta export targets without extra setup.

* **`init_secrets_on_connection(con)`** creates the Azure service-principal
  secret when `STORAGE_TARGET=onelake` and calls `init_s3_secret`, on a connection
  that is already open. The quack server (§5.6) calls it before it starts serving,
  so its clients inherit the secrets; the Silver export calls it before
  `delta_scan` reads a OneLake target.

* **`init_duckdb()`** is the main entry point for a setup without the quack
  server. It creates the database file if needed, installs/loads extensions, sets
  Azure transport to `curl` (required for OneLake/ADLS Gen2) and calls
  `init_secrets_on_connection`.

Security detail: DuckDB's `CREATE SECRET` DDL can't take bound parameters, so the
credentials *are* interpolated into the SQL string — but that statement is
deliberately **never logged**, and only the secret's metadata (name/type) is
printed back for verification.

### 6.4 `dbt_quack.py` — quack adapter for Docker mode

[ddd_python/ddd_dbt/dbt_quack.py](../ddd_python/ddd_dbt/dbt_quack.py)

In Docker, `duckdb_server` is the sole owner of the `.duckdb` file (ADR-12).
`dbt-duckdb` normally opens the file directly, which would violate the
single-writer rule. With `QUACK_SERVER_HOST` set, `dbt/profiles.yml` selects the
`quack` target: an in-memory DuckDB that `ATTACH`es
`quack:<host>:<port>` with the token, under the alias `danish_democracy_data`.

`dbt_quack.py` is the `dbt-quack` console script (`[project.scripts]` in
`pyproject.toml`); Dagster and `dbt_build_with_unique_logfile` run it instead of
`dbt` through `DBT_EXECUTABLE=dbt-quack`. Before it hands over to dbt's own runner
(`dbtRunner`), it applies three patches to `dbt-duckdb`:

1. **`connect`** — every cursor runs `CONNECT danish_democracy_data`, so model and
   test SQL executes on the server, against the server's catalogs.
2. **`is_ducklake`** — databases listed in `QUACK_DUCKLAKE_DATABASES` (default
   `ducklake_catalog`) count as DuckLake, so dbt does not issue the `DROP …
   CASCADE` that DuckLake refuses.
3. **`per_thread`** — each dbt thread gets its own connection, so concurrent
   cursors do not interrupt each other's server session.

`QUACK_SKIP_PATCHES` switches individual patches off to test whether a newer
DuckDB or dbt-duckdb still needs them. Without `QUACK_SERVER_HOST` the script
warns and runs plain dbt.

---

## 7. `ddd_dagster` — orchestration

Location: [ddd_python/ddd_dagster/](../ddd_python/ddd_dagster/). This is the layer
that turns the scripts above into a coordinated, observable, retryable pipeline.

**Mental model of Dagster terms:**
* **Asset** — a thing that gets produced (a Bronze file, a dbt model, a Delta table).
  Defined with `@asset`. Assets declare *dependencies* on other assets, forming a graph.
* **Job** — a selected subset of assets you run together.
* **Schedule** — a cron trigger for a job.
* **Sensor** — code that reacts to events (e.g. a job finishing).
* **Resource** — an injected dependency (here, the dlt pipeline wrapper and the dbt CLI).
* **Definitions** — the single object that registers everything for the Dagster UI.

### 7.1 `_constants.py` and `resources.py`

[_constants.py](../ddd_python/ddd_dagster/_constants.py) holds the shared
`_RETRY_POLICY` (2 retries with exponential backoff, 60s → 120s, for transient
network errors), used by every extraction and export asset, and the
DuckLake-cleanup and Bronze-archive asset keys. The worker count (`MAX_CONCURRENT_WORKERS`, 4) is in
`configuration_variables.py`, because the extraction pool in `ddd_dlt` uses it too
and lower layers do not import from `ddd_dagster`.

[resources.py](../ddd_python/ddd_dagster/resources.py) defines
`DltPipelineResource`, a Dagster `ConfigurableResource` registered under the
resource key `dlt_onelake` (a name kept from when logs went to OneLake; they are
now always written locally). It's a thin wrapper around
`dlt_pipeline_execution_functions.execute_pipeline` and the job-run log writer.
Why inject it as a resource instead of importing the function directly? Three
reasons stated in the docstring: dependency injection, **testability** (you can swap
in a mock), and auditability in the UI. It also has `write_job_run_log` for the
sensors (§7.10).

### 7.2 `assets.py` — DDD extraction assets

[ddd_python/ddd_dagster/assets.py](../ddd_python/ddd_dagster/assets.py)

This module uses the factory pattern (§3.1). `_make_incremental_asset(api_resource)`
and `_make_full_extract_asset(api_resource)` each return a configured `@asset`. At
the bottom, two list comprehensions build the assets by filtering the canonical
entity list against the incremental set:

```python
incremental_assets = [_make_incremental_asset(n) for n in FILE_NAMES if n in INCREMENTAL]
full_extract_assets = [_make_full_extract_asset(n) for n in FILE_NAMES if n not in INCREMENTAL]
all_extraction_assets = incremental_assets + full_extract_assets
```

Each asset:
* Has a key like `["ingestion", "DDD", "aktoer"]` and a `group_name` for the UI.
* Carries the shared `_RETRY_POLICY`.
* Reads its date from a per-run `ExtractionConfig` (a Dagster `Config` class) so you
  can override `date_to_load_from` from the UI Launchpad for backfills.
* Calls `dlt_onelake.execute_pipeline(...)` and returns a `MaterializeResult` whose
  `metadata` (records written, file name, duration, status) shows up in the UI.

### 7.3 `rfam_assets.py` — Rfam extraction assets

[ddd_python/ddd_dagster/rfam_assets.py](../ddd_python/ddd_dagster/rfam_assets.py)

The same two-factory structure for the 7 MySQL tables, using `pipeline_type =
"sql_to_file"` and the `:updated_from` bound parameter for incremental tables. The
structure matches `assets.py`, so both source systems are built the same way.

### 7.4 `export_assets.py` — Delta Lake export assets + ordering barriers

[ddd_python/ddd_dagster/export_assets.py](../ddd_python/ddd_dagster/export_assets.py)

One export asset per Silver table (25) and per Gold table (10, from
`DANISH_DEMOCRACY_MODELS_GOLD`), each calling the
`export_single_*_table` functions from §5.4–5.5. Each opens its own connection with
`open_export_connection()` (§4.4) in a `try/finally` so the connection always closes.

The ordering is done by **barrier assets**: no-op assets that exist only to
sequence the graph:

```text
dbt Gold (all models)
        │
        ▼
barrier_dbt_gold_complete      (no-op gate)
        │
        ▼
Silver exports                 (incremental append)
        │
        ▼
barrier_all_silver_exported    (no-op gate)
        │
        ▼
Gold exports                   (full overwrite)
        │
        ▼
barrier_all_gold_exported      (no-op gate)
        │
        ▼
Data Engineering layer
```

`barrier_dbt_gold_complete` depends on *every* Gold model; each Silver export
depends on the barrier. This forces "all of Gold finishes before any Silver export
starts" without wiring N×M edges by hand. The barriers `pass` — they write no data.

### 7.5 `dbt_assets.py` — integrating the dbt project

[ddd_python/ddd_dagster/dbt_assets.py](../ddd_python/ddd_dagster/dbt_assets.py)

Uses the `dagster-dbt` integration so **every dbt model becomes a Dagster asset**,
giving end-to-end lineage from the dlt extraction asset right through Bronze →
Silver → Gold in the UI.

The custom `DddDbtTranslator` does the important wiring:
* **Group mapping** — maps each model's layer (`bronze`/`silver`/`gold`) to a UI
  group via `fqn[1]`.
* **Source mapping** — this is what connects the two worlds. A dbt *source* named
  `bronze_ddd_afstemning` is rewritten to the asset key
  `["ingestion", "DDD", "afstemning"]` — the exact key the dlt extraction asset
  uses. That's how Dagster draws the edge from "dlt extracted afstemning" to "dbt
  built bronze_afstemning".

`_dbt_multi_asset(...)` builds a `multi_asset` from the dbt manifest for a given
`select` string (`bronze`, `silver`, `gold`, `data_engineering`, or
`resource_type:seed`). The actual execution is `dbt.cli(["build", ...]).stream()`.
The Silver asset reads a `DbtSilverConfig` from the Launchpad: `delete_guard:
false` turns the Silver delete guard off for one run with real mass deletes, and
`skip_late_files: true` logs late Bronze files as skipped
([dbt_transformations.md §3](dbt_transformations.md#3-silver-cdc-into-scd-type-2)).
It also has `full_refresh`, which passes `--full-refresh`; the Silver tables
ignore it (`full_refresh=false`), because Silver is the state.

In Docker mode dbt connects to the quack server (`dbt_quack.py` patches the
`dbt-duckdb` adapter); `QUACK_SERVER_HOST` triggers the quack code path. Outside
Docker, dbt opens the file directly (the quack patch is skipped).

The Data Engineering asset set is an observability layer: dbt models that read
Dagster's *own* SQLite run history via DuckDB's `sqlite_scan()`. The
`dagster_step_failures_raw` model globs `$DAGSTER_HOME/history/runs/*.db` with
`sqlite_scan` — Dagster writes one SQLite file per run and DuckDB can glob them
— so no step-failure export from the sensor is needed.

### 7.6 `jobs.py` — grouping assets into runnable jobs

[ddd_python/ddd_dagster/jobs.py](../ddd_python/ddd_dagster/jobs.py)

Defines multiple jobs via `define_asset_job`, selecting assets by group or by dbt-select
string. The two big design points:

* **Executor choice.** Extraction/export jobs use
  `multiprocess_executor(max_concurrent=4)` for parallelism. dbt jobs use
  `in_process_executor`. `full_pipeline_job` uses `multiprocess_executor` with
  `FULL_PIPELINE_MAX_CONCURRENT` (2); outside Docker, DuckDB allows only one writer,
  so run it with the quack server.
* **No Metabase wrapping needed.** In Docker, Metabase connects to `duckdb_server`
  via quack and stays connected throughout; no stop/start cycle is required.

Default Launchpad configs (`_DDD_INCREMENTAL_CONFIG` etc.) are themselves derived
from the canonical incremental lists, so a new incremental entity needs no change
here. The helper functions (`_bronze_selection`, `_silver_selection`, ...) use
deferred imports inside the function body to avoid a circular import between
`jobs.py` and `dbt_assets.py`. `full_pipeline_job` unions everything into one
end-to-end run, ordered by the asset dependencies.

### 7.7 `ducklake_cleanup_assets.py` — DuckLake catalog maintenance

[ddd_python/ddd_dagster/ducklake_cleanup_assets.py](../ddd_python/ddd_dagster/ducklake_cleanup_assets.py)

A single asset (`ducklake_cleanup_asset`, group `maintenance`) that vacuums the
DuckLake catalog when `SILVER_STORAGE_FORMAT=ducklake` (otherwise it logs a skip
and returns). It opens a server session (`open_server_session`, §4.4), then:

1. `CALL ducklake_expire_snapshots(..., older_than=NOW() - INTERVAL '31 days')` —
   expire snapshots older than 31 days (recent ones are kept for time-travel).
2. `CALL ducklake_delete_orphaned_files(...)` — delete Parquet files no longer
   referenced by the catalog.
3. A filesystem sweep that removes leftover `*_current_temp` directories, which
   earlier versions of the Silver macros left behind (the current macros do not
   create them). In Docker only `duckdb_server` mounts the DuckLake data, so the
   sweep runs there: `ddd_utils/ducklake_maintenance.py` registers it on the
   server as the SQL function `ducklake_sweep_current_temp`, and the asset calls
   that function. Only directories unchanged for 60 minutes are removed.

**Critical invariant:** the sweep **never** touches `*__dbt_tmp` directories —
DuckLake stores *live* table data there (dbt's incremental-append strategy writes
staging Parquet into `…__dbt_tmp/` and the main table's snapshot references those
files). Deleting them corrupts Silver. It runs via `ducklake_cleanup_job`
(`in_process_executor`), which is a **manual** job — there is no schedule for it
(run it on demand now and then).

### 7.8 `bronze_archive_assets.py` — archiving processed Bronze files

[ddd_python/ddd_dagster/bronze_archive_assets.py](../ddd_python/ddd_dagster/bronze_archive_assets.py)

`bronze_archive_asset` (group `maintenance`), run by the on-demand
`bronze_archive_job`, wraps `archive_bronze.run_archive` (§8.5). Its Launchpad
config is `keep_days` (default 7), `compress` (default `true`) and `dry_run`
(default `false`). With `RAW_STORAGE_TARGET=s3` it logs and does nothing. There is
no schedule.

### 7.9 `schedules.py` — when jobs run

[ddd_python/ddd_dagster/schedules.py](../ddd_python/ddd_dagster/schedules.py)

Two cron schedules, both **defaulting to STOPPED** (you enable them in the UI):
the full pipeline at 06:00 and the Data Engineering refresh at 08:00, both
Europe/Copenhagen. Ordering within the pipeline is enforced by asset
dependencies, not by spacing the cron times. (`ducklake_cleanup_job` is
deliberately left unscheduled — see §7.7.)

### 7.10 `sensors.py` — run-status logging

[ddd_python/ddd_dagster/sensors.py](../ddd_python/ddd_dagster/sensors.py)

Two `@run_status_sensor`s (one SUCCESS, one FAILURE) that fire when **any** job
finishes. They pull run + per-step timing from the Dagster instance, build a
structured summary, call `dlt_onelake.write_job_run_log(...)` to append it to
`<DLT_PIPELINE_RUN_LOG_DIR>/DDD/<job_name>_run_log.ndjson`, and send an ntfy.sh
alert when `NTFY_TOPIC` is set. Write or alert failures are downgraded to
warnings so they never break the sensor. Step-failure details are not exported by
the sensors: the `dagster_step_failures_raw` dbt model reads them directly from
Dagster's SQLite run history (`$DAGSTER_HOME/history/runs/*.db`).

### 7.11 `definitions.py` — the registry

[ddd_python/ddd_dagster/definitions.py](../ddd_python/ddd_dagster/definitions.py)

The entry point Dagster loads (via `workspace.yaml`). It imports every asset, job,
schedule, and sensor and assembles the single `Definitions(...)` object — registering the two resources:
`DltPipelineResource()` under key `"dlt_onelake"` and the `DbtCliResource` under
`"dbt"`. When an asset declares a `dlt_onelake` or `dbt` parameter, Dagster injects
these.

---

## 8. The platform-operations scripts

These files in `ddd_utils` operate the *platform* (Docker containers, backups,
the Fabric capacity) or move files around the data (archive, renaming, a storage
migration), rather than transform it. They're standalone CLI tools.

### 8.1 `backup_common.py` — shared backup config

[ddd_python/ddd_utils/backup_common.py](../ddd_python/ddd_utils/backup_common.py)

The single source of truth for backups. Defines a frozen `@dataclass BackupTarget`
(name, source dir, backup dir, which containers to stop, optional restore UID, max
archive age) and a tuple `BACKUP_TARGETS` assembled by `_build_backup_targets()`:
`dagster`, `metabase`, and `duckdb` always, plus `ducklake` when
`SILVER_STORAGE_FORMAT=ducklake`. In that mode `ducklake` (the Parquet data files)
is ordered **before** `duckdb` (which carries the DuckLake catalog), so the catalog
snapshot can never reference a data file the backup missed. Default local retention
is 62 days for `dagster`/`metabase` and 7 days for `duckdb`/`ducklake`. Also holds
the Docker helpers `stop_containers` /
`start_containers` (which check running state first and use `docker stop/start`
directly so they work from inside a container) and `available_timestamps` for
discovering existing archives. Both `backup_platform` and `restore_platform` import
from here and nowhere else.

### 8.2 `backup_platform.py`

[ddd_python/ddd_utils/backup_platform.py](../ddd_python/ddd_utils/backup_platform.py)

For each selected target: stop its containers, wait for DBs to flush
(`FLUSH_WAIT_SECONDS`), create a verified deflate zip, optionally rsync it to a
Hetzner StorageBox, prune archives older than the target's max age, and restart
*only* the containers it stopped. Every target writes an NDJSON log record (typed via
`_LogRecord` `TypedDict`). Container restart happens in a `finally` so a crash mid-
backup still brings services back up. Uses `argparse` for `--targets`.

### 8.3 `restore_platform.py`

[ddd_python/ddd_utils/restore_platform.py](../ddd_python/ddd_utils/restore_platform.py)

The inverse. It **resolves the full restore plan first** (fail fast before touching
live data), prompts for confirmation (skippable with `--yes`), stops containers,
extracts the archive over the live directory, and restarts. When a
target's data dir is owned by a different UID (Metabase runs as UID 2000), it runs
the extraction *inside a Docker container as that UID* so restored files get the
correct ownership. Defaults to the most recent backup; `--timestamp` picks a
specific one.

### 8.4 `fabric_capacity_pause_resume.py`

[ddd_python/ddd_utils/fabric_capacity_pause_resume.py](../ddd_python/ddd_utils/fabric_capacity_pause_resume.py)

Pauses/resumes the Fabric capacity via the Azure Management REST API to save money
when idle. Gets a token from the shared credential, POSTs to the `/suspend` or
`/resume` endpoint, then polls (`wait_for_status`) until the capacity reaches the
target state or times out. Skips the call if already in the desired state.

### 8.5 `archive_bronze.py` — moving processed Bronze files

[ddd_python/ddd_utils/archive_bronze.py](../ddd_python/ddd_utils/archive_bronze.py)

Silver only reads the Bronze files newer than its bookmark, so processed files can
leave the Bronze folders, which keeps the Bronze views small. Per entity the script
archives every file older than the newest processed file (read from the model's
`_last_file` table) and older than `--keep-days` (default 7); the newest processed
file stays, and so does a late file that is not in the model's `_files` log. Each
file is copied to `BRONZE_ARCHIVE_DIR/<source>/<entity>/` (optionally gzipped),
checked against its SHA-256 and listed in `MANIFEST.ndjson` before it is removed;
`--restore` copies files back. The default is a dry run; `--execute` moves the
files. Local Bronze only.

### 8.6 `mark_full_extracts.py` — marking old full extractions

[ddd_python/ddd_utils/mark_full_extracts.py](../ddd_python/ddd_utils/mark_full_extracts.py)

A one-off for Bronze files written before the `_full` marker existed: it finds
the full extractions of incremental entities and renames them to
`<entity>_<timestamp>_full.json`. The extraction run log decides first (start date
`1900-01-01`); without a log record the file size decides (`--ratio` times the
median and `--min-share` of the largest file, default 0.9). Dry run by default,
`--apply` renames. Rename before the first Silver run of a version that logs Bronze
files, or the renamed files look late.

### 8.7 `migrate_silver_to_ducklake.py` — moving Silver into DuckLake

[ddd_python/ddd_utils/migrate_silver_to_ducklake.py](../ddd_python/ddd_utils/migrate_silver_to_ducklake.py)

A one-off for switching `SILVER_STORAGE_FORMAT` from `duckdb` to `ducklake`:
because Silver is the state, its history has to be copied, not rebuilt. The
script attaches the DuckDB file read-only and the DuckLake catalog, copies every
Silver table (`CREATE TABLE … AS SELECT *`) and checks the row counts.

---

## 9. The test suite

Location: [tests/](../tests/), run with `pytest tests/`. Pure helpers get unit
tests; parts of the SQL of the dbt macros are reproduced and run in an in-memory
DuckDB against fixture files, and `test_dbt_silver.py` runs the real Silver
macros with dbt (`DBT_INTEGRATION_TESTS=1`); the Dagster assets, sensors and
export CLIs are tested with only the external boundary stubbed. The `mock_fabric_clients`
fixture in `conftest.py` replaces the OneLake client module, so the whole suite
runs without credentials or network. What each module covers, and what CI adds
(a real `dbt build` on a committed sample), is in [testing.md](testing.md).

---

## 10. Recurring design choices, summarised

These choices recur across the codebase:

1. **Single source of truth** — entity lists live only in
   `configuration_variables.py`; everything else is derived with comprehensions.
   Tests enforce consistency.
2. **Factory functions** — extraction and export assets are all
   produced from a handful of `_make_*` factories in loops.
3. **Lazy loading** — required env vars and Azure imports are deferred until
   first use, so local mode and tests need no credentials.
4. **Three storage switches, branched in few places** — `RAW_STORAGE_TARGET`,
   `SILVER_STORAGE_FORMAT` and `STORAGE_TARGET` are read mainly in
   `path_utils.py`, the dlt destination builder and `dbt/profiles.yml`; the backup
   targets, the DuckLake cleanup and the Bronze archive check them to decide
   whether they apply.
5. **Structured NDJSON logging that never breaks the run** — log writes are wrapped;
   failures become warnings. Logs are queryable with DuckDB.
6. **Defence in depth on SQL** — bound parameters (`:updated_from`), date-format
   validation, and never logging the `CREATE SECRET` statement.
7. **Collect-then-raise** — export and extraction loops attempt *every* item, collect
   failures, and raise once at the end, so one bad table doesn't hide the rest.
8. **Little parallelism where DuckDB writes** — dbt jobs use the in-process
   executor and dbt runs one thread; `full_pipeline_job` runs at most 2 steps at a
   time (the quack server serialises the writes); I/O-bound extraction/export run
   4-wide.
9. **Barriers for ordering** — no-op assets sequence whole stages without wiring
   every cross edge.
10. **Same code, two entry points** — `export_single_*_table` and `execute_pipeline`
    are called both by CLI scripts and by Dagster assets.

---

## 11. How to make common changes

**Add a new Danish Parliament entity:**
1. Add the API name to `DANISH_DEMOCRACY_FILE_NAMES` in
   `configuration_variables.py` (and to `..._INCREMENTAL` if it should be
   incremental).
2. Add its PK to `DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS`.
3. Run `python -m ddd_python.ddd_dbt.generate_dbt_models` to regenerate the Bronze
   and Silver SQL.
4. Run `pytest tests/test_configuration_variables.py` — it will catch a missing
   count or PK.
5. The Dagster extraction asset, dbt assets, and export asset all appear
   automatically because they iterate the config lists.

**Add a new Rfam table:** update `RFAM_TABLE_NAMES`, `RFAM_TABLE_PRIMARY_KEYS`,
and `RFAM_TABLE_QUERIES`, then regenerate.

**Use a primary key of several columns:** write the key as a list in the
`..._TABLE_PRIMARY_KEYS` entry (`"stemme": ["afstemningid", "aktoerid"]`) and
regenerate. Bronze then computes `LKHS_primary_key_1_column` and Silver keys on it
(§4.1). Change the table's data tests in `models/silver/__models*.yml` to that
column, and migrate an existing Silver table first
([dbt_transformations.md §3.1](dbt_transformations.md#31-shared-building-blocks)).

**Change CDC / hashing logic:** that's in the dbt macros
([dbt_transformations.md](dbt_transformations.md)), not Python. Python only decides which macro a
model calls.

**Change how a new column type is serialised in logs:** edit `_json_default` in
`dlt_pipeline_execution_functions.py` and add a case to `test_json_default.py`.

**Run only part of the pipeline locally:**
```bash
# extract two entities from a date
python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data \
    --date_to_load_from 2026-01-01 --file_names_to_retrieve Sag Stemme
# build only Silver
python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile --models_to_select silver
```

**Add a new env var:** put optional ones in the "eager" block of
`get_variables_from_env.py`; put required (must-exist) ones in `_LAZY_REQUIRED`.

---

*This document describes the Python code only. For the SQL see
[dbt_transformations.md](dbt_transformations.md); for the reasons behind the
design, the dependencies and the known improvement points see
[design_decisions.md](design_decisions.md); for infrastructure see
[hetzner_infrastructure.md](hetzner_infrastructure.md).*
