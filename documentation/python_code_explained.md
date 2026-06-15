# The Python Code Explained — A Guide for the Data Engineer

This document explains **every Python module** in this repository: what it does,
how it fits into the pipeline, the design choices behind it, and worked examples.
It is written for a data engineer who knows SQL, dbt, and data modelling well, but
who wants to fully understand the Python "glue" that Claude helped write — so that
you can confidently change, extend, and debug it yourself.

You do **not** need to read this top to bottom. Use the map below to jump to the
part you care about.

---

## Table of contents

1. [The 30-second mental model](#1-the-30-second-mental-model)
2. [How the code is organised](#2-how-the-code-is-organised)
3. [Cross-cutting Python concepts you'll see everywhere](#3-cross-cutting-python-concepts-youll-see-everywhere)
4. [`ddd_utils` — the foundation layer](#4-ddd_utils--the-foundation-layer)
5. [`ddd_dlt` — extraction and export](#5-ddd_dlt--extraction-and-export)
6. [`ddd_dbt` — dbt tooling and code generation](#6-ddd_dbt--dbt-tooling-and-code-generation)
7. [`ddd_dagster` — orchestration](#7-ddd_dagster--orchestration)
8. [The platform-operations scripts (backup, restore, capacity)](#8-the-platform-operations-scripts)
9. [The test suite](#9-the-test-suite)
10. [Recurring design choices, summarised](#10-recurring-design-choices-summarised)
11. [Code quality: an honest assessment](#11-code-quality-an-honest-assessment)
12. [How to make common changes](#12-how-to-make-common-changes)

---

## 1. The 30-second mental model

The pipeline moves data through five stages. Python owns the **bookends** and the
**orchestration**; dbt + DuckDB own the **SQL transformations** in the middle.

```text
      ┌──────────────────┐         ┌────────────────────────┐         ┌──────────────────┐
      │     SOURCES      │         │      DuckDB (dbt)      │         │      OUTPUT      │
      │                  │         │                        │         │                  │
      │  DK Parliament   │         │    Bronze   (views)    │         │    Delta Lake    │
      │    OData API     │ ──────> │  Silver   (CDC/SCD2)   │ ──────> │    on OneLake    │
      │    Rfam MySQL    │         │    Gold     (star)     │         │  (or local FS)   │
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

* **`ddd_dlt`** pulls data out of the API and the MySQL database and lands it as
  JSON/Parquet files (this is "extraction"). It also pushes the finished tables
  out to Delta Lake (this is "export").
* **`ddd_dbt`** writes the dbt SQL model files for you (code generation) and runs
  `dbt build`.
* **`ddd_dagster`** is the conductor: it turns every step above into a Dagster
  *asset*, draws the dependency graph, runs them in the right order, retries
  failures, and logs run summaries.
* **`ddd_utils`** is the shared foundation everything else imports: configuration
  lists, environment variables, path building, string helpers, and the
  cloud-storage client.

The single most important file is
[ddd_python/ddd_utils/configuration_variables.py](../ddd_python/ddd_utils/configuration_variables.py).
It is the *source of truth*: it lists every entity, primary key, and date column.
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
| `ddd_utils` | Config, env vars, paths, strings, cloud clients, backup helpers | (mostly imported, not run) |
| `ddd_dlt` | Extract from sources; export to Delta Lake | `dlt_run_extraction_pipelines_*`, `export_main_*` |
| `ddd_dbt` | Generate dbt SQL; run `dbt build`; init DuckDB | `generate_dbt_models`, `dbt_build_with_unique_logfile` |
| `ddd_dagster` | Orchestrate everything as assets/jobs/schedules | `dagster dev -w workspace.yaml` |

The `__init__.py` files are almost all empty — they exist only to mark each folder
as a Python package so `import ddd_python.ddd_dlt...` works.

---

## 3. Cross-cutting Python concepts you'll see everywhere

Before the module-by-module tour, here are the recurring Python patterns. Once you
recognise these five, most of the code reads easily.

### 3.1 The factory function pattern

Instead of writing 18 nearly-identical asset definitions by hand, the code writes a
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
it — that captured variable is called a **closure**. This is how 18 distinct assets
are produced from a single 40-line factory.

### 3.2 Configuration-driven, not hardcoded

Lists of entities, their primary keys, and their date columns live in
`configuration_variables.py`. Everything else loops over those lists. Adding a new
table is meant to be a *one-file* change. You will see this comment style
throughout:

```python
# Derived from DANISH_DEMOCRACY_FILE_NAMES — one Bronze model per entity.
DANISH_DEMOCRACY_MODELS_BRONZE = [f"bronze_ddd_{normalize_danish_name(n)}" for n in DANISH_DEMOCRACY_FILE_NAMES]
```

### 3.3 Lazy loading of credentials and heavy libraries

Azure SDKs and credentials are only needed when you actually talk to OneLake. So
the code **defers** importing them and reading the env vars until first use. This
lets you run tests, generate dbt models, and use local storage **without any Azure
credentials present**. Two mechanisms achieve this:

* Module-level `__getattr__` in `get_variables_from_env.py` (see §4.3).
* Function-local `import` statements (`from ... import get_fabric_onelake_clients`
  *inside* a function body, not at the top of the file).

### 3.4 The `STORAGE_TARGET` switch (`local` vs `onelake`)

A single environment variable, `STORAGE_TARGET`, flips the entire pipeline between
writing to your local disk (`data/...`) and writing to Microsoft Fabric OneLake
(`abfss://...`). Every place that touches storage checks this flag. This is what
lets the whole thing run on a laptop with zero cloud setup.

### 3.5 Structured run logging as NDJSON

Almost every operation writes a one-line JSON record (NDJSON = newline-delimited
JSON) to a log file — locally or on OneLake. Because each line is valid JSON, you
can later query the logs with DuckDB itself:

```sql
SELECT * FROM read_json_auto('data/logs/DDD/*.ndjson');
```

A logging failure is **never** allowed to break the actual pipeline — log writes
are wrapped in `try/except` that downgrade errors to warnings.

---

## 4. `ddd_utils` — the foundation layer

Location: [ddd_python/ddd_utils/](../ddd_python/ddd_utils/). Everything else
imports from here.

### 4.1 `configuration_variables.py` — the source of truth

[ddd_python/ddd_utils/configuration_variables.py](../ddd_python/ddd_utils/configuration_variables.py)

This file is just **data** — lists and dictionaries, no functions. But it is the
spine of the project. It declares, for both source systems:

* The full list of entities/tables (`DANISH_DEMOCRACY_FILE_NAMES` — 18,
  `RFAM_TABLE_NAMES` — 7).
* Which ones are extracted *incrementally* vs *fully*
  (`..._INCREMENTAL` subsets).
* The derived dbt model names for Bronze and Silver (built with list
  comprehensions, so they can never drift out of sync with the entity list).
* Primary keys per table (`..._TABLE_PRIMARY_KEYS`).
* For Rfam: the source date column per table (`RFAM_TABLE_DATE_COLUMNS`) and the
  SQL query templates (`RFAM_TABLE_QUERIES`).

**Worked example — how a name becomes three model names.** Take the API entity
`"Aktør"`:

```python
normalize_danish_name("Aktør")            # -> "aktoer"   (see §4.2)
# bronze model:  f"bronze_ddd_{...}"       -> "bronze_ddd_aktoer"
# silver model:  bronze -> silver          -> "silver_ddd_aktoer"
```

**Design choice — derived, not duplicated.** Look at how Silver names are built:

```python
DANISH_DEMOCRACY_MODELS_SILVER = [m.replace("bronze_", "silver_", 1) for m in DANISH_DEMOCRACY_MODELS_BRONZE]
```

The Silver list is *computed* from the Bronze list. You physically cannot add a
Bronze entity and forget its Silver counterpart — they're the same loop. The test
file `test_configuration_variables.py` enforces the counts (18, 6, 7, 2) and the
subset relationships, so a typo here fails CI immediately.

**The Rfam query templates** are worth a close look:

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

Two small, heavily-used functions:

**`normalize_danish_name(name)`** converts Danish characters to ASCII and
lowercases, so every layer that maps an API name to an identifier calls this:

```python
normalize_danish_name("Møde")    # -> "moede"
normalize_danish_name("SagstrinAktør")  # -> "sagstrinaktoer"
```

A point worth being precise about: this is **not** a DuckDB limitation. DuckDB
(verified on 1.5.3, the pinned version) happily accepts `ø/æ/å` in schema, table,
and column names — quoted *and* unquoted — and `read_json_auto` preserves the
original Danish column names. The normalisation is a **portability/convention**
choice: dbt model names become `.sql` filenames, identifiers must round-trip
cleanly through Delta Lake / Parquet and downstream tools (Power BI / Fabric), and
staying ASCII avoids having to quote identifiers everywhere and sidesteps
case-folding differences between engines.

The implementation lowercases *first*, then does three `.replace()` calls — so it
only needs to handle the lowercase forms (`ø æ å`) rather than all six. This is the
**single canonical implementation**; the CLAUDE.md note "imported by all callers"
is deliberate — there used to be copies, now there's one.

**`resolve_date_to_load_from(date, default_days, reference_time)`** decides the
lower-bound date for incremental loads. If you pass `None`, it returns
`reference_time - default_days` formatted as `YYYY-MM-DD`. If you pass a string, it
validates the format and returns it unchanged (raising `ValueError` on a bad
format). Passing `reference_time` in (rather than calling `datetime.now()` inside)
makes the function **deterministic and testable** — the tests can feed a fixed
clock.

### 4.3 `get_variables_from_env.py` — lazy environment loading

[ddd_python/ddd_utils/get_variables_from_env.py](../ddd_python/ddd_utils/get_variables_from_env.py)

This is the cleverest small file in the repo, so it's worth understanding fully.

The problem it solves: some env vars are **required** (Azure credentials), others
are **optional** (with sensible defaults). If the module read all required vars at
import time, you couldn't even `import` it to generate dbt models or run unit tests
without Azure credentials in your shell.

The solution: a custom module class, `_LazyEnv(types.ModuleType)`:

* **Optional** vars are read eagerly with `os.getenv(...)` and stored as attributes
  (e.g. `STORAGE_TARGET`, `LOCAL_STORAGE_PATH`, `DUCKDB_DATABASE_LOCATION`). These
  never raise. This block also handles `SILVER_STORAGE_FORMAT` (default `duckdb`,
  validated against `{duckdb, ducklake}` — an invalid value raises immediately)
  plus the two DuckLake paths (`DUCKLAKE_CATALOG_LOCATION`, `DUCKLAKE_DATA_PATH`),
  which are read eagerly and default to `None` when not in DuckLake mode.
* **Required** vars are listed in `_LAZY_REQUIRED` and resolved only on **first
  access**, via `__getattr__`. So `get_variables_from_env.AZURE_CLIENT_SECRET`
  raises `EnvironmentError` *only if you actually touch it* and it's missing.

The final trick is the last line:

```python
sys.modules[__name__] = _mod
```

This **replaces the module object itself** in Python's module cache with the
custom `_LazyEnv` instance. After this, `from ddd_python.ddd_utils import
get_variables_from_env` hands you the lazy object, so `get_variables_from_env.FOO`
goes through the custom `__getattr__`. (The CLAUDE.md memory note about reading via
`python-dotenv` is the *older* design — the current file uses this lazy wrapper.
`load_dotenv()` is still called at the top to populate `os.environ` from `.env`.)

Helper functions:
* `_require(name)` — fetch or raise with a clear message.
* `_int_env(name, default)` — parse an int env var, raising a *useful* error if
  it's set but not a number (rather than a cryptic `ValueError` later).
* `STORAGE_TARGET` is validated against `{"local", "onelake"}` at import — a typo
  fails fast.

### 4.4 `path_utils.py` — building storage paths

[ddd_python/ddd_utils/path_utils.py](../ddd_python/ddd_utils/path_utils.py)

Centralises the "local vs OneLake" path logic so no call site has to repeat it.

* **`build_bronze_destination_path(source_system_code, entity_name)`** returns
  where dlt should land extracted files. Local → `Files/Bronze/DDD/aktoer`; OneLake
  → a path rooted at `FABRIC_ONELAKE_FOLDER_BRONZE`.
* **`build_delta_export_path(layer, table)`** returns a `(path, storage_options)`
  tuple ready for `deltalake.write_deltalake`. For local it also creates the
  directory (`os.makedirs(..., exist_ok=True)`) as a convenience so callers don't
  have to. For OneLake it builds the `abfss://workspace@account.dfs.fabric...` URI
  and fetches a bearer token.

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
covered in [§8](#8-the-platform-operations-scripts).

---

## 5. `ddd_dlt` — extraction and export

Location: [ddd_python/ddd_dlt/](../ddd_python/ddd_dlt/). "dlt" = the
[data load tool](https://dlthub.com) library. This package gets data **in** (from
sources to Bronze files) and **out** (from DuckDB to Delta Lake).

### 5.1 `dlt_pipeline_execution_functions.py` — the extraction engine

[ddd_python/ddd_dlt/dlt_pipeline_execution_functions.py](../ddd_python/ddd_dlt/dlt_pipeline_execution_functions.py)

This 744-line module is the engine room. It exposes **one public entry point**,
`execute_pipeline(pipeline_type, **kwargs)`, which dispatches to one of three
handlers:

| `pipeline_type` | Handler | Use |
|-----------------|---------|-----|
| `"api_to_file"` | `run_api_to_file_pipeline` | Paginated OData/REST → NDJSON |
| `"sql_to_file"` | `run_sql_to_file_pipeline` | SQL query → Parquet or NDJSON |
| `"file_to_file"` | `run_file_to_file_pipeline` | Raw byte copy (no dlt) |

**The key idea: yield records one at a time.** Both the API and SQL handlers define
an inner `@dlt.resource` generator that `yield`s **individual dicts** — one per API
record or per database row — rather than handing dlt one big blob. This lets dlt
infer the schema, manage incremental state, and control memory:

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

**Two flavours of the API resource** — note there are *two* `@dlt.resource`
definitions guarded by `if source_api_incremental_field:`. The incremental one
carries a `dlt.sources.incremental` cursor parameter that dlt detects to manage
state across runs; the full-extract one is a plain generator. The page-fetching
logic is shared by `_iter_odata_pages` so the duplication is minimal.

**The destination** is built by `_make_destination`, which again branches on
`STORAGE_TARGET`: local uses a `file://` bucket, OneLake uses an `az://` bucket with
service-principal credentials. A custom `layout` + `_resolve_path` placeholder
forces dlt to write your exact timestamped filename instead of dlt's default naming.

**Module-level setup** at the top is important and easy to miss:

```python
os.makedirs(get_variables_from_env.DLT_PIPELINES_DIR, exist_ok=True)
os.environ.setdefault("NORMALIZE__DATA_WRITER__DISABLE_COMPRESSION", "true")  # plain .jsonl, not .jsonl.gz
os.environ.setdefault("DESTINATION__FILESYSTEM__ENABLE_DATASET_NAME_NORMALIZATION", "false")
```

These configure dlt's behaviour so the output files are plain NDJSON that the dbt
Bronze `read_json_auto()` views can glob.

**Logging and secret scrubbing.** `execute_pipeline` wraps every run in
`try/except/finally`: it measures wall-clock time, and in the `finally` block writes
an NDJSON log record — *whether or not the run succeeded*. Before logging the call
parameters it runs them through `_scrub_secrets`, which replaces the value of any
key containing `connection_string`/`secret`/`password`/`token` with `"***"`. The
log write itself is wrapped so a logging failure only emits a `warnings.warn`, never
masks the real result.

**`write_log_to_onelake`** is the NDJSON appender. Locally it just opens the file in
append mode. On OneLake it uses the ADLS append/flush API, and explicitly catches
`ResourceNotFoundError` to create the file on first write — a deliberately *specific*
exception rather than a bare `except`.

**`_serialize_trace` / `_json_default`** turn dlt's rich trace object and any exotic
types (like `pendulum.DateTime`) into plain JSON-serialisable values. These are
small but have dedicated unit tests (`test_serialize_trace.py`, `test_json_default.py`).

### 5.2 `dlt_run_extraction_pipelines_danish_parliament_data.py` — DDD orchestrator

[ddd_python/ddd_dlt/dlt_run_extraction_pipelines_danish_parliament_data.py](../ddd_python/ddd_dlt/dlt_run_extraction_pipelines_danish_parliament_data.py)

A runnable script (`python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data`).
It loops over the 18 entities and submits each to `execute_pipeline` via a
`ThreadPoolExecutor(max_workers=4)` — extraction is I/O-bound (waiting on the
network), so threads give real concurrency here.

For each entity it decides the OData filter:

```python
api_filter = (
    f"$filter=opdateringsdato ge DateTime'{date_to_load_from}'&$orderby=id"
    if file_name in incremental_set
    else "$inlinecount=allpages&$orderby=id"
)
```

`incremental_set` is a `set(...)` for O(1) membership tests. After all futures
complete (`as_completed`), it builds a **script-level** NDJSON summary (totals,
per-pipeline status, duration) and writes it once. Any failed pipeline is collected
and re-raised at the end as a single `RuntimeError`, so the process exits non-zero
for the orchestrator to notice.

The `if __name__ == "__main__":` block wires up `argparse` for
`--date_to_load_from` and `--file_names_to_retrieve`, so you can run an ad-hoc
backfill of just a couple of entities from a specific date.

### 5.3 `dlt_run_extraction_pipelines_rfam.py` — Rfam orchestrator

[ddd_python/ddd_dlt/dlt_run_extraction_pipelines_rfam.py](../ddd_python/ddd_dlt/dlt_run_extraction_pipelines_rfam.py)

Structurally identical to the DDD orchestrator (same thread pool, same summary
logging) but for the MySQL source. The notable difference is how it builds the
query and **prevents SQL injection**:

```python
if table_name in incremental_set:
    sql_query  = query_template.format(where_clause=" WHERE updated >= :updated_from")
    sql_params = {"updated_from": date_to_load_from}
else:
    sql_query  = query_template.format(where_clause="")
    sql_params = {}
```

The date is passed as a **named bind parameter** (`:updated_from`), not interpolated
into the SQL string. SQLAlchemy substitutes it safely. Even though
`resolve_date_to_load_from` already validates the date format, this is defence in
depth.

### 5.4 `export_main_silver_to_fabric_silver.py` — Silver → Delta Lake

[ddd_python/ddd_dlt/export_main_silver_to_fabric_silver.py](../ddd_python/ddd_dlt/export_main_silver_to_fabric_silver.py)

Reads each Silver table from DuckDB and writes it to a Delta Lake table. The
interesting logic is **incremental append**:

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

**Read/write split.** DuckDB's `delta` extension is **read-only** at the pinned
version (DuckDB `≥1.5.1,<1.6`) — `delta_scan` reads, but there is no
`COPY ... (FORMAT delta)` writer. So the *existence check* stays on
`deltalake.DeltaTable.is_deltatable` and the *write* stays on
`deltalake.write_deltalake`; only the dedup read moved into DuckDB. A
`_prepare_delta_read` helper `LOAD`s the extension (and, for OneLake, the Azure
stack + the persistent `azure_sp` secret) on the connection before `delta_scan`
runs.

> **Future migration (revisit on the next DuckDB bump).** Newer DuckDB builds add
> a Delta *writer* (`COPY … (FORMAT delta)`), but there is a known **Azure/OneLake
> regression**, so the writer cannot yet replace `write_deltalake` for the
> `onelake` target. Two gates must both clear before dropping the `deltalake`
> dependency from the write path: (1) bump DuckDB to a version with the writer,
> and (2) the Azure regression is fixed. Local writes (`STORAGE_TARGET=local`)
> could move first, but maintaining two write paths isn't worth it for this
> single-node project — wait until both local and Azure writes work, then switch
> Gold (overwrite) and the Silver anti-join result to `COPY … (FORMAT delta)` in
> one go.

**Storage-format aware (DuckDB vs DuckLake).** The connection comes from the
shared `open_export_connection()` (`ddd_utils/path_utils.py`): in native `duckdb`
mode it's a plain read-only connection to the main DuckDB file; in `ducklake`
mode it also attaches the DuckLake catalog read-only (`ATTACH 'ducklake:…' AS
ducklake_catalog … (READ_ONLY)`).  `_silver_source_database()` then resolves the
source to `ducklake_catalog.main_silver.<table>` (DuckLake) or
`<DUCKDB_DATABASE>.main_silver.<table>` (DuckDB), so the export reads the Silver
tables from wherever dbt actually wrote them.  The **Gold** export uses the same
`open_export_connection()`, so its views (which reference
`ducklake_catalog.main_silver`) resolve in DuckLake mode too.

Per-table failures are collected and re-raised as one `RuntimeError` after *all*
tables are attempted, so one bad table doesn't silently skip the rest. The PK comes
from `SILVER_TABLE_PRIMARY_KEYS` (DDD entities use `id`, Rfam tables vary). DuckDB is
opened `read_only=True` for safety.

### 5.5 `export_main_gold_to_fabric_gold.py` — Gold → Delta Lake

[ddd_python/ddd_dlt/export_main_gold_to_fabric_gold.py](../ddd_python/ddd_dlt/export_main_gold_to_fabric_gold.py)

Simpler than Silver: Gold tables are dimensional **views** that are cheap to rebuild,
so every export is a full `mode="overwrite"`. Same read-only DuckDB connection, same
collect-failures-then-raise pattern.

Both export scripts expose `export_single_*_table(connection, table)` functions that
the Dagster export assets import and call directly (see §7.4) — the same code path
works both as a CLI script and inside an orchestrated asset.

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
  `generate_model_bronze` macro, plus a `_latest` view per entity.
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
  handcrafted models (`date`, `individual_votes`).

The string-building uses `PREFIX = '{{'` / `SUFFIX = '}}'` constants so the
generated text contains literal Jinja `{{ ... }}` and `{%- ... -%}` blocks without
fighting Python's own `.format()`. The output is intentionally thin — the real
logic lives in the dbt macros (documented separately in
[dbt_macros.md](dbt_macros.md)); Python just decides *which* macro each model calls
and with *what* parameters (primary key, date column, source system).

The `if __name__ == "__main__":` block at the bottom is the actual recipe: it calls
the three generators for DDD, then again for Rfam with Rfam's config (different
prefix, different env var, different incremental set).

### 6.2 `dbt_build_with_unique_logfile.py` — running dbt

[ddd_python/ddd_dbt/dbt_build_with_unique_logfile.py](../ddd_python/ddd_dbt/dbt_build_with_unique_logfile.py)

A thin wrapper that runs `dbt build --log-format json` as a subprocess, capturing
stdout to a timestamped local log file, and (in OneLake mode only) uploads that log
to Fabric. Returns/raises based on dbt's exit code so a failed build fails the
script. The `--models_to_select` CLI arg passes through to `dbt --select`.

Note the deferred Azure import inside `upload_log_to_azure` — local mode never
touches Azure.

### 6.3 `init_duckdb.py` — bootstrapping the database

[ddd_python/ddd_dbt/init_duckdb.py](../ddd_python/ddd_dbt/init_duckdb.py)

Used in Docker where the `duckdb` CLI isn't installed. It opens the database,
installs/loads the `httpfs`, `azure`, and `delta` extensions, sets the Azure
transport option, and creates a persistent Azure service-principal **secret** so
DuckDB can read OneLake directly.

Security detail: DuckDB's `CREATE SECRET` DDL can't take bound parameters, so the
credentials *are* interpolated into the SQL string — but that statement is
deliberately **never logged**, and only the secret's metadata (name/type/provider) is
printed back for verification.

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
* **Resource** — an injected dependency (here, the OneLake client wrapper).
* **Definitions** — the single object that registers everything for the Dagster UI.

### 7.1 `_constants.py` and `resources.py`

[_constants.py](../ddd_python/ddd_dagster/_constants.py) holds the shared
`_RETRY_POLICY` — 2 retries with exponential backoff (60s → 120s) for transient
network blips — used by every extraction and export asset.

[resources.py](../ddd_python/ddd_dagster/resources.py) defines
`DltOneLakeResource`, a Dagster `ConfigurableResource`. It's a thin wrapper around
`dlt_pipeline_execution_functions.execute_pipeline` and the OneLake log writer.
Why inject it as a resource instead of importing the function directly? Three
reasons stated in the docstring: dependency injection, **testability** (you can swap
in a mock), and auditability in the UI. It also has `write_job_run_log` for the
sensors (§7.7).

### 7.2 `assets.py` — DDD extraction assets

[ddd_python/ddd_dagster/assets.py](../ddd_python/ddd_dagster/assets.py)

The factory pattern (§3.1) in full force. `_make_incremental_asset(api_resource)`
and `_make_full_extract_asset(api_resource)` each return a configured `@asset`. At
the bottom, two list comprehensions build the assets by filtering the canonical
entity list against the incremental set:

```python
incremental_assets   = [_make_incremental_asset(n)  for n in FILE_NAMES if n in INCREMENTAL]
full_extract_assets  = [_make_full_extract_asset(n) for n in FILE_NAMES if n not in INCREMENTAL]
all_extraction_assets = incremental_assets + full_extract_assets
```

Each asset:
* Has a key like `["ingestion", "DDD", "aktoer"]` and a `group_name` for the UI.
* Declares a dependency on `stop_metabase_asset` (see §7.6).
* Carries the shared `_RETRY_POLICY`.
* Reads its date from a per-run `ExtractionConfig` (a Dagster `Config` class) so you
  can override `date_to_load_from` from the UI Launchpad for backfills.
* Calls `dlt_onelake.execute_pipeline(...)` and returns a `MaterializeResult` whose
  `metadata` (records written, file name, duration, status) shows up in the UI.

### 7.3 `rfam_assets.py` — Rfam extraction assets

[ddd_python/ddd_dagster/rfam_assets.py](../ddd_python/ddd_dagster/rfam_assets.py)

The same two-factory structure for the 7 MySQL tables, using `pipeline_type =
"sql_to_file"` and the `:updated_from` bound parameter for incremental tables. Kept
deliberately parallel to `assets.py` so the two source systems feel architecturally
identical.

### 7.4 `export_assets.py` — Delta Lake export assets + ordering barriers

[ddd_python/ddd_dagster/export_assets.py](../ddd_python/ddd_dagster/export_assets.py)

One export asset per Silver table (25) and per Gold table (9), each calling the
`export_single_*_table` functions from §5.4–5.5. Each opens its own read-only DuckDB
connection in a `try/finally` so the connection always closes.

The clever bit is the **barrier assets** — no-op assets that exist purely to
sequence the graph:

```text
dbt Gold (all models)
        │
        ▼
barrier_dbt_gold_complete      (no-op gate)
        │
        ▼
Silver exports                 (25 tables, incremental append)
        │
        ▼
barrier_all_silver_exported    (no-op gate)
        │
        ▼
Gold exports                   (9 tables, full overwrite)
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

`_dbt_multi_asset_with_metabase(...)` builds a `multi_asset` from the dbt manifest
for a given `select` string (`bronze`, `silver`, `gold`, `data_engineering`, or
`resource_type:seed`), and merges in the `stop_metabase_asset` dependency. The
actual execution is `dbt.cli(["build", ...]).stream()`. The Silver asset reads a
`DbtSilverConfig` so you can tick `full_refresh: true` in the UI to pass
`--full-refresh`.

The Data Engineering asset set is an observability layer: dbt models that read
Dagster's *own* SQLite run history via DuckDB's `sqlite_scan()`, turning the
pipeline's run metadata into queryable fact/dimension tables.

### 7.6 `metabase_control_assets.py` — pausing Metabase during runs

[ddd_python/ddd_dagster/metabase_control_assets.py](../ddd_python/ddd_dagster/metabase_control_assets.py)

Two assets that shell out to scripts: `stop_metabase_asset` runs
`./stop_metabase_and_wait.sh` *before* the pipeline, and `start_metabase_asset` runs
`./start_metabase_and_wait.sh` *after* everything. Why? Metabase holds a read
connection to the DuckDB file, and DuckDB allows only a single writer. Stopping
Metabase frees the file so dbt can write. Every materialising asset declares a
dependency on `stop_metabase_asset`; `start_metabase_asset` is built dynamically to
depend on *all* of them (so it runs last):

```python
start_metabase_asset = build_start_metabase_asset(_asset_keys(_materialization_assets))
```

### 7.7 `jobs.py` — grouping assets into runnable jobs

[ddd_python/ddd_dagster/jobs.py](../ddd_python/ddd_dagster/jobs.py)

Defines ~18 jobs via `define_asset_job`, selecting assets by group or by dbt-select
string. The two big design points:

* **Executor choice.** Extraction/export jobs use
  `multiprocess_executor(max_concurrent=4)` for parallelism. dbt jobs and the
  `full_pipeline_job` use `in_process_executor` — because DuckDB allows only **one
  writer**, dbt steps must run strictly sequentially.
* **Metabase wrapping.** `_with_metabase_control(selection)` sandwiches any selection
  between `stop_metabase_asset` and `start_metabase_asset` so every job pauses
  Metabase around its work.

Default Launchpad configs (`_DDD_INCREMENTAL_CONFIG` etc.) are themselves derived
from the canonical incremental lists, so a new incremental entity needs no change
here. The helper functions (`_bronze_selection`, `_silver_selection`, ...) use
deferred imports inside the function body to dodge a circular import between
`jobs.py` and `dbt_assets.py`. `full_pipeline_job` unions everything into one
sequential end-to-end run.

### 7.7b `ducklake_cleanup_assets.py` — DuckLake catalog maintenance

[ddd_python/ddd_dagster/ducklake_cleanup_assets.py](../ddd_python/ddd_dagster/ducklake_cleanup_assets.py)

A single asset (`ducklake_cleanup_asset`, group `maintenance`) that vacuums the
DuckLake catalog when `SILVER_STORAGE_FORMAT=ducklake` (otherwise it logs a skip
and returns). It attaches the catalog, then:

1. `CALL ducklake_expire_snapshots(..., older_than=NOW() - INTERVAL '31 days')` —
   expire snapshots older than 31 days (recent ones are kept for time-travel).
2. `CALL ducklake_delete_orphaned_files(...)` — delete Parquet files no longer
   referenced by the catalog.
3. A filesystem sweep that removes leftover `*_current_temp` directories from the
   Silver pre/post-hooks, then re-asserts `o+r`/`o+rx` on the data tree so Metabase
   (a different UID) can still read it.

**Critical invariant:** the sweep **never** touches `*__dbt_tmp` directories —
DuckLake stores *live* table data there (dbt's incremental-append strategy writes
staging Parquet into `…__dbt_tmp/` and the main table's snapshot references those
files). Deleting them corrupts Silver. It runs via `ducklake_cleanup_job`
(`in_process_executor`, Metabase-wrapped), which is a **manual** job — there is
no schedule for it (run it on demand after a large `--full-refresh`).

### 7.8 `schedules.py` — when jobs run

[ddd_python/ddd_dagster/schedules.py](../ddd_python/ddd_dagster/schedules.py)

Two cron schedules, both **defaulting to STOPPED** (you enable them in the UI):
the full pipeline at 06:00 and the Data Engineering refresh at 08:00, both
Europe/Copenhagen. Ordering within the pipeline is enforced by asset
dependencies, not by spacing the cron times. (`ducklake_cleanup_job` is
deliberately left unscheduled — see §7.7b.)

### 7.9 `sensors.py` — run-status logging

[ddd_python/ddd_dagster/sensors.py](../ddd_python/ddd_dagster/sensors.py)

Two `@run_status_sensor`s (one SUCCESS, one FAILURE) that fire when monitored
extraction jobs finish. They pull run + per-step timing from the Dagster instance,
build a structured summary, and call `dlt_onelake.write_job_run_log(...)` to append
an NDJSON record to OneLake — the Dagster-native equivalent of the script-level
summary the old standalone script wrote. A write failure is downgraded to a warning
so it never blocks the next run.

### 7.10 `definitions.py` — the registry

[ddd_python/ddd_dagster/definitions.py](../ddd_python/ddd_dagster/definitions.py)

The entry point Dagster loads (via `workspace.yaml`). It imports every asset, job,
schedule, and sensor, builds the dynamic `start_metabase_asset`, and assembles the
single `Definitions(...)` object — registering the two resources:
`DltOneLakeResource()` under key `"dlt_onelake"` and the `DbtCliResource` under
`"dbt"`. When an asset declares a `dlt_onelake` or `dbt` parameter, Dagster injects
these.

---

## 8. The platform-operations scripts

These four files in `ddd_utils` operate the *platform* (Docker containers, backups,
the Fabric capacity), not the data. They're standalone CLI tools.

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
extracts the archive over the live directory, and restarts. The neat detail: when a
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

---

## 9. The test suite

Location: [tests/](../tests/). **132 tests across 15 modules**, runnable with
`pytest tests/`. They split into two kinds.

**Unit tests** (fast, no database) check the pure-Python logic:

| File | What it verifies |
|------|------------------|
| `test_configuration_variables.py` | Entity counts (18/6/7/2), incremental ⊂ all, PK/date/query maps cover every table, no duplicates |
| `test_string_utils.py` | Every Danish-char replacement and date resolution path |
| `test_generate_dbt_models.py` | The generator picks the right macro (incremental vs full) and emits `_cv`/`_latest` correctly |
| `test_path_utils.py` | Local vs OneLake path construction and storage options |
| `test_require_env.py` | Lazy env var raises only when missing |
| `test_scrub_secrets.py` | Sensitive keys redacted, others preserved |
| `test_serialize_trace.py`, `test_json_default.py` | dlt trace + exotic-type serialisation |

**Integration tests** (spin up a real in-memory DuckDB and run the actual dbt SQL /
macros against fixture JSON):

| File | What it verifies |
|------|------------------|
| `test_integration_bronze.py` | `read_json_auto`, filename extraction, `_latest` view |
| `test_integration_silver_cdc.py` | CDC insert/update/delete detection, `_cv` view, dedup |
| `test_integration_gold.py` | Surrogate keys, SCD2 date chaining, the "Unknown" id=0 row, fact joins |
| `test_integration_e2e_pipeline.py` | Bronze → Silver → Delta round-trip with incremental append |
| `test_export_silver.py`, `test_export_gold.py` | Export logic with mocked OneLake |

The shared fixture in `conftest.py`, `mock_fabric_clients`, patches the OneLake
client into `sys.modules` so export tests run without any cloud connection — it
returns a fake token. This is what makes the whole suite runnable on a laptop with
no Azure setup.

---

## 10. Recurring design choices, summarised

These themes appear over and over. Internalise them and the code becomes
predictable:

1. **Single source of truth** — entity lists live only in
   `configuration_variables.py`; everything else is derived with comprehensions.
   Tests enforce consistency.
2. **Factory functions** — 18 DDD assets, 7 Rfam assets, 34 export assets are all
   produced from a handful of `_make_*` factories in loops.
3. **Lazy everything** — required env vars and Azure imports are deferred until
   first use, so local mode and tests need no credentials.
4. **One storage switch** — `STORAGE_TARGET` flips local ↔ OneLake everywhere; the
   branching is centralised in `path_utils.py` and the dlt destination builder.
5. **Structured NDJSON logging that never breaks the run** — log writes are wrapped;
   failures become warnings. Logs are queryable with DuckDB.
6. **Defence in depth on SQL** — bound parameters (`:updated_from`), date-format
   validation, and never logging the `CREATE SECRET` statement.
7. **Collect-then-raise** — export and extraction loops attempt *every* item, collect
   failures, and raise once at the end, so one bad table doesn't hide the rest.
8. **Sequential where DuckDB requires it** — dbt and the full pipeline use the
   in-process executor (single writer); I/O-bound extraction/export run 4-wide.
9. **Barriers for ordering** — no-op assets sequence whole stages without wiring
   every cross edge.
10. **Same code, two entry points** — `export_single_*_table` and `execute_pipeline`
    are called both by CLI scripts and by Dagster assets.

---

## 11. Code quality: an honest assessment

The pipeline works, and the code is **well above** the average data-engineering
side-project: it is consistently documented, type-hinted, configuration-driven, and
defended against the obvious foot-guns (SQL injection, leaked secrets, silent log
failures). What follows is a deliberately *critical* read — the things a senior
engineer would flag in review even though nothing here is broken today. None of these
is urgent; they are the difference between "works" and "robust at scale / easy for the
next person."

### 11.1 What is genuinely good (so the critique is in context)

* **One source of truth, enforced by tests.** `configuration_variables.py` plus
  `test_configuration_variables.py` is exactly the right pattern, and rare to see done.
* **Docstrings are excellent** — most modules explain *why*, not just *what*. The
  module docstring in `dlt_pipeline_execution_functions.py` is better than most
  libraries ship.
* **Security hygiene is deliberate** — bound SQL parameters, `_scrub_secrets`, never
  logging the `CREATE SECRET` statement, non-root containers.
* **Error semantics are thoughtful** — collect-then-raise in the export/extraction
  loops, and log-write failures downgraded to warnings so they never mask the real
  result.

### 11.2 Duplication: the two extraction orchestrators

[dlt_run_extraction_pipelines_danish_parliament_data.py](../ddd_python/ddd_dlt/dlt_run_extraction_pipelines_danish_parliament_data.py)
and
[dlt_run_extraction_pipelines_rfam.py](../ddd_python/ddd_dlt/dlt_run_extraction_pipelines_rfam.py)
are ~85% identical: the same `ThreadPoolExecutor` loop, the same `as_completed`
result-collection, and a **verbatim copy** of the ~25-line script-level NDJSON summary
block. The only real differences are the filter/query construction and a couple of
constants.

*Why it matters:* a change to the summary schema or the concurrency model has to be
made in two places and they will eventually drift. *Improvement:* extract a shared
`run_extraction(source_system_code, build_task, items, ...)` helper (or a small class)
that owns the thread pool and the summary log, and pass in a per-system callback that
builds each `execute_pipeline` call. This is the same factory thinking already used so
well in the Dagster assets — it just wasn't applied to the standalone scripts.

### 11.3 `get_variables_from_env.py`: clever, but fragile and un-idiomatic

The `_LazyEnv(types.ModuleType)` + `sys.modules[__name__] = _mod` swap (see §4.3)
achieves real lazy loading, but at a cost:

* It **defeats static analysis** — type checkers and IDE autocomplete cannot see the
  attributes, which is why the file is peppered with `# type: ignore[attr-defined]`.
* Replacing a module object in `sys.modules` is a known foot-gun (import edge cases,
  pickling, reload behaviour) that a future maintainer may not expect.

*Improvement:* the modern idioms give the same laziness without the swap:
module-level `__getattr__` (PEP 562) alone, or — cleaner still — a typed settings
object via `pydantic-settings`/a frozen dataclass with lazily-resolved properties.
Either restores autocomplete and lets you delete every `# type: ignore`.

### 11.4 SQL-as-f-strings in the model generator

[generate_dbt_models.py](../ddd_python/ddd_dbt/generate_dbt_models.py) builds dbt model
files by concatenating f-strings with `PREFIX='{{'` / `SUFFIX='}}'` constants to emit
literal Jinja. It works, but the multi-line f-string assembling the Silver model
(config block + macro call + `_cv` view) is hard to read and easy to break with a
stray comma or quote. *Improvement:* use Jinja2 template files (the very engine dbt
already uses) or at least `textwrap.dedent` with named `.format()` fields. The logic
("which macro, which PK, which date column") stays in Python; only the *rendering*
moves to templates, which is where it belongs.

### 11.5 Subprocess robustness

* [metabase_control_assets.py](../ddd_python/ddd_dagster/metabase_control_assets.py)
  calls `subprocess.run(["./stop_metabase_and_wait.sh"], check=True)` with a
  **relative path and no timeout**. The relative path silently depends on Dagster's
  working directory; if that ever changes, the asset fails confusingly. And a hung
  script would block the run forever.
* [dbt_build_with_unique_logfile.py](../ddd_python/ddd_dbt/dbt_build_with_unique_logfile.py)
  and the backup/restore `docker`/`rsync` calls likewise run without timeouts.

*Improvement:* resolve script paths relative to a known anchor (e.g.
`Path(__file__).resolve().parents[...]`, as `dbt_assets.py` already does for the dbt
project dir) and pass `timeout=` to long-running subprocess calls.

### 11.6 Test coverage skews toward SQL, away from the Python glue

The 132 tests are strong on what they cover — config consistency, the dbt CDC/SCD2
logic, path/string helpers, export logic. But the **orchestration layer is largely
untested**: there are no tests for the Dagster assets/jobs/sensors
(`assets.py`, `rfam_assets.py`, `export_assets.py` barriers, `jobs.py`, `sensors.py`),
nor for `backup_platform`/`restore_platform` or `fabric_capacity_pause_resume`.

*Why it matters:* the factory functions and barrier wiring are exactly the kind of code
that breaks silently on a refactor (a renamed asset key, a wrong `deps=` list).
*Improvement:* Dagster ships testing utilities — `materialize([...])` with a mocked
`DltOneLakeResource`, and `build_asset_context()` — that make these cheap to test
without touching the network. Even a handful of "the graph has the expected keys and
dependencies" tests would catch the most likely regressions.

### 11.7 The Silver export anti-join (resolved: now reads via `delta_scan`)

*Historical note — this concern has been addressed.* The incremental path used to
do `connection.register(..., target_table.to_pyarrow_table())`, pulling the
**entire existing Delta table** into memory to compute the anti-join. It now
`LEFT JOIN`s against `delta_scan('<target_path>')` directly inside DuckDB, so the
target is read with projection pushdown and is never fully materialised in Python
(see §5.4). The write still uses `deltalake.write_deltalake` because DuckDB's
delta extension is read-only. *Further improvement (only if volumes grow):* push a
`LKHS_date_valid_from` lower-bound predicate into the `delta_scan` so even the
join-key scan is bounded, rather than scanning all of history's keys.

### 11.8 Tooling and CI gaps

* **No linter/formatter and no type-checker in the dev workflow.** `pyproject.toml`'s
  `dev` extra is just `pytest`. The code *looks* consistently formatted, but nothing
  enforces it, and the `# type: ignore` comments show `mypy`/`pyright` isn't run.
  *Improvement:* add `ruff` (lint + format) and `mypy` to the `dev` extra and a CI
  step. This is a ~1-hour change that pays off on every future edit.
* **Counts live in prose.** Numbers like "53 Bronze views / 50 Silver models / 18
  entities" appear in docstrings and `CLAUDE.md`. They drift (the `app` vs `appuser`
  Docker-user mismatch between the Dockerfile and `CLAUDE.md` is a live example). Where
  it's cheap, derive counts from the config lists instead of writing them down.

### 11.9 Minor / stylistic

* **`full_pipeline_job` serialises extraction too.** It uses `in_process_executor` for
  the *whole* graph to honour DuckDB's single-writer rule — but extraction doesn't
  write to DuckDB, so it loses the 4-way concurrency it has in the standalone
  extraction jobs. A run-level concurrency limit (or splitting extraction into a
  separate concurrent job that the schedule chains) would recover that without
  endangering the dbt steps.
* **`_TS_MICROSEC.sub` runs on every string field of every record**, not just
  timestamps — a small, broad cost. Negligible today; worth knowing.
* **Logging-failure handling is inconsistent**: the dlt module uses `warnings.warn`,
  the sensors use `logger.warning`. Pick one convention.
* **`_make_destination` leans on dlt's internal `layout`/placeholder API**, which is
  more coupled to dlt's version than the rest of the code; pin dlt tightly or add a
  smoke test that fails loudly if the file-naming contract changes.

### 11.10 If you had one day to invest

In rough priority order:

1. **Add `ruff` + `mypy` to `dev` and CI** — highest leverage, lowest effort; catches
   future mistakes automatically.
2. **De-duplicate the two extraction orchestrators** (§11.2) — removes the most likely
   drift point in the codebase.
3. **Add Dagster-level tests** for asset keys and dependency wiring (§11.6) — protects
   the part most exposed to refactors.
4. **Replace the `sys.modules` env swap** with PEP 562 `__getattr__` or
   `pydantic-settings` (§11.3) — restores tooling support, deletes the `type: ignore`s.

Everything else is opportunistic: do it when you're already in that file.

---

## 12. How to make common changes

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
`RFAM_TABLE_DATE_COLUMNS`, and `RFAM_TABLE_QUERIES`, then regenerate.

**Change CDC / hashing logic:** that's in the dbt macros
([dbt_macros.md](dbt_macros.md)), not Python. Python only decides which macro a
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

*This document describes the Python code only. For the SQL transformation logic see
[silver_model_logic.md](silver_model_logic.md) and [dbt_macros.md](dbt_macros.md);
for the libraries used see [python_libraries.md](python_libraries.md); for
infrastructure see [hetzner_infrastructure.md](hetzner_infrastructure.md).*
