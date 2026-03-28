# dbt Macros — Reference Documentation

Last updated: March 2026

This document describes all 9 Jinja macros in `dbt/macros/`. They are the core
building blocks of the pipeline: every Bronze view, every Silver CDC table, and
every surrogate key in Gold is produced by calling one of these macros.

---

## Table of Contents

1. [Utility macros](#utility-macros)
   - [`cast_hash_to_bigint`](#cast_hash_to_bigint)
   - [`generate_base_for_hash`](#generate_base_for_hash)
2. [Bronze layer macros](#bronze-layer-macros)
   - [`generate_model_bronze`](#generate_model_bronze)
   - [`generate_model_bronze_latest`](#generate_model_bronze_latest)
3. [Silver layer macros](#silver-layer-macros)
   - [`generate_model_silver_full_extraction`](#generate_model_silver_full_extraction)
   - [`generate_model_silver_incr_extraction`](#generate_model_silver_incr_extraction)
4. [Silver hook macros](#silver-hook-macros)
   - [`generate_pre_hook_silver`](#generate_pre_hook_silver)
   - [`generate_pre_hook_silver_full_refresh`](#generate_pre_hook_silver_full_refresh)
   - [`generate_post_hook_silver`](#generate_post_hook_silver)

---

## Utility Macros

### `cast_hash_to_bigint`

**File:** [dbt/macros/cast_hash_to_bigint.sql](../dbt/macros/cast_hash_to_bigint.sql)

```jinja
{{ cast_hash_to_bigint(columns_to_hash) }}
```

**Purpose:** Wraps DuckDB's built-in `hash()` function and remaps its `UBIGINT`
result to a signed `BIGINT`, making the surrogate key usable in Power BI (which
does not accept unsigned 64-bit integers).

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `columns_to_hash` | expression | Any SQL expression accepted by DuckDB's `hash()` — typically a column or `CONCAT(...)` result |

**How it works:**

DuckDB's `hash()` returns a `UBIGINT` in the range `[0, 2^64 − 1]`. The signed
`BIGINT` range is `[−2^63, 2^63 − 1]`. Values that fit in the signed range
pass through unchanged. Values above `9223372036854775807` (i.e., `2^63 − 1`)
are mapped to their negative mirror: `−1 × (value − 2^63 + 1)`.

```sql
CAST(
    CASE
        WHEN hash(col) > 9223372036854775807
            THEN -1 * (hash(col) - 9223372036854775807)
        ELSE hash(col)
    END
    AS BIGINT
)
```

**Used by:** Gold layer models to generate surrogate dimension keys (e.g.,
`dim_actor.actor_key`).

**Trade-offs:** The remapping is not a true modular reduction — two distinct
`UBIGINT` values can theoretically produce the same `BIGINT`. In practice,
collision probability over the sizes of these datasets is negligible.

---

### `generate_base_for_hash`

**File:** [dbt/macros/generate_base_for_hash.sql](../dbt/macros/generate_base_for_hash.sql)

```jinja
{{ generate_base_for_hash(table_name, columns_to_exclude, primary_key_columns) }}
```

**Purpose:** Builds a deterministic, stable `CONCAT(...)` expression over every
non-excluded, non-PK column in a Bronze table. The result is passed to
`sha256()` in the Silver macros to produce `LKHS_hash_value` — the fingerprint
used for CDC change detection.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_name` | string | Bronze table name as it appears in `information_schema.columns` |
| `columns_to_exclude` | SQL list | Comma-separated column names (inside a SQL `IN (...)`) to omit from the hash — typically tracking columns (`LKHS_date_inserted`, `LKHS_pipeline_execution_inserted`, `LKHS_filename`) |
| `primary_key_columns` | SQL list | Primary key column(s) to omit — the PK itself should not affect the change fingerprint |

**How it works:**

1. Queries `information_schema.columns` at compile time via `run_query()` to
   retrieve all columns except those in `columns_to_exclude` and
   `primary_key_columns`, ordered by `ordinal_position`.
2. Wraps each column as `COALESCE(col::VARCHAR, '<NULL>')` so `NULL` values
   produce a stable, non-null token (configurable via the dbt variable
   `hash_null_replacement`, default `<NULL>`).
3. Separates values with the delimiter `]##[` (configurable via
   `hash_delimiter`) to prevent accidental hash collisions across column
   boundaries (e.g., `"AB"` + `"C"` vs `"A"` + `"BC"`).
4. Returns a single `CONCAT(col1, delim, col2, delim, ...)` expression.

**Example output (simplified):**

```sql
CONCAT(
    COALESCE(navn::VARCHAR,'<NULL>'),']##[',
    COALESCE(efternavn::VARCHAR,'<NULL>'),']##[',
    COALESCE(foedselsdato::VARCHAR,'<NULL>'),']##['
)
```

**Used by:** Both Silver macros, passed as the `base_for_hash` argument.

**dbt project variables consumed:**

| Variable | Default | Purpose |
|----------|---------|---------|
| `hash_null_replacement` | `<NULL>` | Token substituted for `NULL` column values |
| `hash_delimiter` | `]##[` | Separator between column values in the concat string |

---

## Bronze Layer Macros

### `generate_model_bronze`

**File:** [dbt/macros/generate_model_bronze.sql](../dbt/macros/generate_model_bronze.sql)

```jinja
{{ generate_model_bronze(table_name, source_system_code, source_name) }}
```

**Purpose:** Generates a Bronze view that reads from a dbt source (defined in
`__sources.yml`) and exposes all API columns while stripping internal dlt
metadata columns and normalising the `filename` path to just the basename.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_name` | string | dbt source table name (e.g., `afstemning`) |
| `source_system_code` | string | Short code written to `LKHS_source_system_code` — identifies where the row came from (e.g., `DDD`, `RFAM`) |
| `source_name` | string | dbt source block name in `__sources.yml` |

**How it works:**

```sql
SELECT DISTINCT COLUMNS(c -> c != 'filename' AND NOT starts_with(c, '_dlt_'))
,      SUBSTRING(src.filename, ...) AS LKHS_filename
,      '{{ source_system_code }}' AS LKHS_source_system_code
FROM {{ source(source_name, table_name) }} src
```

- `COLUMNS(lambda)` — DuckDB column-glob syntax: selects all columns except
  `filename` and any `_dlt_*` internal columns added by the dlt loader.
- `SUBSTRING(... POSITION('/' IN REVERSE(filename)) ...)` — extracts just the
  filename from the full path (e.g., `stemme_20240101_120000.json`).
- `DISTINCT` — deduplicates rows that may appear in overlapping extraction
  windows.
- Materialized as a `view` (no data stored, reads JSON files on every query).

**Used by:** Every Bronze model (`bronze_ddd_*`, `bronze_rfam_*`). The SQL file
for each entity contains exactly one call to this macro.

---

### `generate_model_bronze_latest`

**File:** [dbt/macros/generate_model_bronze_latest.sql](../dbt/macros/generate_model_bronze_latest.sql)

```jinja
{{ generate_model_bronze_latest(file_name, source_system_code, source_name, data_source_env_var='DANISH_DEMOCRACY_DATA_SOURCE') }}
```

**Purpose:** Generates a `_latest` Bronze view that reads **only the most
recently extracted file** for an entity. This is the snapshot of current source
data used for delete detection during Silver `--full-refresh` runs.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `file_name` | string | Entity file stem (e.g., `afstemning`) — used to glob `afstemning_*.json*` |
| `source_system_code` | string | Written to `LKHS_source_system_code` |
| `source_name` | string | dbt source block name |
| `data_source_env_var` | string | Environment variable holding the Bronze storage path (default: `DANISH_DEMOCRACY_DATA_SOURCE`) |

**How it works:**

```sql
WITH cte_most_recent_file AS (
    SELECT MAX(filename) AS most_recent_file
    FROM read_text('.../file_name/file_name_*.json*')
)
SELECT DISTINCT COLUMNS(c -> c != 'filename' AND NOT starts_with(c, '_dlt_'))
,      SUBSTRING(filename, ...) AS LKHS_filename
,      '...' AS LKHS_source_system_code
,      'N' AS LKHS_deleted_ind
FROM   read_json_auto('.../file_name/file_name_*.json*', filename=True)
WHERE  filename = (SELECT most_recent_file FROM cte_most_recent_file)
```

- `read_text()` over the glob is used cheaply to find `MAX(filename)` without
  parsing JSON — filenames sort lexicographically and timestamps are embedded in
  the name (`YYYYMMDD_HHMMSS`), so `MAX` reliably returns the newest file.
- `read_json_auto()` then reads only that single file.
- `LKHS_deleted_ind = 'N'` — every row in the latest file is assumed to exist
  in the source; absence from this view during a Silver full-refresh flags a delete.

**Note on glob pattern:** `*.json*` matches both `.json` (legacy extraction
format) and `.jsonl` (current dlt filesystem destination format).

**Used by:** The delete branch of `generate_model_silver_incr_extraction` during
`--full-refresh`.

---

## Silver Layer Macros

Both Silver macros implement **SCD Type 2** via **Change Data Capture**,
appending rows with `LKHS_cdc_operation` values of `'I'` (Insert), `'U'`
(Update), or `'D'` (Delete). See
[silver_model_logic.md](silver_model_logic.md) for a detailed walkthrough of
the CDC logic and the critical operational rules around Bronze file management.

### `generate_model_silver_full_extraction`

**File:** [dbt/macros/generate_model_silver_full_extraction.sql](../dbt/macros/generate_model_silver_full_extraction.sql)

```jinja
{{ generate_model_silver_full_extraction(
    file_name, bronze_table_name, primary_key_columns,
    date_column, base_for_hash,
    data_source_env_var='DANISH_DEMOCRACY_DATA_SOURCE'
) }}
```

**Used for:** The 12 full-extract entities (static reference tables where every
extraction contains the complete current dataset). Examples: `afstemningstype`,
`mødestatus`, `periode`, all Rfam tables except `family` and `genome`.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `file_name` | string | Entity file stem, used to glob Bronze files |
| `bronze_table_name` | string | dbt ref name for the Bronze view |
| `primary_key_columns` | string | Primary key column name (single column) |
| `date_column` | string or falsy | Source system date column for `LKHS_date_inserted_src`; if absent, falls back to deriving the date from the filename timestamp |
| `base_for_hash` | expression | Output of `generate_base_for_hash` — the CONCAT expression to hash |
| `data_source_env_var` | string | Env var for Bronze storage path |

**CDC approach:**

Because each extraction file is a full snapshot, change detection compares
**adjacent files** using `LAG()` on the ordered file list:

- **Insert (`I`):** Record exists in file N but `LKHS_primary_key_previous IS NULL`
  (not in file N−1).
- **Update (`U`):** Record exists in both file N and file N−1, but hash differs.
- **Delete (`D`):** Record exists in file N but is absent from file N+1 (detected
  via a `LEFT JOIN` on `CTE_FILES.LKHS_filename_next`). The delete row carries
  the timestamp of file N+1 as `LKHS_date_valid_from`.

Deletes for the last file in the sequence are intentionally deferred — they
cannot be confirmed until a subsequent extraction arrives. Only deletes where
`LKHS_filename_next IS NOT NULL` are emitted.

**Incremental guard:**

```sql
WHERE (LKHS_filename >= last_file OR last_file IS NULL)
{% if is_incremental() %}
AND NOT EXISTS (SELECT pk FROM {{ this }} WHERE pk = ... AND LKHS_date_valid_from = ...)
{% endif %}
```

The `_last_file` bookmark (maintained by the pre/post hooks) ensures already-
processed files are skipped. The `NOT EXISTS` deduplication guard prevents
duplicate rows if the model is re-run over the same file.

---

### `generate_model_silver_incr_extraction`

**File:** [dbt/macros/generate_model_silver_incr_extraction.sql](../dbt/macros/generate_model_silver_incr_extraction.sql)

```jinja
{{ generate_model_silver_incr_extraction(
    file_name, bronze_table_name, primary_key_columns,
    date_column, base_for_hash,
    data_source_env_var='DANISH_DEMOCRACY_DATA_SOURCE'
) }}
```

**Used for:** The 6 incrementally extracted entities (high-volume, frequently
changing tables filtered by `opdateringsdato` at extraction time). Examples:
`aktør`, `møde`, `sag`, `sagstrin`, `sagstrinaktør`, `stemme`.

**Parameters:** Same signature as `generate_model_silver_full_extraction`.

**Key difference — delete detection:**

Because each extraction file contains **only changed records** (not the full
set), a record absent from the latest file does not mean it was deleted — it
simply means it was not modified recently. Delete detection therefore requires
an explicit comparison against the latest Bronze snapshot:

- **Normal incremental run:** No delete detection. Only `'I'` and `'U'` rows
  are produced by comparing each record's current hash against the previous
  hash for the same primary key (`LAG(LKHS_hash_value) OVER (PARTITION BY pk)`).
- **`--full-refresh` run:** Before the model runs, `generate_pre_hook_silver_full_refresh`
  snapshots the current Silver table into `_current_temp`. The macro then adds
  two extra `UNION ALL` branches:
  - Preserve existing `'D'` rows from `_current_temp` (they remain deleted).
  - Emit new `'D'` rows for records that are in `_current_temp` (currently live)
    but absent from the `_latest` Bronze view (i.e., they have truly disappeared
    from the source).

**dbt model config emitted:**

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    unique_key=[primary_key_columns, 'LKHS_date_valid_from'],
    pre_hook  = "{{ generate_pre_hook_silver_full_refresh(...) }}",
    post_hook = "DROP TABLE IF EXISTS schema.model_current_temp;"
) }}
```

The `pre_hook` inside the `config()` call is a nested Jinja expansion — the
string `"{{ generate_pre_hook_silver_full_refresh(...) }}"` is re-rendered by
dbt at run time, not at compile time.

---

## Silver Hook Macros

These three macros are called as dbt hooks (before/after the model SQL runs)
to maintain the `_last_file` bookmark table and the `_current_temp` snapshot
table that the Silver CDC logic depends on.

### `generate_pre_hook_silver`

**File:** [dbt/macros/generate_pre_hook_silver.sql](../dbt/macros/generate_pre_hook_silver.sql)

```jinja
{{ generate_pre_hook_silver(file_name, data_source_env_var='DANISH_DEMOCRACY_DATA_SOURCE') }}
```

**Purpose:** Creates the `_last_file` bookmark table if it does not already
exist, with the correct schema but zero rows (`WHERE 1 = 0`).

**Why this is needed:** Both Silver macros reference
`{{ this.schema }}.{{ this.name }}_last_file` in a `WHERE` clause. On the very
first run the table does not exist. This pre-hook ensures it exists (empty) so
the `SELECT ... FROM _last_file` returns `NULL` rather than raising an error.
A `NULL` result from `_last_file` causes the main query to process all files
(correct first-run behaviour).

**Called by:** Both Silver macros include this as their `pre_hook` in the model
`config()`.

---

### `generate_pre_hook_silver_full_refresh`

**File:** [dbt/macros/generate_pre_hook_silver_full_refresh.sql](../dbt/macros/generate_pre_hook_silver_full_refresh.sql)

```jinja
{{ generate_pre_hook_silver_full_refresh(primary_key_columns='id') }}
```

**Purpose:** On a `--full-refresh` run only, snapshots the current Silver table
into a temporary table (`_current_temp`) that the incremental Silver macro can
join against for delete detection.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `primary_key_columns` | string | Primary key column name (default `id`) |

**Behaviour:**

```
if NOT is_incremental():
    DROP TABLE IF EXISTS schema.model_current_temp
    if the Silver table already exists:
        CREATE TABLE schema.model_current_temp AS
            SELECT * FROM schema.model
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY LKHS_source_system_code, pk
                ORDER BY LKHS_date_valid_from DESC
            ) = 1
```

- Only executes on `--full-refresh` (i.e., when `is_incremental()` returns
  `False`).
- If the Silver table does not yet exist (true first run), the `CREATE TABLE` is
  skipped — there is nothing to snapshot, and delete detection is irrelevant.
- The `QUALIFY ROW_NUMBER() = 1` selects the **latest version** of each record
  — the row that is currently active in the source.

**Used by:** `generate_model_silver_incr_extraction` via its `pre_hook` config.
The post-hook `DROP TABLE IF EXISTS schema.model_current_temp` (inlined in the
macro config) cleans it up after the model finishes.

---

### `generate_post_hook_silver`

**File:** [dbt/macros/generate_post_hook_silver.sql](../dbt/macros/generate_post_hook_silver.sql)

```jinja
{{ generate_post_hook_silver(file_name, data_source_env_var='DANISH_DEMOCRACY_DATA_SOURCE') }}
```

**Purpose:** After a successful Silver model run, updates the `_last_file`
bookmark table to record the filename of the most recently processed file.
On the next run, the Silver macro uses this bookmark to skip files that have
already been processed.

**How it works:**

```sql
DROP TABLE IF EXISTS schema.model_last_file;
CREATE TABLE schema.model_last_file AS
SELECT LKHS_filename, LKHS_date_valid_from, LKHS_filename_previous, ...
FROM (
    SELECT SUBSTRING(filename, ...) AS LKHS_filename, LAG(...) AS LKHS_filename_previous, ...
    FROM read_text('.../file_name_*.json*')
    QUALIFY ROW_NUMBER() OVER (ORDER BY LKHS_filename DESC) = 1
)
```

- Re-scans the Bronze file directory at post-hook time to find the current
  latest file (guarantees correctness even if new files arrived between the
  pre-hook and model execution).
- `QUALIFY ROW_NUMBER() = 1` on `ORDER BY LKHS_filename DESC` keeps only the
  single most-recent row.
- The `LKHS_filename_previous` column is what the Silver macro's `WHERE` clause
  reads — it allows the **next** run to reprocess the previous file's window
  boundary to catch any records that landed late.

---

## How the Macros Fit Together

```
Bronze JSON files on disk
        │
        ▼
generate_model_bronze()          ← Bronze view: all files, all columns
generate_model_bronze_latest()   ← Bronze _latest view: newest file only
        │
        ▼
[pre_hook] generate_pre_hook_silver()                  ← create _last_file if missing
[pre_hook] generate_pre_hook_silver_full_refresh()     ← snapshot _current_temp on --full-refresh
        │
        ▼
generate_model_silver_full_extraction()  ─┐
generate_model_silver_incr_extraction()  ─┘  ← CDC: I / U / D rows, uses generate_base_for_hash()
        │
        ▼
[post_hook] generate_post_hook_silver()   ← advance _last_file bookmark
        │
        ▼
Gold views
    cast_hash_to_bigint()   ← surrogate key from SHA256 hash → signed BIGINT
```

Each Silver model execution follows this sequence:

1. **Pre-hook `generate_pre_hook_silver`** — ensures `_last_file` exists.
2. **Pre-hook `generate_pre_hook_silver_full_refresh`** — (incremental macro only,
   `--full-refresh` path) snapshots current Silver into `_current_temp`.
3. **Main model SQL** — reads Bronze, computes hashes via `generate_base_for_hash`,
   applies CDC logic, filters by `_last_file` bookmark, deduplicates with
   `NOT EXISTS`.
4. **Post-hook** (inline in incremental macro) — drops `_current_temp`.
5. **Post-hook `generate_post_hook_silver`** — advances `_last_file` bookmark to
   the newest processed file.
