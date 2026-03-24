# The Modern Data Platform Handbook

## A Practical Guide with Working Code

**Version 1.0 — March 2026**

*Architecture blueprint and production implementation for a budget-conscious, metadata-driven data platform using open-source tools — illustrated with the Danish Parliament open data pipeline.*

---

## Table of Contents

- [Part I — Platform Architecture](#part-i--platform-architecture)
  - [Chapter 1: Executive Summary](#chapter-1-executive-summary)
  - [Chapter 2: Architecture Overview](#chapter-2-architecture-overview)
  - [Chapter 3: The Tech Stack](#chapter-3-the-tech-stack)
  - [Chapter 4: Daily Execution Sequence](#chapter-4-daily-execution-sequence)
- [Part II — Component Deep Dives](#part-ii--component-deep-dives)
  - [Chapter 5: Storage — Microsoft Fabric OneLake](#chapter-5-storage--microsoft-fabric-onelake)
  - [Chapter 6: Compute — DuckDB](#chapter-6-compute--duckdb)
  - [Chapter 7: Ingestion — dlt](#chapter-7-ingestion--dlt)
  - [Chapter 8: Transformation — dbt](#chapter-8-transformation--dbt)
  - [Chapter 9: The Bronze Layer](#chapter-9-the-bronze-layer)
  - [Chapter 10: The Silver Layer — SCD Type 2](#chapter-10-the-silver-layer--scd-type-2)
  - [Chapter 11: Delete Handling in the Silver Layer](#chapter-11-delete-handling-in-the-silver-layer)
  - [Chapter 12: The Gold Layer — Star Schema](#chapter-12-the-gold-layer--star-schema)
  - [Chapter 13: Orchestration — Dagster](#chapter-13-orchestration--dagster)
  - [Chapter 14: Export — Delta Lake](#chapter-14-export--delta-lake)
  - [Chapter 15: Reporting — Power BI](#chapter-15-reporting--power-bi)
  - [Chapter 16: CI/CD & Runtime](#chapter-16-cicd--runtime)
- [Part III — Patterns and Code Generation](#part-iii--patterns-and-code-generation)
  - [Chapter 17: Code Generation — DRY Model Management](#chapter-17-code-generation--dry-model-management)
  - [Chapter 18: Configuration as Code](#chapter-18-configuration-as-code)
  - [Chapter 19: Naming Conventions and Metadata Columns](#chapter-19-naming-conventions-and-metadata-columns)
- [Part IV — Operations and Evolution](#part-iv--operations-and-evolution)
  - [Chapter 20: Advantages and Limitations](#chapter-20-advantages-and-limitations)
  - [Chapter 21: Cost Breakdown](#chapter-21-cost-breakdown)
  - [Chapter 22: Upgrade Path](#chapter-22-upgrade-path)
  - [Chapter 23: Operational Concerns](#chapter-23-operational-concerns)
  - [Chapter 24: The Data Engineering Dashboard](#chapter-24-the-data-engineering-dashboard)

---

# Part I — Platform Architecture

---

## Chapter 1: Executive Summary

This handbook describes a modern, metadata-driven data platform designed for small companies seeking enterprise-grade capabilities on a minimal budget. The platform leverages primarily free-tier services and open-source tools to deliver a complete medallion architecture with comprehensive observability and governance.

Every concept in this handbook is illustrated with working code from the **Danish Democracy Data** pipeline — a production system that ingests 18 entities from the Danish Parliament's OData REST API (`https://oda.ft.dk/api`), historizes them through an SCD Type 2 Silver layer, and publishes a star schema Gold layer for Power BI consumption via Microsoft Fabric OneLake.

The architecture combines ten components into a cohesive stack:

| Layer | Technology | Cost | Purpose |
|-------|-----------|------|---------|
| Storage | Microsoft Fabric OneLake | Included with Power BI PPU | Data lake (Bronze, Silver, Gold) |
| Compute | DuckDB / MotherDuck | Free (open source) / cloud option | In-process analytical engine |
| Transformation | dbt-core + dbt-duckdb | Free (open source) | SQL-based transformations |
| Ingestion | dlt (data load tool) | Free (open source) | Python-based ELT pipelines |
| Orchestration | Dagster | Free (open source) | Python-native orchestration with built-in UI |
| Export | deltalake + PyArrow | Free (open source) | Delta Lake table writes |
| Reporting | Power BI Pro / PPU | Pro €12,10 / PPU €20,80 per user/month | Business intelligence |
| Runtime | Azure Container Instances | ~€7/month | Production pipeline execution |
| CI/CD | GitHub Actions | €5/user/month | Automation, version control |
| Auth | azure-identity | Free (open source) | Service principal authentication |

The total monthly cost for a 3-user team is on the order of **~€80/month** — a fraction of what commercial alternatives like Snowflake, Databricks, or Fivetran would cost.

### The Danish Parliament Pipeline at a Glance

```
Danish Parliament        OneLake Bronze          DuckDB/MotherDuck        OneLake Gold
OData REST API           (JSON files)            Silver + Gold            (Delta Lake)
┌──────────────┐  dlt    ┌──────────────┐  dbt   ┌──────────────┐  Py    ┌──────────────┐
│ 18 entities  │────────►│ Timestamped  │───────►│ SCD2 history │───────►│ Star schema  │
│ (actors,     │  4 max  │ JSON per     │        │ + star schema│        │ Delta tables │
│  meetings,   │ threads │ entity per   │        │ views        │        │ for Power BI │
│  cases, ...) │         │ extraction   │        │              │        │              │
└──────────────┘         └──────────────┘        └──────────────┘        └──────────────┘
                                         Dagster orchestrates the entire flow
```

**Source data:** 18 entities from the Danish Parliament's OData API — parliament members (aktører), meetings (møder), cases (sager), votes (stemmer), and reference tables.

**Extraction strategies:** 6 entities support incremental extraction (filtered by `opdateringsdato`); 12 reference tables are full-loaded every run.

**Output:** A dimensional Gold layer with English names (actor, meeting, case, vote) exported as Delta Lake tables to OneLake for Power BI.

---

## Chapter 2: Architecture Overview

The platform follows a medallion architecture (Bronze → Silver → Gold) within Microsoft Fabric OneLake. Data flows from source systems through dlt ingestion pipelines into the Bronze landing zone; DuckDB and dbt process it through the Silver and Gold layers; and Power BI consumes it for reporting.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Source Systems                                                         │
│  ├── Danish Parliament OData API (https://oda.ft.dk/api)               │
│  ├── APIs / SaaS                                                        │
│  └── Databases / File Systems                                           │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│  ETL Runtime (Dagster orchestration)                                    │
│  ├── dlt Ingestion (concurrent, max 4 threads)                         │
│  │   └── Writes timestamped JSON files to OneLake Bronze                │
│  ├── DuckDB / MotherDuck (in-process compute)                          │
│  └── dbt Transformations (Bronze views → Silver tables → Gold views)   │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
┌───────────────────────────────▼─────────────────────────────────────────┐
│  Microsoft Fabric OneLake                                               │
│  ├── Bronze/  (timestamped JSON files per entity)                      │
│  ├── Silver/  (Delta Lake tables — SCD Type 2 history)                 │
│  ├── Gold/    (Delta Lake tables — star schema for Power BI)           │
│  └── Logs/    (execution logs for observability)                        │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                         Power BI (Pro/PPU)
                    DeltaLake.Table M connector
                        or Direct Lake
```

### Data Flow — Medallion Layers

```
Source Data ──dlt──► Bronze (Raw JSON) ──dbt views──► Silver (SCD2 Tables) ──dbt views──► Gold (Star Schema)
                                                                                              │
                                                                                     Delta Lake export
                                                                                     to OneLake → Power BI
```

---

## Chapter 3: The Tech Stack

### Production Stack (Danish Parliament Pipeline)

| Layer | Technology | Version | Configuration |
|-------|-----------|---------|---------------|
| Storage | Microsoft Fabric OneLake | ADLS Gen2 | `abfss://onelake.dfs.fabric.microsoft.com/...` |
| Compute | DuckDB + MotherDuck | DuckDB 1.4, dbt-duckdb 1.9.6 | `md:danish_democracy_data_md?motherduck_token=...` |
| Transformation | dbt-core | 1.10 | Project: `danish_democracy_data` |
| Ingestion | dlt | 1.17.1 | `ThreadPoolExecutor(max_workers=4)` |
| Orchestration | Dagster | 1.7+ | Factory-pattern assets, multiprocess executor |
| Export | deltalake + PyArrow | deltalake 1.1.4, pyarrow 21.0 | Incremental Silver, full-overwrite Gold |
| Auth | azure-identity | Service principal | `ClientSecretCredential` |
| Config | python-dotenv | 1.0+ | Lazy `__getattr__` env var loading |

### Project Directory Structure

```
dbt_duckdb_demo/
├── dbt/                                # dbt project root
│   ├── dbt_project.yml                 # Project config, schema/materialization
│   ├── profiles.yml                    # DuckDB/MotherDuck connection
│   ├── packages.yml                    # dbt-utils, dbt-expectations
│   ├── models/
│   │   ├── bronze/                     # 18 views + 18 _latest views
│   │   │   ├── __sources.yml           # External JSON source definitions
│   │   │   ├── bronze_aktoer.sql       # Generated: one-liner macro call
│   │   │   ├── bronze_aktoer_latest.sql
│   │   │   └── ...                     # (36 total Bronze models)
│   │   ├── silver/                     # 18 incremental tables + 18 _cv views
│   │   │   ├── silver_aktoer.sql       # Generated: macro + config
│   │   │   ├── silver_aktoer_cv.sql    # Generated: current-version view
│   │   │   └── ...                     # (36 total Silver models)
│   │   └── gold/                       # 9 star-schema views + 9 _cv views
│   │       ├── actor.sql               # Hand-written: Danish → English
│   │       ├── actor_cv.sql            # Generated: current-version view
│   │       ├── vote.sql
│   │       ├── meeting.sql
│   │       ├── case.sql
│   │       └── ...                     # (18 total Gold models)
│   ├── macros/                         # 9 parametric macros
│   │   ├── generate_model_bronze.sql
│   │   ├── generate_model_bronze_latest.sql
│   │   ├── generate_model_silver_incr_extraction.sql
│   │   ├── generate_model_silver_full_extraction.sql
│   │   ├── generate_base_for_hash.sql
│   │   ├── generate_pre_hook_silver.sql
│   │   ├── generate_post_hook_silver.sql
│   │   └── cast_hash_to_bigint.sql
│   └── seeds/                          # Date dimension + lookups
│
├── ddd_python/
│   ├── ddd_dagster/                    # Dagster orchestration
│   │   ├── definitions.py              # Single entry point (defs object)
│   │   ├── assets.py                   # 18 extraction assets (factory pattern)
│   │   ├── dbt_assets.py              # dbt asset integration (3 groups)
│   │   ├── export_assets.py           # Silver & Gold export assets
│   │   ├── jobs.py                    # 10 job definitions
│   │   ├── schedules.py              # Daily schedule
│   │   ├── sensors.py                # Success/failure sensors
│   │   └── resources.py              # DltOneLakeResource
│   │
│   ├── ddd_dlt/                       # dlt + Delta Lake export
│   │   ├── dlt_run_extraction_pipelines_danish_parliament_data.py
│   │   ├── dlt_pipeline_execution_functions.py
│   │   ├── export_main_silver_to_fabric_silver.py
│   │   └── export_main_gold_to_fabric_gold.py
│   │
│   ├── ddd_dbt/                       # Code generator
│   │   └── generate_dbt_models.py     # Generates Bronze/Silver/Gold SQL files
│   │
│   └── ddd_utils/
│       ├── configuration_variables.py  # Entity lists (single source of truth)
│       ├── get_variables_from_env.py   # Lazy env var loading
│       └── get_fabric_onelake_clients.py
│
├── .env                               # Credentials (git-ignored)
└── workspace.yaml                     # Dagster workspace entry-point
```

---

## Chapter 4: Daily Execution Sequence

The platform runs as a Dagster pipeline with explicit asset dependencies enforcing execution order:

| Step | Dagster Job | Component | Action | Duration |
|------|------------|-----------|--------|----------|
| 1 | `danish_parliament_incremental_job` | dlt | 6 incremental entities extracted (filtered by `opdateringsdato`) | ~5 min |
| 2 | `danish_parliament_full_extract_job` | dlt | 12 reference entities extracted (full load) | ~10 min |
| 3 | `dbt_bronze_job` | dbt | 18 Bronze views + 18 `_latest` views created | <1 min |
| 4 | `dbt_silver_job` | dbt | 18 Silver incremental tables updated (SCD2 CDC) | ~5 min |
| 5 | `dbt_gold_job` | dbt | 9 Gold star schema views + 9 `_cv` views created | ~2 min |
| 6 | `export_silver_job` | deltalake | Silver → OneLake Delta Lake (incremental append) | ~3 min |
| 7 | `export_gold_job` | deltalake | Gold → OneLake Delta Lake (full overwrite) | ~2 min |

The `danish_parliament_full_pipeline_job` chains all of these into a single end-to-end run. Extraction and export jobs use `multiprocess_executor` with `max_concurrent=4`; dbt jobs use `in_process_executor` (DuckDB single-writer constraint).

---

# Part II — Component Deep Dives

---

## Chapter 5: Storage — Microsoft Fabric OneLake

OneLake is the unified data lake that underpins the platform. It comes included with Power BI Premium Per User licenses — since you already need Power BI PPU for reporting, the storage is effectively free. OneLake supports direct file access through the Azure Blob Filesystem (ABFS) protocol.

### Configuration

The OneLake path is configured as a dbt variable:

```yaml
# dbt/dbt_project.yml
vars:
  danish_democracy_data_source: "abfss://onelake.dfs.fabric.microsoft.com/<YOUR_WORKSPACE>/<YOUR_LAKEHOUSE>.Lakehouse/Files/Bronze/DDD"
```

### Folder Structure

```
abfss://...<YOUR_LAKEHOUSE>.Lakehouse/Files/
├── Bronze/DDD/
│   ├── aktoer/
│   │   ├── aktoer_20250115_020000.json    # Timestamped JSON per extraction run
│   │   ├── aktoer_20250116_020000.json
│   │   └── ...
│   ├── moede/
│   ├── sag/
│   ├── stemme/
│   └── (14 more entity folders)
├── Silver/DDD/
│   ├── silver_aktoer/                      # Delta Lake tables (SCD2)
│   ├── silver_moede/
│   └── ...
└── Gold/DDD/
    ├── actor/                              # Delta Lake tables (star schema)
    ├── meeting/
    ├── case/
    ├── vote/
    └── ...
```

### Access Pattern

All access uses an Azure AD service principal via `ClientSecretCredential`:

```python
# ddd_python/ddd_utils/get_variables_from_env.py (simplified)
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
```

> **Implementation note:** The DuckDB → ABFS → OneLake → Delta Lake path is the most integration-heavy part of this architecture. DuckDB's `azure` extension, ABFS authentication via service principal, and Delta Lake writes via delta-rs all work, but this specific combination is newer than traditional cloud warehouse setups. Test this path thoroughly in your DEV environment.

---

## Chapter 6: Compute — DuckDB

DuckDB is the analytical engine at the heart of the platform. It runs as an in-process database — no server, no cluster, no always-on cost. Despite its simplicity, DuckDB delivers remarkable performance on analytical workloads, processing millions of rows per second on a single core.

### Connection Profile

```yaml
# dbt/profiles.yml
danish_democracy_data:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DUCKDB_DATABASE_LOCATION') }}"
      extensions: [httpfs, parquet, azure, delta]
      settings:
        azure_transport_option_type: 'curl'
      external_root: 'data/curated'
```

The `DUCKDB_DATABASE_LOCATION` can point to a local file (`/path/to/db.duckdb`) or a MotherDuck cloud instance (`md:danish_democracy_data_md?motherduck_token=...`). The same dbt SQL runs unchanged against either target.

### Extensions

```sql
INSTALL azure;   -- Azure Blob / OneLake access via ABFS
INSTALL delta;   -- Delta Lake read support
INSTALL httpfs;  -- HTTP(S) file access
INSTALL parquet; -- Parquet file support
```

These extensions are loaded automatically by dbt-duckdb based on the `extensions` list in `profiles.yml`.

### MotherDuck — Cloud-Hosted DuckDB

The Danish Parliament pipeline uses MotherDuck for production, demonstrating the upgrade path from local DuckDB to a cloud-hosted service. MotherDuck adds persistent shared state and web-based querying while keeping the same SQL dialect. This validates a key architectural claim: components can be upgraded independently without changing the transformation logic.

---

## Chapter 7: Ingestion — dlt

dlt (data load tool) is a lightweight Python library for building data ingestion pipelines. It handles schema inference, incremental loading, data type mapping, and error handling with minimal code.

### Pipeline Structure

The Danish Parliament pipeline extracts 18 entities from an OData REST API with **concurrent execution**:

```python
# ddd_python/ddd_dlt/dlt_run_extraction_pipelines_danish_parliament_data.py (simplified)
from concurrent.futures import ThreadPoolExecutor

incremental_set = set(configuration_variables.DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL)

with ThreadPoolExecutor(max_workers=4) as executor:
    for file_name in file_names_to_retrieve:
        # Incremental entities filter by date
        api_filter = (
            f"$filter=opdateringsdato ge DateTime'{date_to_load_from}'&$orderby=id"
            if file_name in incremental_set
            else "$inlinecount=allpages&$orderby=id"
        )
        executor.submit(dpef.execute_pipeline, ...)
```

### Incremental vs. Full Load

| Strategy | Entities (6) | API Filter | Rationale |
|----------|-------------|-----------|-----------|
| **Incremental** | Aktør, Møde, Sag, Sagstrin, SagstrinAktør, Stemme | `$filter=opdateringsdato ge DateTime'{date}'` | Large tables; only fetch changes |
| **Full load** | Afstemning, Afstemningstype, Aktoertype, + 9 more | `$inlinecount=allpages&$orderby=id` | Small reference tables; fetch all |

### Pipeline Execution Functions

The unified pipeline executor supports three modes:

```python
# ddd_python/ddd_dlt/dlt_pipeline_execution_functions.py
# 1. api_to_file  — OData/REST → NDJSON on OneLake (extraction)
# 2. sql_to_file  — SQL query → Parquet on OneLake
# 3. file_to_file — Local file → OneLake (raw copy)
```

Each extraction writes a timestamped JSON file with Danish character replacement:

```
aktoer_20250115_020000.json     # ø→oe, æ→ae, å→aa for filesystem compatibility
moede_20250115_020000.json
sagstrinaktoer_20250115_020000.json
```

The timestamp in the filename becomes the `LKHS_date_valid_from` in the Silver layer — providing precise temporal tracking without requiring the source system to provide it.

---

## Chapter 8: Transformation — dbt

dbt brings software engineering practices — version control, testing, documentation, modularity — to SQL-based data transformations. All transformation logic is written in standard SQL, managed in Git, and executed locally by DuckDB.

### Project Configuration

```yaml
# dbt/dbt_project.yml
name: 'danish_democracy_data'
config-version: 2
profile: 'danish_democracy_data'

vars:
  danish_democracy_data_source: "abfss://onelake.dfs.fabric.microsoft.com/<YOUR_WORKSPACE>/<YOUR_LAKEHOUSE>.Lakehouse/Files/Bronze/DDD"
  bronze_columns_to_exclude_in_silver_hash: "'LKHS_date_inserted','LKHS_pipeline_execution_inserted','LKHS_filename'"
  hash_null_replacement: "'<NULL>'"
  hash_delimiter: "']##['"

models:
  danish_democracy_data:
    bronze:
      +schema: bronze
      +materialized: view          # Raw data as views — no storage cost
    silver:
      +schema: silver
      +materialized: table         # Persisted for SCD2 history
    gold:
      +schema: gold
      +materialized: view          # Derived from Silver — always fresh
```

The three layers have distinct materialization strategies:
- **Bronze: views** — no storage cost; reads raw JSON from OneLake on query
- **Silver: tables** — persisted with incremental append for SCD2 history
- **Gold: views** — derived from Silver; always reflects the latest data

### Hash-Based Change Detection

The platform uses SHA-256 hashing for change detection (stored in the `LKHS_hash_value` column). A dynamic macro builds the hash expression by querying `information_schema.columns` — no manual column lists required:

```sql
-- dbt/macros/generate_base_for_hash.sql
{% macro generate_base_for_hash(table_name, columns_to_exclude, primary_key_columns) %}
    {%- set query -%}
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{{ table_name }}'
        AND column_name NOT IN ( {{ columns_to_exclude }} )
        AND column_name NOT IN ( {{ primary_key_columns }} )
        ORDER BY ordinal_position
    {%- endset -%}
    {%- set results = run_query(query) -%}
    {%- set columns = [] -%}
    {%- for row in results -%}
        {%- do columns.append(
            'COALESCE(' + row['column_name'] + '::VARCHAR,'
            + var('hash_null_replacement') + '),'
            + var('hash_delimiter')
        ) -%}
    {%- endfor -%}
    {%- set colstring = columns|join(',') -%}
    CONCAT( {{ colstring }} )
{% endmacro %}
```

This macro:
1. Queries `information_schema.columns` for all column names in the Bronze table
2. Excludes metadata columns (`LKHS_date_inserted`, `LKHS_filename`, etc.) and primary keys
3. Wraps each column in `COALESCE(...::VARCHAR, '<NULL>')` to handle NULLs
4. Concatenates with `']##['` delimiters
5. The result is passed to `sha256()` in the Silver macro

**Why dynamic hash generation matters:** Adding a column to the source API automatically includes it in the change detection hash — no code changes needed.

---

## Chapter 9: The Bronze Layer

Bronze models read raw JSON from OneLake using DuckDB's `read_json_auto()`, preserving all source columns unchanged while adding lakehouse metadata.

### Source Definitions

```yaml
# dbt/models/bronze/__sources.yml
version: 2
sources:
  - name: danish_parliament
    schema: danish_parliament
    description: Data from the Danish Parliament
    loaded_at_field: LKHS_date_inserted
    freshness:
      warn_after: {count: 2, period: day}
      error_after: {count: 7, period: day}
    tables:
    - name: bronze_aktoer
      description: One record per member in the Danish Parliament
      meta:
        external_location: "read_json_auto('{{ var('danish_democracy_data_source') }}/aktoer/aktoer_*.json*', filename=True, union_by_name = true)"
    - name: bronze_moede
      description: One record per meeting in the Danish Parliament
      meta:
        external_location: "read_json_auto('{{ var('danish_democracy_data_source') }}/moede/moede_*.json*', filename=True, union_by_name = true)"
    # ... (18 sources total, one per entity)
```

Each source uses a glob pattern (`aktoer_*.json*`) that unions all timestamped JSON files for that entity. The `filename=True` parameter exposes the file path as a column — critical for deriving timestamps in the Silver layer.

### Bronze View Macro

```sql
-- dbt/macros/generate_model_bronze.sql
{%- macro generate_model_bronze(table_name, source_system_code, source_name) -%}
SELECT DISTINCT COLUMNS(c -> c != 'filename' AND NOT starts_with(c, '_dlt_'))
,      SUBSTRING(src.filename,
           LENGTH(src.filename) - POSITION('/' IN REVERSE(src.filename)) + 2
       ) AS LKHS_filename
,      '{{ source_system_code }}' AS LKHS_source_system_code
FROM {{ source('danish_parliament', table_name) }} src
{%- endmacro -%}
```

This macro:
- Selects all columns except `filename` (internal DuckDB path) and `_dlt_` columns (dlt metadata)
- Extracts just the filename from the full path (e.g., `aktoer_20250115_020000.json`)
- Adds the source system code (`DDD`)

### Bronze Latest View Macro

For each entity, a companion `_latest` view reads only the most recent JSON file. This is essential for delete detection in the Silver layer:

```sql
-- dbt/macros/generate_model_bronze_latest.sql
{%- macro generate_model_bronze_latest(file_name, source_system_code, source_name) -%}
WITH cte_most_recent_file AS (
    SELECT MAX(filename) AS most_recent_file
    FROM read_text('{{ var('danish_democracy_data_source') }}/{{ file_name }}/{{ file_name }}_*.json*')
)
SELECT DISTINCT COLUMNS(c -> c != 'filename' AND NOT starts_with(c, '_dlt_'))
,      SUBSTRING(filename,
           LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2
       ) AS LKHS_filename
,      '{{ source_system_code }}' AS LKHS_source_system_code
,      'N' AS LKHS_deleted_ind
FROM   read_json_auto('{{ var('danish_democracy_data_source') }}/{{ file_name }}/{{ file_name }}_*.json*',
                       filename=True)
WHERE  filename = (SELECT most_recent_file FROM cte_most_recent_file)
{%- endmacro -%}
```

### Generated Model Files

Each Bronze model is a one-liner invoking the macro:

```sql
-- dbt/models/bronze/bronze_aktoer.sql (generated)
{{ generate_model_bronze(this.name,'DDD','danish_parliament') }}
```

```sql
-- dbt/models/bronze/bronze_aktoer_latest.sql (generated)
{{ generate_model_bronze_latest('aktoer','DDD','danish_parliament') }}
```

All 36 Bronze model files are generated by the code generator (Chapter 17).

---

## Chapter 10: The Silver Layer — SCD Type 2

The Silver layer preserves the full change history of every record using an insert-only SCD Type 2 pattern. No rows are ever updated or deleted — history is maintained by appending new versions with CDC classifications.

### Silver Metadata Columns

| Column | Type | Purpose |
|--------|------|---------|
| `LKHS_hash_value` | VARCHAR | SHA-256 of all non-key business columns (used for CDC, not surrogate keys) |
| `LKHS_date_valid_from` | DATETIME | When this version was observed (from filename timestamp) |
| `LKHS_date_inserted_src` | DATETIME | Earliest source date for this key |
| `LKHS_date_inserted` | DATETIME | When dbt loaded this row |
| `LKHS_cdc_operation` | VARCHAR | `'I'` (insert), `'U'` (update), or `'D'` (delete) |
| `LKHS_filename` | VARCHAR | Source JSON filename for traceability |
| `LKHS_source_system_code` | VARCHAR | Source identifier (`'DDD'`) |

### Incremental Silver Macro (for 6 date-filterable entities)

```sql
-- dbt/macros/generate_model_silver_incr_extraction.sql
{%- macro generate_model_silver_incr_extraction(
        file_name, bronze_table_name, primary_key_columns,
        date_column, base_for_hash) -%}

{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    unique_key=['id','LKHS_date_valid_from'],
    pre_hook  = "{{ generate_pre_hook_silver_full_refresh() }}",
    post_hook = "DROP TABLE IF EXISTS {{ this.schema }}.{{ this.name }}_current_temp;"
) }}

-- Step 1: Read Bronze data and compute hash
WITH CTE_BRONZE AS (
    SELECT src.*,
           sha256({{ base_for_hash }}) AS LKHS_hash_value,
           CAST(MIN({{ date_column }}) OVER (
               PARTITION BY {{ primary_key_columns }}
           ) AS DATETIME) AS LKHS_date_inserted_src
    FROM {{ ref(bronze_table_name) }} src
),

-- Step 2: Build file timeline (extract timestamps from filenames)
CTE_FILES AS (
    SELECT LKHS_filename,
           strptime(
               SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),
               '%Y%m%d_%H%M%S'
           ) AS LKHS_date_valid_from,
           LAG(LKHS_filename) OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
    FROM (
        SELECT SUBSTRING(filename,
                   LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2
               ) AS LKHS_filename
        FROM read_text('{{ var("danish_democracy_data_source") }}/{{ file_name }}/{{ file_name }}_*.json')
    ) files
),

-- Step 3: Join Bronze with file timeline, compute hash lag
CTE_BRONZE_INCL_LAG AS (
    SELECT CTE_BRONZE.*,
           CTE_FILES.LKHS_date_valid_from,
           LAG(CTE_BRONZE.LKHS_hash_value) OVER (
               PARTITION BY {{ primary_key_columns }}
               ORDER BY CTE_BRONZE.LKHS_filename
           ) AS LKHS_hash_value_previous
    FROM CTE_BRONZE
    INNER JOIN CTE_FILES ON CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
),

-- Step 4: Classify CDC operations
CTE_ALL_ROWS AS (
    SELECT ...,
           CASE
               WHEN LKHS_hash_value_previous IS NULL THEN 'I'           -- New key = Insert
               WHEN LKHS_hash_value != LKHS_hash_value_previous THEN 'U' -- Hash changed = Update
           END AS LKHS_cdc_operation
    FROM CTE_BRONZE_INCL_LAG
    WHERE LKHS_cdc_operation IN ('I','U')
    -- Delete detection handled on full-refresh path (see Chapter 11)
)

-- Step 5: Filter to unprocessed files and deduplicate
SELECT * FROM CTE_ALL_ROWS
WHERE LKHS_filename >= (SELECT LKHS_filename_previous FROM {{ this.schema }}.{{ this.name }}_last_file)
{% if is_incremental() %}
AND NOT EXISTS (
    SELECT id FROM {{ this }}
    WHERE id = CTE_ALL_ROWS.id
    AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from
)
{% endif %}
{%- endmacro -%}
```

### Full-Load Silver Macro (for 12 reference entities)

The full-load macro uses a different change detection strategy — it joins across file boundaries to detect both changes and deletions:

```sql
-- dbt/macros/generate_model_silver_full_extraction.sql (key difference)
-- Instead of LAG() within a partition, it LEFT JOINs the previous file's data:
CTE_BRONZE_INCL_LAG AS (
    SELECT CTE_BRONZE.*,
           CTE_FILES.LKHS_date_valid_from,
           CTE_BRONZE_PREVIOUS.LKHS_hash_value AS LKHS_hash_value_previous,
           CTE_BRONZE_PREVIOUS.id AS LKHS_primary_key_previous
    FROM CTE_BRONZE
    INNER JOIN CTE_FILES ON CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
    -- Join to the PREVIOUS file's data for the same key
    LEFT JOIN CTE_BRONZE CTE_BRONZE_PREVIOUS
        ON CTE_FILES.LKHS_filename_previous = CTE_BRONZE_PREVIOUS.LKHS_filename
        AND CTE_BRONZE.id = CTE_BRONZE_PREVIOUS.id
)
```

This enables automatic **delete detection**: if a key exists in file N but not in file N+1, a `'D'` row is appended (see Chapter 11).

### Pre/Post Hooks for File Tracking

The Silver macro uses dbt hooks to track which files have been processed:

```sql
-- dbt/macros/generate_pre_hook_silver.sql
-- Creates a _last_file tracking table with the most recent processed filename
CREATE TABLE IF NOT EXISTS {{ this.schema }}.{{ this.name }}_last_file AS
SELECT ...
FROM (
    SELECT LKHS_filename,
           strptime(SUBSTRING(LKHS_filename, ...), '%Y%m%d_%H%M%S') AS LKHS_date_valid_from,
           LAG(LKHS_filename) OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
    FROM (SELECT SUBSTRING(filename, ...) AS LKHS_filename
          FROM read_text('{{ var("danish_democracy_data_source") }}/{{ file_name }}/{{ file_name }}_*.json'))
    WHERE 1 = 0  -- Empty on first run
) processed_files
```

```sql
-- dbt/macros/generate_post_hook_silver.sql
-- After the model runs, updates _last_file to the most recent file
DROP TABLE IF EXISTS {{ this.schema }}.{{ this.name }}_last_file;
CREATE TABLE {{ this.schema }}.{{ this.name }}_last_file AS
SELECT ...
QUALIFY ROW_NUMBER() OVER (ORDER BY files.LKHS_filename DESC) = 1
```

### Current-Version Views

Each Silver table has a `_cv` (current version) view for downstream consumption:

```sql
-- dbt/models/silver/silver_aktoer_cv.sql (generated)
{{ config( materialized='view' ) }}
SELECT src.*
FROM {{ ref('silver_aktoer') }} src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY src.LKHS_source_system_code, src.id
    ORDER BY src.LKHS_date_valid_from DESC
) = 1
```

The `QUALIFY` clause is DuckDB's concise alternative to wrapping in a subquery with `WHERE rn = 1`. The Gold layer reads exclusively from these current-version views.

---

## Chapter 11: Delete Handling in the Silver Layer

A business key in the Silver layer can go through any sequence of insert → update → delete → re-insert. Each transition is recorded as a new appended row. The current-version view always reflects the latest action.

```
Timeline for id = 42 (a parliament member):

LKHS_date_valid_from    LKHS_cdc_operation   LKHS_hash_value
─────────────────────────────────────────────────────────────
2025-01-15 02:00        I                    hash_v1         ← First seen
2025-02-01 02:00        U                    hash_v2         ← Name changed
2025-03-10 02:00        D                    hash_v2         ← Removed from source
2025-04-01 02:00        I                    hash_v3         ← Re-appeared

Current-version view → shows hash_v3, LKHS_cdc_operation = 'I'
```

### Full-Load Delete Detection

For the 12 full-load entities, the Silver macro detects deletes automatically: any key present in file N but absent from file N+1 has been deleted.

```sql
-- From generate_model_silver_full_extraction.sql (delete detection section)
UNION ALL
SELECT  CTE_BRONZE_INCL_LAG.* EXCLUDE (...),
        CTE_FILES.LKHS_filename_next AS LKHS_filename,
        LKHS_date_valid_from_next AS LKHS_date_valid_from,
        'D' AS LKHS_cdc_operation
FROM    CTE_BRONZE_INCL_LAG
INNER JOIN CTE_FILES
    ON CTE_BRONZE_INCL_LAG.LKHS_filename = CTE_FILES.LKHS_filename
LEFT JOIN CTE_BRONZE_INCL_LAG CTE_BRONZE_INCL_LAG_NEXT
    ON CTE_FILES.LKHS_filename_next = CTE_BRONZE_INCL_LAG_NEXT.LKHS_filename
    AND CTE_BRONZE_INCL_LAG.id = CTE_BRONZE_INCL_LAG_NEXT.id
WHERE   CTE_BRONZE_INCL_LAG_NEXT.id IS NULL          -- Missing from next file = deleted
AND     CTE_FILES.LKHS_filename_next IS NOT NULL       -- Not the latest file
```

### Incremental Delete Detection (Full-Refresh Path)

For the 6 incremental entities, delete detection runs during full-refresh only (not during incremental runs). The macro compares the current Silver state against the latest Bronze file:

```sql
-- From generate_model_silver_incr_extraction.sql (full-refresh delete detection)
UNION ALL
SELECT  cv.* EXCLUDE (LKHS_date_inserted, LKHS_cdc_operation, LKHS_date_valid_from),
        CTE_FILE_LATEST.LKHS_date_valid_from,
        'D' AS LKHS_cdc_operation
FROM    {{ this.schema }}.{{ this.name }}_current_temp cv
CROSS JOIN CTE_FILE_LATEST
LEFT JOIN {{ ref(bronze_latest_version) }} bronze_latest
    ON cv.id = bronze_latest.id
WHERE   cv.LKHS_cdc_operation != 'D'      -- Not already deleted
AND     bronze_latest.id IS NULL            -- Missing from latest Bronze = deleted
```

The `cv.LKHS_cdc_operation != 'D'` guard prevents redundant delete markers — once a key is marked deleted, no further delete rows are appended until it reappears.

---

## Chapter 12: The Gold Layer — Star Schema

The Gold layer builds a classic star schema from Silver current-version views. Danish entity names are translated to English, surrogate keys are generated, and SCD Type 2 validity windows are computed.

### Surrogate Key Generation

Gold surrogate keys use DuckDB's built-in `hash()` function, which produces a 64-bit unsigned integer (`UBIGINT`). Power BI does not accept `UBIGINT`, so this macro maps it to a signed `BIGINT` by folding values above `BIGINT_MAX` into the negative range:

```sql
-- dbt/macros/cast_hash_to_bigint.sql
{%- macro cast_hash_to_bigint(columns_to_hash) -%}
    CAST(
        CASE
            WHEN hash({{ columns_to_hash }}) > 9223372036854775807
                THEN -1 * (hash({{ columns_to_hash }}) - 9223372036854775807)
            ELSE hash({{ columns_to_hash }})
        END
        AS BIGINT
    )
{%- endmacro -%}
```

### Actor Dimension (Gold)

```sql
-- dbt/models/gold/actor.sql
WITH actor AS (
    SELECT
        {{ cast_hash_to_bigint(
            'src.LKHS_source_system_code,src.id,src.LKHS_date_valid_from'
        ) }} AS LKHS_actor_id,
        src.*,
        LEAD(src.LKHS_date_valid_from, 1, CAST('9999-12-31' AS DATETIME))
            OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from)
            AS LKHS_date_valid_to,
        ROW_NUMBER()
            OVER (PARTITION BY src.LKHS_source_system_code, src.id
                  ORDER BY src.LKHS_date_valid_from)
            AS LKHS_row_version
    FROM {{ ref('silver_aktoer') }} src
),
actor_type AS (
    SELECT * FROM {{ ref('actor_type_cv') }}
)
SELECT
    actor.LKHS_actor_id,
    actor_type.LKHS_actor_type_id,
    CONCAT(actor.LKHS_source_system_code, '-', CAST(actor.id AS VARCHAR)) AS actor_bk,
    actor_type.actor_type_danish,
    actor_type.actor_type_english,
    actor.gruppenavnkort     AS group_short_name,
    actor.navn               AS full_name,           -- Danish → English column names
    actor.fornavn            AS first_name,
    actor.efternavn          AS last_name,
    actor.biografi           AS biography,
    -- XML parsing: extract gender from embedded biography XML
    substring(actor.biografi,
        position('<sex>' in biografi) + length('<sex>'),
        position('</sex>' in biografi) - position('<sex>' in biografi) - length('<sex>')
    ) AS gender_danish,
    CASE substring(actor.biografi, ...)
        WHEN 'Mand' THEN 'Male'
        WHEN 'Kvinde' THEN 'Female'
        ELSE ...
    END AS gender_english,
    -- XML parsing: extract dates
    try_strptime(substring(actor.biografi, ...), '%d-%m-%Y') AS birth_date,
    try_strptime(substring(actor.biografi, ...), '%d-%m-%Y') AS death_date,
    -- XML parsing: extract party
    substring(actor.biografi, ...) AS party_name,
    substring(actor.biografi, ...) AS party_short_name,
    -- SCD2 metadata
    actor.LKHS_date_inserted_src,
    CASE WHEN actor.LKHS_row_version = 1
        THEN CAST('1900-01-01' AS DATETIME)
        ELSE CAST(actor.LKHS_date_valid_from AS DATETIME)
    END AS LKHS_date_valid_from,
    actor.LKHS_date_valid_to,
    actor.LKHS_row_version,
    actor.LKHS_source_system_code
FROM actor
LEFT JOIN actor_type
    ON CONCAT(actor.LKHS_source_system_code, '-', CAST(actor.typeid AS VARCHAR))
       = actor_type.actor_type_bk

-- FK safety row: every dimension has an 'Unknown' row at id=0
UNION ALL
SELECT 0 AS LKHS_actor_id,
       0 AS LKHS_actor_type_id,
       'Unknown' AS actor_bk,
       'Ukendt' AS actor_type_danish,
       'Unknown' AS actor_type_english,
       NULL AS group_short_name,
       ...
       CAST('1900-01-01' AS DATETIME) AS LKHS_date_valid_from,
       CAST('9999-12-31' AS DATETIME) AS LKHS_date_valid_to,
       1 AS LKHS_row_version,
       'LKHS' AS LKHS_source_system_code
```

### Key Gold Layer Patterns

**1. FK Safety Rows (id=0):** Every dimension includes an always-present "Unknown" row at id=0. Any fact row with an unknown foreign key joins to this row rather than producing a NULL join — critical for Power BI where broken relationships cause confusing results.

**2. SCD Type 2 via LEAD():** `LKHS_date_valid_to` is computed at query time using `LEAD()` — not stored. This preserves the insert-only contract in Silver while giving Gold consumers temporal queryability.

**3. Danish → English Translation:** Column names are translated (`navn` → `full_name`, `fornavn` → `first_name`); entity names are translated (`aktoer` → `actor`, `moede` → `meeting`).

**4. XML Parsing in SQL:** The `biografi` field contains embedded XML with `<sex>`, `<born>`, `<died>`, `<party>` tags. DuckDB's `substring()` and `position()` handle the extraction without an XML parser dependency.

### Vote Fact Table (Gold)

```sql
-- dbt/models/gold/vote.sql
WITH vote AS (
    SELECT {{ cast_hash_to_bigint('src.LKHS_source_system_code,src.id,src.LKHS_date_valid_from') }}
               AS LKHS_vote_id,
           src.*,
           LEAD(src.LKHS_date_valid_from, 1, CAST('9999-12-31' AS DATETIME))
               OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from)
               AS LKHS_date_valid_to,
           ROW_NUMBER()
               OVER (PARTITION BY src.LKHS_source_system_code, src.id
                     ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
    FROM {{ ref('silver_afstemning') }} src
),
vote_type AS (
    SELECT * FROM {{ ref('vote_type_cv') }}
)
SELECT  vote.LKHS_vote_id,
        vote_type.LKHS_vote_type_id,
        CONCAT(vote.LKHS_source_system_code, '-', CAST(vote.id AS VARCHAR)) AS vote_bk,
        vote.nummer          AS vote_number,
        vote.konklusion      AS conclusion,
        vote.vedtaget        AS approved,
        vote.kommentar       AS vote_comment,
        vote_type.vote_type_danish,
        vote_type.vote_type_english,
        ...
FROM vote
LEFT JOIN vote_type ON ...
UNION ALL
SELECT 0, 0, 'Unknown', NULL, NULL, NULL, NULL, 'Ukendt', 'Unknown', ...
```

### Gold Entity Map

| Silver Entity (Danish) | Gold Table (English) | Description |
|------------------------|---------------------|-------------|
| silver_aktoer | actor | Parliament members, ministers, lobbyists |
| silver_aktoertype | actor_type | Actor type lookup |
| silver_afstemning | vote | Party-level votes on cases |
| silver_afstemningstype | vote_type | Vote type lookup |
| silver_moede | meeting | Parliament meetings |
| silver_moedestatus | meeting_status | Meeting status lookup |
| silver_moedetype | meeting_type | Meeting type lookup |
| silver_sag | case | Parliamentary cases (bills, questions) |
| silver_stemme | individual_vote | Individual member votes |

---

## Chapter 13: Orchestration — Dagster

Dagster is the orchestration layer that ties all pieces together. It is a Python-native framework built around **software-defined assets** — data artifacts declared as Python functions with explicit dependencies.

### Asset Factory Pattern

The 18 extraction assets are created with a factory pattern — two functions generate all 18 definitions:

```python
# ddd_python/ddd_dagster/assets.py

class ExtractionConfig(Config):
    """Per-run configuration for extraction assets."""
    date_to_load_from: str | None = None  # Override for backfills

_RETRY_POLICY = RetryPolicy(max_retries=2, delay=60, backoff=Backoff.EXPONENTIAL)


def _make_incremental_asset(api_resource: str) -> AssetsDefinition:
    """Factory: returns an @asset for an incremental OData resource."""
    base = _base_name(api_resource)  # Danish chars → ASCII

    @asset(
        name=base,
        key_prefix=["ingestion", "DDD"],
        group_name="ingestion_DDD_incremental",
        retry_policy=_RETRY_POLICY,
        description=f"Incremental extraction of {api_resource}...",
    )
    def _incremental_asset(
        context: AssetExecutionContext,
        config: ExtractionConfig,
        dlt_onelake: DltOneLakeResource,
    ) -> MaterializeResult:
        # Resolve date_to_load_from
        if config.date_to_load_from is not None:
            date_from = config.date_to_load_from
        else:
            default_days = configuration_variables.DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD
            date_from = f"{datetime.now(timezone.utc) - timedelta(days=default_days):%Y-%m-%d}"

        ts = datetime.now(timezone.utc)
        destination_file = f"{base}_{ts:%Y%m%d_%H%M%S}.json"
        api_filter = f"$filter=opdateringsdato ge DateTime'{date_from}'&$orderby=id"

        result = dlt_onelake.execute_pipeline(
            pipeline_type="api_to_file",
            pipeline_name=base,
            source_api_base_url=get_variables_from_env.DANISH_DEMOCRACY_BASE_URL,
            source_api_resource=api_resource,
            source_api_filter=api_filter,
            destination_directory_path=_destination_path(base),
            destination_file_name=destination_file,
        )

        return MaterializeResult(metadata={
            "records_written": MetadataValue.int(result.get("records_written") or 0),
            "destination_file": MetadataValue.text(destination_file),
            "date_from": MetadataValue.text(date_from),
        })

    return _incremental_asset


# Materialise asset lists from configuration
incremental_assets = [_make_incremental_asset(name)
                      for name in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES
                      if name in _INCREMENTAL_NAMES]

full_extract_assets = [_make_full_extract_asset(name)
                       for name in configuration_variables.DANISH_DEMOCRACY_FILE_NAMES
                       if name not in _INCREMENTAL_NAMES]

all_extraction_assets = incremental_assets + full_extract_assets
```

### Job Definitions

```python
# ddd_python/ddd_dagster/jobs.py

# Shared executor: 4-way concurrency, mirrors ThreadPoolExecutor(max_workers=4)
_concurrent_executor = multiprocess_executor.configured({"max_concurrent": 4})

# Extraction jobs
danish_parliament_incremental_job = define_asset_job(
    name="danish_parliament_incremental_job",
    selection=AssetSelection.groups("ingestion_DDD_incremental"),
    executor_def=_concurrent_executor,
)

danish_parliament_full_extract_job = define_asset_job(
    name="danish_parliament_full_extract_job",
    selection=AssetSelection.groups("ingestion_DDD_full_extract"),
    executor_def=_concurrent_executor,
)

# dbt jobs (sequential — DuckDB single-writer constraint)
dbt_bronze_job = define_asset_job(
    name="dbt_bronze_job",
    selection=_bronze_selection(),
    executor_def=in_process_executor,
)

dbt_silver_job = define_asset_job(
    name="dbt_silver_job",
    selection=_silver_selection(),
    executor_def=in_process_executor,
)

dbt_gold_job = define_asset_job(
    name="dbt_gold_job",
    selection=_gold_selection(),
    executor_def=in_process_executor,
)

# Export jobs (concurrent — writing to separate Delta tables)
export_silver_job = define_asset_job(
    name="export_silver_job",
    selection=AssetSelection.groups("export_silver"),
    executor_def=_concurrent_executor,
)

export_gold_job = define_asset_job(
    name="export_gold_job",
    selection=AssetSelection.groups("export_gold"),
    executor_def=_concurrent_executor,
)

# End-to-end pipeline
danish_parliament_full_pipeline_job = define_asset_job(
    name="danish_parliament_full_pipeline_job",
    selection=(
        AssetSelection.groups("ingestion_DDD_incremental", "ingestion_DDD_full_extract")
        | build_dbt_asset_selection([dbt_bronze_assets])
        | build_dbt_asset_selection([dbt_silver_assets])
        | build_dbt_asset_selection([dbt_gold_assets])
        | AssetSelection.groups("export_silver", "export_gold")
    ),
    executor_def=_concurrent_executor,
)
```

### Definitions Entry Point

```python
# ddd_python/ddd_dagster/definitions.py
from dagster import Definitions
from dagster_dbt import DbtCliResource

defs = Definitions(
    assets=[
        *all_extraction_assets,        # 18 dlt extraction assets
        dbt_seeds_assets,              # dbt seed data
        dbt_bronze_assets,             # 36 Bronze views
        dbt_silver_assets,             # 36 Silver models
        dbt_gold_assets,               # 18 Gold models
        *all_export_assets,            # Silver + Gold Delta exports
    ],
    jobs=[
        danish_parliament_incremental_job,
        danish_parliament_full_extract_job,
        danish_parliament_all_job,
        dbt_seeds_job, dbt_bronze_job, dbt_silver_job, dbt_gold_job,
        export_silver_job, export_gold_job,
        danish_parliament_full_pipeline_job,
    ],
    schedules=[danish_parliament_full_pipeline_schedule],
    sensors=[
        danish_parliament_run_success_sensor,
        danish_parliament_run_failure_sensor,
    ],
    resources={
        "dlt_onelake": DltOneLakeResource(),
        "dbt": DbtCliResource(
            project_dir=_DBT_PROJECT_DIR,
            profiles_dir=_DBT_PROJECT_DIR,
        ),
    },
)
```

### Running the Dagster UI

```bash
# Local development — opens at http://localhost:3000
dagster dev -f ddd_python/ddd_dagster/definitions.py

# Or via workspace file
dagster dev -w workspace.yaml
```

### The Asset Graph

```
dlt: aktoer ──────┐
dlt: moede ───────┤
dlt: sag ─────────┤──► dbt Bronze (18 views + 18 _latest views)
dlt: stemme ──────┤         │
dlt: (14 more) ──┘         ▼
                    dbt Silver (18 incremental tables + 18 _cv views)
                            │                        │
                            ▼                        ▼
                    export Silver              dbt Gold (9 views + 9 _cv views)
                    (→ OneLake Delta)                 │
                                                      ▼
                                              export Gold (→ OneLake Delta)
```

---

## Chapter 14: Export — Delta Lake

After dbt materializes the star schema in DuckDB, Python scripts export the tables to OneLake as Delta Lake.

### Silver Export (Incremental Append)

```python
# ddd_python/ddd_dlt/export_main_silver_to_fabric_silver.py (simplified)
def export_single_silver_table(connection, table):
    if DeltaTable.is_deltatable(target_table_path, storage_options=storage_options):
        # Incremental: find rows in DuckDB not yet in Delta
        result = connection.execute(f"""
            SELECT src.*
            FROM main_silver.{table} src
            LEFT JOIN target tgt
                ON src.id = tgt.id
                AND src.LKHS_date_valid_from = tgt.LKHS_date_valid_from
            WHERE tgt.id IS NULL
        """)
        df = result.fetch_arrow_table()
        if df.num_rows > 0:
            write_deltalake(target_table_path, df, mode="append",
                            storage_options=storage_options)
    else:
        # First load: full overwrite
        result = connection.execute(f"SELECT * FROM main_silver.{table}")
        df = result.fetch_arrow_table()
        write_deltalake(target_table_path, df, mode="overwrite",
                        storage_options=storage_options)
```

### Gold Export (Full Overwrite)

```python
# ddd_python/ddd_dlt/export_main_gold_to_fabric_gold.py (simplified)
# Gold is rebuilt from scratch every run — always overwrite
result = connection.execute(f"SELECT * FROM main_gold.{table}")
df = result.fetch_arrow_table()
write_deltalake(target_table_path, df, mode="overwrite",
                storage_options=storage_options)
```

---

## Chapter 15: Reporting — Power BI

Power BI consumes the Gold Delta Lake tables on OneLake. Two connection patterns are supported:

### Pattern A — Direct Lake (Fabric Capacity)

If your workspace runs on Fabric capacity, use Direct Lake storage mode — best interactivity, lowest operational overhead. Open the OneLake catalog in Power BI Desktop, select the Lakehouse, and connect.

### Pattern B — Power Query Delta Connector (Import Mode)

Without Fabric capacity, use the `DeltaLake.Table` M function:

```powerquery
// Shared query: OneLakeRoot (Enable load: off)
let
    Source = AzureStorage.DataLake(
        "https://onelake.dfs.fabric.microsoft.com",
        [HierarchicalNavigation = true]
    )
in Source

// Shared function: fnGoldTable (Enable load: off)
let
    fnGoldTable = (tableName as text) as table =>
        let
            GoldFolder = OneLakeRoot
                {[Name = WorkspaceName]}[Data]
                {[Name = LakehouseName & ".Lakehouse"]}[Data]
                {[Name = "Files"]}[Data]
                {[Name = "gold"]}[Data]
                {[Name = tableName]}[Data]
        in DeltaLake.Table(GoldFolder)
in fnGoldTable

// Each table query:
let Result = fnGoldTable("actor") in Result
let Result = fnGoldTable("meeting") in Result
let Result = fnGoldTable("vote") in Result
```

### Power BI Compatibility

The `cast_hash_to_bigint` macro exists specifically for Power BI — it converts DuckDB's `hash()` output (an unsigned 64-bit integer) to a signed `BIGINT` that Power BI accepts. Note: this is DuckDB's built-in `hash()` function, not SHA-256. SHA-256 is used separately in the Silver layer for change detection (`LKHS_hash_value`).

---

## Chapter 16: CI/CD & Runtime

### Runtime Options

| Platform | Spec | Billing Model | Cost (1h/day) |
|----------|------|--------------|---------------|
| **Azure Container Instances** | 4 vCPU, 16 GB | Per-second | ~€7/month |
| **Azure Container Apps (D4)** | 4 vCPU, 16 GB | Per-node | ~€5–8/month |
| **Google Cloud Run Jobs** | Up to 8 vCPU | Per-second | ~€5–10/month |
| **GitHub Actions runner** | 4 vCPU, 16 GB | Per-minute | ~€0 (within free tier) |

### Environment Configuration

The same codebase runs in both DEV and PROD — only the OneLake paths differ, controlled by environment variables:

```bash
ENVIRONMENT=dev    # or prod
FABRIC_ONELAKE_FOLDER_BRONZE=.../${ENVIRONMENT}/Bronze
FABRIC_ONELAKE_FOLDER_SILVER=.../${ENVIRONMENT}/Silver
FABRIC_ONELAKE_FOLDER_GOLD=.../${ENVIRONMENT}/Gold
```

---

# Part III — Patterns and Code Generation

---

## Chapter 17: Code Generation — DRY Model Management

With 18 entities × 4 models each = 72+ model files, writing them by hand would be unsustainable. The solution is a Python code generator that reads the entity list and produces SQL files.

### The Entity List (Single Source of Truth)

```python
# ddd_python/ddd_utils/configuration_variables.py
DANISH_DEMOCRACY_FILE_NAMES = [
    "Aktør", "Aktørtype", "Afstemning", "Afstemningstype",
    "Dokument", "Dokumentkategori", "Emneord", "Emneordstype",
    "Fil", "Kolonne", "Møde", "Mødestatus", "Mødetype",
    "Omtryk", "Periode", "Sag", "Sagstrin", "SagstrinAktør",
    "Sagskategori", "Sagsstatus", "Sagstrinsstatus", "Sagstrinstype",
    "Sagstype", "Stemme", "Stemmetype"
]

DANISH_DEMOCRACY_FILE_NAMES_INCREMENTAL = [
    "Aktør", "Møde", "Sagstrin", "Sag", "SagstrinAktør", "Stemme"
]
```

### The Code Generator

```python
# ddd_python/ddd_dbt/generate_dbt_models.py

def generate_dbt_models_bronze(table_names, file_names, source_system_code="DDD"):
    """Generate Bronze view + _latest view per entity."""
    for table_name in table_names:
        # bronze_aktoer.sql → {{ generate_model_bronze(this.name,'DDD','danish_parliament') }}
        query = f"{{{{ generate_model_bronze(this.name,'{source_system_code}','danish_parliament') }}}}"
        with open(f"{target_dir}/{table_name.lower()}.sql", "w") as f:
            f.write(query)

    for file_name in file_names:
        file_name = file_name.replace("ø","oe").replace("æ","ae").replace("å","aa").lower()
        # bronze_aktoer_latest.sql → {{ generate_model_bronze_latest('aktoer','DDD','danish_parliament') }}
        query = f"{{{{ generate_model_bronze_latest('{file_name}','{source_system_code}','danish_parliament') }}}}"
        with open(f"{target_dir}/bronze_{file_name}_latest.sql", "w") as f:
            f.write(query)


def generate_dbt_models_silver(table_names):
    """Generate Silver incremental table + _cv view per entity."""
    for table_name in table_names:
        model_name = table_name.lower()
        file_name = model_name.replace("silver_", "", 1)

        # Select macro based on incremental vs full-load
        if model_name in _INCREMENTAL_SILVER_MODELS:
            macro_name = "generate_model_silver_incr_extraction"
        else:
            macro_name = "generate_model_silver_full_extraction"

        # Silver table: config + macro call
        query = (
            "{%- set bronze_table_name = this.name.replace('silver', 'bronze', 1) -%}\n"
            "{%- set base_for_hash = generate_base_for_hash(...) -%}\n"
            "{{ config(materialized='incremental', incremental_strategy='append', ...) }}\n"
            f"{{{{ {macro_name}(file_name='{file_name}', ...) }}}}\n"
        )
        with open(f"{target_dir}/{model_name}.sql", "w") as f:
            f.write(query)

        # Current-version view
        query_cv = (
            "{{ config(materialized='view') }}\n"
            f"SELECT src.* FROM {{{{ ref('{model_name}') }}}} src\n"
            "QUALIFY ROW_NUMBER() OVER (...) = 1\n"
        )
        with open(f"{target_dir}/{model_name}_cv.sql", "w") as f:
            f.write(query_cv)


def generate_dbt_models_gold_cv(table_names):
    """Generate Gold _cv views (exclude metadata, pick latest version)."""
    for table_name in [t for t in table_names if t != "date"]:
        query = (
            f"SELECT src.* EXCLUDE (LKHS_date_inserted_src, LKHS_date_valid_from, ...)\n"
            f"FROM {{{{ ref('{table_name}') }}}} src\n"
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code, "
            f"src.{table_name}_bk ORDER BY src.LKHS_date_valid_from DESC) = 1\n"
        )
        with open(f"{target_dir}/{table_name.lower()}_cv.sql", "w") as f:
            f.write(query)


if __name__ == "__main__":
    generate_dbt_models_bronze(MODELS_BRONZE, FILE_NAMES, "DDD")
    generate_dbt_models_silver(MODELS_SILVER)
    generate_dbt_models_gold_cv(MODELS_GOLD)
```

**Adding a new entity requires only:**
1. Add the entity name to `configuration_variables.py`
2. Run `python -m ddd_python.ddd_dbt.generate_dbt_models`
3. Write the Gold hand-crafted model (Danish → English column mapping)

The Bronze views, Silver CDC tables, Silver _cv views, and Gold _cv views are all generated automatically.

---

## Chapter 18: Configuration as Code

### Environment Variables (python-dotenv)

```python
# ddd_python/ddd_utils/get_variables_from_env.py
# Lazy module pattern — defers credential resolution at import time
# Allows code generation to run without Azure credentials present

from dotenv import load_dotenv
load_dotenv()

# Key variable groups:
# Fabric/OneLake
FABRIC_WORKSPACE = os.getenv("FABRIC_WORKSPACE")
FABRIC_ONELAKE_STORAGE_ACCOUNT = os.getenv("FABRIC_ONELAKE_STORAGE_ACCOUNT")
FABRIC_ONELAKE_FOLDER_BRONZE = os.getenv("FABRIC_ONELAKE_FOLDER_BRONZE")
FABRIC_ONELAKE_FOLDER_SILVER = os.getenv("FABRIC_ONELAKE_FOLDER_SILVER")
FABRIC_ONELAKE_FOLDER_GOLD = os.getenv("FABRIC_ONELAKE_FOLDER_GOLD")

# DuckDB/dbt
DUCKDB_DATABASE_LOCATION = os.getenv("DUCKDB_DATABASE_LOCATION")
DBT_PROJECT_DIRECTORY = os.getenv("DBT_PROJECT_DIRECTORY")
DBT_MODELS_DIRECTORY = os.getenv("DBT_MODELS_DIRECTORY")

# Azure AD (service principal)
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
```

### dbt Project Variables

```yaml
# dbt/dbt_project.yml
vars:
  danish_democracy_data_source: "abfss://..."
  bronze_columns_to_exclude_in_silver_hash: "'LKHS_date_inserted','LKHS_pipeline_execution_inserted','LKHS_filename'"
  hash_null_replacement: "'<NULL>'"
  hash_delimiter: "']##['"
```

---

## Chapter 19: Naming Conventions and Metadata Columns

### The LKHS_ Prefix System

All technical metadata columns use the `LKHS_` prefix (Lakehouse) — providing a clear distinction between business data and pipeline metadata:

| Column | Layer | Purpose |
|--------|-------|---------|
| `LKHS_source_system_code` | Bronze+ | Source identifier (`'DDD'`) |
| `LKHS_filename` | Bronze+ | Timestamped extraction filename |
| `LKHS_date_inserted_src` | Silver+ | Earliest source date for this key |
| `LKHS_date_inserted` | Silver | When dbt loaded this row |
| `LKHS_date_valid_from` | Silver+ | SCD2 valid-from (from filename timestamp) |
| `LKHS_date_valid_to` | Gold | SCD2 valid-to (via LEAD window function) |
| `LKHS_hash_value` | Silver | SHA-256 of all non-key business columns (used for CDC, not surrogate keys) |
| `LKHS_cdc_operation` | Silver | CDC operation: `'I'`/`'U'`/`'D'` |
| `LKHS_row_version` | Gold | Row version number for SCD2 |
| `LKHS_deleted_ind` | Bronze latest | `'N'` (not deleted) — for delete detection |

### Danish → English Translation

| Danish | English | Domain |
|--------|---------|--------|
| aktør | actor | Entity |
| møde | meeting | Entity |
| sag | case | Entity |
| stemme | vote | Entity |
| navn | full_name | Column |
| fornavn | first_name | Column |
| efternavn | last_name | Column |
| opdateringsdato | updated_at | Column |
| vedtaget | approved | Column |
| konklusion | conclusion | Column |

### Character Replacement

Danish characters are replaced for filesystem compatibility: `ø→oe`, `æ→ae`, `å→aa`.

---

# Part IV — Operations and Evolution

---

## Chapter 20: Advantages and Limitations

### Advantages

1. **Extremely low cost** — open-source core with minimal cloud spend (~€80/month for a 3-user team)
2. **Proper layer separation** — Bronze preserves raw data, Silver historizes changes, Gold serves business users
3. **Software engineering practices** — version control, testing, modularity, CI/CD via dbt and Dagster
4. **SCD Type 2 done right** — insert-only history with hash-based change detection and automatic delete detection
5. **Code generation** — adding an entity requires updating one Python list; 72+ model files generated automatically
6. **Factory-pattern orchestration** — 18 Dagster assets from two factory functions; DRY and consistent
7. **Delta Lake as serving format** — ACID transactions, Power BI compatibility, schema evolution
8. **Python-native, testable** — orchestration is Python, reviewed in PRs, testable with pytest
9. **Open source core** — DuckDB, dbt, dlt, Dagster — no vendor lock-in

### Limitations

1. **Batch only** — not suitable for real-time or streaming use cases
2. **Diverse skill set** — needs SQL, Python, dbt, Dagster, Git, cloud knowledge
3. **DuckDB scale limits** — single-node, in-memory; datasets > 16 GB need different strategies
4. **Integration complexity** — DuckDB ↔ OneLake ↔ Delta Lake requires custom glue code
5. **OneLake coupling** — storage tied to Fabric ecosystem via PPU licensing
6. **Power BI PPU gate** — premium features require PPU; consumers also need PPU unless on capacity

---

## Chapter 21: Cost Breakdown

### Example: 3-User Team (1 Developer, 2 Analysts)

| Component | Monthly Cost |
|-----------|-------------|
| Power BI Pro (3 users × €12,10) | €36,30 |
| ACI runtime (4 vCPU / 16 GB, ~1h/day) | ~€7 |
| GitHub Pro (1 user) | €5 |
| OneLake storage (included with PPU) | €0 |
| DuckDB / dbt / dlt / Dagster | €0 (open source) |
| **Total** | **~€48** |

With PPU instead of Pro for premium features: ~€67–80/month.

### What Changes Cost the Most

1. **Moving from nightly to near-real-time** — more compute runs, more log volume
2. **Direct Lake / Fabric capacity** — introduces capacity decisions
3. **Data growth** — log retention and partitioning discipline usually matter more than storage price

---

## Chapter 22: Upgrade Path

| Component | Current | Upgrade To | Trigger |
|-----------|---------|------------|---------|
| Compute | DuckDB → MotherDuck (already done) | Fabric Spark | Data exceeds memory |
| Runtime | ACI | ACA / Cloud Run / AKS | Multiple workloads |
| Metadata | Python config + .env | YAML or Azure SQL | 50+ sources |
| Transform | dbt Core | dbt Cloud | Team > 5 developers |
| Ingestion | dlt (custom) | Fivetran / Airbyte | 100+ managed connectors |
| Orchestration | Dagster (self-hosted) | Dagster Cloud | Managed scheduling |
| Storage | OneLake | Azure Blob Storage | Decouple from Fabric |

The Danish Parliament pipeline has already taken the first upgrade step: moving from local DuckDB to MotherDuck for cloud persistence — validating that components can be swapped independently.

---

## Chapter 23: Operational Concerns

### Backfill Strategy

The Dagster `ExtractionConfig` allows backfills from the UI or CLI:

```python
class ExtractionConfig(Config):
    date_to_load_from: str | None = None  # Supply specific date for backfill
```

From the Dagster Launchpad: set `date_to_load_from` to the desired historical date. From the CLI:

```bash
dagster job execute -m ddd_python.ddd_dagster.definitions \
    -j danish_parliament_incremental_job \
    --config '{"ops":{"aktoer":{"config":{"date_to_load_from":"2025-01-01"}}}}'
```

### Secret Rotation

| Secret | Where Used | Rotation |
|--------|-----------|----------|
| Azure Service Principal (client_secret) | OneLake access | Every 6–12 months |
| MotherDuck token | DuckDB cloud connection | Per policy |
| GitHub PAT | Container registry | Tied to PAT expiry |

Procedure: generate new credential → update `.env` / GitHub Actions secrets → test in DEV → verify PROD → delete old credential.

### OneLake Exit Path

Migration to plain Azure Blob Storage is straightforward:
1. Create ADLS Gen2 storage account
2. Update ABFS paths in `.env` and `dbt_project.yml`
3. All dbt models, macros, and pipeline code remain unchanged

---

## Chapter 24: The Data Engineering Dashboard

The platform generates structured logs from three independent systems: Dagster event logs, dbt `run_results.json`, and dlt pipeline summaries. These can be unified into a dimensional model for observability.

### Architecture

```
JSON logs on OneLake          DuckDB views (dbt)             Delta Lake on OneLake
┌─────────────────┐     ┌──────────────────────────┐     ┌────────────────────┐
│ logs/dagster/    │     │ stg_dagster_run          │     │ warehouse/         │
│ logs/dbt/        │────►│ stg_dbt_invocation       │────►│  dim_tool/         │
│ logs/dlt/        │     │ stg_dlt_run              │     │  dim_workload/     │
│                  │     │     ↓                    │     │  fact_execution/   │
│                  │     │ int_execution_union       │     │                    │
│                  │     │ int_execution_lineage     │     │                    │
│                  │     │     ↓                    │     │                    │
│                  │     │ dim_* + fact_execution    │     │                    │
└─────────────────┘     └──────────────────────────┘     └────────────────────┘
                                                                  │
                                                          Power BI Dashboard
```

### Star Schema

The grain of `fact_execution` is one row per tool invocation — a Dagster run, a dbt invocation, or a dlt pipeline run. Dimensions include:

- `dim_tool` — tool name and version (dagster, dbt, dlt)
- `dim_workload` — execution hierarchy path (e.g., `dagster:nightly > dlt:aktoer`)
- `dim_execution_status` — SUCCESS, FAILED, UNKNOWN
- `dim_environment` — dev, prod
- `dim_trigger` — scheduled, manual
- `dim_date` — calendar date dimension
- `dim_time_minute` — time-of-day dimension (minute grain)

### DAX Measures

```dax
Total Executions = COUNTROWS(fact_execution)

Success Rate =
    DIVIDE(
        CALCULATE(COUNTROWS(fact_execution), dim_execution_status[is_success] = TRUE),
        COUNTROWS(fact_execution), 0
    )

Avg Duration (min) = AVERAGE(fact_execution[duration_ms]) / 60000

Hours Since Last Success =
    DATEDIFF(
        MAXX(FILTER(fact_execution, RELATED(dim_execution_status[is_success]) = TRUE),
             fact_execution[finished_at_utc]),
        NOW(), HOUR
    )
```

---

## Summary: From Architecture to Production Code

| Architecture Concept | Chapter | Implementation File |
|---------------------|---------|---------------------|
| Medallion layers | Ch. 2 | `dbt/dbt_project.yml` |
| OneLake storage | Ch. 5 | `dbt_project.yml` vars |
| DuckDB/MotherDuck | Ch. 6 | `dbt/profiles.yml` |
| dlt ingestion | Ch. 7 | `ddd_python/ddd_dlt/` |
| dbt transformation | Ch. 8 | `dbt/macros/*.sql` |
| Bronze raw views | Ch. 9 | `generate_model_bronze.sql` |
| Silver SCD2 | Ch. 10 | `generate_model_silver_*.sql` |
| Delete detection | Ch. 11 | Silver macros (full-refresh path) |
| Gold star schema | Ch. 12 | `dbt/models/gold/actor.sql` etc. |
| Dagster orchestration | Ch. 13 | `ddd_python/ddd_dagster/` |
| Delta Lake export | Ch. 14 | `export_main_*.py` |
| Power BI | Ch. 15 | `cast_hash_to_bigint.sql` |
| Code generation | Ch. 17 | `generate_dbt_models.py` |
| Config as code | Ch. 18 | `configuration_variables.py` |
| LKHS_ naming | Ch. 19 | All models |
| Hash-based CDC | Ch. 10 | `generate_base_for_hash.sql` |
| Factory-pattern assets | Ch. 13 | `ddd_python/ddd_dagster/assets.py` |
| FK safety rows | Ch. 12 | `dbt/models/gold/actor.sql` (UNION ALL) |
| Surrogate keys | Ch. 12 | `cast_hash_to_bigint.sql` |
| Concurrent extraction | Ch. 7 | `dlt_run_extraction_pipelines_*.py` |

---

*This handbook provides both the architectural blueprint and the production code to build a modern data platform. Every concept is grounded in working code from the Danish Parliament pipeline — a system that processes 18 entities from source to star schema using exclusively open-source tools on a minimal budget.*

**Document Version:** 1.0
**Last Updated:** March 2026
