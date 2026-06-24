# Design Decisions and Trade-offs

Last updated: June 2026

This document explains the architectural decisions made in this project: what problem each decision solves, what alternatives were evaluated, and what was gained and lost. It is written for data engineers and architects who want to understand the reasoning, not just the mechanics.

For implementation details and how-to guides, see [python_code_explained.md](python_code_explained.md), [silver_model_logic.md](silver_model_logic.md), and [dbt_macros.md](dbt_macros.md). For the system overview and tech stack, see [CLAUDE.md](../CLAUDE.md).

---

## ADR-1: DuckDB as the single query engine for all layers

**Decision:** Use DuckDB as the sole query engine for Bronze, Silver, and Gold. All data lives in a single `.duckdb` file (or optionally in DuckLake, a separate but attached database). No Snowflake, BigQuery, Spark, or MotherDuck.

**Context:** This is a learning/reference project intended to run on a low-cost infrastructure (a single €35/month server including storage and backups). The data volume is modest: ~18 parliamentary entities (millions of rows) and 7 Rfam tables (millions of rows). Fast iteration and ease of understanding matter more than distributed-query optimization.

**Alternatives considered:**

- **Snowflake / BigQuery.** Industry standard. Horizontal scalability. But: requires paying per query; API-only (no local debugging); difficult to run on a laptop for development.
- **Spark.** Open source. Distributed. But: overkill for single-node data; complex to set up; not a "just data" engine (requires scheduler, cluster management).
- **MotherDuck.** Hybrid local + cloud DuckDB. But: vendor lock-in; complicates offline mode; adds credentials overhead.
- **Plain PostgreSQL.** Stable. But: slower for analytical queries than a columnar engine; window functions are less idiomatic.

**Trade-offs:**

*Gained:*
- Runs on a laptop with no cloud setup. Extraction and transformation happen in the same `.duckdb` file — no data movement between systems.
- Single-node simplicity. No distributed query coordination, no eventual consistency, no shuffle overhead.
- Fast iteration during development. Change a macro, re-run dbt, see results instantly.
- Parquet and Delta support are first-class. Native `read_parquet()`, `read_json_auto()`, and the `delta_scan` extension.

*Lost:*
- **Cannot scale horizontally.** If rows grow to >10 billion, a single-node architecture becomes painful. But at that scale, you'd be replacing the entire platform anyway — this is not a steppingstone.
- **Single-writer constraint.** Only one process may hold a write connection at a time. This shapes the executor strategy (see ADR-5). Metabase must be stopped around dbt runs.
- **No built-in replication or failover.** A corrupted `.duckdb` file is catastrophic (mitigated by backups, but not automatic failover).

**Consequences:**

- The entire job scheduler (Dagster) must respect the single-writer rule — dbt jobs use `in_process_executor`, not multiprocess.
- Bronze/Silver/Gold all live in the same database file, making backup / restore atomic.
- Metabase integration requires explicit lifecycle management (stop before writes, start after).
- SQL syntax is DuckDB-specific (e.g., `QUALIFY`, `COLUMNS(lambda)`) — queries would need porting to migrate to a different engine.

---

## ADR-2: Medallion architecture with explicit layer semantics

**Decision:** Implement a three-layer medallion (Bronze → Silver → Gold) with strict rules:
- **Bronze:** Read-only views over raw extracted files. Zero transformation, one view per entity plus a `_latest` view.
- **Silver:** Persisted, incremental CDC tables with SCD Type 2 history. One table per entity plus a `_cv` (current-version) view.
- **Gold:** Read-only views. Star-schema modeling on top of Silver `_cv` views. No new data, pure presentation layer.

**Context:** Medallion is the modern standard for data lakehouses. This project needed a structure that separated concerns clearly and made the pipeline observable — you should always be able to query Bronze to see what was extracted, Silver to understand what changed, and Gold to answer business questions.

**Alternatives considered:**

- **Two-layer (Raw → Processed).** Simpler naming. But: loses the distinction between "captured change" (Silver) and "modeled for consumption" (Gold). Gold queries would have to recompute CDC logic or reference half-finished Silver tables.
- **Direct extraction to Gold.** Skip Bronze/Silver. But: no auditability (if a Gold metric is wrong, you can't trace back to the raw input). No historical record. No ability to re-run Silver logic with a bug fix.
- **Flat schema (all tables in one schema).** Simpler file layout. But: harder to understand which tables are internal vs. production; no clear isolation between layers.

**Trade-offs:**

*Gained:*
- **Auditability.** Every layer is queryable. A bug in Gold logic doesn't destroy the Silver history — re-run dbt, and Gold updates.
- **Separation of concerns.** Bronze never fails (it just reads files). Silver changes are always tracked (insert/update/delete detected via hash). Gold is pure math.
- **Incremental rebuilds.** You can re-run Silver without re-extracting Bronze, or re-run Gold without re-processing Silver.

*Lost:*
- **Complexity.** Three layers with inter-dependencies, not two. The CDC logic in Silver is non-trivial (see ADR-3). Adding a new entity means code-generating models at three layers, not one.
- **Storage overhead.** Silver stores full SCD Type 2 history (every version of every row). A daily update to actor #42 means two Silver rows (one with the old hash, one with the new). On large tables, this can grow 2–3× the latest-version size over a year.
- **Query latency for consumers.** Gold is views, not materialized tables. A complex star-schema join in Gold re-reads Silver `_cv` views (which are themselves views) every time. Not a problem for dashboard refreshes every hour, but not suitable for low-latency transactional systems.

**Consequences:**

- dbt model counts scale with entity count. Each Bronze entity generates a main view and a `_latest` variant; each Silver entity generates a CDC table and a `_cv` current-version view; Gold includes star-schema views for key analytics dimensions plus corresponding `_cv` views. Adding a new entity touches all three layers.
- The `LKHS_` column prefix is used consistently across all tables so downstream tools (Power BI, Metabase) see the tracking metadata as opaque.

---

## ADR-3: Hash-based CDC (SHA256) vs. log-based CDC

**Decision:** Detect changes via SHA-256 hashing of all business columns (excluding tracking columns). When the hash differs between Bronze snapshots, a row has changed (insert, update, or delete).

**Context:** The extraction sources (Danish Parliament OData API and Rfam MySQL) do not provide change feeds or transaction logs. They only provide snapshots: the OData API returns all entities as of a date, and the MySQL tables return all rows or rows with `updated >= date`. To detect changes, we compare snapshots.

**Alternatives considered:**

- **Debezium + Kafka.** Enterprise-grade CDC. Captures exact transaction order and old/new row pairs. But: adds operational complexity (Kafka cluster); requires code inside the source databases (connectors); overkill for public read-only data sources.
- **Column-by-column comparison.** For each row, compare each column value to the previous snapshot. But: slow (O(n × m) where n = rows, m = columns) and hard to express in SQL.
- **Timestamp-based CDC.** Rely on the source's `updated` or `modified` timestamp. But: clocks can skew, and if a row is updated twice in the same second, only one update is seen.
- **Surrogate key + sequence number.** Assume sources provide monotonic row IDs. But: not all sources do; and a deleted row's ID can be reused.

**Trade-offs:**

*Gained:*
- **Minimal infrastructure.** No Debezium, no Kafka, no external dependency beyond DuckDB's hash function. Bronze files are just JSON/Parquet.
- **Simple to implement.** SHA256 in SQL is one function call; the macro logic is straightforward.
- **Works with append-only extraction.** Each new Bronze file doesn't need to be a full snapshot (for incremental entities) — the hash comparison works across any sequence of files.
- **Deterministic and reproducible.** Same input always produces the same hash. `--full-refresh` of Silver always produces identical output.

*Lost:*
- **No row-level operation semantics.** Hash comparison detects "something changed," not "columns X, Y, Z changed." For precise auditing (e.g., "who changed this field and when?"), you'd need column-by-column tracking.
- **Late-arriving data and backfills are tricky.** If a late insert of row #42 arrives days after the initial insert, the hash changes. The `LKHS_date_valid_from` will carry the late file's date, not the actual insertion date (which may be lost). Workaround: the `LKHS_date_inserted_src` column preserves the earliest `opdateringsdato` per entity from Bronze, but full row-level timestamps are not kept.
- **Hash collisions (theoretical).** Two different rows could theoretically produce the same SHA256 hash. Probability is negligible for these data volumes, but it's a non-zero risk. Mitigated by: not doing critical decisions on hash alone (always confirm with business logic).
- **Excludes tracking columns from the hash.** The `LKHS_` columns themselves don't affect change detection. If a row's hash is the same but `LKHS_date_inserted` changed, it's treated as "no change." Correct for CDC logic, but means Silver cannot track when dbt re-ran if no business data changed.

**Consequences:**

- Every Silver table has `LKHS_hash_value` (SHA256 hex string) and `LKHS_cdc_operation` (I/U/D) columns. The hash is stable across runs so deduplication guards can use it.
- The `generate_base_for_hash` macro dynamically builds the hash expression by querying `information_schema.columns` at compile time. This is hidden from users but means dbt needs `DESCRIBE` or equivalent privileges.

---

## ADR-4: dlt for extraction, not a custom HTTP client

**Decision:** Use dlt (Data Load Tool) to orchestrate extraction from both the OData API and the MySQL database, writing to Bronze as JSON/Parquet files.

**Context:** Extraction needs to handle pagination (OData), state management (tracking which date range was last loaded), and schema inference (don't hardcode column lists). These are not trivial.

**Alternatives considered:**

- **Custom Python HTTP client.** Direct `requests` library calls with manual pagination loops. But: error-prone pagination (need to parse `@odata.nextLink`); manual retry logic; schema changes break the script.
- **Apache Sqoop.** Purpose-built for JDBC to Hadoop. But: Java-heavy; overkill for this data volume; not suitable for local development.
- **Azure Data Factory.** Fully managed. But: expensive; vendor lock-in; difficult to version-control pipelines (defined in UI).

**Trade-offs:**

*Gained:*
- **Schema inference.** dlt's `read_json_auto` tells DuckDB "figure out what columns exist," not "I hardcoded them." New API fields are automatically visible.
- **Checkpoint management.** dlt's `load_info` and `pipeline.state` track which files were loaded, enabling idempotent re-runs (if you retry the same extraction, dlt avoids re-loading the same file).
- **Multi-destination abstraction.** dlt can write to local filesystem (`file://`) or Azure Data Lake (`az://`) with one config change.

*Lost:*
- **Incremental cursor NOT used.** Notably: dlt has a built-in `incremental` parameter that can track the last value of a column (e.g., `max(updated)`) and automatically filter on the next run. This project **does not use it**. Instead, the extraction scripts manually construct the OData `$filter=opdateringsdato ge DateTime'...'` and the MySQL `WHERE updated >= :date`. Why? Because the incremental cursor is global per resource, and some entities are extracted both fully and incrementally in different jobs. Using dlt's cursor would couple the two and create confusion. This is a deliberate design: **dlt is used for format conversion and schema inference, not for incremental state**. The `DLT_PIPELINES_DIR` directory contains dlt's internal checkpoints, but the CDC logic (deciding which date to load from) lives entirely in Python / Dagster layer.
- **file_to_file pipeline type is not really dlt.** Some entities (seed data) are just copied from one location to another. This is wrapped in dlt's abstraction for consistency, but it's really just `shutil.copy` with dlt's directory layout. dlt adds little value here, but the consistency is worth it.

**Consequences:**

- Extraction is fast but I/O-bound (network round-trips for OData pagination, database queries for Rfam). Dagster extraction jobs use `multiprocess_executor` (max 4 workers) for concurrency.
- dlt's internal layout (`_dlt_*` metadata columns) is stripped out by Bronze views, so downstream sees clean data.

---

## ADR-5: Dagster software-defined assets, not Airflow DAGs

**Decision:** Use Dagster (not Airflow) as the orchestrator. Model the pipeline as **software-defined assets** (not tasks/operators). Every extraction, dbt model, and export is an asset with explicit dependencies.

**Context:** Orchestration is about "run X, then Y, then Z." Task-DAG tools (Airflow, Prefect) model this as directed graphs of tasks. Asset-DAG tools (Dagster, dbt Cloud) model it as producers and consumers of data objects. This project chose assets because the mapping is clearer: "aktoer asset produced, silver_aktoer asset consumes it."

**Alternatives considered:**

- **Airflow.** Mature. Wide adoption. But: task-centric mental model requires you to invent artificial tasks for "run dbt model X" instead of saying "produce the aktoer asset." Operator boilerplate is heavy. Harder to test individual assets without mocking the full DAG.
- **cron + shell scripts.** Simplest. But: no retry logic, no alerting, no UI to monitor failures, no history.
- **dbt Cloud.** Native dbt integration. But: only orchestrates dbt; doesn't handle extraction (dlt) or export. Would need to bolt on external tooling.

**Trade-offs:**

*Gained:*
- **Asset-centric view.** Each dbt model automatically becomes an asset. Extraction tasks explicitly declare their output assets. Export tasks declare which assets they read. The lineage is correct by construction, not by manual wiring.
- **Op/asset factory pattern.** Extraction assets (DDD and Rfam) are created via factory functions in loops, not separate blocks of boilerplate. Same for export. Change one factory, and all assets update.
- **Testability.** Dagster's `materialize([...])` helper lets you test a subset of the asset graph in isolation.
- **Integrated run history.** Every Dagster run is logged to SQLite (`DAGSTER_HOME`). The `data_engineering` layer reads this history and makes it queryable — "which assets materialized yesterday, and for how long?"

*Lost:*
- **Learning curve.** Dagster's resource/asset/job/schedule model is different from Airflow's operator/DAG/sensor model. The docs are good, but it takes time.
- **Not as widely deployed.** Airflow is at more companies. If you need to hand off to a team unfamiliar with Dagster, they're starting from zero.
- **Single-writer bottleneck is explicit (in a good way).** Because the project chose DuckDB (ADR-1), and DuckDB enforces single-writer, dbt jobs cannot run in parallel. Dagster makes this visible: dbt jobs must use `in_process_executor`. An Airflow equivalent would hide the constraint until runtime (and then fail mysteriously with lock timeouts). Here, the code and execution model align.

**Consequences:**

- Two executor strategies: `multiprocess_executor` for extraction/export (I/O-bound, 4 workers), and `in_process_executor` for dbt (single-writer constraint).
- Metabase lifecycle is controlled via two mechanisms: a private `_with_metabase_control(selection)` helper in `jobs.py` that wraps asset selections with `stop_metabase_asset` and `start_metabase_asset`, and per-asset `deps=[STOP_METABASE_ASSET_KEY]` declarations that prevent any asset from starting until Metabase is stopped. This dual enforcement ensures DuckDB write access is exclusive and correct by construction.

---

## ADR-6: Switchable Silver storage (DuckDB native vs. DuckLake)

**Decision:** Make the Silver layer storage pluggable. The `SILVER_STORAGE_FORMAT` env var switches between:
- `duckdb` (default) — Silver tables live in the main `.duckdb` file.
- `ducklake` — Silver tables live in a [DuckLake](https://ducklake.select) catalog (Parquet files + metadata).

This is **independent** of `STORAGE_TARGET`, which only controls the Delta Lake export destination (local vs. OneLake).

**Context:** During development, we realized that DuckDB's native binary format is opaque (hard to inspect, troubleshoot, or migrate). DuckLake is an open table format (Parquet data + JSON-based catalog) that makes the data portable. But DuckLake adds complexity (another extension, another file to manage). The solution: make both available, switchable at runtime.

**Alternatives considered:**

- **Always use DuckLake.** Portability from day one. But: adds operational complexity (manage two file hierarchies) and makes the first-run setup harder.
- **Always use native DuckDB.** Simplicity. But: locks you into DuckDB's binary format; if you ever need to migrate to Parquet or Delta, it's a full export job.
- **Always use Delta Lake for Silver.** Delta Lake is portable and has tooling (Databricks, Polars, etc.). But: Delta writes are slower than DuckDB native (test ran ~1.5× slower on the Silver layer), and it would eliminate the "offline on a laptop" use case (Delta requires external coordination for ACID).

**Trade-offs:**

*Gained:*
- **Optionality.** Start with native DuckDB (fast, simple). Later, switch to DuckLake if you need portability.
- **Offline development + production compatibility.** In dev: `SILVER_STORAGE_FORMAT=duckdb`, no catalog overhead. In production: `SILVER_STORAGE_FORMAT=ducklake`, Parquet files are portable and inspectable with Pandas / Polars.
- **DuckLake catalog has time-travel.** Snapshots can be versioned; you can query "what did Silver look like 30 days ago?"

*Lost:*
- **Operational complexity.** Two code paths. The DuckLake helpers (`_current_temp` tables, bookmark tracking) must work in both modes, so all helper-table names are fully qualified with `{{ this.database }}`. This constraint is invisible in `duckdb` mode (one database) but critical in `ducklake` mode (two databases, single-transaction isolation).
- **No automatic migration.** Switching `SILVER_STORAGE_FORMAT` does not migrate existing data — you must run `dbt build --select silver --full-refresh` to rebuild from Bronze. This is safe but can take hours on large histories.
- **Size overhead in DuckLake mode.** Parquet is columnar and compresses well, but it's not as space-efficient as DuckDB's native format for this scale (~10 GB native vs. ~15 GB Parquet in our tests).

**Consequences:**

- dbt profile selection is driven by `SILVER_STORAGE_FORMAT`: always `local_ducklake` if `ducklake` mode, otherwise `local` or `onelake` per `STORAGE_TARGET`.
- Backup/restore logic knows about both storage modes: in ducklake mode, the data files are backed up separately from the catalog, and restored in file-first order (data before catalog).
- Downstream tools (Metabase, DBeaver) must load the `ducklake` extension and `ATTACH` the catalog in ducklake mode. The Metabase Dockerfile bakes in the extension; DBeaver users need manual setup.

---

## ADR-7: Delta Lake export via PyArrow + deltalake (not DuckDB native delta writer)

**Decision:** For Delta Lake export, use the following split:
- **Read:** `delta_scan('<target_path>')` via DuckDB's delta extension. This reads the target Delta table as a SQL relation without materializing it into memory.
- **Write:** `deltalake.write_deltalake()` via the Python `deltalake` library, backed by PyArrow.

**Context:** At the pinned DuckDB version (≥1.5.1, <1.6), the delta extension is **read-only**. There is no `COPY ... (FORMAT delta)` writer. Newer DuckDB versions add a Delta writer, but there is a known **Azure/OneLake regression** that makes Delta writes fail on Fabric. The compromise: use the delta extension only for reads (dedup anti-join), and `deltalake` for writes.

**Alternatives considered:**

- **Use `deltalake` for both read and write.** Uniform approach. But: reading the full target table into PyArrow memory (via `DeltaTable(...).to_pyarrow_table()`) defeats the purpose of incremental append — you materialize the entire existing table, then filter to find new rows. For a 1 GB Delta table, this is wasteful.
- **Upgrade DuckDB and use the native writer.** Future-proof. But: blocks on the Azure regression fix. Once the regression is resolved and a new DuckDB version is released with the writer, revisit this. For now, waiting is safer than shipping broken code.
- **Materialize to Parquet, then ingest as Delta.** Avoid the `deltalake` library entirely. But: extra file I/O and temporary storage overhead.

**Trade-offs:**

*Gained:*
- **Memory-efficient dedup read.** The anti-join (Silver records not yet exported) runs inside DuckDB with projection pushdown. Only join keys are fetched from the target, not entire rows. For a 100 M-row Silver table exporting 1 M new rows, this is the difference between materializing 100 M rows vs. materializing 1 M + 1 M rows.
- **Clean separation of concerns.** DuckDB handles SQL (read + join + filter). PyArrow/deltalake handles Delta metadata (schema, partitioning, version history).

*Lost:*
- **Mixed read/write libraries.** Code couples two libraries. If you ever need to swap one, both paths are affected.
- **Dependency on `deltalake` library.** Adds a maintained external dependency. If `deltalake` ever stops being maintained or pivots, this breaks. (Currently, Databricks maintains it, so risk is low.)
- **Azure regression blocks upgrade.** We are pinned to a DuckDB version with a read-only Delta extension until the Azure regression is fixed in a newer version. This is a hard stop for any DuckDB bump.

**Consequences:**

- On the next DuckDB version bump (when both a writer-capable version exists AND the Azure regression is fixed), revisit this decision. The entire write path could move to `COPY ... (FORMAT delta)`.
- Gold export always does full overwrite (no dedup needed), so it still uses `deltalake.write_deltalake()` with PyArrow but doesn't need to read the target.

---

## ADR-8: Single source of truth in configuration_variables.py

**Decision:** All entity lists (DDD entities, Rfam tables, their PKs, date columns, SQL queries) live in one Python file: `ddd_python/ddd_utils/configuration_variables.py`. Everywhere else in the codebase (dbt model generation, Dagster assets, tests), lists are derived via comprehensions or loop over this file.

**Context:** A data pipeline has many named things: tables, columns, primary keys. If each name is hardcoded in 5 places, you'll inevitably update 4 of them and miss the 5th. A single source of truth prevents this.

**Alternatives considered:**

- **Metadata in the database.** Store entity lists in a DuckDB table. Pros: queryable, version-controllable via dbt seeds. Cons: chicken-and-egg problem (need the DB to know what to extract).
- **YAML config file (separate from code).** Familiar, human-readable. Pros: clear separation of data and logic. Cons: parsing overhead; not as easy to validate at import time; harder to tie tests to the config.
- **Multiple files per layer.** Bronze config in Bronze generator, Silver config in Silver generator, etc. Pros: decoupled. Cons: redundancy; easy to drift.

**Trade-offs:**

*Gained:*
- **Single point of truth.** Add an entity: update `DANISH_DEMOCRACY_FILE_NAMES` and `DANISH_DEMOCRACY_TABLE_PRIMARY_KEYS`. All extraction assets, dbt Bronze models, dbt Silver models, and tests automatically use the new entity.
- **Validated on import.** `configuration_variables.py` has import-time guards (ValueError exceptions, not assertions) that check: entity counts and subsets are consistent, all entities have PKs, all Rfam tables have SQL queries. These run before `dbt generate` or any extraction, catching errors early.
- **No duplication.** Derived lists like `DANISH_DEMOCRACY_MODELS_SILVER = [m.replace("bronze_", "silver_", 1) for m in DANISH_DEMOCRACY_MODELS_BRONZE]` are guaranteed to stay in sync.

*Lost:*
- **Fat import.** Importing `configuration_variables` pulls in the entire config graph: all entity names, all PKs, all Rfam queries. For a tiny script that only needs one entity name, this is overkill. Mitigated by: the module is small (~200 lines) and imports are cheap in Python.
- **Less flexible at runtime.** You can't add an entity without code changes (to this file). In a fully metadata-driven system, you could add an entity by inserting a row into a table. This is a non-issue for a learning project, but it's a constraint.

**Consequences:**

- Test file `test_configuration_variables.py` enforces the constraints at CI time: if someone accidentally deletes a PK, the test fails.
- The `generate_dbt_models.py` script reads this config at runtime and generates SQL files for Bronze (views: one main and one `_latest` per entity, plus utilities), Silver (one CDC table and one `_cv` view per entity), and Gold (star-schema views). A new entity immediately generates multiple new files.

---

## ADR-9: Lazy environment loading via PEP 562 __getattr__

**Decision:** Use a module-level `__getattr__` function (PEP 562) in `get_variables_from_env.py` to defer loading of Azure credentials and other required env vars until first access. Optional vars are loaded eagerly at import time.

**Context:** The project must run in multiple modes:
- **Local mode (laptop):** No Azure credentials needed. Tests, code generation, and extraction should work without them.
- **OneLake mode (production):** Azure credentials are required. They must be validated early so failures are loud, not silent.

If every required var were read at module import time, you couldn't even `import configuration_variables` to run tests on a laptop.

**Alternatives considered:**

- **Eager loading, with fallback to None.** Read all vars at import. If missing, set to `None`. But: downstream code can't distinguish "not configured" from "misconfigured as None." Bugs are silent.
- **Try/except at every call site.** Let callers handle `EnvironmentError`. But: error handling is inconsistent; errors are far from the root cause.
- **Module class + sys.modules swap.** Replace the module object in `sys.modules` with a custom class that implements `__getattr__`. But: breaks IDE autocomplete and static type checkers.
- **Pytest fixtures to mock env vars.** Use fixtures in tests only. But: doesn't solve the problem for local code generation or manual testing.

**Trade-offs:**

*Gained:*
- **Lazy resolution.** Required vars are fetched only when used. If you never call an export function, Azure secrets are never read. This enables local/test modes without credentials.
- **IDE support.** Module-level `__getattr__` is standard Python (PEP 562). IDEs and type checkers understand it correctly — no `# type: ignore` hacks needed.
- **Clear semantics.** Optional vars are plain module globals (resolve at import). Required vars go through `__getattr__` (resolve on first access). The distinction is clear in the code.
- **Early failure on required vars.** The first time you call `get_variables_from_env.AZURE_CLIENT_SECRET`, if it's missing, you get a clear error message ("EnvironmentError: AZURE_CLIENT_SECRET not set"). Better than a silent failure downstream.

*Lost:*
- **Slightly less obvious.** A newcomer reading the code might not realize that accessing a name in the module can raise an `EnvironmentError`. Mitigated by: docstrings and comments in the module.
- **No static analysis of required vs. optional.** A type checker can't automatically know that `STORAGE_TARGET` is optional but `AZURE_CLIENT_SECRET` is required. You have to read the code or docs. But: this is a minor issue, and the `_LAZY_REQUIRED` dict documents the distinction.

**Consequences:**

- Every Azure-dependent function (export, OneLake logging) must be designed to defer the Azure import. This is done with function-local `import` statements inside the OneLake branch.
- Tests can mock `os.environ` without worrying about pre-loaded credentials.

---

## Summary: Why These Choices

| Choice | Core Reason | Acceptable For |
|--------|------------|-----------------|
| DuckDB | Single-node simplicity, runs on laptop | Scale: millions of rows; not billions |
| Medallion | Clear layer semantics, auditability | Learning project, BI dashboards |
| Hash-based CDC | No external infrastructure (Debezium), snapshot-based sources | Historical tracking, not row-level audit trail |
| dlt | Schema inference, multi-destination, state mgmt | Standard extraction patterns (API + DB) |
| Dagster | Asset-centric, testable, integrated observability | Workflow <100 assets; small team |
| DuckDB-or-DuckLake | Optionality, offline + portable | Neither is a constraint; pick by use case |
| Delta export split (delta_scan + deltalake) | Memory efficiency, workaround Azure regression | Known regression is temporary |
| Single source of truth | Maintainability, DRY | Entity-driven systems (not arbitrary code) |
| Lazy env vars | Local mode without credentials | Multi-mode systems with optional features |

---

## What Was Traded Away

### Not Addressed in This Project

1. **Multi-tenancy.** All data for all entities in one `.duckdb` file. No row-level security, no per-tenant queries. Acceptable for a single-user learning project; not acceptable for SaaS.
2. **Streaming ingestion.** Bronze is batch-only (extracts every 6 hours). No real-time SQL as data arrives. For a parliamentary body, batch is sufficient.
3. **Schema evolution support.** If an API adds a new field, the extraction discovers it (dlt's schema inference), but old Bronze files still have the old schema. The `COLUMNS(lambda)` view syntax masks this, but it's not seamless. A production system would have explicit schema versioning.
4. **Exactly-once semantics.** The pipeline deduplicates via `NOT EXISTS` guards, but it doesn't enforce transactional exactly-once between Bronze and Silver. If an extraction fails mid-upload, a partial file might be recovered on retry. Risk is low because dlt implements its own state management, but it's not a formal guarantee.
5. **Cross-source joins in Silver.** The Silver layer separates DDD and Rfam cleanly. Joining an actor (DDD) to a family (Rfam) must happen in Gold. For this data model, it's the right choice, but a more tightly integrated schema would allow Silver-level joins.
6. **Formal data contracts.** No schema registry (like Confluent Schema Registry). dbt tests exist, but there's no external specification of "Rfam family table MUST have columns X, Y, Z." For a public API, the API specification is the contract; for MySQL, there's no equivalent.

### Conscious Constraints

1. **No automatic scaling.** DuckDB scales vertically (bigger machine), not horizontally (more machines). At 10 billion rows, rethink the platform.
2. **No distributed query.** Analytical queries can't span multiple machines. For this data, not needed; for a multi-source federation, it would be.
3. **No streaming state.** Incremental extraction is date-windowed, not event-based. The next extraction can take days; there's no real-time state updates. Acceptable for parliamentary data (meeting records are not real-time).

---

*See [CLAUDE.md](../CLAUDE.md) for the system overview and [python_code_explained.md](python_code_explained.md) for implementation details.*
