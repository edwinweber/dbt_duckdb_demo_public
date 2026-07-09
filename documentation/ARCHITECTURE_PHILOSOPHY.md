# Architecture Philosophy: When to Build a Single-Server Data Lakehouse

## The Problem This Solves

You have:
- **Multiple heterogeneous sources:** A REST API (the Danish Parliament OData endpoint), a MySQL database (Rfam bioinformatics), possibly CSV exports from SaaS tools.
- **A clean consumption layer:** A BI tool (Metabase), a data lakehouse (Microsoft Fabric OneLake), or Power BI connected to Delta Lake files.
- **A realistic budget and team:** No dedicated data platform team, no Databricks or Synapse budget, no tolerance for hidden vendor lock-in via proprietary operators.
- **A scale reality:** Your active data fits on one machine today and will for years. Active dataset: 1–5 GB. Total history: under 50 GB. Not 50 TB.

The question: build the medallion pipeline on Snowflake, BigQuery, Spark, or a single server?

This project chooses **single-server simplicity** — DuckDB + dbt + Dagster on one machine. It's cheaper, yes, but the real reason: **at this scale, distributed system complexity adds risk without solving a real problem**.

## The Trade-offs

### You Win

1. **One command to run the full pipeline locally.** `dagster dev -w workspace.yaml` or `python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile`. No cluster setup, no cloud credentials, no data leaving your laptop.
2. **Debugging is direct SQL.** Failures reduce to: open DuckDB CLI, run the query, see the rows. No distributed shuffle to trace, no query queue, no logs scattered across three systems.
3. **Infrastructure cost: ~€30/month on Hetzner.** Or €0 on your laptop. No per-query billing, no seat licenses, no surprise bills.
4. **The entire system is code.** dbt models, Dagster jobs, Python extractors, Docker Compose — all in one git repo, code-reviewed, fully reproducible.
5. **Adding a new data source is one config file change.** `configuration_variables.py` is the source of truth. Add an entity to the lists, run `generate_dbt_models`, done.
6. **Storage is switchable without rewriting.** Three independent switches — Bronze/Silver location (local vs. S3), Silver format (DuckDB native vs. DuckLake), export destination (local vs. OneLake) — all controlled by environment variables.

### You Lose

1. **No horizontal scale.** At 10 billion rows, you're replacing the entire platform. This isn't a stepping stone; it's a hard ceiling.
2. **Single-writer bottleneck.** DuckDB allows only one writer process at a time. dbt jobs must run sequentially. Managed by Dagster's `in_process_executor` and stopping Metabase during writes, but it's a hard constraint.
3. **No failover.** Server failure means restore from backup (automated nightly). No read replicas, no hot standbys.
4. **Incremental deletes are captured only at full-refresh.** The OData API provides date-filtered incremental feeds, not CDC streams. A record silently deleted at the source (no timestamp update) won't appear as a delete in Silver until a `--full-refresh` re-processes all Bronze files. See [ADR-3](design_decisions.md#adr-3-hash-based-cdc-sha256-vs-log-based-cdc) and *Incremental Delete Limitation* below.

## When to Use This Pattern

**This is for you if:**
- Active data fits under 500 GB (total history under 2 TB)
- You control the deployment infrastructure — a single cloud VM, or your laptop
- Your team knows dbt, SQL, and Python
- You need a medallion architecture with CDC and SCD Type 2 full history
- You value "see every decision in one codebase" over "enterprise tooling with a UI"
- You need incremental ingestion from REST APIs and SQL databases, not event streams
- You're feeding Azure Data Lake or Microsoft Fabric but want a clean Delta Lake pipeline without ADF or Synapse

**This is NOT for you if:**
- You need sub-second ad-hoc queries on multi-terabyte datasets — use Snowflake or BigQuery
- You need real-time or near-real-time ingestion — this is daily batch only
- You need incremental hard-delete detection between batch runs — the OData API has no tombstones; deletes are captured only at `--full-refresh`
- Your team needs multi-writer concurrency on the transformation layer
- You need compliance row-level security at the data layer (this is file-system access control only)
- You are building infrastructure for multiple tenants or external consumers

## The Three Core Design Decisions

These three decisions shape everything else. Full rationale in [design_decisions.md](design_decisions.md).

### 1. Hash-Based CDC, Not Debezium

Changes are detected by comparing SHA-256 hashes of row content between successive Bronze snapshots. Why not Debezium or column-by-column comparison?

- **No Kafka infrastructure** required. Debezium needs a Kafka cluster and database CDC capability (binlog for MySQL, WAL for Postgres). Neither the Danish Parliament API nor Rfam MySQL allows it.
- **Sources only provide snapshots.** REST APIs and SQL dumps give you the current state — there is no transaction log to tail. Snapshot-to-snapshot comparison is the only viable approach.
- **Trade-off:** The hash detects *that something changed* but not *which columns changed*. For a full column-level audit trail, you would add column-by-column tracking. For "what changed and when," the current SCD Type 2 history is sufficient.

### 2. DuckDB as the Sole Transformation Engine

Every transformation runs in DuckDB — Bronze views, Silver CDC tables, Gold star schema, Delta Lake export reads. Why not Spark?

- **No JVM, no cluster configuration, no shuffle management.** DuckDB handles everything via a single Python API call.
- **SQL dialect parity.** dbt models written for DuckDB are standard columnar SQL. `QUALIFY`, `EXCLUDE`, `read_json_auto`, and `read_parquet` are the only non-portable parts — all clearly isolated.
- **Trade-off:** Single-writer constraint. Horizontal scaling requires migrating the transformation layer.

### 3. Switchable Silver Storage

Silver tables can live in DuckDB native format (fast, opaque binary) or DuckLake (Parquet + catalog, portable and inspectable). The switch is one environment variable.

- **DuckDB native** is the default — simpler, faster, better initial tooling support.
- **DuckLake** adds snapshot time-travel, Parquet file portability, and S3 storage support — at the cost of ~1.5× storage overhead and additional operational complexity.
- **Orthogonality:** The Silver storage format is independent of the Bronze raw-file storage location (local vs. S3) and the export destination (local filesystem vs. OneLake). You can mix and match freely.

## Incremental Delete Limitation

This limitation is worth stating plainly because it comes up in every CDC discussion.

**The situation:** Danish Parliament incremental entities (Aktør, Møde, Sag, Sagstrin, SagstrinAktør, Stemme) are fetched with a date filter: `$filter=opdateringsdato ge DateTime('2024-01-01')`. Each extraction run fetches only records modified since the last run. A record that is silently removed from the source — with no modification timestamp update — will never appear in any incremental extraction file. It is therefore never detected as deleted between runs.

**When deletes ARE captured:** When a `--full-refresh` is run, the Silver macro anti-joins the current Silver history against `bronze_*_latest` (a cumulative view over all accumulated Bronze files). Keys present in Silver but absent from the full Bronze keyset are emitted as `D` rows. This works correctly as long as:
1. The initial load pulled a complete snapshot (no date filter, or a very old start date).
2. Bronze files are never pruned below that baseline.

Both invariants hold in this deployment.

**The alternative for real-time delete detection** would be a periodic full-snapshot reconciliation run: fetch the complete keyset (no date filter) weekly, anti-join against Silver `_cv`, emit `D` rows for missing keys. This is the standard incremental-for-speed, periodic-full-for-deletes pattern. It is not currently implemented — the nightly `--full-refresh` option is sufficient for the analytical use case.

**Why this is acceptable here:** The analytical questions this pipeline answers — "how did member X vote on case Y?" — are not impacted by a lag of hours or days in delete propagation. For compliance use cases requiring immediate delete detection, this pattern would need the periodic reconciliation layer.

## What Is Already Built

The project is not a sketch. As of the current version, it runs in production on a Hetzner server:

- **Medallion Bronze → Silver → Gold** with SCD Type 2 full history and current-version (`_cv`) views
- **Two independent source systems:** Danish Parliament OData (18 entities) + Rfam MySQL (7 tables)
- **Hash-based CDC** detecting Inserts, Updates, and Deletes on both incremental and full-extract sources
- **Dagster orchestration** with two daily schedules (disabled by default), retry policies, run-status sensors with ntfy.sh push notifications, and Metabase pause/resume around dbt runs
- **Three orthogonal storage modes** for Bronze/Silver/export independently switchable via env vars
- **Delta Lake export** to local filesystem or Microsoft Fabric OneLake (Silver incremental append + Gold full overwrite)
- **Automatic backup** to Hetzner StorageBox with grandfather-father-son retention
- **Metabase BI layer** reading DuckDB directly, with a custom Docker image supporting DuckLake
- **Over 200 tests** (unit, integration, end-to-end; real DuckDB in-process, no mocks)

## Design Patterns That Scale

Despite the single-node focus, the code demonstrates patterns applicable to larger systems:

1. **Asset factory pattern.** New entities are registered by one-line config changes. The same asset factory generates extraction, Bronze, Silver, and Gold assets simultaneously.
2. **Medallion with hash-based CDC.** No CDC database infrastructure (Kafka, Debezium) — works purely on snapshots. Applicable to any REST API + batch SQL system.
3. **Executor strategy split.** Extraction/export use concurrent workers; transformation uses serial execution. This respects the single-writer constraint without losing concurrency where it helps.
4. **Storage format negotiation.** The same dbt code runs against local DuckDB, DuckLake, or OneLake via environment variables. No code change to switch destinations.
5. **Observability layer reads the orchestrator.** dbt models query Dagster's SQLite event logs (`sqlite_scan`) to build dashboards of run times, failures, and asset lineage. Pure SQL, no extra infrastructure.

## Further Reading

- [README](../README.md) — quickstart, architecture diagram, running instructions
- [design_decisions.md](design_decisions.md) — 10 ADRs covering every major architectural choice
- [python_code_explained.md](python_code_explained.md) — module-by-module guide to the Python codebase
- [silver_model_logic.md](silver_model_logic.md) — detailed walkthrough of the CDC/SCD2 macros
- [CLAUDE.md](../CLAUDE.md) — canonical reference for system architecture and naming conventions
