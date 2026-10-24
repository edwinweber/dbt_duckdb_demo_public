# Danish Democracy Data Pipeline — Management Summary

## What This Project Is

This is a **demo project** that builds a data engineering pipeline with low-cost tooling. It extracts open data from two sources — the Danish Parliament (Folketing) OData entities including members of parliament, meetings, cases, and votes from the official REST API at `oda.ft.dk`, and the Rfam public MySQL database with RNA family data at EBI — and transforms it through a medallion architecture (Bronze → Silver → Gold).

The pipeline has **three independent storage switches**, each a single environment variable:

- **Bronze + DuckLake storage** (`RAW_STORAGE_TARGET`) — **local** filesystem (default, free) or **S3-compatible storage** (RustFS locally, AWS S3 in production). Controls where dlt writes extracted files and where DuckLake Parquet data lives. Independent of the other two switches.
- **Silver table storage** (`SILVER_STORAGE_FORMAT`) — native **DuckDB** tables (default) or **DuckLake** (Parquet data files + a SQL catalog, adding snapshot time-travel). Both keep full SCD Type 2 history; the choice is about *where* Silver lives, not what it contains.
- **Delta Lake export target** (`STORAGE_TARGET`) — **local** filesystem (free, fully offline), **S3-compatible storage** (RustFS locally, AWS S3 in production), or **Microsoft Fabric OneLake** (cloud, pay-per-use). The export is optional and only matters when sharing data with external tooling.

The extraction, transformation, orchestration, BI, and backup layers are all open-source and run for free. S3-compatible storage (RustFS) is also free and local. Microsoft Fabric is the only commercial component, and only when you opt into the cloud export.

---

## Architecture

```text
┌──────────────────────────────────────────────────────────────────────────┐
│                         DAGSTER ORCHESTRATOR                            │
│              (daily schedule · 06:00 Europe/Copenhagen)                 │
│                                                                         │
│  ┌─────────────┐     ┌──────────────────────┐     ┌──────────────────┐ │
│  │  EXTRACT     │     │  TRANSFORM           │     │  EXPORT          │ │
│  │  (dlt + py)  │────▶│  (dbt + DuckDB)      │────▶│  (deltalake +   │ │
│  │              │     │                      │     │   pyarrow)       │ │
│  │  Entities    │     │  SQL models          │     │  Delta Lake      │ │
│  │  from API +   │     │  + macros            │     │  tables          │ │
│  │  SQL database │     │                      │     │                 │ │
│  └──────┬───────┘     └──────────┬───────────┘     └────────┬─────────┘ │
└─────────┼────────────────────────┼──────────────────────────┼───────────┘
          │                        │                          │
          ▼                        ▼                          ▼
┌─────────────────┐   ┌───────────────────────┐   ┌───────────────────────┐
│   BRONZE        │   │   SILVER              │   │   GOLD                │
│   (NDJSON views)│   │   (SCD Type 2 tables) │   │   (Analytic views)    │
│                 │   │   CDC with hash-based  │   │   English names,      │
│   Entities      │   │   change detection     │   │   surrogate keys,     │
│   as raw JSON   │   │   Full history kept    │   │   XML parsing         │
└─────────────────┘   └───────────────────────┘   └───────────────────────┘
          │                        │                          │
          └────────────────────────┼──────────────────────────┘
                                   ▼
                    ┌──────────────────────────┐
                    │  DELTA EXPORT TARGET     │
                    │  (optional)              │
                    │                          │
                    │  A: Local disk (free)    │
                    │  B: S3 / RustFS           │
                    │  C: Microsoft Fabric     │
                    │     OneLake (ADLS)       │
                    └──────────────────────────┘
```

**Three orthogonal switches.**

> - The **Bronze and DuckLake** layers live on the local filesystem (default) or S3 — set by `RAW_STORAGE_TARGET`.
> - The **Silver** layer is stored either as native DuckDB tables (default) or as **DuckLake** (Parquet + catalog) — set by `SILVER_STORAGE_FORMAT`.
> - The optional **Delta Lake export** writes to local disk, S3, or Fabric OneLake — set by `STORAGE_TARGET`.
>
> Metabase queries DuckDB through the pipeline's own DuckDB server process and needs **no export** at all.

### Data Flow Summary

| Step | Tool | What Happens |
| --- | --- | --- |
| **Extract** | Python + dlt | Entities fetched from Danish Parliament API and tables from Rfam MySQL (each incremental or full extract). Written as timestamped JSON. Lands on local disk or S3 (per `RAW_STORAGE_TARGET`). |
| **Bronze** | dbt views | Raw JSON exposed as queryable views via DuckDB. |
| **Silver** | dbt incremental tables | SCD Type 2 history with SHA-256 hash-based change detection. Inserts, updates, and deletes tracked. Silver holds the state: each run reads only the new extraction files, and processed files are moved to an archive on demand. Stored as native DuckDB tables or DuckLake Parquet (per `SILVER_STORAGE_FORMAT`). DuckLake can live locally or on S3 (per `RAW_STORAGE_TARGET`). |
| **Gold** | dbt views | Business-friendly English names, surrogate keys (signed BIGINT for Power BI compatibility), XML biography parsing, current-version views. |
| **Export** (optional) | DuckDB `delta_scan` + deltalake | Silver and Gold written as Delta Lake tables (incremental append for Silver, overwrite for Gold). Dedup read runs inside DuckDB; write uses the `deltalake` library. Target is local disk, S3, or OneLake (per `STORAGE_TARGET`). |

---

## What It Demonstrates

- **Medallion architecture** with SCD Type 2 historical tracking across entities from multiple sources
- **Runs locally or in the cloud**: entirely on a laptop with Docker (free), connected to S3-compatible RustFS (free, local), or connected to Microsoft Fabric — same codebase
- **Switchable storage at three layers**: Bronze/DuckLake on local disk or S3 (`RAW_STORAGE_TARGET`); Silver as native DuckDB or DuckLake Parquet (`SILVER_STORAGE_FORMAT`); Delta exports to local, S3, or Fabric (`STORAGE_TARGET`)
- **Daily automation** via Dagster (two schedules, disabled by default) with run-status sensors, NDJSON run logs and ntfy.sh alerts
- **Code-generated models**: dbt SQL models generated from macros and a Python generator for consistency
- **Bundled BI layer**: Metabase connects to the DuckDB server process for ad-hoc queries and dashboards — no export required
- **Self-observability**: a dbt layer reads Dagster's own run history so the pipeline can report on its own runs
- **Backup & restore**: a DuckLake-aware backup system archives each stateful service and ships off-site to a Hetzner StorageBox
- **Cost-aware design**: built-in Fabric capacity pause/resume to reduce cloud spend; RustFS for local S3-like development

## What It Does Not Do

In production it runs on a single small server (a Hetzner CPX42), but it is a
reference project rather than an enterprise platform, and intentionally leaves out:

- **Real-time data** — batch only, daily schedule
- **Enterprise alerting** — run-status sensors write NDJSON summaries and send ntfy.sh push notifications, but there is no email, Slack, or PagerDuty integration
- **High availability** — a single server, no failover or horizontal scaling
- **Multi-environment setup** — no dev/staging/prod separation
- **Network security in depth** — access is locked down by a Hetzner firewall IP allowlist (two whitelisted IPs) and key-only SSH; there is **no TLS/reverse proxy** because there is no public endpoint
- **Fine-grained access control** — beyond storage-backend permissions (Fabric or local filesystem)

---

## Tool Costs

| Tool | License | Cost | Notes |
| --- | --- | --- | --- |
| **DuckDB** | MIT (open source) | **Free** | Local analytical database; in Docker one small DuckDB server process (quack) owns the file. No license fees. |
| **dbt-core + dbt-duckdb** | Apache 2.0 (open source) | **Free** | Transformation framework. No dbt Cloud required. |
| **dlt** | Apache 2.0 (open source) | **Free** | Data extraction library. |
| **Dagster** | Apache 2.0 (open source) | **Free** | Orchestrator. Self-hosted via Docker, no Dagster Cloud required. |
| **deltalake** | Apache 2.0 (open source) | **Free** | Delta Lake writer for Python. |
| **Docker** | Apache 2.0 | **Free** | Docker Engine (Docker Desktop may require a license for enterprises >250 employees). |
| **DuckLake** | MIT (open source) | **Free** | Optional Silver storage format (Parquet + catalog). Parquet on local disk or S3 — no service to pay for. |
| **Metabase** | AGPL / open source | **Free** | Self-hosted BI, connects to the DuckDB server process. |
| **Hetzner Cloud** | Commercial (optional) | **~€35/mo** | The production server (CPX42, 8 vCPU / 16 GB) + block volumes + StorageBox. Only if you self-host rather than run locally. |
| **Microsoft Fabric** | Commercial (optional) | **Varies** | Only needed for the OneLake export. F2 capacity starts at ~€0.26/hour (~€190/month). OneLake storage: ~€0.023/GB/month. Can be paused when idle. |
| **Azure Service Principal** | Included with Azure AD | **Free** | Required only for Fabric/OneLake authentication. |
| **Danish Parliament API** | Public / open data | **Free** | No API key required. No rate limiting documented. |

### Cost Scenarios

| Scenario | Monthly Cost |
| --- | --- |
| **Fully local** (Docker + DuckDB on a laptop) | **€0** |
| **Self-hosted** (Hetzner server + volumes + off-site backups) | **~€35** |
| **Self-hosted + Fabric OneLake** (F2, 8h/day, paused overnight) | **~€100** |
| **Self-hosted + Fabric OneLake** (F2, always on) | **~€225** |

> The `fabric_capacity_pause_resume.py` CLI (`… pause` / `… resume`) pauses or resumes the Fabric capacity. It is not wired into Dagster; schedule it (e.g. with cron) around the export window to keep costs in the lower range.

---

## Key Configuration

| Item | Details |
| --- | --- |
| Data sources | Danish Parliament API (18 OData entities) + Rfam MySQL database (7 tables); each incremental or full extract |
| Silver storage formats | DuckDB native tables (default) or DuckLake (Parquet + catalog) |
| Docker services | DuckDB server, Dagster UI, Metabase; one-off runner and backup |
| License | MIT |

> For current model and macro counts, run `find dbt/models -name '*.sql' | wc -l` and `ls dbt/macros/*.sql | wc -l`.
