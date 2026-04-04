# Danish Democracy Data Pipeline — Technical Summary

Last updated: April 2026

## What This Project Is

This is a **demo project** that builds a modern data engineering pipeline using low-cost tooling. It extracts open data from two sources — the Danish Parliament (Folketing) (18 OData entities including members of parliament, meetings, cases, and votes) from the official REST API at `oda.ft.dk`, and the Rfam public MySQL database (7 tables of RNA family data) at EBI — and transforms it through a medallion architecture (Bronze → Silver → Gold).

The pipeline supports two storage backends, switched via a single environment variable: **local storage** (free, fully offline — either Docker volumes or a plain local folder without Docker) or **Microsoft Fabric OneLake** (cloud, pay-per-use). The extraction, transformation, and orchestration layers are all open-source; Fabric is the only commercial component.

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
│  │  25 entities  │     │  SQL models          │     │  Delta Lake      │ │
│  │  from API +   │     │  + macros            │     │  tables          │ │
│  │  SQL database │     │                      │     │                 │ │
│  └──────┬───────┘     └──────────┬───────────┘     └────────┬─────────┘ │
└─────────┼────────────────────────┼──────────────────────────┼───────────┘
          │                        │                          │
          ▼                        ▼                          ▼
┌─────────────────┐   ┌───────────────────────┐   ┌───────────────────────┐
│   BRONZE        │   │   SILVER              │   │   GOLD                │
│   (JSON files)  │   │   (SCD Type 2 tables) │   │   (Analytic views)    │
│                 │   │   CDC with hash-based  │   │   English names,      │
│   25 entities   │   │   change detection     │   │   surrogate keys,     │
│   as raw JSON   │   │   Full history kept    │   │   XML parsing         │
└─────────────────┘   └───────────────────────┘   └───────────────────────┘
          │                        │                          │
          └────────────────────────┼──────────────────────────┘
                                   ▼
                    ┌──────────────────────────┐
                    │  STORAGE BACKEND         │
                    │                          │
                    │  Option A: Local Docker  │
                    │  volumes (free)          │
                    │                          │
                    │  Option B: Microsoft     │
                    │  Fabric OneLake (ADLS)   │
                    └──────────────────────────┘
```

### Data Flow Summary

| Step | Tool | What Happens |
| --- | --- | --- |
| **Extract** | Python + dlt | 18 entities fetched from Danish Parliament API (6 incremental, 12 full) + 7 tables from Rfam MySQL database (2 incremental, 5 full). Written as timestamped JSON. |
| **Bronze** | dbt views | Raw JSON exposed as queryable views via DuckDB. |
| **Silver** | dbt incremental tables | SCD Type 2 history with hash-based change detection. Inserts, updates, and deletes tracked. |
| **Gold** | dbt views | Business-friendly English names, surrogate keys (signed BIGINT for Power BI compatibility), XML biography parsing, current-version views. |
| **Export** | Python + deltalake | Silver and Gold tables written as Delta Lake tables (incremental append for Silver, overwrite for Gold). |

---

## What It Demonstrates

- **Medallion architecture** with full SCD Type 2 historical tracking across all 25 entities (18 DDD + 7 Rfam)
- **Runs anywhere**: entirely on a laptop with Docker (free), or connected to Microsoft Fabric — same codebase
- **Daily automation** via Dagster with health checks, sensors, and per-run log files
- **Code-generated models**: dbt SQL models generated from macros and a Python generator for consistency
- **Cost-aware design**: built-in Fabric capacity pause/resume to minimize cloud spend

## What It Does Not Do

This is a demo, not a production system. It demonstrates real-world patterns but does not include:

- **Real-time data** — batch only, daily schedule
- **Alerting** — no email, Slack, or PagerDuty notifications
- **Dashboards** — produces analytics-ready tables but no BI layer
- **Multi-environment setup** — no dev/staging/prod separation
- **Access control** — depends on the storage backend (Fabric permissions or local filesystem)

---

## Tool Costs

| Tool | License | Cost | Notes |
| --- | --- | --- | --- |
| **DuckDB** | MIT (open source) | **Free** | Local analytical database. No server, no license fees. |
| **dbt-core** | Apache 2.0 (open source) | **Free** | Transformation framework. No dbt Cloud required. |
| **dlt** | Apache 2.0 (open source) | **Free** | Data extraction library. |
| **Dagster** | Apache 2.0 (open source) | **Free** | Orchestrator. Self-hosted via Docker, no Dagster Cloud required. |
| **deltalake** | Apache 2.0 (open source) | **Free** | Delta Lake writer for Python. |
| **Docker** | Apache 2.0 | **Free** | Docker Engine (Docker Desktop may require a license for enterprises >250 employees). |
| **MotherDuck** | Commercial (optional) | **Free tier available** | Cloud-hosted DuckDB. Free tier: 10 GB storage, shared compute. Pro: ~$25/month. Not required — local DuckDB works. |
| **Microsoft Fabric** | Commercial (optional) | **Varies** | Only needed for OneLake mode. F2 capacity starts at ~€0.26/hour (~€190/month). OneLake storage: ~€0.023/GB/month. Can be paused when idle. |
| **Azure Service Principal** | Included with Azure AD | **Free** | Required only for Fabric/OneLake authentication. |
| **Danish Parliament API** | Public / open data | **Free** | No API key required. No rate limiting documented. |

### Cost Scenarios

| Scenario | Monthly Cost |
| --- | --- |
| **Fully local** (Docker + DuckDB on disk) | **€0** |
| **Local + MotherDuck Free** (cloud query sharing) | **€0** |
| **Local + MotherDuck Pro** | **~€25** |
| **Fabric OneLake** (F2, 8h/day active, paused overnight) | **~€65** |
| **Fabric OneLake** (F2, always on) | **~€190** |

> The built-in `fabric_capacity_pause_resume.py` utility can automatically pause and resume the Fabric capacity around pipeline runs, keeping costs in the lower range.

---

## Key Numbers

As of April 2026:

| Metric | Value |
| --- | --- |
| Data source entities | 25 (18 DDD + 7 Rfam) |
| DDD incremental entities | 6 (date-filtered) |
| DDD full-refresh entities | 12 |
| Rfam incremental tables | 2 (date-filtered) |
| Rfam full-extract tables | 5 |
| Docker services | 2 (runner + Dagster UI) |
| License | MIT |

> Model and macro counts change as the project evolves. Run `find dbt/models -name '*.sql' | wc -l` and `ls dbt/macros/*.sql | wc -l` for current counts.
