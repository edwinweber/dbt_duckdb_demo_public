# Single-Server Data Engineering: A Reference Implementation

[![CI](https://github.com/edwinweber/dbt_duckdb_demo/actions/workflows/ci.yml/badge.svg)](https://github.com/edwinweber/dbt_duckdb_demo/actions/workflows/ci.yml)
[![dbt docs](https://img.shields.io/badge/dbt%20docs-lineage-FF694B)](https://edwinweber.github.io/dbt_duckdb_demo/)

![Architecture overview: the Danish Parliament OData API and the Rfam MySQL database are extracted by dlt into Bronze files, transformed by dbt in DuckDB through Bronze views, Silver history tables (DuckDB or DuckLake) and Gold star-schema views, and optionally exported as Delta Lake tables to the local filesystem, S3 or Microsoft Fabric OneLake. Dagster orchestrates; Metabase and Power BI consume.](documentation/assets/architecture-overview.svg)

**Danish Democracy Data** is a working data pipeline that runs daily on a
€35/month server, including storage and backups. It extracts open data from the
**Danish Parliament OData API** and the public **Rfam MySQL database**, keeps
the full change history in a **Bronze → Silver → Gold** medallion in DuckDB, and
can export Delta Lake tables to the local filesystem, S3 or **Microsoft Fabric
OneLake**. It is built from open-source tools (dlt, dbt, DuckDB, Dagster,
Metabase) as a reference for data engineers who want to see how these pieces fit
together on one server, without cloud lock-in.

It is deliberately single-node: no Kubernetes, no high availability. The
trade-off is simplicity and cost; the patterns still apply on a larger platform.

## The 30-second tour

| Question | Answer |
| --- | --- |
| What does it do? | Pulls 18 Danish Parliament entities and 7 Rfam tables, keeps every version in Silver (SCD Type 2), models a star schema in Gold, and optionally publishes Delta Lake tables. |
| Where is the main SQL? | Hash-based CDC in [`dbt/macros/`](dbt/macros/) ([explained](documentation/dbt_transformations.md)); star schema in [`dbt/models/gold/`](dbt/models/gold/). |
| Where is the main Python? | One pipeline engine behind both the CLI and Dagster: [`dlt_pipeline_execution_functions.py`](ddd_python/ddd_dlt/dlt_pipeline_execution_functions.py); asset factories in [`ddd_dagster/`](ddd_python/ddd_dagster/). |
| Can I see it run without credentials? | CI runs a real `dbt build` (122 models, 2 seeds, 263 data tests) on a committed 1.6 MB sample in [`tests/fixtures/bronze_sample`](tests/fixtures/bronze_sample); the resulting lineage is published as [dbt docs](https://edwinweber.github.io/dbt_duckdb_demo/). |
| How is it verified? | About 300 pytest tests with an 80% coverage floor, ruff and mypy, `dbt parse` on all three storage targets, real dbt runs of every documented Silver case, S3 tests against a real S3-compatible server, and a Docker build with a smoke test. |
| Why these trade-offs? | [design_decisions.md](documentation/design_decisions.md) lists the alternatives and what was given up. |

## Quick start (Docker)

```bash
git clone https://github.com/edwinweber/dbt_duckdb_demo.git
cd dbt_duckdb_demo
cp .env.example .env            # defaults work for a local run
docker compose build

docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data
docker compose run --rm run python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam
docker compose run --rm run python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile
docker compose up dagster       # UI at http://localhost:3000
```

The containers use host directories under `/data`; create them once as described
in [docker/DOCKER_USAGE.md](docker/DOCKER_USAGE.md#one-time-host-setup). For a
host install without Docker, see [Local setup](#local-setup-without-docker).

## Architecture

```text
 Danish Parliament OData API     Rfam MySQL (public)
              │   dlt: incremental (lookback window) or full extract
              ▼
 Bronze files  Files/Bronze/<DDD|RFAM>/<entity>/<entity>_<timestamp>.json
              │   on local disk or S3            (RAW_STORAGE_TARGET)
              ▼
 ┌────────────────────────── dbt + DuckDB ──────────────────────────┐
 │ Bronze   views: read_json_auto over the files                    │
 │ Silver   hash-based CDC → SCD Type 2 tables + _cv views          │
 │          in the .duckdb file or DuckLake (SILVER_STORAGE_FORMAT) │
 │ Gold     star schema views: actor, case, meeting, vote, …        │
 └───────────────┬───────────────────────────────┬──────────────────┘
                 │ read directly                 │ optional Delta Lake export
                 ▼                               ▼ (STORAGE_TARGET)
            Metabase                   local disk / S3 / Fabric OneLake → Power BI

 Dagster runs it all daily (06:00 Europe/Copenhagen), stopping Metabase while dbt writes.
```

The three switches are independent; [storage_combinations.md](documentation/storage_combinations.md)
shows where files land in each of the 12 combinations. OneLake is only ever an
export target: Bronze files and logs stay local (or, for Bronze, on S3).

## Main design features

- **Hash-based CDC with SCD Type 2.** Every Silver table keeps every version of
  every row. Changes are found by comparing a SHA-256 hash of the business
  columns across extraction files. Full-extract tables detect deletes on every
  run; date-filtered incremental entities detect them on a `--full-refresh`
  after a full extraction ([details](documentation/dbt_transformations.md#3-silver-cdc-into-scd-type-2)).
- **Stateless incremental extraction.** Incremental entities reload a fixed
  lookback window (31 days for the API, 365 for Rfam) instead of keeping a
  cursor. The overlap is harmless because unchanged rows hash the same, so reruns
  and failed runs need no state repair.
- **DuckDB's single writer.** dbt jobs run in one process while extraction and
  export run four-wide; Metabase is stopped before dbt writes and restarted
  afterwards, also when a run fails.
- **Switchable Silver storage.** Native DuckDB tables, or DuckLake (Parquet plus
  a catalog, locally or on S3), with no change to the models.
- **Delta export with an in-database dedup.** The Silver export anti-joins
  against the existing Delta table with DuckDB's `delta_scan`, so only new
  versions are appended.
- **Generated models.** Bronze and Silver models come from one configuration
  file, so adding an entity is a list change plus a regeneration; the Gold star
  schema is hand-written.
- **The pipeline watches itself.** A dbt layer reads Dagster's own SQLite run
  history and the step failures exported by the failure sensor, and models them
  as a small star schema for Metabase.

## Is this pattern for you?

It fits when your data fits on one server, daily batch is fast enough, your
sources give snapshots rather than change streams, and you want history and a
dimensional model as code in one repository, possibly feeding Fabric or Power
BI without ADF or Synapse. It does not fit real-time ingestion, multi-terabyte
ad-hoc queries, several concurrent writers, row-level security or high
availability. More in [design_decisions.md](documentation/design_decisions.md#the-problem-and-when-this-design-fits).

## Tech stack

| Layer | Tool |
| --- | --- |
| Language | Python ≥ 3.13 |
| Extraction | dlt ≥ 1.31 |
| Transformation | dbt-oss 2.0.5 (standalone binary) + dbt-utils 1.4.1 |
| Query engine | DuckDB ≥ 1.5.6, < 1.6 |
| Silver storage | DuckDB tables or DuckLake |
| Orchestration | Dagster ≥ 1.13.26 |
| Export | `deltalake` + PyArrow → local, S3 or Microsoft Fabric OneLake |
| BI | Metabase (reads the DuckDB file); Power BI via OneLake |
| Alerting | ntfy.sh push notifications (optional) |
| Deployment | Docker Compose on a Hetzner CPX42 |

## Data sources

- **Danish Parliament** (`https://oda.ft.dk/api`, no authentication): 18 OData
  entities. Six change often and are extracted incrementally on
  `opdateringsdato` (Aktør, Møde, Sag, Sagstrin, SagstrinAktør, Stemme); the 12
  lookup entities are extracted in full every run.
- **Rfam** (`mysql-rfam-public.ebi.ac.uk:4497`, read-only): 7 tables. `family`
  and `genome` are incremental on `updated`; the other five are extracted in
  full. Rfam is unrelated to parliament data on purpose: a SQL source with
  different keys and shapes shows that the pipeline is not tied to one source.

## Local setup (without Docker)

```bash
python3.13 -m venv .venv && source .venv/bin/activate
pip install -e ".[dagster,dev]"
bash scripts/install_dbt_oss.sh          # dbt-oss 2.0.5 → ~/.local/bin/dbt
cp .env.example .env                     # replace the /path/to/repo placeholders
set -a && source .env && set +a          # the dbt CLI reads its settings from the environment

dbt deps --project-dir dbt --profiles-dir dbt
dbt seed --project-dir dbt --profiles-dir dbt

export DAGSTER_HOME="$(pwd)/.dagster"
dagster dev -w workspace.yaml            # UI at http://localhost:3000
```

In the Dagster UI, run `danish_parliament_all_job` and `rfam_all_job` to
extract, then `full_pipeline_job` for the whole chain. Two schedules ship
disabled: the full pipeline at 06:00 and an observability refresh at 08:00
(Europe/Copenhagen). Every command-line entry point is listed in
[project_reference.md](documentation/project_reference.md#running-the-project).

## Tests

```bash
pytest tests/                          # no credentials or network needed
ruff check . && ruff format --check . && mypy --package ddd_python
```

To reproduce the CI `dbt build` on the committed sample:

```bash
export DANISH_DEMOCRACY_DATA_SOURCE=$PWD/tests/fixtures/bronze_sample/DDD
export RFAM_DATA_SOURCE=$PWD/tests/fixtures/bronze_sample/RFAM
export DUCKDB_DATABASE_LOCATION=/tmp/sample.duckdb DUCKDB_DATABASE=sample
dbt build --project-dir dbt --profiles-dir dbt --target local --exclude path:models/data_engineering
```

The strategy is in [documentation/testing.md](documentation/testing.md).

## Lessons learned

- **DuckDB allows one writer per file.** A dbt run waits for a lock while
  another process, such as Metabase, holds a connection. dbt therefore runs in
  one process, and Metabase is stopped during dbt runs.
- **A lookback window works instead of a cursor when the next layer is
  idempotent.** The hash CDC ignores the overlap, and there is no state to
  repair after a failed run.
- **Deletes need a complete snapshot.** An incremental extract cannot show a
  deletion, so a `--full-refresh` of an incremental entity must follow a full
  extraction; on a partial file it records false deletes.
- **DuckDB 1.5 reads Delta but cannot write it,** so the export reads with
  DuckDB and writes with `deltalake`.
- **DuckLake keeps very small tables inline in the catalog.** Not every table
  appears as a Parquet file on disk; this is expected.
- **Lazy configuration allows offline work.** Code generation and tests import
  the configuration without Azure credentials, because required variables are
  resolved on first use.

## Documentation

| Read this | For |
| --- | --- |
| [project_reference.md](documentation/project_reference.md) | The canonical reference: modules, dbt targets, storage switches, environment variables, conventions |
| [design_decisions.md](documentation/design_decisions.md) | Why: ten decisions with alternatives and trade-offs, dependencies, principles, known limitations |
| [dbt_transformations.md](documentation/dbt_transformations.md) | The dbt layers, every macro, the Silver CDC logic in depth, compiled SQL |
| [python_code_explained.md](documentation/python_code_explained.md) | A guided tour of every Python module |
| [storage_combinations.md](documentation/storage_combinations.md) | The three storage switches, all 12 combinations, example configurations |
| [daily_pipeline_execution.md](documentation/daily_pipeline_execution.md) | What a scheduled run does, step by step |
| [runbook.md](documentation/runbook.md) | Failures, fixes and recovery commands |
| [testing.md](documentation/testing.md) | Test strategy and CI |
| [docker/DOCKER_USAGE.md](docker/DOCKER_USAGE.md) | Running with Docker, backups and restores |
| [hetzner_infrastructure.md](documentation/hetzner_infrastructure.md) | The production server: volumes, SSH, firewall, deploys |
| [management-summary.md](documentation/management-summary.md) | A non-technical summary with costs |

## Project structure

| Path | Contents |
| --- | --- |
| `ddd_python/` | Python package: `ddd_dlt` (extraction, export), `ddd_dbt` (model generator, dbt runner), `ddd_dagster` (orchestration), `ddd_utils` (configuration, paths, backups) |
| `dbt/` | dbt project: models, macros, seeds, tests |
| `tests/` | pytest suite and the CI data sample |
| `docker/`, `docker-compose*.yml` | Images and services (`run`, `dagster`, `metabase`, `backup`; MinIO as an override) |
| `scripts/` | Operator scripts: dbt-oss install, Metabase stop/start, deploy, backup cron, CI sample |
| `documentation/` | The guides above |

## License and contact

MIT, see [LICENSE](LICENSE). Built and maintained by
[Edwin Weber](https://github.com/edwinweber). If you have a similar use case,
open an issue or get in touch through GitHub.
