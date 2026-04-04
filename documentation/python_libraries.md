# Python Libraries Used in Danish Democracy Data

This document describes every Python library declared in `pyproject.toml`, why it was chosen, and what role it plays in the pipeline.

---

## Core Dependencies

### Extraction & Loading

#### `dlt` (Data Load Tool) `>=1.24,<2`
**Role:** Incremental data extraction from the Danish Parliament OData API and the Rfam MySQL database into local files or OneLake.

**Why chosen:**
- Purpose-built for extract-load pipelines with first-class support for incremental loading, state management, and automatic schema inference.
- Handles the mechanics of pagination, retries, and checkpointing so the pipeline code stays declarative.
- Supports multiple destinations (filesystem, databases, cloud storage) via a unified interface — the same pipeline code works whether writing locally or to OneLake.
- Actively maintained with a large community; widely adopted in the modern data engineering ecosystem.

#### `requests` `>=2.33`
**Role:** HTTP client used internally by `dlt` for OData API calls, and directly in any custom API probing code.

**Why chosen:**
- The de-facto standard Python HTTP library — readable, well-documented, and supported by virtually every tutorial and reference.
- Simpler interface than `urllib` / `http.client` for REST API work; avoids the complexity of `httpx` or `aiohttp` when async is not required.

#### `python-dotenv` `>=1.0`
**Role:** Loads environment variables from a `.env` file at startup, making secrets and paths configurable without hard-coding them.

**Why chosen:**
- The standard approach for twelve-factor app configuration in Python projects.
- Zero-config: place a `.env` file in the project root and all variables are available via `os.environ` / `os.getenv`.
- Used throughout `get_variables_from_env.py` with a lazy-loading pattern so importing the module for testing never fails when credentials are absent.

---

### Azure / Fabric OneLake

#### `adlfs` `>=2026.2`
**Role:** Filesystem abstraction over Azure Data Lake Storage Gen2 (ADLS Gen2), used by `dlt` and `pyarrow` to read/write files on OneLake transparently.

**Why chosen:**
- Implements the `fsspec` interface, meaning any library that accepts a filesystem object (dlt, PyArrow, pandas) can address OneLake paths as if they were local paths.
- The canonical fsspec-compatible ADLS driver — no alternative has comparable adoption for this use case.

#### `azure-identity` `>=1.25`
**Role:** Provides credential objects (service principal, managed identity, interactive browser) for authenticating to Azure services.

**Why chosen:**
- Official Microsoft SDK; supports the full range of authentication flows with a single `ClientSecretCredential` or `DefaultAzureCredential` object.
- Integrates directly with `adlfs`, `azure-storage-file-datalake`, and the DuckDB `azure` extension.

#### `azure-storage-file-datalake` `>=12.23`
**Role:** Azure SDK client for ADLS Gen2 file-system operations — listing directories, reading file properties, and writing log files to OneLake.

**Why chosen:**
- Official Microsoft SDK; exposes `DataLakeServiceClient` and `DataLakeFileClient` for fine-grained operations (e.g., appending JSON log lines to a log file on OneLake) that are not covered by the higher-level `adlfs` fsspec layer.

---

### Transformation

#### `dbt-core` `>=1.10,<1.12`

**Role:** SQL transformation framework that runs the Bronze → Silver → Gold models, enforces tests, and manages model dependencies via a DAG.

**Why chosen:**
- The industry standard for SQL-based data transformation; widely adopted across data teams and cloud platforms.
- Supports incremental materializations, macros (Jinja templating), data tests, and documentation generation out of the box.
- Upper-bound set to `<1.12` because `dagster-dbt 0.28.x` (the Dagster integration layer) declares `dbt-core<1.12` as a hard dependency constraint.

#### `dbt-duckdb` `>=1.10,<2`

**Role:** DuckDB adapter for dbt-core — connects dbt to a local `.duckdb` file (or MotherDuck) and enables DuckDB-specific SQL features.

**Why chosen:**
- The only maintained DuckDB adapter for dbt; developed in close coordination with the DuckDB project.
- Supports the `httpfs`, `delta`, `azure`, and `parquet` DuckDB extensions that are central to reading JSON from OneLake and writing Delta tables.
- Makes it possible to run the full transformation stack locally without any cloud infrastructure.

#### `duckdb` `>=1.5.1,<1.6`
**Role:** The embedded analytical query engine that executes all SQL — Bronze views, Silver CDC tables, and Gold star-schema views.

**Why chosen:**
- Columnar, in-process OLAP database that requires zero server setup; ideal for a demo/reference project.
- Native support for reading JSON, Parquet, and Delta Lake; integrates directly with PyArrow and cloud storage via extensions.
- Exceptional performance for analytical workloads on a single machine.
- Version-pinned to `<1.6` to lock the extension ABI and avoid unexpected breaking changes in the DuckDB extension ecosystem.

---

### Export to Delta Lake

#### `deltalake` `>=1.5`
**Role:** Writes Delta Lake tables from PyArrow `RecordBatch` objects to OneLake (Silver incremental export and Gold full-overwrite export).

**Why chosen:**
- The `delta-rs`-backed Python library — a high-performance Rust implementation of the Delta Lake protocol that does not require Spark.
- Supports ACID writes, schema enforcement, and `mode="overwrite"` / `mode="append"` — exactly the semantics needed for the incremental Silver and full-refresh Gold export patterns.
- Wide adoption as the Spark-free Delta Lake writer of choice in the Python ecosystem.

#### `pyarrow` `>=17`
**Role:** In-memory columnar data format used as the interchange layer between DuckDB query results and Delta Lake writes.

**Why chosen:**
- Standard columnar memory format shared by DuckDB, `deltalake`, `adlfs`, and pandas — all four libraries speak Arrow natively, so no serialization overhead between layers.
- Required dependency of `deltalake`; using it directly gives full control over schema, partitioning, and batch size when writing Delta tables.

---

### SQL Source Pipeline

#### `sqlalchemy` `>=2.0`
**Role:** Database connection layer for the Rfam MySQL source — creates connection engines, manages connection pools, and executes parameterized SQL queries.

**Why chosen:**
- The standard Python database abstraction layer; works with any DBAPI-2 driver via a unified `create_engine()` API.
- Version 2.0 brings a cleaner, fully typed interface compared to the legacy 1.x API.
- Used here with `connect_timeout=30` and `engine.dispose()` in a `finally` block for production-safe connection handling.

#### `pymysql` `>=1.1`
**Role:** Pure-Python MySQL DBAPI-2 driver used by SQLAlchemy to connect to the public Rfam MySQL database at EBI.

**Why chosen:**
- Pure-Python implementation requires no compiled C extension, making it easy to install in any environment (including Docker on ARM).
- Widely used as the drop-in MySQL driver for SQLAlchemy when the C-based `mysqlclient` is not available or desired.

---

## Optional Dependencies

### Orchestration (`[dagster]`)

#### `dagster` `>=1.12,<2`

**Role:** Orchestration framework — defines assets, jobs, schedules, and sensors; provides the Dagster UI for monitoring pipeline runs.

**Why chosen:**
- Asset-based orchestration model maps naturally to the medallion architecture: each Bronze, Silver, and Gold model is a software-defined asset with explicit lineage.
- Built-in support for retries, partitions, run history, and alerting without external infrastructure.
- `dagster-dbt` integration represents dbt models as Dagster assets, enabling mixed Python/SQL pipelines in a single DAG.
- Lower bound set to `>=1.12` to match the lockstep versioning with `dagster-dbt 0.28.x` (0.28.x = 1.12.x by Dagster's offset convention).

#### `dagster-webserver` `>=1.12,<2`

**Role:** Serves the Dagster UI (`http://localhost:3000`) for local development and monitoring.

**Why chosen:** Ships with Dagster; the standard way to run the web UI locally.

#### `dagster-dbt` `>=0.28,<0.29`
**Role:** Dagster integration that wraps dbt models as Dagster software-defined assets, enabling dbt runs to be orchestrated alongside Python extraction and export assets.

**Why chosen:**
- Official integration maintained by the Dagster team; provides `DbtCliResource` and `@dbt_assets` decorator.
- Version-pinned to `0.28.x` (matching `dagster 1.12.x`) to ensure compatibility.

---

### Development & Testing (`[dev]`)

#### `pytest` `>=8.0`
**Role:** Test runner for all 93 tests across 12 modules (unit, integration, and end-to-end).

**Why chosen:**
- The dominant Python test framework; fixture system, parametrization, and plugin ecosystem make it suitable for both unit and integration tests.
- Version 8.x brings improved error messages and performance over the 7.x series.

#### `pandas` `>=2.0`
**Role:** Used exclusively in `test_export_silver.py` to construct test DataFrames and verify Delta Lake round-trip correctness.

**Why chosen:**
- Convenient for creating in-memory tabular data in tests; `pandas` DataFrame ↔ PyArrow Table conversion is lossless.
- Not declared as an explicit `[dev]` dependency in `pyproject.toml`; available as a transitive dependency of other packages. Used only in tests.

---

## Standard Library Modules (no install required)

| Module | Usage |
|---|---|
| `concurrent.futures` | `ThreadPoolExecutor` for parallel extraction (max 4 workers) |
| `re` | Date-parameter validation against `\d{4}-\d{2}-\d{2}` before SQL interpolation |
| `os` / `pathlib` | Path construction for local storage and dbt project directory |
| `json` | Serializing pipeline run metadata and log records |
| `warnings` | Surfacing non-fatal errors (e.g., OneLake log write failures) without swallowing them |
| `datetime` | Timestamp generation for incremental watermarks and file naming |
| `logging` | Structured logging throughout the extraction and transformation layers |
