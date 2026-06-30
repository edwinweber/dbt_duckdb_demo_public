# Python Libraries Used in Danish Democracy Data

This document describes every Python library declared in `pyproject.toml`, why it was chosen, and what role it plays in the pipeline.

---

## Core Dependencies

### Extraction & Loading

#### `dlt` (Data Load Tool) `>=1.28.1,<2`
**Role:** Incremental data extraction from the Danish Parliament OData API and the Rfam MySQL database into local files or OneLake.

**Why chosen:**
- Purpose-built for extract-load pipelines with first-class support for incremental loading, state management, and automatic schema inference.
- Handles the mechanics of pagination, retries, and checkpointing so the pipeline code stays declarative.
- Supports multiple destinations (filesystem, databases, cloud storage) via a unified interface — the same pipeline code works whether writing locally or to OneLake.
- Actively maintained with a large community; widely adopted in the modern data engineering ecosystem.

#### `requests` `>=2.34.2`
**Role:** HTTP client used internally by `dlt` for OData API calls, and directly in any custom API probing code.

**Why chosen:**
- A widely used, well-documented Python HTTP library — readable and supported by virtually every tutorial and reference.
- Simpler interface than `urllib` / `http.client` for REST API work; avoids the complexity of `httpx` or `aiohttp` when async is not required.

#### `python-dotenv` `>=1.0`
**Role:** Loads environment variables from a `.env` file at startup, making secrets and paths configurable without hard-coding them.

**Why chosen:**
- The standard approach for twelve-factor app configuration in Python projects.
- Zero-config: place a `.env` file in the project root and all variables are available via `os.environ` / `os.getenv`.
- Used throughout `get_variables_from_env.py` with a lazy-loading pattern so importing the module for testing never fails when credentials are absent.

---

### Azure / Fabric OneLake

#### `adlfs` `>=2026.5.0`
**Role:** Filesystem abstraction over Azure Data Lake Storage Gen2 (ADLS Gen2), used by `dlt` and `pyarrow` to read/write files on OneLake transparently.

**Why chosen:**
- Implements the `fsspec` interface, meaning any library that accepts a filesystem object (dlt, PyArrow, pandas) can address OneLake paths as if they were local paths.
- The canonical fsspec-compatible ADLS driver — no alternative has comparable adoption for this use case.

#### `azure-identity` `>=1.25.3`
**Role:** Provides credential objects (service principal, managed identity, interactive browser) for authenticating to Azure services.

**Why chosen:**
- Official Microsoft SDK; supports the full range of authentication flows with a single `ClientSecretCredential` or `DefaultAzureCredential` object.
- Integrates directly with `adlfs`, `azure-storage-file-datalake`, and the DuckDB `azure` extension.

#### `azure-storage-file-datalake` `>=12.25.0`
**Role:** Azure SDK client for ADLS Gen2 file-system operations — listing directories, reading file properties, and writing log files to OneLake.

**Why chosen:**
- Official Microsoft SDK; exposes `DataLakeServiceClient` and `DataLakeFileClient` for fine-grained operations (e.g., appending JSON log lines to a log file on OneLake) that are not covered by the higher-level `adlfs` fsspec layer.

---

### Transformation

#### `dbt-core` `>=1.11,<1.12`

**Role:** SQL transformation framework that runs the Bronze → Silver → Gold models, enforces tests, and manages model dependencies via a DAG.

**Why chosen:**
- A widely adopted framework for SQL-based data transformation, used across many data teams and cloud platforms.
- Supports incremental materializations, macros (Jinja templating), data tests, and documentation generation out of the box.
- Upper-bound set to `<1.12` because `dagster-dbt 0.29.x` (the Dagster integration layer) declares `dbt-core<1.12` as a hard dependency constraint.

#### `dbt-duckdb` `>=1.10,<2`

**Role:** DuckDB adapter for dbt-core — connects dbt to a local `.duckdb` file and enables DuckDB-specific SQL features.

**Why chosen:**
- The standard DuckDB adapter for dbt; developed in close coordination with the DuckDB project.
- Supports the `httpfs`, `delta`, `azure`, and `parquet` DuckDB extensions that are central to reading JSON from OneLake and writing Delta tables.
- Makes it possible to run the full transformation stack locally without any cloud infrastructure.

#### `duckdb` `>=1.5.4,<1.6`
**Role:** The embedded analytical query engine that executes all SQL — Bronze views, Silver CDC tables, and Gold star-schema views.

**Why chosen:**
- Columnar, in-process OLAP database that requires zero server setup; ideal for a demo/reference project.
- Native support for reading JSON, Parquet, and Delta Lake; integrates directly with PyArrow and cloud storage via extensions.
- Strong performance for analytical workloads on a single machine.
- Version-pinned to `<1.6` to lock the extension ABI and avoid unexpected breaking changes in the DuckDB extension ecosystem.

---

### Export to Delta Lake

#### `deltalake` `>=1.6.1`
**Role:** Writes Delta Lake tables from PyArrow `RecordBatch` objects to OneLake (Silver incremental export and Gold full-overwrite export).

**Why chosen:**
- The `delta-rs`-backed Python library — a high-performance Rust implementation of the Delta Lake protocol that does not require Spark.
- Supports ACID writes, schema enforcement, and `mode="overwrite"` / `mode="append"` — exactly the semantics needed for the incremental Silver and full-refresh Gold export patterns.
- Wide adoption as the Spark-free Delta Lake writer of choice in the Python ecosystem.

#### `pyarrow` `>=24.0.0`
**Role:** In-memory columnar data format used as the interchange layer between DuckDB query results and Delta Lake writes.

**Why chosen:**
- Standard columnar memory format shared by DuckDB, `deltalake`, `adlfs`, and pandas — all four libraries speak Arrow natively, so no serialization overhead between layers.
- Required dependency of `deltalake`; using it directly gives full control over schema, partitioning, and batch size when writing Delta tables.

---

### SQL Source Pipeline

#### `sqlalchemy` `>=2.0.51`
**Role:** Database connection layer for the Rfam MySQL source — creates connection engines, manages connection pools, and executes parameterized SQL queries.

**Why chosen:**
- The standard Python database abstraction layer; works with any DBAPI-2 driver via a unified `create_engine()` API.
- Version 2.0 brings a cleaner, fully typed interface compared to the legacy 1.x API.
- Used here with `connect_timeout=30` and `engine.dispose()` in a `finally` block for production-safe connection handling.

#### `pymysql` `>=1.2.0`
**Role:** Pure-Python MySQL DBAPI-2 driver used by SQLAlchemy to connect to the public Rfam MySQL database at EBI.

**Why chosen:**
- Pure-Python implementation requires no compiled C extension, making it easy to install in any environment (including Docker on ARM).
- Widely used as the drop-in MySQL driver for SQLAlchemy when the C-based `mysqlclient` is not available or desired.

---

## Optional Dependencies

### Orchestration (`[dagster]`)

#### `dagster` `>=1.13.11,<2`

**Role:** Orchestration framework — defines assets, jobs, schedules, and sensors; provides the Dagster UI for monitoring pipeline runs.

**Why chosen:**
- Asset-based orchestration model maps naturally to the medallion architecture: each Bronze, Silver, and Gold model is a software-defined asset with explicit lineage.
- Built-in support for retries, partitions, run history, and alerting without external infrastructure.
- `dagster-dbt` integration represents dbt models as Dagster assets, enabling mixed Python/SQL pipelines in a single DAG.
- Lower bound set to `>=1.13.11` to match the lockstep versioning with `dagster-dbt 0.29.x` (0.29.x = 1.13.x by Dagster's offset convention).

#### `dagster-webserver` `>=1.13.11,<2`

**Role:** Serves the Dagster UI (`http://localhost:3000`) for local development and monitoring.

**Why chosen:** Ships with Dagster; the standard way to run the web UI locally.

#### `dagster-dbt` `>=0.29.11,<1`
**Role:** Dagster integration that wraps dbt models as Dagster software-defined assets, enabling dbt runs to be orchestrated alongside Python extraction and export assets.

**Why chosen:**
- Official integration maintained by the Dagster team; provides `DbtCliResource` and `@dbt_assets` decorator.
- Lower bound set to `>=0.29.11` (matching `dagster 1.13.x`) to ensure compatibility.

---

### Development & Testing (`[dev]`)

#### `pytest` `>=9.1.1`
**Role:** Test runner for the test suite (unit, integration, and end-to-end).

**Why chosen:**
- The dominant Python test framework; fixture system, parametrization, and plugin ecosystem make it suitable for both unit and integration tests.
- Version 9.x brings improved error messages and performance over the 8.x series.

#### `pytest-cov` `>=5.0`
**Role:** Coverage measurement plugin for pytest — generates coverage reports to ensure test suites exercise the codebase adequately.

**Why chosen:**
- Standard tool for pytest-based projects; integrates seamlessly with the test runner and CI pipelines.

#### `ruff` `>=0.15.20`
**Role:** Fast Python linter and formatter used for code style enforcement and static analysis.

**Why chosen:**
- Single-tool replacement for flake8, isort, and other legacy linters; written in Rust for speed.
- Configured in `pyproject.toml` with rules for error detection (E/F), import sorting (I), and code simplification (SIM).

#### `mypy` `>=1.10`
**Role:** Static type checker for Python — detects type errors at development time without running code.

**Why chosen:**
- Industry-standard type checker; configured in `pyproject.toml` with `disallow_untyped_defs = true` to enforce complete type annotations on public functions.
- Prevents class of bugs related to incorrect function signatures and attribute access.

#### `types-requests`
**Role:** Type stubs for the `requests` library — provides type hints so mypy can check code that uses the requests HTTP library.

**Why chosen:**
- Required for strict mypy checking when `requests` is used; without it, mypy raises "import-untyped" errors on `import requests`.

#### `pandas` `>=3.0.3`
**Role:** Never imported directly. DuckDB's `.fetchdf()` returns a pandas `DataFrame`, which the integration tests (`test_integration_bronze.py`, `test_integration_silver_cdc.py`, `test_integration_gold.py`, `test_integration_e2e_pipeline.py`) use to read query results and assert on them (e.g. `df["col"].tolist()`).

**Why present:**
- Declared in the `[dev]` extras in `pyproject.toml` with version constraint `>=3.0.3`.
- `DataFrame` indexing is an ergonomic way to assert on DuckDB query results in tests. (The export tests, by contrast, build their fixtures with PyArrow `pa.table(...)`, not pandas.)

---

## Standard Library Modules (no install required)

| Module | Usage |
|---|---|
| `concurrent.futures` | `ThreadPoolExecutor` for parallel extraction (max 4 workers) |
| `argparse` | CLI argument parsing for the extraction, export, dbt-build, and backup/restore entry points |
| `subprocess` | Running Docker and dbt commands — Metabase start/stop, backup/restore, and `dbt build` |
| `zipfile` | Creating and verifying (and extracting, on restore) the backup zip archives |
| `re` | Date-parameter validation against `\d{4}-\d{2}-\d{2}` before SQL interpolation |
| `os` / `pathlib` | Path construction for local storage and dbt project directory |
| `json` | Serializing pipeline run metadata and log records |
| `warnings` | Surfacing non-fatal errors (e.g., OneLake log write failures) without swallowing them |
| `datetime` | Timestamp generation for incremental watermarks and file naming |
| `logging` | Structured logging throughout the extraction and transformation layers |
