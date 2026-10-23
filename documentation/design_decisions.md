# Design Decisions and Trade-offs

Why the project is built the way it is: the problem it targets, the ten
architectural decisions (with the alternatives that were considered and what was
given up), the dependencies and why each is there, the engineering principles
that recur in the code, and the known limitations.

For *how* things work see [project_reference.md](project_reference.md),
[python_code_explained.md](python_code_explained.md) and
[dbt_transformations.md](dbt_transformations.md).

---

## The problem and when this design fits

The setting: a few heterogeneous sources (here a REST API and a MySQL
database), a need for full history and a clean dimensional model, a small team
and budget, and data that fits comfortably on one machine. The largest table in
this project, individual votes, has about two million rows; the whole modelled
state is a few hundred MB.

At that scale a distributed engine adds cost and failure modes without solving a
real problem. So the project runs everything on **one server**: dlt, dbt and
DuckDB, orchestrated by Dagster, for about €35/month on Hetzner, or €0 on a
laptop.

**A good fit when:**

- the active data fits on one machine (hundreds of GB, not tens of TB);
- daily batch is fast enough;
- sources are APIs or databases that give snapshots, not change streams;
- you want a medallion with CDC and SCD Type 2 history, all as code in one repo;
- you may need to feed Microsoft Fabric / Power BI, but don't want ADF or Synapse.

**Not a fit when** you need real-time ingestion, multi-terabyte ad-hoc
queries, several concurrent writers on the transformation layer, row-level
security, multi-tenancy, or high availability.

---

## ADR-1: DuckDB as the single query engine

**Decision.** DuckDB runs every transformation: Bronze views, Silver CDC tables,
Gold views and the export reads. Bronze, Gold and (by default) Silver live in one
local `.duckdb` file.

**Alternatives.** Snowflake/BigQuery (pay per query, no offline development);
Spark (cluster overhead for single-node data); MotherDuck (vendor dependency,
complicates offline use); PostgreSQL (row store, slower for analytics).

**Gained.** Runs on a laptop with no cloud setup; direct debugging (open the
file, run the query); first-class JSON, Parquet and Delta reads; no per-query
bill.

**Given up.**

- **No horizontal scale.** Growth means a bigger machine; far beyond this scale
  the platform would be replaced, not extended.
- **Single writer.** One process may write the file at a time. This shapes the
  orchestration (ADR-5): dbt runs serially and Metabase, which holds a read
  connection, is stopped during dbt runs.
- **No failover.** A corrupt file means restoring from backup.
- **DuckDB-specific SQL** (`QUALIFY`, `COLUMNS(lambda)`, `EXCLUDE`,
  `read_json_auto`) would need porting to another engine.

---

## ADR-2: Medallion with strict layer roles

**Decision.** Bronze = views over the raw files, no transformation. Silver =
append-only CDC tables with full SCD Type 2 history, plus `_cv` current-version
views; it historises but deliberately does not clean or enrich. Gold = star
schema views with English names and surrogate keys.

**Alternatives.** Two layers (loses the split between "captured change" and
"modelled for consumption"); loading straight into a model (no audit trail, no
replay after a bug fix).

**Gained.** Every layer is queryable, so a wrong Gold number can be traced to
the raw file. Silver can be rebuilt from Bronze, and Gold re-run without
touching Silver.

**Given up.** More models (generated, see ADR-8). Silver keeps every version of
every row. Gold is views, so each query recomputes its joins; fine for
dashboards, not for low-latency serving.

**Schema evolution.** New source columns flow through: `read_json_auto` with
`union_by_name` picks them up in Bronze, the hash expression is rebuilt from
the Bronze view's current columns (`DESCRIBE`), and Silver uses `on_schema_change='append_new_columns'`.
Renames and type changes need a deliberate change of the Silver table, because
Silver is the state and is not rebuilt.

---

## ADR-3: Hash-based CDC instead of log-based CDC

**Decision.** Detect changes by comparing a SHA-256 hash of each row's business
columns across successive Bronze files.

**Context.** Neither source offers a change feed: the OData API returns rows
filtered by modification date, and the public Rfam database is read-only.
Snapshot comparison is the only option.

**Alternatives.** Debezium + Kafka (needs binlog access and a Kafka cluster; not
possible on these sources); column-by-column comparison (verbose, slow); trusting
the source's update timestamp (it can be wrong, and says nothing about deletes).

**Gained.** No infrastructure beyond DuckDB; deterministic (the same files always
give the same history); overlapping extraction windows are free, because
unchanged rows hash the same.

**Given up.**

- The hash says *that* a row changed, not *which* columns changed.
- **Deletes of incremental entities are only detected at a full extraction.**
  A deleted record just stops appearing in date-filtered extracts. Full
  extractions are marked `_full` in the file name (`date_to_load_from` of
  `1900-01-01` or earlier). When a `_full` file is the only new file, every key
  that is alive in Silver and missing from it gets a `D`; a key that comes back
  later gets an `I`. A deleted key stays visible until the next full extraction,
  so run one regularly, and only a complete extraction may carry the marker.
  Full-extract entities detect deletes on every run. Details:
  [dbt_transformations.md §3.4](dbt_transformations.md#34-deletes-of-incremental-entities-_full-files).
- Tracking columns are excluded from the hash, so a re-run with no data change
  leaves no trace in Silver (intended).

---

## ADR-4: dlt for extraction, without its incremental cursor

**Decision.** dlt writes the Bronze files. The project code fetches the data
(an OData pagination loop over `requests`, streamed SQLAlchemy queries) and
yields records one at a time; dlt handles batching, schema inference and writing
NDJSON to the local filesystem or S3.

**Alternatives.** Hand-written file writing (reinvents batching and
destinations); Azure Data Factory (paid, GUI-defined, hard to version);
Airbyte/Meltano (heavier runtime for two sources).

**Incremental state: a lookback window, not a cursor.** dlt can track a
high-water mark, but this project builds the filter itself from a fixed
lookback window (31 days for DDD, 365 for Rfam, overridable per run). There is
no cursor to corrupt or rewind, a backfill is just an earlier
`date_to_load_from`, and the overlap costs nothing because of ADR-3.
`DLT_PIPELINES_DIR` holds only dlt's working directory.

**Given up.** Some data is re-extracted every day. The `file_to_file` pipeline
type is a plain byte copy behind the same dispatcher, kept for future sources
and currently unused.

---

## ADR-5: Dagster software-defined assets

**Decision.** Every extraction, dbt model and export is a Dagster asset with
declared dependencies; `dagster-dbt` turns each dbt model into an asset and maps
dbt sources onto the extraction assets, so lineage runs from API to Delta table.

**Alternatives.** Airflow (task-centric, more boilerplate, harder to test
assets in isolation); cron + scripts (no retries, history or UI); dbt Cloud
(orchestrates dbt only).

**Gained.** Lineage by construction; assets built by factories; `materialize()`
for tests; the run history in SQLite feeds the observability layer.

**Given up.** A smaller user base than Airflow; another concept set to learn.

**Consequences of the single writer (ADR-1).**

- dbt jobs use `in_process_executor`; extraction and export jobs run four-wide
  with `multiprocess_executor`; `full_pipeline_job` runs two steps at a time
  (`FULL_PIPELINE_MAX_CONCURRENT`). Asset dependencies keep the layer order, and
  with `duckdb_server` parallel dbt steps are separate quack clients.
- In Docker, Metabase and Dagster are quack clients of `duckdb_server`, which owns
  the file, so Metabase is not stopped around writing jobs (see ADR-12). Outside
  Docker only one process may write the file at a time.

---

## ADR-6: Switchable Silver storage (DuckDB or DuckLake)

**Decision.** `SILVER_STORAGE_FORMAT=duckdb` (default) keeps Silver in the main
file; `ducklake` stores it as Parquet files plus a DuckLake catalog, attached as
`ducklake_catalog`. Bronze and Gold are unaffected.

**Alternatives.** Only DuckDB (the history is locked in DuckDB's binary format);
only DuckLake (more moving parts for every user); Delta Lake for Silver (needs a
writer DuckDB does not have, see ADR-7).

**Gained.** An open, inspectable format and snapshot time travel when wanted,
with the simpler native format as the default. With `RAW_STORAGE_TARGET=s3` the
Parquet data can live in object storage.

**Given up.**

- Two code paths. Every Silver helper table must be qualified with
  `{{ this.database }}`, because DuckDB will not let one transaction write to two
  databases.
- Switching moves no data: Silver is the state, so for `duckdb` → `ducklake` run
  `migrate_silver_to_ducklake`.
- Parquet takes more space than the native format, and BI tools need the
  `ducklake` extension plus an `ATTACH` (Metabase's image has it built in).
- The catalog needs occasional maintenance (`ducklake_cleanup_job`).

---

## ADR-7: Delta export reads with DuckDB, writes with `deltalake`

**Decision.** The Silver export finds new rows inside DuckDB: an anti-join of
the Silver table against `delta_scan('<target>')` on (primary key,
`LKHS_date_valid_from`). Only those rows are written, with
`deltalake.write_deltalake`. Gold is a full overwrite and does not read the
target.

**Context.** DuckDB ≥ 2.0 can write Delta directly via its delta extension,
but there is an Azure/OneLake regression in the DuckDB delta writer; the
`deltalake` library write path works correctly for all three export destinations
(local, S3, OneLake). The split read/write approach is therefore kept.

**Alternatives.** Read the target with `DeltaTable(...).to_pyarrow_table()`
(loads the whole target into memory every run); Parquet then convert (extra I/O).

**Gained.** Only the join keys of the target are read, with projection pushdown;
the write path is a mature library.

**Given up.** Two libraries in one code path. Revisit once the Azure regression
in the DuckDB delta writer is fixed.

---

## ADR-8: One source of truth for entities

**Decision.** `ddd_python/ddd_utils/configuration_variables.py` holds every
entity list, primary key and Rfam query. Model names and other
lists are derived from it with comprehensions; the model generator, Dagster
asset factories and tests all read it.

**Alternatives.** A metadata table (the extractor would need the database to know
what to extract); YAML (another format to validate); per-layer lists (drift).

**Gained.** Adding an entity is a list change plus `generate_dbt_models`.
Import-time checks (`ValueError`, so they survive `python -O`) catch a table
without a primary key; `test_configuration_variables.py`
checks subsets and counts.

**Given up.** No adding entities at runtime. The Gold star schema is still
modelled by hand, by design.

---

## ADR-9: Lazy environment variables (PEP 562)

**Decision.** `get_variables_from_env.py` reads optional variables at import
and resolves required ones (Azure credentials, Fabric workspace) through a
module-level `__getattr__` on first access, raising `OSError` with the variable
name when one is missing. A `.pyi` stub declares the lazy names for mypy.

**Why.** Code generation, tests and local runs import the configuration module
but never need Azure. Eager loading would make every import fail without
credentials; defaulting to `None` would turn misconfiguration into silent
bugs.

**Given up.** Attribute access on a module can raise, which is less obvious to a
newcomer; the docstring and the `_LAZY_REQUIRED` map spell it out. The storage
switches themselves are validated eagerly, so a typo fails at import.

---

## ADR-10: S3-compatible raw storage, independent of the export

**Decision.** `RAW_STORAGE_TARGET=local|s3` decides where Bronze files and
DuckLake Parquet data live. It is independent of `STORAGE_TARGET`, which only
decides where the Delta export goes. The DuckDB file and the DuckLake catalog
always stay local.

**Alternatives.** Always S3 (breaks offline development); one combined switch
(cannot express "Bronze on S3, export to OneLake").

**Gained.** Durable, shareable raw data when needed (RustFS locally, AWS S3 in
production) while the default stays offline. With DuckLake on S3,
`DUCKLAKE_DATA_PATH` is derived from the bucket variables.

**Given up.** Three switches to explain instead of one (12 combinations, see
[storage_combinations.md](storage_combinations.md)) and more variables to set. In
S3 mode the local DuckLake backup is skipped; bucket versioning is the
durability mechanism.

---

## ADR-11: dbt-core + dbt-duckdb (replaced dbt-oss)

**Decision.** The project uses `dbt-core` + `dbt-duckdb` (Python packages,
pinned as ranges), replacing the earlier `dbt-oss 2.0.8`.

**Why dbt-oss was removed.** `dbt-oss` 2.0.8 bundled DuckDB 1.5.4 inside the
Rust binary. When the project upgraded to DuckDB ≥ 2.0, dbt-oss could not
connect to the quack RPC server (the protocol changed between DuckDB versions).
There is no upgrade path for dbt-oss 2.x to support a newer embedded DuckDB
without a rebuild of the Rust binary. Additionally, `dbt-oss` ships the `dbt`
Python namespace; installing it alongside `dagster-dbt` (which also pulls in
`dbt-core`) causes a `NoneType.enabled` crash in dagster-dbt's NodeSelector
(issue #34215). `dbt-core` + `dbt-duckdb` do not have this conflict.

**Alternatives.** The proprietary `dbt` distribution (same Python packages as
`dbt-core` under a different license); `dbt-oss` 2.x rebuilt against DuckDB 2.x
(not publicly available).

**Gained.** Compatible with DuckDB ≥ 2.0 and the quack server. The adapter
(`dbt-duckdb`) can be patched via `dbt_quack.py` to connect via quack RPC
instead of opening the file directly, which is how Docker mode achieves the
single-writer guarantee. The toolchain remains fully open source (Apache 2.0 /
MIT).

**Given up.** The single executable convenience of dbt-oss. The `dbt` binary is
now the dbt-core CLI entry point.

**Compatibility note.** Do not reinstall `dbt-oss` alongside `dbt-core`; the
shared `dbt` namespace conflict will crash dagster-dbt. Run `pip check` after
any dbt-related package change.

---

## ADR-12: DuckDB 2.0 and the quack single-writer server

**Decision.** The project runs DuckDB ≥ 2.0 and, in Docker mode, routes all
database access through a single long-lived `duckdb_quack_server.py` process
that owns the `.duckdb` file exclusively. All other containers (dagster, run,
metabase) connect via quack RPC (token-authenticated, TCP).

**Context.** DuckDB has a strict single-writer rule; before this change,
Metabase had to be stopped before every dbt run, with shell scripts coordinating
the Docker lifecycle. DuckDB 2.0 ships the `quack` extension which exposes a
read-write server interface. A dedicated server container that holds the one
write connection removes the stop/start cycle entirely.

**Rationale.**
- `duckdb_server` container: mounts `/data/duckdb` and `/data/dagster`; all
  other services connect via `QUACK_SERVER_HOST=duckdb_server`.
- `dbt_quack.py` monkey-patches `dbt-duckdb`'s environment class to open a
  quack connection when `QUACK_SERVER_HOST` is set, leaving the local-file code
  path unchanged for non-Docker use.
- `QUACK_SERVER_TOKEN` is required (≥ 4 chars) and SQL-escaped with
  `token.replace("'", "''")` before interpolation (quack's `CREATE SECRET` does
  not accept bound parameters).
- `dagster_step_failures_raw` switches from NDJSON files to a `sqlite_scan` of
  `$DAGSTER_HOME/history/runs/*.db`, which the server already mounts; the
  failure sensor no longer exports step-failure NDJSON files.

**Alternatives.** DuckDB WAL mode with concurrent readers (not supported; DuckDB
does not offer a native server mode before 2.0); MotherDuck (cloud dependency,
not wanted for this project).

**Gained.** No Metabase stop/start cycle; simpler Dagster job definitions;
Metabase stays connected while dbt runs. The quack adapter is activated only by
`QUACK_SERVER_HOST`, so local (non-Docker) usage is unaffected.

**Given up.** An additional container to keep running; the quack extension is
relatively new and its API may change before DuckDB 3.0. Outside Docker the
single-writer rule still applies; do not open a second read-write DuckDB
connection from Python.

---

## Dependencies and why each is there

Versions are pinned as ranges in [`pyproject.toml`](../pyproject.toml).

| Package | Role here |
| --- | --- |
| `dlt` | Writes extracted records as NDJSON Bronze files to local disk or S3 (ADR-4) |
| `requests` | OData pagination loop (follows `odata.nextLink`, 30 s timeout) |
| `sqlalchemy` + `pymysql` | Rfam extraction: streamed queries with bound parameters; pure-Python driver, no C build in the image |
| `python-dotenv` | Loads `.env` before configuration is read |
| `duckdb` | The query engine (ADR-1); pinned `>=2.0.0,<3` |
| `deltalake` + `pyarrow` | Writes Delta tables; Arrow is the zero-copy hand-off from DuckDB (ADR-7) |
| `azure-identity` + `azure-storage-file-datalake` | OneLake export only: service-principal token for `deltalake`, and the Fabric capacity pause/resume CLI. Imported lazily |
| `dagster`, `dagster-webserver`, `dagster-dbt` | Orchestration and UI (`[dagster]` extra); `dagster-dbt` drives the dbt binary in CLI mode |
| `dbt-core` + `dbt-duckdb` + `dbt-utils` | dbt transformation layer; `dbt-duckdb` is the DuckDB adapter; `dbt_quack.py` patches it for quack RPC in Docker (ADR-11, ADR-12) |
| `pytest`, `pytest-cov`, `ruff`, `mypy`, `types-requests`, `pandas`, `boto3` | Tests, coverage, lint/format, types (`[dev]` extra). pandas only reads query results in tests; boto3 only creates buckets in the S3 tests |

**dbt-core + dbt-duckdb are installed with pip.** `dbt-core` provides the `dbt`
CLI; `dbt-duckdb` provides the DuckDB adapter. `dagster-dbt` stays in CLI mode
(`DbtCliResource`). Do not reinstall `dbt-oss` alongside these packages; they
share the `dbt` Python namespace and will crash dagster-dbt (issue #34215).
Why `dbt-core` and not `dbt-oss`: ADR-11.

**Capped on purpose:** `grpcio-health-checking<1.82` in the `[dagster]` extra,
because newer versions need `protobuf>=7` while Dagster caps `protobuf<7`.

---

## Engineering principles that recur in the code

1. **One source of truth.** Entity lists, backup targets and the Danish name
   normalisation each exist once and are imported everywhere else.
2. **Generate the repetitive models, write the star schema by hand.** 50 Silver and 53
   Bronze models are one macro call each; the Gold star schema is written by
   hand.
3. **Business logic lives in SQL.** Python moves data and orchestrates; meaning
   is defined in dbt models, not in Python or in the BI tool.
4. **Deterministic and re-runnable.** Content hashes, `run_started_at` instead
   of `now()`, and `NOT EXISTS` guards make every stage safe to repeat.
5. **Fail fast and loud.** Storage switches are validated at import, dates are
   validated before they reach a query, required variables raise with their
   name, and extraction and export loops attempt every item, then raise once.
6. **Observability never breaks the run.** Every pipeline call writes an NDJSON
   log record, with secrets scrubbed; a failed log write or alert becomes a
   warning.
7. **Defence proportionate to the risk.** Bound SQL parameters, connection
   timeouts, non-root containers and a firewall allowlist; no TLS, because
   nothing is public.
8. **Test the real thing.** The CDC logic is tested by running SQL in DuckDB on
   fixture files, and CI runs a real `dbt build` on a committed sample.

---

## Known limitations and possible improvements

Deliberate limits of a single-node reference project:

- Batch only (daily); no streaming.
- One server: no high availability or failover (nightly backups instead), and
  no dev/staging/prod split.
- Access control is the firewall plus file permissions; no row-level security.
- Deletes in incremental entities are only seen at a full extraction (a `_full`
  file), so they are as late as the full extractions are rare (ADR-3).
- Silver is the state, not a function of the Bronze files: a run compares only
  the files newer than the model's bookmark with Silver's latest version of each
  key, and Silver models ignore `--full-refresh`. That keeps a run's cost
  independent of the history and lets processed Bronze files be archived
  (`bronze_archive_job`, on demand); the price is that repairs go through backups
  or a replay instead of a rebuild. Bronze plus its archive still holds every
  extract, which is what a replay reads. Because a wrong Silver run cannot be
  undone by a refresh, a run stops on a probable mass delete (the delete guard)
  and on late files, and Bronze is cast to Silver's column types. Details:
  [dbt_transformations.md §3](dbt_transformations.md#3-silver-cdc-into-scd-type-2).

Improvements worth making, roughly in order of value:

1. **Request-level retries for the OData loop.** Retries are per Dagster asset
   (two attempts, exponential back-off), so a failure on page 47 of 50 restarts
   the whole entity.
2. **Timeouts on the remaining subprocess calls.** The Metabase scripts and the
   backup `docker`/`rsync`/`ssh` calls have no `timeout=`, so a hung Docker
   daemon would block a run (the dbt call already has one).
3. **Concurrent extraction inside `full_pipeline_job`.** Its in-process executor
   also serialises extraction, which never touches the `.duckdb` file. A
   run-level concurrency limit for the dbt steps would remove that.
4. **Test the backup/restore sequence.** `backup_platform.py` is excluded from
   coverage; a test with a stubbed `subprocess.run` could assert that stopped
   containers always restart.
5. **Extend the Silver grain test to Rfam.** The singular test
   `silver_no_duplicate_id_date_valid_from` covers the 18 DDD tables only, because
   Rfam primary keys vary; `SILVER_TABLE_PRIMARY_KEYS` has the keys it would need.
6. **A lockfile** (for example `uv.lock`) for reproducible installs.
7. **Broader secret scrubbing.** `_scrub_secrets` matches four substrings in
   top-level keys only; keys like `api_key` or nested dictionaries would pass.
8. **Explicit Rfam column lists** instead of `SELECT *`, so an upstream schema
   change is noticed at the boundary.
9. **Volume and anomaly tests** (row counts within bounds, no empty daily load)
   on top of the existing correctness tests.
