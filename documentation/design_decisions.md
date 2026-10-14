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
`information_schema`, and Silver uses `on_schema_change='append_new_columns'`.
Renames, type changes and dropped columns need a deliberate change plus a
`--full-refresh` of the affected model.

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
- **Deletes of incremental entities are only detected on a `--full-refresh`.**
  A deleted record just stops appearing in date-filtered extracts. A full
  refresh compares Silver's current state with the newest Bronze file
  (`bronze_<entity>_latest`), so that file must be a **complete** snapshot:
  run a full extraction (early `date_to_load_from`) first. A periodic
  full-keyset reconciliation would close the gap; it is not implemented.
  Full-extract entities detect deletes on every run. Details:
  [dbt_transformations.md §3.4](dbt_transformations.md#34-deletes-of-incremental-entities---full-refresh-only).
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

- dbt jobs and `full_pipeline_job` use `in_process_executor`; extraction and
  export jobs run four-wide with `multiprocess_executor`. Inside
  `full_pipeline_job` the extraction steps therefore also run one by one.
- Metabase is stopped before and started after every job that writes
  (`_with_metabase_control`), and every writing asset depends on
  `stop_metabase_asset`. If a run fails, the failure sensor restarts Metabase.

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
- Switching moves no data: rebuild Silver from Bronze or, for
  `duckdb` → `ducklake`, run `migrate_silver_to_ducklake`.
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

**Context.** At the pinned DuckDB version (≥1.5.6, <1.6) the delta extension
can only read. Newer DuckDB versions can write Delta, but have an Azure/OneLake
regression.

**Alternatives.** Read the target with `DeltaTable(...).to_pyarrow_table()`
(loads the whole target into memory every run); Parquet then convert (extra I/O).

**Gained.** Only the join keys of the target are read, with projection pushdown;
the write path is a mature library.

**Given up.** Two libraries in one code path. Revisit on the next DuckDB upgrade,
once a writer-capable version is pinned **and** the Azure regression is fixed.

---

## ADR-8: One source of truth for entities

**Decision.** `ddd_python/ddd_utils/configuration_variables.py` holds every
entity list, primary key, date column and Rfam query. Model names and other
lists are derived from it with comprehensions; the model generator, Dagster
asset factories and tests all read it.

**Alternatives.** A metadata table (the extractor would need the database to know
what to extract); YAML (another format to validate); per-layer lists (drift).

**Gained.** Adding an entity is a list change plus `generate_dbt_models`.
Import-time checks (`ValueError`, so they survive `python -O`) catch a table
without a primary key or date-column entry; `test_configuration_variables.py`
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

**Gained.** Durable, shareable raw data when needed (MinIO locally, AWS S3 in
production) while the default stays offline. With DuckLake on S3,
`DUCKLAKE_DATA_PATH` is derived from the bucket variables.

**Given up.** Three switches to explain instead of one (12 combinations, see
[storage_combinations.md](storage_combinations.md)) and more variables to set. In
S3 mode the local DuckLake backup is skipped; bucket versioning is the
durability mechanism.

---

## Dependencies and why each is there

Versions are pinned as ranges in [`pyproject.toml`](../pyproject.toml).

| Package | Role here |
| --- | --- |
| `dlt` | Writes extracted records as NDJSON Bronze files to local disk or S3 (ADR-4) |
| `requests` | OData pagination loop (follows `odata.nextLink`, 30 s timeout) |
| `sqlalchemy` + `pymysql` | Rfam extraction: streamed queries with bound parameters; pure-Python driver, no C build in the image |
| `python-dotenv` | Loads `.env` before configuration is read |
| `duckdb` | The query engine (ADR-1); pinned `<1.6` for the delta extension (ADR-7) |
| `deltalake` + `pyarrow` | Writes Delta tables; Arrow is the zero-copy hand-off from DuckDB |
| `azure-identity` + `azure-storage-file-datalake` | OneLake export only: service-principal token for `deltalake`, and the Fabric capacity pause/resume CLI. Imported lazily |
| `dagster`, `dagster-webserver`, `dagster-dbt` | Orchestration and UI (`[dagster]` extra); `dagster-dbt` drives the dbt binary in CLI mode |
| `pytest`, `pytest-cov`, `ruff`, `mypy`, `types-requests`, `pandas`, `boto3` | Tests, coverage, lint/format, types (`[dev]` extra). pandas only reads query results in tests; boto3 only creates buckets in the S3 tests |

**dbt is not a Python dependency.** dbt-oss 2.0.5 is a standalone binary with
the DuckDB adapter built in, installed by `scripts/install_dbt_oss.sh` (and in
the Dockerfile) with a SHA-256 check. Installing a dbt Python package next to
`dagster-dbt` would switch it out of CLI mode.

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
- Deletes in incremental entities need a full extraction plus a
  `--full-refresh` (ADR-3).
- Bronze can only be pruned down to a complete file that is older than
  `LKHS_filename_previous` in the model's `_last_file` table. Silver compares
  each version with the key's previous version in Bronze, and a normal run
  evaluates the files from `LKHS_filename_previous` onwards again. For
  incremental entities that means a full extraction after which at least two
  newer files have been processed; full-extract entities need their last three
  processed files. Pruning more gives extra `I` rows in Silver, and a full
  refresh after pruning keeps only the history in the remaining files. Details:
  [dbt_transformations.md §3.6](dbt_transformations.md#36-never-delete-bronze-files-arbitrarily).

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
