# Storage Combinations

Where every file lands for each combination of the three storage switches, and
the variables each one needs.

## The three switches

| Switch | Values (default first) | Controls |
| --- | --- | --- |
| `RAW_STORAGE_TARGET` | `local` \| `s3` | Bronze extraction files, and DuckLake Parquet data |
| `SILVER_STORAGE_FORMAT` | `duckdb` \| `ducklake` | Whether Silver tables are native DuckDB tables or DuckLake (Parquet + catalog) |
| `STORAGE_TARGET` | `local` \| `s3` \| `onelake` | Destination of the optional Delta Lake **export** of Silver and Gold, nothing else |

The switches are independent, giving 2 × 2 × 3 = 12 combinations. All three are
validated when the configuration is imported, so a typo fails immediately.

Some things never move:

- The **`.duckdb` file** (Bronze views, Gold views and, in `duckdb` mode, the
  Silver tables) is always on the local disk of the machine that runs dbt.
- The **DuckLake catalog** (`.ducklake` file) is always local; only its Parquet
  data can go to S3.
- **Logs** from dlt, dbt and the Dagster sensors are always written locally
  (`DLT_PIPELINE_RUN_LOG_DIR`, `DBT_LOGS_DIRECTORY`).
- **OneLake is only an export target.** Bronze files and logs never go there.

## All 12 combinations

Paths assume `LOCAL_STORAGE_PATH=/data/local` (the Docker setting) and buckets
named `ddd-bronze`, `ddd-ducklake` and `ddd-delta`.

| # | Raw | Silver | Export | dbt target | Bronze files | Silver tables | Delta export |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | local | duckdb | local | `local` | `/data/local/Files/Bronze/…` | `.duckdb`, `main_silver` | `/data/local/Files/{Silver,Gold}/…` |
| 2 | local | duckdb | s3 | `local` | `/data/local/Files/Bronze/…` | `.duckdb`, `main_silver` | `s3://ddd-delta/Files/{Silver,Gold}/…` |
| 3 | local | duckdb | onelake | `onelake` | `/data/local/Files/Bronze/…` | `.duckdb`, `main_silver` | `abfss://<ws>@onelake…/<folder>/…` |
| 4 | local | ducklake | local | `local_ducklake` | `/data/local/Files/Bronze/…` | `/data/ducklake/` + catalog | `/data/local/Files/{Silver,Gold}/…` |
| 5 | local | ducklake | s3 | `local_ducklake` | `/data/local/Files/Bronze/…` | `/data/ducklake/` + catalog | `s3://ddd-delta/Files/{Silver,Gold}/…` |
| 6 | local | ducklake | onelake | `local_ducklake` | `/data/local/Files/Bronze/…` | `/data/ducklake/` + catalog | `abfss://<ws>@onelake…/<folder>/…` |
| 7 | s3 | duckdb | local | `local` | `s3://ddd-bronze/Files/Bronze/…` | `.duckdb`, `main_silver` | `/data/local/Files/{Silver,Gold}/…` |
| 8 | s3 | duckdb | s3 | `local` | `s3://ddd-bronze/Files/Bronze/…` | `.duckdb`, `main_silver` | `s3://ddd-delta/Files/{Silver,Gold}/…` |
| 9 | s3 | duckdb | onelake | `onelake` | `s3://ddd-bronze/Files/Bronze/…` | `.duckdb`, `main_silver` | `abfss://<ws>@onelake…/<folder>/…` |
| 10 | s3 | ducklake | local | `local_ducklake_s3` | `s3://ddd-bronze/Files/Bronze/…` | `s3://ddd-ducklake/` + catalog | `/data/local/Files/{Silver,Gold}/…` |
| 11 | s3 | ducklake | s3 | `local_ducklake_s3` | `s3://ddd-bronze/Files/Bronze/…` | `s3://ddd-ducklake/` + catalog | `s3://ddd-delta/Files/{Silver,Gold}/…` |
| 12 | s3 | ducklake | onelake | `local_ducklake_s3` | `s3://ddd-bronze/Files/Bronze/…` | `s3://ddd-ducklake/` + catalog | `abfss://<ws>@onelake…/<folder>/…` |

`dbt/profiles.yml` picks the target in this order: `ducklake` + raw `s3` →
`local_ducklake_s3`; `ducklake` → `local_ducklake`; export `onelake` →
`onelake`; otherwise `local`.

---

## Bronze files (`RAW_STORAGE_TARGET`)

dlt writes one file per extraction:
`Files/Bronze/<DDD|RFAM>/<entity>/<entity>_<YYYYMMDD_HHMMSS>.json`, NDJSON for
both sources.

| | Local | S3 |
| --- | --- | --- |
| Written to | `<LOCAL_STORAGE_PATH>/Files/Bronze/…` | `s3://<S3_BUCKET_BRONZE>/[<S3_PREFIX_BRONZE>/]Files/Bronze/…` |
| dbt reads from | `DANISH_DEMOCRACY_DATA_SOURCE`, `RFAM_DATA_SOURCE` | same variables, set to the `s3://` URIs |
| Required | `LOCAL_STORAGE_PATH` | `S3_BUCKET_BRONZE` + the shared S3 variables below |

The dbt read paths are separate variables, so they must match where dlt writes:

```bash
# local (host run)
DANISH_DEMOCRACY_DATA_SOURCE=/path/to/data/Files/Bronze/DDD
RFAM_DATA_SOURCE=/path/to/data/Files/Bronze/RFAM
# S3 (prepend S3_PREFIX_BRONZE if set)
DANISH_DEMOCRACY_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/DDD
RFAM_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/RFAM
```

In Docker, `docker-compose.yml` sets the read paths to the container mount
(`/data/local/Files/Bronze/…`); for S3 set `DANISH_DEMOCRACY_DATA_SOURCE_DOCKER`
and `RFAM_DATA_SOURCE_DOCKER` to the `s3://` URIs instead.

## Silver tables (`SILVER_STORAGE_FORMAT`)

| | `duckdb` | `ducklake` |
| --- | --- | --- |
| Tables live in | main `.duckdb` file, schema `main_silver` | `ducklake_catalog.main_silver` |
| Data files | inside the `.duckdb` file | Parquet under `DUCKLAKE_DATA_PATH` |
| Required | — | `DUCKLAKE_CATALOG_LOCATION`, plus `DUCKLAKE_DATA_PATH` (local) or `S3_BUCKET_DUCKLAKE` (S3) |

- In S3 mode `DUCKLAKE_DATA_PATH` is **derived** as
  `s3://<S3_BUCKET_DUCKLAKE>/<S3_PREFIX_DUCKLAKE>/`; a value set by hand is
  ignored.
- The catalog and data directory are created on the first `ATTACH`.
- DuckLake keeps very small tables inline in the catalog instead of as Parquet
  files, so not every table shows up on disk; `CALL ducklake_flush_inlined_data(...)`
  writes them out.
- Live table data sits in `…/__dbt_tmp/` directories (dbt's append strategy
  stages files there and the catalog references them). Never delete them;
  `ducklake_cleanup_job` deliberately leaves them alone.
- Querying Silver outside dbt in DuckLake mode needs the extension and an attach:

  ```sql
  LOAD ducklake;
  ATTACH 'ducklake:/data/duckdb/ducklake_catalog.ducklake' AS ducklake_catalog
    (READ_ONLY, DATA_PATH '/data/ducklake');
  ```

## Delta export (`STORAGE_TARGET`)

Silver is appended incrementally, Gold is overwritten; one directory per table
(`silver_ddd_aktoer`, `actor`, …).

| | Path | Required |
| --- | --- | --- |
| `local` | `<LOCAL_STORAGE_PATH>/Files/{Silver,Gold}/<table>/` | — |
| `s3` | `s3://<S3_BUCKET_DELTA>/[<S3_PREFIX_DELTA>/]Files/{Silver,Gold}/<table>/` | `S3_BUCKET_DELTA` + shared S3 variables |
| `onelake` | `abfss://<FABRIC_WORKSPACE>@<FABRIC_ONELAKE_STORAGE_ACCOUNT>.dfs.fabric.microsoft.com/<FABRIC_ONELAKE_FOLDER_SILVER or _GOLD>/<table>/` | the Fabric variables + `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` |

## Shared S3 variables

Used by both `RAW_STORAGE_TARGET=s3` and `STORAGE_TARGET=s3`.

| Variable | RustFS (local) | AWS S3 |
| --- | --- | --- |
| `S3_ENDPOINT` | `http://rustfs:9000` | empty |
| `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` | `rustfsadmin` / `rustfsadmin` (or your own) | IAM keys |
| `S3_REGION` | `us-east-1` | your region |
| `S3_USE_SSL` | `false` | `true` |
| `S3_URL_STYLE` | `path` (**required**; `vhost` gives 400 errors) | `vhost` |

---

## Example profiles

**1. Fully local (the default).** No credentials needed.

```bash
RAW_STORAGE_TARGET=local
SILVER_STORAGE_FORMAT=duckdb
STORAGE_TARGET=local
LOCAL_STORAGE_PATH=/data/local
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
```

**2. Everything on RustFS, Silver in DuckLake** (combination 11).

```bash
RAW_STORAGE_TARGET=s3
SILVER_STORAGE_FORMAT=ducklake
STORAGE_TARGET=s3
DUCKLAKE_CATALOG_LOCATION=/data/duckdb/ducklake_catalog.ducklake
S3_ENDPOINT=http://rustfs:9000
S3_ACCESS_KEY_ID=rustfsadmin
S3_SECRET_ACCESS_KEY=rustfsadmin
S3_REGION=us-east-1
S3_USE_SSL=false
S3_URL_STYLE=path
S3_BUCKET_BRONZE=ddd-bronze
S3_BUCKET_DUCKLAKE=ddd-ducklake
S3_BUCKET_DELTA=ddd-delta
DANISH_DEMOCRACY_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/DDD
RFAM_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/RFAM
```

**3. Local pipeline, export to Fabric** (combination 3).

```bash
RAW_STORAGE_TARGET=local
SILVER_STORAGE_FORMAT=duckdb
STORAGE_TARGET=onelake
FABRIC_WORKSPACE=my-workspace
FABRIC_ONELAKE_STORAGE_ACCOUNT=onelake
FABRIC_ONELAKE_FOLDER_SILVER=MyLakehouse.Lakehouse/Files/Silver
FABRIC_ONELAKE_FOLDER_GOLD=MyLakehouse.Lakehouse/Files/Gold
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...
```

A paused Fabric capacity makes the export fail with `CapacityNotActive`; resume
it with `python -m ddd_python.ddd_utils.fabric_capacity_pause_resume resume`.

---

## RustFS for local development

```bash
docker compose -f docker-compose.yml -f docker-compose.rustfs.yml up -d rustfs
# S3 API on http://localhost:9000, console on http://localhost:9001
```

Buckets must exist before the first extraction. Create them in the console
or with the `aws` CLI:

```bash
export AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin
aws --endpoint-url http://localhost:9000 s3 mb s3://ddd-bronze
aws --endpoint-url http://localhost:9000 s3 mb s3://ddd-ducklake   # only with SILVER_STORAGE_FORMAT=ducklake
aws --endpoint-url http://localhost:9000 s3 mb s3://ddd-delta      # only with STORAGE_TARGET=s3
```

On AWS: `aws s3 mb s3://ddd-bronze --region <region>` and so on.

## Changing storage later

Changing a switch moves no data.

- **Bronze to S3:** copy the existing `Files/Bronze/` tree into the bucket (for
  example `aws s3 sync` or `rclone sync`) before the next run, so Silver keeps its
  file history.
- **Silver to DuckLake:** copy the history with
  `python -m ddd_python.ddd_utils.migrate_silver_to_ducklake`. Silver is the state:
  a first build in the new location would only derive what the Bronze files still
  hold, without the deletes of incremental entities.
- **Export target:** the next export writes a fresh Delta table at the new
  location; Silver is exported in full the first time.
