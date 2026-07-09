# Storage Combinations Reference

This document traces exactly where files land for every combination of the three independent storage switches in the Danish Democracy Data project.

## Overview

The project has three orthogonal storage configuration dimensions:

1. **`RAW_STORAGE_TARGET`** (`local` | `s3`) — controls where Bronze raw files (JSON/Parquet from dlt) and DuckLake Silver Parquet data live
2. **`SILVER_STORAGE_FORMAT`** (`duckdb` | `ducklake`) — controls whether Silver tables are native DuckDB tables or DuckLake-managed Parquet
3. **`STORAGE_TARGET`** (`local` | `s3` | `onelake`) — controls Delta Lake **export** destination for Silver and Gold (does not affect Bronze, Silver table storage, or DuckDB file location)

These switches are independent: you can set `RAW_STORAGE_TARGET=s3` while `STORAGE_TARGET=onelake`, or `SILVER_STORAGE_FORMAT=ducklake` while `RAW_STORAGE_TARGET=local`. There are 2 × 2 × 3 = **12 valid combinations**.

The **DuckDB file itself (`.duckdb`)** always stays on the orchestration machine's local disk, regardless of these settings, because DuckDB enforces single-writer access. Bronze views and Gold views always live in this file. Silver tables live in it (if `SILVER_STORAGE_FORMAT=duckdb`) or in the DuckLake catalog (if `ducklake`).

---

## Quick Reference: All 12 Combinations

| # | `RAW_STORAGE_TARGET` | `SILVER_STORAGE_FORMAT` | `STORAGE_TARGET` | dbt Target | Bronze | Silver Tables | Delta Export |
|---|---|---|---|---|---|---|---|
| 1 | local | duckdb | local | `local` | `/data/Files/Bronze/...` | main `.duckdb` / `main_silver` schema | `/data/Files/{Silver,Gold}/...` |
| 2 | local | duckdb | s3 | `local` | `/data/Files/Bronze/...` | main `.duckdb` / `main_silver` schema | `s3://ddd-delta/Files/{Silver,Gold}/...` |
| 3 | local | duckdb | onelake | `onelake` | `/data/Files/Bronze/...` | main `.duckdb` / `main_silver` schema | `abfss://{ws}@onelake.../Files/{Silver,Gold}/...` |
| 4 | local | ducklake | local | `local_ducklake` | `/data/Files/Bronze/...` | `/data/ducklake/...` (Parquet) + catalog | `/data/Files/{Silver,Gold}/...` |
| 5 | local | ducklake | s3 | `local_ducklake` | `/data/Files/Bronze/...` | `/data/ducklake/...` (Parquet) + catalog | `s3://ddd-delta/Files/{Silver,Gold}/...` |
| 6 | local | ducklake | onelake | `local_ducklake` | `/data/Files/Bronze/...` | `/data/ducklake/...` (Parquet) + catalog | `abfss://{ws}@onelake.../Files/{Silver,Gold}/...` |
| 7 | s3 | duckdb | local | `local` | `s3://ddd-bronze/Files/Bronze/...` | main `.duckdb` / `main_silver` schema | `/data/Files/{Silver,Gold}/...` |
| 8 | s3 | duckdb | s3 | `local` | `s3://ddd-bronze/Files/Bronze/...` | main `.duckdb` / `main_silver` schema | `s3://ddd-delta/Files/{Silver,Gold}/...` |
| 9 | s3 | duckdb | onelake | `onelake` | `s3://ddd-bronze/Files/Bronze/...` | main `.duckdb` / `main_silver` schema | `abfss://{ws}@onelake.../Files/{Silver,Gold}/...` |
| 10 | s3 | ducklake | local | `local_ducklake_s3` | `s3://ddd-bronze/Files/Bronze/...` | `s3://ddd-ducklake/...` (Parquet) + catalog | `/data/Files/{Silver,Gold}/...` |
| 11 | s3 | ducklake | s3 | `local_ducklake_s3` | `s3://ddd-bronze/Files/Bronze/...` | `s3://ddd-ducklake/...` (Parquet) + catalog | `s3://ddd-delta/Files/{Silver,Gold}/...` |
| 12 | s3 | ducklake | onelake | `local_ducklake_s3` | `s3://ddd-bronze/Files/Bronze/...` | `s3://ddd-ducklake/...` (Parquet) + catalog | `abfss://{ws}@onelake.../Files/{Silver,Gold}/...` |

---

## Bronze Raw File Storage

Bronze raw files are extracted by dlt and stored based on `RAW_STORAGE_TARGET`.

The relative path structure is always the same: `Files/Bronze/{source_system_code}/{entity_name}/`. The absolute location (local disk vs S3) depends on the switch.

### Local (`RAW_STORAGE_TARGET=local`)

**Location:** `{LOCAL_STORAGE_PATH}/Files/Bronze/{source}/{entity}/`

**Default value of `LOCAL_STORAGE_PATH`:** `data` (from `.env`)

**Real example paths:**

```
# Danish Democracy Parliament data
/data/Files/Bronze/DDD/aktoer/
/data/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json
/data/Files/Bronze/DDD/stemme/
/data/Files/Bronze/DDD/stemme/stemme_20260102_090000.json

# Rfam data
/data/Files/Bronze/RFAM/family/
/data/Files/Bronze/RFAM/family/family_20260101_120000.parquet
/data/Files/Bronze/RFAM/genome/
/data/Files/Bronze/RFAM/genome/genome_20260102_090000.parquet
```

**Env vars:**
- `LOCAL_STORAGE_PATH`: local filesystem base path (default: `data`)
- `DANISH_DEMOCRACY_DATA_SOURCE`: must be set to `{LOCAL_STORAGE_PATH}/Files/Bronze/DDD` or a specific path; dlt reads from this location after extraction
- `RFAM_DATA_SOURCE`: must be set to `{LOCAL_STORAGE_PATH}/Files/Bronze/RFAM`

### S3-compatible Storage (`RAW_STORAGE_TARGET=s3`)

**Location:** `s3://{S3_BUCKET_BRONZE}/{S3_PREFIX_BRONZE}/Files/Bronze/{source}/{entity}/`

**Real example paths:**

```
# Danish Democracy Parliament data
s3://ddd-bronze/Files/Bronze/DDD/aktoer/
s3://ddd-bronze/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json
s3://ddd-bronze/Files/Bronze/DDD/stemme/
s3://ddd-bronze/Files/Bronze/DDD/stemme/stemme_20260102_090000.json

# If S3_PREFIX_BRONZE is set (e.g., "raw")
s3://ddd-bronze/raw/Files/Bronze/DDD/aktoer/
s3://ddd-bronze/raw/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json

# Rfam data
s3://ddd-bronze/Files/Bronze/RFAM/family/
s3://ddd-bronze/Files/Bronze/RFAM/family/family_20260101_120000.parquet
s3://ddd-bronze/Files/Bronze/RFAM/genome/
s3://ddd-bronze/Files/Bronze/RFAM/genome/genome_20260102_090000.parquet
```

**Env vars:**
- `S3_BUCKET_BRONZE`: required bucket name (e.g., `ddd-bronze`)
- `S3_PREFIX_BRONZE`: optional prefix/folder (default: empty)
- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`: shared S3 credentials
- `S3_ENDPOINT`: MinIO endpoint (e.g., `http://minio:9000`); empty for AWS S3
- `S3_URL_STYLE`: `path` for MinIO, `vhost` for AWS S3 (default: `path`)
- `S3_USE_SSL`: `false` for MinIO, `true` for AWS S3 (default: `false`)
- `DANISH_DEMOCRACY_DATA_SOURCE`: must be set to `s3://ddd-bronze/Files/Bronze/DDD` (or with prefix)
- `RFAM_DATA_SOURCE`: must be set to `s3://ddd-bronze/Files/Bronze/RFAM` (or with prefix)

---

## Silver Storage: Native DuckDB

When `SILVER_STORAGE_FORMAT=duckdb` (the default), Silver tables are created as native DuckDB tables.

**Location:** Inside the main DuckDB file at `{DUCKDB_DATABASE_LOCATION}`, schema `main_silver`

**Default value of `DUCKDB_DATABASE_LOCATION`:** `duckdb/danish_democracy_data.duckdb`

**Real example:**

```
# DuckDB file (always local, single-writer)
/data/duckdb/danish_democracy_data.duckdb

# Silver tables inside the file
SELECT * FROM danish_democracy_data.main_silver.silver_ddd_aktoer;
SELECT * FROM danish_democracy_data.main_silver.silver_ddd_stemme;
SELECT * FROM danish_democracy_data.main_silver.silver_rfam_family;

# Current-version views (expose the latest version of each row)
SELECT * FROM danish_democracy_data.main_silver.silver_ddd_aktoer_cv;
SELECT * FROM danish_democracy_data.main_silver.silver_rfam_family_cv;
```

**Env vars:**
- `DUCKDB_DATABASE_LOCATION`: path to the `.duckdb` file (always local)
- `DUCKDB_DATABASE`: database name used in queries (default: derived from filename if not set)

**dbt target:** `local` (for all `STORAGE_TARGET` values) or `onelake` (if `STORAGE_TARGET=onelake`)

---

## Silver Storage: DuckLake (Parquet + Catalog)

When `SILVER_STORAGE_FORMAT=ducklake`, Silver tables are stored as Parquet files managed by DuckLake, with a catalog metadata file.

### Local DuckLake (`SILVER_STORAGE_FORMAT=ducklake` + `RAW_STORAGE_TARGET=local`)

**Catalog file:** `{DUCKLAKE_CATALOG_LOCATION}` (always local; default: `/data/duckdb/ducklake_catalog.ducklake`)

**Data directory:** `{DUCKLAKE_DATA_PATH}` (always local; default: `/data/ducklake`)

**Real example paths:**

```
# DuckLake catalog file (metadata)
/data/duckdb/ducklake_catalog.ducklake

# DuckLake Parquet data files
/data/ducklake/__dbt_tmp/  # Live table data
/data/ducklake/<other>/    # Other internal directories

# DuckLake inline small tables (stored inside the catalog, not as separate .parquet files)
# Large tables are flushed out with CALL ducklake_flush_inlined_data(...)

# DuckDB main file (still holds Bronze views and Gold views)
/data/duckdb/danish_democracy_data.duckdb
```

**Env vars:**
- `DUCKLAKE_CATALOG_LOCATION`: required; path to the catalog `.ducklake` file
- `DUCKLAKE_DATA_PATH`: required; directory for Parquet data
- `SILVER_STORAGE_FORMAT`: must be `ducklake`

**dbt target:** `local_ducklake`

### S3-backed DuckLake (`SILVER_STORAGE_FORMAT=ducklake` + `RAW_STORAGE_TARGET=s3`)

**Catalog file:** `{DUCKLAKE_CATALOG_LOCATION}` (always local; default: `/data/duckdb/ducklake_catalog.ducklake`)

**Data directory:** Auto-derived from S3 bucket vars: `s3://{S3_BUCKET_DUCKLAKE}/{S3_PREFIX_DUCKLAKE}/`

**Real example paths:**

```
# DuckLake catalog file (always local)
/data/duckdb/ducklake_catalog.ducklake

# DuckLake Parquet data on S3 (auto-derived from bucket vars)
s3://ddd-ducklake/  (if S3_PREFIX_DUCKLAKE is empty)
s3://ddd-ducklake/__dbt_tmp/  # Live table data
s3://ddd-ducklake/<other>/    # Other internal directories

# With optional prefix
s3://ddd-ducklake/silver/
s3://ddd-ducklake/silver/__dbt_tmp/

# DuckDB main file (still holds Bronze views and Gold views, always local)
/data/duckdb/danish_democracy_data.duckdb
```

**Env vars:**
- `DUCKLAKE_CATALOG_LOCATION`: required; local path to the catalog `.ducklake` file
- `S3_BUCKET_DUCKLAKE`: required; S3 bucket for Parquet data
- `S3_PREFIX_DUCKLAKE`: optional prefix (default: empty); prepend to all S3 paths
- `DUCKLAKE_DATA_PATH`: **auto-derived**, do not set manually; computed as `s3://{S3_BUCKET_DUCKLAKE}/{S3_PREFIX_DUCKLAKE}/`
- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`: shared S3 credentials
- `S3_ENDPOINT`, `S3_URL_STYLE`, `S3_USE_SSL`: S3 connection settings (same as `RAW_STORAGE_TARGET=s3`)

**dbt target:** `local_ducklake_s3`

**Important:** When switching to S3-backed DuckLake, run:
```bash
dbt build --select tag:silver --full-refresh
```
to rebuild Silver from Bronze and materialize the Parquet files on S3. The DuckLake catalog is created automatically on first `ATTACH`.

---

## Delta Lake Export Storage

Delta Lake exports are governed solely by `STORAGE_TARGET` and are independent of `RAW_STORAGE_TARGET` and `SILVER_STORAGE_FORMAT`.

**Layer paths:** Silver exports go to `Files/Silver/`, Gold exports go to `Files/Gold/`

**Naming:** Each table gets its own subdirectory named after the table (e.g., `silver_ddd_aktoer`, `actor`)

### Local (`STORAGE_TARGET=local`)

**Location:** `{LOCAL_STORAGE_PATH}/Files/{Layer}/{table}/`

**Real example paths:**

```
# Silver exports
/data/Files/Silver/silver_ddd_aktoer/
/data/Files/Silver/silver_ddd_aktoer/_delta_log/
/data/Files/Silver/silver_ddd_aktoer/_delta_log/00000000000000000000.json
/data/Files/Silver/silver_ddd_aktoer/_delta_log/00000000000000000001.json
/data/Files/Silver/silver_ddd_aktoer/part-00000-...parquet

/data/Files/Silver/silver_rfam_family/
/data/Files/Silver/silver_rfam_family/part-00000-...parquet

# Gold exports
/data/Files/Gold/actor/
/data/Files/Gold/actor/part-00000-...parquet
/data/Files/Gold/vote/
/data/Files/Gold/vote/part-00000-...parquet
```

**Env vars:**
- `LOCAL_STORAGE_PATH`: local filesystem base path (default: `data`)
- `STORAGE_TARGET`: must be `local`

### S3 (`STORAGE_TARGET=s3`)

**Location:** `s3://{S3_BUCKET_DELTA}/{S3_PREFIX_DELTA}/Files/{Layer}/{table}/`

**Real example paths:**

```
# Silver exports
s3://ddd-delta/Files/Silver/silver_ddd_aktoer/
s3://ddd-delta/Files/Silver/silver_ddd_aktoer/_delta_log/
s3://ddd-delta/Files/Silver/silver_ddd_aktoer/_delta_log/00000000000000000000.json
s3://ddd-delta/Files/Silver/silver_ddd_aktoer/part-00000-...parquet

# With prefix
s3://ddd-delta/exports/Files/Silver/silver_ddd_aktoer/
s3://ddd-delta/exports/Files/Silver/silver_ddd_aktoer/part-00000-...parquet

# Gold exports
s3://ddd-delta/Files/Gold/actor/
s3://ddd-delta/Files/Gold/actor/part-00000-...parquet
```

**Env vars:**
- `S3_BUCKET_DELTA`: required; S3 bucket for Delta exports
- `S3_PREFIX_DELTA`: optional prefix (default: empty)
- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`: required S3 credentials
- `S3_ENDPOINT`, `S3_URL_STYLE`, `S3_USE_SSL`: S3 connection settings
- `STORAGE_TARGET`: must be `s3`

### OneLake (`STORAGE_TARGET=onelake`)

**Location:** `abfss://{FABRIC_WORKSPACE}@{FABRIC_ONELAKE_STORAGE_ACCOUNT}.dfs.fabric.microsoft.com/{folder}/{table}/`

**Real example paths:**

```
# Silver exports
abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Silver/silver_ddd_aktoer/
abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Silver/silver_ddd_aktoer/_delta_log/
abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Silver/silver_ddd_aktoer/part-00000-...parquet

# Gold exports
abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Gold/actor/
abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Gold/actor/part-00000-...parquet
```

**Env vars:**
- `FABRIC_WORKSPACE`: required; workspace name
- `FABRIC_ONELAKE_STORAGE_ACCOUNT`: required; storage account (usually `onelake`)
- `FABRIC_ONELAKE_FOLDER_SILVER`: required; folder path for Silver exports (e.g., `MyLakehouse.Lakehouse/Files/Silver`)
- `FABRIC_ONELAKE_FOLDER_GOLD`: required; folder path for Gold exports (e.g., `MyLakehouse.Lakehouse/Files/Gold`)
- `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`: Azure service principal credentials
- `STORAGE_TARGET`: must be `onelake`

---

## Complete Walkthrough: All 12 Combinations

Each section below shows the configuration and resulting file locations for one combination.

### Combination 1: `local` / `duckdb` / `local`

**Env vars:**
```bash
RAW_STORAGE_TARGET=local
SILVER_STORAGE_FORMAT=duckdb
STORAGE_TARGET=local
LOCAL_STORAGE_PATH=/data
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
```

**dbt target:** `local`

**Files:**
- **Bronze:** `/data/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **Silver tables:** Inside `/data/duckdb/danish_democracy_data.duckdb`, schema `main_silver`
- **Silver queries:** `SELECT * FROM danish_democracy_data.main_silver.silver_ddd_aktoer`
- **Delta export (Silver):** `/data/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `/data/Files/Gold/actor/`

---

### Combination 2: `local` / `duckdb` / `s3`

**Env vars:**
```bash
RAW_STORAGE_TARGET=local
SILVER_STORAGE_FORMAT=duckdb
STORAGE_TARGET=s3
LOCAL_STORAGE_PATH=/data
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
S3_BUCKET_DELTA=ddd-delta
S3_PREFIX_DELTA=
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://minio:9000
S3_URL_STYLE=path
S3_USE_SSL=false
```

**dbt target:** `local`

**Files:**
- **Bronze:** `/data/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **Silver tables:** Inside `/data/duckdb/danish_democracy_data.duckdb`, schema `main_silver`
- **Delta export (Silver):** `s3://ddd-delta/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `s3://ddd-delta/Files/Gold/actor/`

---

### Combination 3: `local` / `duckdb` / `onelake`

**Env vars:**
```bash
RAW_STORAGE_TARGET=local
SILVER_STORAGE_FORMAT=duckdb
STORAGE_TARGET=onelake
LOCAL_STORAGE_PATH=/data
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
FABRIC_WORKSPACE=my-workspace
FABRIC_ONELAKE_STORAGE_ACCOUNT=onelake
FABRIC_ONELAKE_FOLDER_SILVER=MyLakehouse.Lakehouse/Files/Silver
FABRIC_ONELAKE_FOLDER_GOLD=MyLakehouse.Lakehouse/Files/Gold
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...
```

**dbt target:** `onelake`

**Files:**
- **Bronze:** `/data/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **Silver tables:** Inside `/data/duckdb/danish_democracy_data.duckdb`, schema `main_silver`
- **Delta export (Silver):** `abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Gold/actor/`

---

### Combination 4: `local` / `ducklake` / `local`

**Env vars:**
```bash
RAW_STORAGE_TARGET=local
SILVER_STORAGE_FORMAT=ducklake
STORAGE_TARGET=local
LOCAL_STORAGE_PATH=/data
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
DUCKLAKE_CATALOG_LOCATION=/data/duckdb/ducklake_catalog.ducklake
DUCKLAKE_DATA_PATH=/data/ducklake
```

**dbt target:** `local_ducklake`

**Files:**
- **Bronze:** `/data/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **DuckLake catalog:** `/data/duckdb/ducklake_catalog.ducklake` (metadata)
- **DuckLake Parquet:** `/data/ducklake/__dbt_tmp/` (live table data)
- **Silver tables:** Accessed via `SELECT * FROM ducklake_catalog.main_silver.silver_ddd_aktoer`
- **Delta export (Silver):** `/data/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `/data/Files/Gold/actor/`

---

### Combination 5: `local` / `ducklake` / `s3`

**Env vars:**
```bash
RAW_STORAGE_TARGET=local
SILVER_STORAGE_FORMAT=ducklake
STORAGE_TARGET=s3
LOCAL_STORAGE_PATH=/data
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
DUCKLAKE_CATALOG_LOCATION=/data/duckdb/ducklake_catalog.ducklake
DUCKLAKE_DATA_PATH=/data/ducklake
S3_BUCKET_DELTA=ddd-delta
S3_PREFIX_DELTA=
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://minio:9000
S3_URL_STYLE=path
S3_USE_SSL=false
```

**dbt target:** `local_ducklake`

**Files:**
- **Bronze:** `/data/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **DuckLake catalog:** `/data/duckdb/ducklake_catalog.ducklake` (metadata)
- **DuckLake Parquet:** `/data/ducklake/__dbt_tmp/` (live table data)
- **Silver tables:** Accessed via `SELECT * FROM ducklake_catalog.main_silver.silver_ddd_aktoer`
- **Delta export (Silver):** `s3://ddd-delta/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `s3://ddd-delta/Files/Gold/actor/`

---

### Combination 6: `local` / `ducklake` / `onelake`

**Env vars:**
```bash
RAW_STORAGE_TARGET=local
SILVER_STORAGE_FORMAT=ducklake
STORAGE_TARGET=onelake
LOCAL_STORAGE_PATH=/data
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
DUCKLAKE_CATALOG_LOCATION=/data/duckdb/ducklake_catalog.ducklake
DUCKLAKE_DATA_PATH=/data/ducklake
FABRIC_WORKSPACE=my-workspace
FABRIC_ONELAKE_STORAGE_ACCOUNT=onelake
FABRIC_ONELAKE_FOLDER_SILVER=MyLakehouse.Lakehouse/Files/Silver
FABRIC_ONELAKE_FOLDER_GOLD=MyLakehouse.Lakehouse/Files/Gold
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...
```

**dbt target:** `local_ducklake`

**Files:**
- **Bronze:** `/data/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **DuckLake catalog:** `/data/duckdb/ducklake_catalog.ducklake` (metadata)
- **DuckLake Parquet:** `/data/ducklake/__dbt_tmp/` (live table data)
- **Silver tables:** Accessed via `SELECT * FROM ducklake_catalog.main_silver.silver_ddd_aktoer`
- **Delta export (Silver):** `abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Gold/actor/`

---

### Combination 7: `s3` / `duckdb` / `local`

**Env vars:**
```bash
RAW_STORAGE_TARGET=s3
SILVER_STORAGE_FORMAT=duckdb
STORAGE_TARGET=local
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
LOCAL_STORAGE_PATH=/data
S3_BUCKET_BRONZE=ddd-bronze
S3_PREFIX_BRONZE=
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://minio:9000
S3_URL_STYLE=path
S3_USE_SSL=false
S3_REGION=us-east-1
DANISH_DEMOCRACY_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/DDD
RFAM_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/RFAM
```

**dbt target:** `local`

**Files:**
- **Bronze:** `s3://ddd-bronze/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **Silver tables:** Inside `/data/duckdb/danish_democracy_data.duckdb`, schema `main_silver`
- **Delta export (Silver):** `/data/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `/data/Files/Gold/actor/`

---

### Combination 8: `s3` / `duckdb` / `s3`

**Env vars:**
```bash
RAW_STORAGE_TARGET=s3
SILVER_STORAGE_FORMAT=duckdb
STORAGE_TARGET=s3
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
S3_BUCKET_BRONZE=ddd-bronze
S3_PREFIX_BRONZE=
S3_BUCKET_DELTA=ddd-delta
S3_PREFIX_DELTA=
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://minio:9000
S3_URL_STYLE=path
S3_USE_SSL=false
S3_REGION=us-east-1
DANISH_DEMOCRACY_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/DDD
RFAM_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/RFAM
```

**dbt target:** `local`

**Files:**
- **Bronze:** `s3://ddd-bronze/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **Silver tables:** Inside `/data/duckdb/danish_democracy_data.duckdb`, schema `main_silver`
- **Delta export (Silver):** `s3://ddd-delta/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `s3://ddd-delta/Files/Gold/actor/`

---

### Combination 9: `s3` / `duckdb` / `onelake`

**Env vars:**
```bash
RAW_STORAGE_TARGET=s3
SILVER_STORAGE_FORMAT=duckdb
STORAGE_TARGET=onelake
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
S3_BUCKET_BRONZE=ddd-bronze
S3_PREFIX_BRONZE=
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://minio:9000
S3_URL_STYLE=path
S3_USE_SSL=false
S3_REGION=us-east-1
DANISH_DEMOCRACY_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/DDD
RFAM_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/RFAM
FABRIC_WORKSPACE=my-workspace
FABRIC_ONELAKE_STORAGE_ACCOUNT=onelake
FABRIC_ONELAKE_FOLDER_SILVER=MyLakehouse.Lakehouse/Files/Silver
FABRIC_ONELAKE_FOLDER_GOLD=MyLakehouse.Lakehouse/Files/Gold
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...
```

**dbt target:** `onelake`

**Files:**
- **Bronze:** `s3://ddd-bronze/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **Silver tables:** Inside `/data/duckdb/danish_democracy_data.duckdb`, schema `main_silver`
- **Delta export (Silver):** `abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Gold/actor/`

---

### Combination 10: `s3` / `ducklake` / `local`

**Env vars:**
```bash
RAW_STORAGE_TARGET=s3
SILVER_STORAGE_FORMAT=ducklake
STORAGE_TARGET=local
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
LOCAL_STORAGE_PATH=/data
DUCKLAKE_CATALOG_LOCATION=/data/duckdb/ducklake_catalog.ducklake
# DUCKLAKE_DATA_PATH is auto-derived from S3 vars (do not set manually)
S3_BUCKET_BRONZE=ddd-bronze
S3_PREFIX_BRONZE=
S3_BUCKET_DUCKLAKE=ddd-ducklake
S3_PREFIX_DUCKLAKE=
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://minio:9000
S3_URL_STYLE=path
S3_USE_SSL=false
S3_REGION=us-east-1
DANISH_DEMOCRACY_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/DDD
RFAM_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/RFAM
```

**dbt target:** `local_ducklake_s3`

**Files:**
- **Bronze:** `s3://ddd-bronze/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **DuckLake catalog:** `/data/duckdb/ducklake_catalog.ducklake` (metadata, always local)
- **DuckLake Parquet:** `s3://ddd-ducklake/__dbt_tmp/` (live table data)
- **Silver tables:** Accessed via `SELECT * FROM ducklake_catalog.main_silver.silver_ddd_aktoer`
- **Delta export (Silver):** `/data/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `/data/Files/Gold/actor/`

---

### Combination 11: `s3` / `ducklake` / `s3`

**Env vars:**
```bash
RAW_STORAGE_TARGET=s3
SILVER_STORAGE_FORMAT=ducklake
STORAGE_TARGET=s3
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
DUCKLAKE_CATALOG_LOCATION=/data/duckdb/ducklake_catalog.ducklake
# DUCKLAKE_DATA_PATH is auto-derived from S3 vars (do not set manually)
S3_BUCKET_BRONZE=ddd-bronze
S3_PREFIX_BRONZE=
S3_BUCKET_DUCKLAKE=ddd-ducklake
S3_PREFIX_DUCKLAKE=
S3_BUCKET_DELTA=ddd-delta
S3_PREFIX_DELTA=
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://minio:9000
S3_URL_STYLE=path
S3_USE_SSL=false
S3_REGION=us-east-1
DANISH_DEMOCRACY_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/DDD
RFAM_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/RFAM
```

**dbt target:** `local_ducklake_s3`

**Files:**
- **Bronze:** `s3://ddd-bronze/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **DuckLake catalog:** `/data/duckdb/ducklake_catalog.ducklake` (metadata, always local)
- **DuckLake Parquet:** `s3://ddd-ducklake/__dbt_tmp/` (live table data)
- **Silver tables:** Accessed via `SELECT * FROM ducklake_catalog.main_silver.silver_ddd_aktoer`
- **Delta export (Silver):** `s3://ddd-delta/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `s3://ddd-delta/Files/Gold/actor/`

---

### Combination 12: `s3` / `ducklake` / `onelake`

**Env vars:**
```bash
RAW_STORAGE_TARGET=s3
SILVER_STORAGE_FORMAT=ducklake
STORAGE_TARGET=onelake
DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb
DUCKDB_DATABASE=danish_democracy_data
DUCKLAKE_CATALOG_LOCATION=/data/duckdb/ducklake_catalog.ducklake
# DUCKLAKE_DATA_PATH is auto-derived from S3 vars (do not set manually)
S3_BUCKET_BRONZE=ddd-bronze
S3_PREFIX_BRONZE=
S3_BUCKET_DUCKLAKE=ddd-ducklake
S3_PREFIX_DUCKLAKE=
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://minio:9000
S3_URL_STYLE=path
S3_USE_SSL=false
S3_REGION=us-east-1
DANISH_DEMOCRACY_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/DDD
RFAM_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/RFAM
FABRIC_WORKSPACE=my-workspace
FABRIC_ONELAKE_STORAGE_ACCOUNT=onelake
FABRIC_ONELAKE_FOLDER_SILVER=MyLakehouse.Lakehouse/Files/Silver
FABRIC_ONELAKE_FOLDER_GOLD=MyLakehouse.Lakehouse/Files/Gold
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...
```

**dbt target:** `local_ducklake_s3`

**Files:**
- **Bronze:** `s3://ddd-bronze/Files/Bronze/DDD/aktoer/aktoer_20260101_120000.json`
- **DuckLake catalog:** `/data/duckdb/ducklake_catalog.ducklake` (metadata, always local)
- **DuckLake Parquet:** `s3://ddd-ducklake/__dbt_tmp/` (live table data)
- **Silver tables:** Accessed via `SELECT * FROM ducklake_catalog.main_silver.silver_ddd_aktoer`
- **Delta export (Silver):** `abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Silver/silver_ddd_aktoer/`
- **Delta export (Gold):** `abfss://my-workspace@onelake.dfs.fabric.microsoft.com/MyLakehouse.Lakehouse/Files/Gold/actor/`

---

## MinIO Local Development Setup

For local development with S3-compatible storage, use MinIO.

### Starting MinIO

```bash
docker compose -f docker-compose.yml -f docker-compose.minio.yml up minio
```

MinIO runs on:
- **S3 API:** `http://localhost:9000`
- **Console UI:** `http://localhost:9001`

### Env vars for MinIO

```bash
RAW_STORAGE_TARGET=s3
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_REGION=us-east-1
S3_USE_SSL=false
S3_URL_STYLE=path
S3_BUCKET_BRONZE=ddd-bronze
S3_BUCKET_DUCKLAKE=ddd-ducklake
S3_BUCKET_DELTA=ddd-delta
```

### Creating Buckets

**Via MinIO Console:**
1. Open `http://localhost:9001`
2. Log in with `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY`
3. Click "Object Browser" → "+" to create new buckets
4. Create: `ddd-bronze`, `ddd-ducklake` (if using DuckLake), `ddd-delta`

**Via `mc` CLI (if installed):**
```bash
mc alias set ddd http://localhost:9000 minioadmin minioadmin
mc mb ddd/ddd-bronze
mc mb ddd/ddd-ducklake    # if SILVER_STORAGE_FORMAT=ducklake
mc mb ddd/ddd-delta
```

**Buckets must exist before extraction runs.** If a bucket is missing, extraction fails when dlt tries to write Bronze files.

---

## Gotchas and Important Notes

### DuckDB is single-writer

The `.duckdb` file always stays on the orchestration machine and allows only one writer. Metabase and any other downstream connections that read from DuckDB must close during `dbt` runs. Dagster stops/starts Metabase around transformation jobs automatically.

### MinIO requires `URL_STYLE=path`

For MinIO, **always set `S3_URL_STYLE=path`**. Setting it to `vhost` causes 400 errors. AWS S3 uses `vhost`, but MinIO does not support it.

### DuckLake data path is auto-derived in S3 mode

When `RAW_STORAGE_TARGET=s3` and `SILVER_STORAGE_FORMAT=ducklake`, **do not manually set `DUCKLAKE_DATA_PATH`**. It is automatically derived from `S3_BUCKET_DUCKLAKE` and `S3_PREFIX_DUCKLAKE`:

```
DUCKLAKE_DATA_PATH = s3://{S3_BUCKET_DUCKLAKE}/{S3_PREFIX_DUCKLAKE}/
```

Any manually-set `DUCKLAKE_DATA_PATH` is silently overridden; the S3 bucket/prefix vars are the source of truth.

### DuckLake catalog always stays local

Even in S3-backed DuckLake mode, the DuckLake **catalog** (the `.ducklake` file) stays on the local orchestration machine at `DUCKLAKE_CATALOG_LOCATION`. Only the Parquet **data** files go to S3. The catalog references the data files, so it must be accessible when the DuckDB process reads Silver tables.

### Small tables are inlined in the DuckLake catalog

DuckLake stores very small tables inline inside the catalog file rather than as separate `.parquet` files on disk. To force all tables into Parquet, run:

```sql
CALL ducklake_flush_inlined_data();
```

This is a no-op when `SILVER_STORAGE_FORMAT=duckdb`.

### Log files are always local

Pipeline run logs (dlt extraction logs, dbt logs) are written to the local filesystem under `DLT_PIPELINE_RUN_LOG_DIR` regardless of `STORAGE_TARGET` or `RAW_STORAGE_TARGET`. The logs directory structure is:

```
{DLT_PIPELINE_RUN_LOG_DIR}/
  DDD/
    afstemning/
      <run_logs>
  RFAM/
    family/
      <run_logs>
```

### Bronze never exports to OneLake

Bronze raw files (extracted by dlt) can live on local disk or S3 (controlled by `RAW_STORAGE_TARGET`), but **never on OneLake**. Only Delta Lake exports (Silver and Gold) go to OneLake when `STORAGE_TARGET=onelake`.

### Switching Silver storage format requires a full refresh

When switching `SILVER_STORAGE_FORMAT` from `duckdb` to `ducklake` (or vice versa), rebuild the Silver layer from Bronze:

```bash
dbt build --select tag:silver --full-refresh
```

This does **not** migrate existing data automatically — it rebuilds Silver from the Bronze files, materializing new Parquet files if switching to DuckLake.

---

## Entity Naming

dlt filenames follow the pattern `{entity}_{YYYYMMDD_HHMMSS}.{ext}` where the entity name is the API entity name or SQL table name (Danish characters normalized: ø→oe, æ→ae, å→aa):

**Danish Democracy examples:**
- `Aktør` (Actor) → `aktoer` → `aktoer_20260101_120000.json`
- `Møde` (Meeting) → `moede` → `moede_20260102_090000.json`
- `Stemme` (Vote) → `stemme` → `stemme_20260103_140000.json`

**Rfam examples:**
- `family` → `family_20260101_120000.parquet`
- `genome` → `genome_20260102_090000.parquet`

dbt models follow `{layer}_{source}_{entity}`:
- `bronze_ddd_aktoer`, `silver_ddd_aktoer` (Bronze view, Silver table)
- `bronze_rfam_family`, `silver_rfam_family`
- Gold models use English names: `actor`, `vote`, `meeting`

---

## Example: Migrating from Local to S3

To move Bronze and Delta export to S3-compatible storage while keeping DuckDB native tables:

1. Create S3 buckets: `ddd-bronze`, `ddd-delta` (via MinIO console or `mc` CLI)
2. Update `.env`:
   ```bash
   RAW_STORAGE_TARGET=s3
   STORAGE_TARGET=s3
   S3_ENDPOINT=http://minio:9000
   S3_ACCESS_KEY_ID=minioadmin
   S3_SECRET_ACCESS_KEY=minioadmin
   S3_REGION=us-east-1
   S3_USE_SSL=false
   S3_URL_STYLE=path
   S3_BUCKET_BRONZE=ddd-bronze
   S3_BUCKET_DELTA=ddd-delta
   DANISH_DEMOCRACY_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/DDD
   RFAM_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/RFAM
   SILVER_STORAGE_FORMAT=duckdb
   ```
3. Run extraction: `python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data`
4. Run transformation: `python -m ddd_python.ddd_dbt.dbt_build_with_unique_logfile`
5. Run export: `python -m ddd_python.ddd_dlt.export_silver` and `export_gold`

Bronze files are now on S3; Silver tables remain in the local `.duckdb` file; Delta exports go to S3.

---

## Example: Moving to S3-backed DuckLake

To move Silver to S3-backed DuckLake:

1. Create S3 buckets: `ddd-bronze`, `ddd-ducklake`, `ddd-delta` (via MinIO console)
2. Update `.env`:
   ```bash
   RAW_STORAGE_TARGET=s3
   SILVER_STORAGE_FORMAT=ducklake
   STORAGE_TARGET=s3
   DUCKLAKE_CATALOG_LOCATION=/data/duckdb/ducklake_catalog.ducklake
   # DUCKLAKE_DATA_PATH is auto-derived from S3 bucket vars
   S3_ENDPOINT=http://minio:9000
   S3_ACCESS_KEY_ID=minioadmin
   S3_SECRET_ACCESS_KEY=minioadmin
   S3_BUCKET_BRONZE=ddd-bronze
   S3_BUCKET_DUCKLAKE=ddd-ducklake
   S3_BUCKET_DELTA=ddd-delta
   DANISH_DEMOCRACY_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/DDD
   RFAM_DATA_SOURCE=s3://ddd-bronze/Files/Bronze/RFAM
   ```
3. Run extraction: `python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data`
4. **Full refresh** Silver from Bronze: `dbt build --select tag:silver --full-refresh`
5. Run export: `python -m ddd_python.ddd_dlt.export_silver` and `export_gold`

Bronze files are on S3; Silver Parquet is on S3; catalog is local; Delta exports go to S3.
