# Silver Model Logic — SCD Type 2 via CDC

Last updated: March 2026

This document explains the two Silver model macros used in this project. Both implement **Slowly Changing Dimension Type 2** (SCD Type 2) by detecting Insert, Update, and Delete operations through **Change Data Capture** (CDC) logic. The key difference is how they detect **deletes**, because the two Bronze extraction patterns provide different information.

> **Critical operational rule:** Bronze files must never be deleted arbitrarily.
> The Silver CDC logic depends on an append-only Bronze file history. Deleting
> files breaks the `LAG()`/LEFT JOIN chain and causes duplicate Inserts. See
> [Never Delete Bronze Files](#warning-never-delete-bronze-files) for details
> and safe deletion rules.

## Overview

| Aspect | Full Bronze Refresh | Incremental Bronze Refresh |
| -------- | ------------------- | ------------------------- |
| Macro | `generate_model_silver_full_extraction` | `generate_model_silver_incr_extraction` |
| Bronze entities | 12 static/reference tables (aktoertype, moedestatus, periode, etc.) | 6 frequently changing tables (aktoer, moede, sag, sagstrin, sagstrinaktoer, stemme) |
| Bronze data | Every extraction contains **all** records | Each extraction contains only **changed** records (filtered by `opdateringsdato`) |
| Insert detection | Record exists in file N but not in file N-1 | First occurrence of a record (no previous hash) |
| Update detection | Hash changed between file N-1 and file N | Hash changed between consecutive files for same id |
| Delete detection | Record exists in file N but **not** in file N+1 | Only on `--full-refresh`: compare current version against latest Bronze file |

## Shared Components

### Pre-hook: `generate_pre_hook_silver`

Creates the `_last_file` bookmark table if it doesn't exist (with zero rows via `WHERE 1 = 0`). This ensures the main query can always reference it.

### Post-hook: `generate_post_hook_silver`

Drops and recreates the `_last_file` bookmark table with a single row: the **latest** bronze file and its predecessor. This bookmark tells the next run where to start processing.

```text
_last_file table:
┌─────────────────────────────────┬──────────────────────┬─────────────────────────────────┐
│          LKHS_filename          │ LKHS_date_valid_from │     LKHS_filename_previous      │
├─────────────────────────────────┼──────────────────────┼─────────────────────────────────┤
│ aktoertype_20260315_215320.json │ 2026-03-15 21:53:20  │ aktoertype_20260315_214402.json │
└─────────────────────────────────┴──────────────────────┴─────────────────────────────────┘
```

On the next run, the main query uses `LKHS_filename_previous` as the starting point — it reprocesses from the **second-to-last** file to catch any changes between the last two files that may not have been fully processed.

### CTE_BRONZE

Reads all records from the Bronze view, computes a SHA-256 hash over all business columns (excluding metadata columns), and captures the earliest `opdateringsdato` per entity as `LKHS_date_inserted_src`.

### CTE_FILES

Lists all Bronze JSON files on storage (via `read_text` glob), extracts the timestamp from each filename, and uses `LAG()`/`LEAD()` window functions to identify the previous and next file in chronological order.

### Deduplication Guard

Both macros include a `NOT EXISTS` check on incremental runs to prevent duplicate rows:

```sql
AND NOT EXISTS (
    SELECT id FROM {{ this }}
    WHERE id = CTE_ALL_ROWS.id
    AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from
)
```

### Current Version View (`_cv`)

Each silver table has a companion `_cv` view that shows only the latest version of each record:

```sql
SELECT src.*
FROM silver_<entity> src
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY src.LKHS_source_system_code, src.id
    ORDER BY src.LKHS_date_valid_from DESC
) = 1
```

---

## Full Bronze Refresh (`generate_model_silver_full_extraction`)

**Used for:** 12 entities where every Bronze extraction contains the complete dataset (e.g., aktoertype, moedestatus, periode, sagskategori).

### How It Works

Since every Bronze file is a **full snapshot**, the macro can compare consecutive snapshots to detect all three CDC operations:

#### Insert Detection

A LEFT JOIN from file N to file N-1 on `id`. If the record doesn't exist in file N-1 (`LKHS_primary_key_previous IS NULL`), it's an Insert.

#### Update Detection

Same LEFT JOIN. If the record exists in both files but the hash differs (`LKHS_hash_value != LKHS_hash_value_previous`), it's an Update.

#### Delete Detection

A LEFT JOIN from file N to file N+1 on `id`. If the record exists in file N but **not** in file N+1 (`CTE_BRONZE_INCL_LAG_NEXT.id IS NULL`), and file N+1 exists (`LKHS_filename_next IS NOT NULL`), it's a Delete. The delete record is timestamped with the **next** file's date.

### Example: aktoertype (13 actor types)

**Initial load** — first file `aktoertype_20260311_114219.json`:

```text
┌────┬───────────────┬────────────────────┐
│ id │ type          │ LKHS_cdc_operation │
├────┼───────────────┼────────────────────┤
│  1 │ Ministerområde│ I                  │  ← No previous file → Insert
│  2 │ Ministertitel │ I                  │
│ ...│ ...           │ ...                │
│ 13 │ Tværpol. netv.│ I                  │
└────┴───────────────┴────────────────────┘
```

**Subsequent load** — files `_20260314_181343.json` and `_20260315_214402.json` contain the same 13 records with identical hashes → **no new rows appended** (no I, U, or D detected).

**Hypothetical delete** — if id=13 disappeared from file N+1:

```text
File N:   ids [1,2,...,12,13]
File N+1: ids [1,2,...,12]      ← id=13 missing

Result: a row for id=13 with LKHS_cdc_operation='D',
        timestamped with file N+1's date
```

### Data Flow Diagram

```text
Bronze file N-1    Bronze file N    Bronze file N+1
   (previous)         (current)        (next)
       │                  │                │
       └────LEFT JOIN─────┤                │
            on id         │                │
                          │                │
        ┌─────────────────┘                │
        │                                  │
   If id NOT in N-1 → INSERT               │
   If id in N-1 but hash differs → UPDATE   │
        │                                  │
        └──────────LEFT JOIN───────────────┘
                   on id

   If id NOT in N+1 → DELETE (timestamped with N+1 date)
```

---

## Incremental Bronze Refresh (`generate_model_silver_incr_extraction`)

**Used for:** 6 entities where bronze extractions contain only **changed** records since the last extraction (e.g., aktoer, moede, sag, sagstrin, sagstrinaktoer, stemme).

### How It Works — Incremental

Since each Bronze file only contains records that were modified, comparing consecutive files by LEFT JOIN (as in the full macro) would incorrectly flag all records in file N as "new" — because most of them won't exist in file N-1 (they simply weren't modified then). Instead, this macro uses `LAG()` over the hash **partitioned by id** across all files.

#### Insert Detection — Incremental

`LAG(LKHS_hash_value) OVER (PARTITION BY id ORDER BY LKHS_filename)` returns NULL for the first occurrence of any id → Insert.

#### Update Detection — Incremental

Same `LAG()`. If the hash differs from the previous hash for the same id → Update.

#### Delete Detection — Only on `--full-refresh`

Incremental bronze files **cannot** detect deletes during normal runs — if a record is deleted at the source, it simply stops appearing in the incremental extractions. There's no "absence" signal.

Deletes are only detected during a `--full-refresh` run, which triggers the following logic:

> **Prerequisite:** The latest Bronze file **must contain all rows currently present
> in the source system** — not just the rows that changed. Delete detection works by
> comparing the current Silver state against this latest Bronze file. If the file only
> contains a partial extract (e.g., an incremental snapshot), any record missing from
> it will be incorrectly flagged as deleted. Before running `--full-refresh` on an
> incremental entity, always run a **full extraction** first to ensure the latest
> Bronze file is a complete snapshot.

1. **Pre-hook** (`generate_pre_hook_silver_full_refresh`): Before the table is rebuilt, the current Silver table is saved to a temp table (`_current_temp`), keeping only the latest version of each record.

2. **Main query** (full-refresh branch): After computing all Inserts and Updates from the full Bronze history, it adds two UNION ALL branches:
   - **Carry forward existing deletes**: Any rows in `_current_temp` that already have `LKHS_cdc_operation = 'D'` are preserved.
   - **Detect new deletes**: Records in `_current_temp` (non-deleted) that do NOT exist in `bronze_<entity>_latest` are marked as Delete, timestamped with the latest file's date.

3. **Post-hook**: The `_current_temp` table is dropped.

### Example: aktoer (actors/persons/organizations)

**Normal incremental run** — new file `aktoer_20260315_214102.json` contains 2,032 changed records:

```text
For each record in the file:
- LAG(hash) IS NULL for this id → first time we see it → INSERT
- LAG(hash) differs → data changed → UPDATE
- LAG(hash) is the same → no change → filtered out (not I or U)
```

**Result:** only genuinely new or changed records are appended. Records that appeared in previous files with the same hash are ignored.

**Full-refresh delete detection:**

```text
1. Pre-hook saves current silver to _current_temp (latest version per id)

2. Main query:
   _current_temp (latest versions)     bronze_aktoer_latest (newest file)
        │                                       │
        └──────────LEFT JOIN on id──────────────┘
                          │
        If id NOT in bronze_latest → DELETE
        (record was removed from source)

3. Post-hook drops _current_temp
```

### Example Timeline for aktoer id=10 (Grønlandsudvalget)

```text
┌─────────────────────────────────┬──────────────────────────────────┬────────────────────┐
│          LKHS_filename          │         opdateringsdato          │ LKHS_cdc_operation │
├─────────────────────────────────┼──────────────────────────────────┼────────────────────┤
│ aktoer_20260311_114219.json     │ 2026-03-10T19:18:41.990000+00:00 │ I                  │
│ aktoer_20260315_214102.json     │ 2026-03-13T19:19:25.440000+00:00 │ U                  │
└─────────────────────────────────┴──────────────────────────────────┴────────────────────┘

Row 1: First appearance → INSERT
Row 2: opdateringsdato changed → hash differs → UPDATE
        Both versions kept (SCD Type 2)
```

---

## Summary: Why Delete Logic Differs

| | Full Bronze Refresh | Incremental Bronze Refresh |
| --- | --- | --- |
| **Can detect deletes on normal run?** | Yes — compare file N to file N+1 | No — absence is invisible |
| **Delete mechanism** | LEFT JOIN current file → next file; missing id = Delete | LEFT JOIN `_current_temp` → `bronze_latest`; missing id = Delete |
| **When deletes are detected** | Every run (between consecutive file pairs) | Only on `--full-refresh` |
| **Delete timestamp** | File N+1's date | Latest file's date |

The fundamental reason: **full extractions** always contain every record, so a missing record in the next file is a reliable delete signal. **Incremental extractions** only contain changed records, so a record missing from the next file simply means it wasn't modified — not that it was deleted. The only way to detect deletes in incremental entities is to compare the current Silver state against a full snapshot (the latest Bronze file, which does contain all records from the source API).

---

## Bookmark & Idempotency

The `_last_file` bookmark table ensures:

1. **Efficiency**: Only files from `LKHS_filename_previous` onward are processed, avoiding reprocessing the entire history.
2. **Correctness**: Reprocessing starts from the second-to-last file (not the last) to catch cross-file changes.
3. **Idempotency**: The `NOT EXISTS` guard prevents duplicate rows even if the same files are processed multiple times.

### Bronze Models Are Views — No Separate Refresh Needed

The bronze models (`bronze_<entity>` and `bronze_<entity>_latest`) are materialized as **views**, not tables. They read directly from the JSON files on OneLake via `read_json_auto()` at query time. This means:

- Bronze views always reflect the **current state** of files on storage — there is no cached/stale data.
- Running `dbt run --select bronze_*` only (re)creates the view definitions, it does not load data.
- When the Silver model runs, it queries the Bronze view, which in turn reads all JSON files live from OneLake.

Because they are views, new files added by ingestion are automatically visible to the Silver models without re-running bronze. However, the Bronze models are still included in the production job because the view definitions themselves could change (e.g., if the generation macro is modified). Re-running bronze ensures the latest view definition is always deployed before silver processes the data.

### Batch vs. Sequential Processing

You can run extractions and silver refreshes in any combination — **the historized silver table will always end up with the same content**.

| Approach | Steps | Final silver result |
| ---------- | ----- | ------------------- |
| **Sequential (run silver after each extraction)** | Extract → Silver, Extract → Silver, Extract → Silver | Same |
| **Batch (run silver once after all extractions)** | Extract, Extract, Extract → Silver once | Same |
| **Mixed** | Extract, Extract → Silver, Extract → Silver | Same |

This is a deliberate design property. The pipeline never requires you to silver-refresh after every single extraction. You can let extractions accumulate across an entire day and process them all in one silver run at the end — the result in the historized table is identical to having processed each file immediately after it was extracted.

#### Why This Works — Full Bronze Refresh Entities

For the 12 full-snapshot entities (aktoertype, moedestatus, etc.), the macro compares **every consecutive file pair** (F_N−1 → F_N) using a LEFT JOIN in `CTE_BRONZE_INCL_LAG`. Each pair is evaluated independently and self-contained — it does not matter whether those pairs were created hours or days apart, or how many files accumulated before silver ran.

The `_last_file` bookmark tells the next incremental silver run where to start (from `LKHS_filename_previous`). All file pairs from that point onward are processed in a single pass. Any pairs that were already processed on a prior run are skipped by the `NOT EXISTS` deduplication guard.

**Example:** 4 extractions ran today, silver ran once at end of day

```text
Files on storage:
  F1 (08:00)   F2 (12:00)   F3 (16:00)   F4 (20:00)

Silver runs once — processes all consecutive pairs:
  F1 vs NULL  →  Insert all ids in F1
  F2 vs F1    →  Insert/Update records that changed; Delete records missing from F2
  F3 vs F2    →  Insert/Update/Delete between F2 and F3
  F4 vs F3    →  Insert/Update/Delete between F3 and F4

_last_file bookmark now points to: LKHS_filename = F4, LKHS_filename_previous = F3
```

This is **identical** to running silver after each extraction:

```text
Silver run 1 (after F1): F1 vs NULL → I for all ids
Silver run 2 (after F2): F2 vs F1   → I/U/D
Silver run 3 (after F3): F3 vs F2   → I/U/D
Silver run 4 (after F4): F4 vs F3   → I/U/D
```

Each file-pair transition is captured exactly once, in order, regardless of when silver runs.

#### Why This Works — Incremental Bronze Refresh Entities

For the 6 incremental entities (aktoer, moede, sag, etc.), the macro uses `LAG(LKHS_hash_value) OVER (PARTITION BY id ORDER BY LKHS_filename)` to detect changes across all files simultaneously. Since the `LAG()` window function operates over the full file history in a single pass, it naturally processes all accumulated files — whether there are 2 or 20 of them — and produces the same I/U rows as if each file had been processed one at a time.

**Example:** id=42 changes in F2, again in F4, and is unchanged in F3

```text
Batch (silver runs once after F1–F4):
  id=42 in F1: LAG=NULL        → INSERT
  id=42 in F2: LAG=hash_F1     → UPDATE (hash changed)
  id=42 in F3: LAG=hash_F2     → no row (same hash, filtered out)
  id=42 in F4: LAG=hash_F2     → UPDATE (hash changed again)
  → 3 rows in silver: [I, U, U]

Sequential (silver after each file):
  Silver 1: F1 — id=42: LAG=NULL        → INSERT   (1 row)
  Silver 2: F2 — id=42: LAG=hash_F1     → UPDATE   (1 row)
  Silver 3: F3 — id=42: LAG=hash_F2     → no change (0 rows)
  Silver 4: F4 — id=42: LAG=hash_F2     → UPDATE   (1 row)
  → 3 rows in silver: [I, U, U]
```

The `NOT EXISTS` deduplication guard ensures that on each incremental silver run, rows already present in the silver table (from prior runs) are never re-inserted — even if the `LAG()` logic would re-derive them from the file history.

#### The Two Safeguards That Make This Work

1. **`_last_file` bookmark** — On each incremental run, the Silver query only processes files from `LKHS_filename_previous` onward (it reprocesses the second-to-last already-processed file to ensure no cross-file transitions are missed). This makes sequential runs efficient: only new files are touched, not the full history.

2. **`NOT EXISTS` deduplication guard** — Prevents any row (identified by `id` + `LKHS_date_valid_from`) from being inserted twice, even if the same file pair is re-evaluated. This makes the Silver refresh **idempotent**: running it twice in a row produces no additional rows.

Together these two mechanisms guarantee that regardless of how you interleave extractions and silver refreshes, every file transition is captured exactly once and in the correct chronological order.

### WARNING: Never Delete Bronze Files

The silver CDC logic assumes the Bronze file history is **append-only**. Deleting extracted files from OneLake breaks the `LAG()`/LEFT JOIN chain and causes **duplicate Inserts** in silver.

**Why it happens:** The `CTE_FILES` CTE builds the file timeline using `LAG()` and `LEAD()` window functions over all files currently on storage. When a file is removed, the chain is recalculated — a file that previously had a predecessor now has `LAG() = NULL`, making every record in that file look like a first-time Insert.

**Example:** 5 files exist on storage, and you delete F2 and F3:

```text
Before deletion — file chain:
F1 (prev=NULL) → F2 (prev=F1) → F3 (prev=F2) → F4 (prev=F3) → F5 (prev=F4)

After deleting F2 and F3 — file chain recalculates:
F1 (prev=NULL) → F4 (prev=F1) → F5 (prev=F4)
```

The comparison for F4 is now against F1 instead of F3. Any record that was inserted or updated in F2 or F3 but remained unchanged in F4 will appear as if nothing happened (compared against F1). Worse, any record that existed in F3 but not F1 will now be flagged as a **new Insert** in F4, creating a duplicate row in silver.

```text
Before deletion — silver for id=7:
F1: not present
F2: Insert (id=7 appears for the first time)
F3: no change (same hash)
F4: Update (hash changed)
→ Silver has 2 rows: [I, U]

After deleting F2, F3 — silver for id=7:
F1: not present
F4: Insert (id=7 not in F1 → treated as new!)   ← DUPLICATE INSERT
→ Silver now has 3 rows: [I, U, I]  ← WRONG
```

**Safe deletion rule:** You can safely delete bronze files that are **older than** the `LKHS_filename_previous` value in the `_last_file` table. These files will never be reprocessed on an incremental run. Keep all files from `LKHS_filename_previous` onward — there may be multiple unprocessed files if several ingestions ran before the last silver refresh.

**WARNING: Silver becomes the only full history.** Once you delete old bronze files, the Silver tables are the **sole record** of all historical changes (Inserts, Updates, Deletes) that occurred in those files. If the Silver tables are lost or corrupted after bronze files have been deleted, that history cannot be reconstructed. Treat silver tables with extra care — ensure they are backed up or replicated before deleting any bronze files.

If you need a `--full-refresh` after deleting old files, this still works correctly:

1. Run a fresh full extraction first (creates a new file with all current source data)
2. Run silver with `--full-refresh`

Since the latest file contains all records from the source, the `LAG()` chain only needs that one file — all records appear as Inserts. Delete detection uses the pre-hook `_current_temp` table compared against `bronze_latest`, which also only needs the latest file. Old deleted files are not needed.

---

## Appendix: Compiled SQL Examples

The following are the actual SQL statements executed by DuckDB after dbt compiles the Jinja macros. These examples show incremental runs (table already exists). Storage paths (`abfss://...`) are abbreviated for readability.

<details>
<summary><strong>Full Bronze Refresh — <code>silver_aktoertype</code> (incremental run)</strong></summary>

**Pre-hook:** Creates the `_last_file` bookmark table if it doesn't exist (empty, via `WHERE 1 = 0`):

```sql
CREATE TABLE IF NOT EXISTS main_silver.silver_aktoertype_last_file AS
SELECT processed_files.*
,      CAST('2026-03-16 06:43:00.302' AS DATETIME) AS LKHS_date_inserted
FROM
(
SELECT LKHS_filename
,      strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),'%Y%m%d_%H%M%S') AS LKHS_date_valid_from
,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
,      LAG(strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),'%Y%m%d_%H%M%S')) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_previous
FROM    (SELECT SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
        FROM read_text('abfss://.../aktoertype/aktoertype_*.json')
        ) files
WHERE 1 = 0
) processed_files
```

**Main query:** Compares each Bronze file to its predecessor via LEFT JOIN, detects I/U/D, filters by `_last_file` bookmark, and deduplicates against existing silver rows:

```sql
WITH CTE_BRONZE AS (
SELECT src.*
,sha256(CONCAT(
    COALESCE(type::VARCHAR,'<NULL>'),']##[',
    COALESCE(opdateringsdato::VARCHAR,'<NULL>'),']##[',
    COALESCE(LKHS_source_system_code::VARCHAR,'<NULL>'),']##['
)) AS LKHS_hash_value
,CONCAT(...) AS LKHS_base_for_hash_value
,CAST(MIN(opdateringsdato) OVER (PARTITION BY id) AS DATETIME) AS LKHS_date_inserted_src
FROM main_bronze.bronze_aktoertype src
)
,CTE_FILES AS (
    SELECT LKHS_filename
    ,      strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),'%Y%m%d_%H%M%S') AS LKHS_date_valid_from
    ,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename)  AS LKHS_filename_previous
    ,      LEAD(LKHS_filename) OVER (ORDER BY LKHS_filename)  AS LKHS_filename_next
    ,      LAG(strptime(...))  OVER (ORDER BY LKHS_filename)  AS LKHS_date_valid_from_previous
    ,      LEAD(strptime(...)) OVER (ORDER BY LKHS_filename)  AS LKHS_date_valid_from_next
    FROM (SELECT SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
          FROM read_text('abfss://.../aktoertype/aktoertype_*.json')
         ) files
)
,CTE_BRONZE_INCL_LAG AS (
    SELECT CTE_BRONZE.*
    ,      CTE_FILES.LKHS_date_valid_from
    ,      CTE_BRONZE_PREVIOUS.LKHS_hash_value AS LKHS_hash_value_previous
    ,      CTE_BRONZE_PREVIOUS.id              AS LKHS_primary_key_previous
    FROM       CTE_BRONZE
    INNER JOIN CTE_FILES
    ON         CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
    LEFT JOIN  CTE_BRONZE CTE_BRONZE_PREVIOUS                        -- compare to previous file
    ON         CTE_FILES.LKHS_filename_previous = CTE_BRONZE_PREVIOUS.LKHS_filename
    AND        CTE_BRONZE.id = CTE_BRONZE_PREVIOUS.id
)
,CTE_ALL_ROWS AS
(
-- Inserts and Updates
SELECT CTE_BRONZE_INCL_LAG.* EXCLUDE (LKHS_base_for_hash_value, LKHS_hash_value_previous, LKHS_primary_key_previous, LKHS_filename, LKHS_date_valid_from)
,      CTE_BRONZE_INCL_LAG.LKHS_filename
,      CTE_BRONZE_INCL_LAG.LKHS_date_valid_from
,      CAST('2026-03-16 06:43:00.302' AS DATETIME) AS LKHS_date_inserted
,      CASE
           WHEN LKHS_primary_key_previous IS NULL THEN 'I'                                          -- not in previous file → Insert
           WHEN LKHS_primary_key_previous IS NOT NULL AND LKHS_hash_value != LKHS_hash_value_previous THEN 'U'  -- hash changed → Update
       END AS LKHS_cdc_operation
FROM   CTE_BRONZE_INCL_LAG
WHERE  LKHS_cdc_operation IN ('I','U')

UNION ALL

-- Deletes: record in file N but NOT in file N+1
SELECT CTE_BRONZE_INCL_LAG.* EXCLUDE (LKHS_base_for_hash_value, LKHS_hash_value_previous, LKHS_primary_key_previous, LKHS_filename, LKHS_date_valid_from)
,      CTE_FILES.LKHS_filename_next       AS LKHS_filename          -- timestamped with next file
,      LKHS_date_valid_from_next           AS LKHS_date_valid_from
,      CAST('2026-03-16 06:43:00.302' AS DATETIME) AS LKHS_date_inserted
,      'D' AS LKHS_cdc_operation
FROM       CTE_BRONZE_INCL_LAG
INNER JOIN CTE_FILES
ON         CTE_BRONZE_INCL_LAG.LKHS_filename = CTE_FILES.LKHS_filename
LEFT JOIN  CTE_BRONZE_INCL_LAG CTE_BRONZE_INCL_LAG_NEXT             -- compare to next file
ON         CTE_FILES.LKHS_filename_next = CTE_BRONZE_INCL_LAG_NEXT.LKHS_filename
AND        CTE_BRONZE_INCL_LAG.id = CTE_BRONZE_INCL_LAG_NEXT.id
WHERE      CTE_BRONZE_INCL_LAG_NEXT.id IS NULL                      -- missing in next file → Delete
AND        CTE_FILES.LKHS_filename_next IS NOT NULL                  -- but next file must exist
)
SELECT CTE_ALL_ROWS.*
FROM   CTE_ALL_ROWS
WHERE  (CTE_ALL_ROWS.LKHS_filename >= (SELECT LKHS_filename_previous FROM main_silver.silver_aktoertype_last_file)
        OR (SELECT LKHS_filename_previous FROM main_silver.silver_aktoertype_last_file) IS NULL
       )
-- Deduplication: don't re-insert rows already in silver
AND NOT EXISTS (
    SELECT id FROM main_silver.silver_aktoertype
    WHERE id = CTE_ALL_ROWS.id
    AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from
)
```

**Post-hook:** Drops and recreates `_last_file` with the latest file as bookmark:

```sql
DROP TABLE IF EXISTS main_silver.silver_aktoertype_last_file;
CREATE TABLE main_silver.silver_aktoertype_last_file AS
SELECT processed_files.*
,      CAST('2026-03-16 06:43:00.302' AS DATETIME) AS LKHS_date_inserted
FROM
(
SELECT LKHS_filename
,      strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),'%Y%m%d_%H%M%S') AS LKHS_date_valid_from
,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
,      LAG(strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),'%Y%m%d_%H%M%S')) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_previous
FROM    (SELECT SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
        FROM read_text('abfss://.../aktoertype/aktoertype_*.json')
        ) files
QUALIFY ROW_NUMBER() OVER (ORDER BY files.LKHS_filename DESC) = 1
) processed_files
```

</details>

<details>
<summary><strong>Incremental Bronze Refresh — <code>silver_aktoer</code> (incremental run)</strong></summary>

### Incremental Bronze Refresh — `silver_aktoer` (incremental run)

**Pre-hook:** Same as above — creates `_last_file` bookmark if it doesn't exist.

**Main query:** Uses `LAG()` over hash partitioned by id (instead of LEFT JOIN to previous file), no delete detection on incremental runs:

```sql
WITH CTE_BRONZE AS (
SELECT src.*
,sha256(CONCAT(
    COALESCE(typeid::VARCHAR,'<NULL>'),']##[',
    COALESCE(gruppenavnkort::VARCHAR,'<NULL>'),']##[',
    COALESCE(navn::VARCHAR,'<NULL>'),']##[',
    COALESCE(periodeid::VARCHAR,'<NULL>'),']##[',
    COALESCE(opdateringsdato::VARCHAR,'<NULL>'),']##[',
    COALESCE(startdato::VARCHAR,'<NULL>'),']##[',
    COALESCE(slutdato::VARCHAR,'<NULL>'),']##[',
    COALESCE(fornavn::VARCHAR,'<NULL>'),']##[',
    COALESCE(efternavn::VARCHAR,'<NULL>'),']##[',
    COALESCE(biografi::VARCHAR,'<NULL>'),']##[',
    COALESCE(LKHS_source_system_code::VARCHAR,'<NULL>'),']##['
)) AS LKHS_hash_value
,CONCAT(...) AS LKHS_base_for_hash_value
,CAST(MIN(opdateringsdato) OVER (PARTITION BY id) AS DATETIME) AS LKHS_date_inserted_src
FROM main_bronze.bronze_aktoer src
)
,CTE_FILES AS (
    SELECT LKHS_filename
    ,      strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),'%Y%m%d_%H%M%S') AS LKHS_date_valid_from
    ,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename)  AS LKHS_filename_previous
    ,      LEAD(LKHS_filename) OVER (ORDER BY LKHS_filename)  AS LKHS_filename_next
    ,      LAG(strptime(...))  OVER (ORDER BY LKHS_filename)  AS LKHS_date_valid_from_previous
    ,      LEAD(strptime(...)) OVER (ORDER BY LKHS_filename)  AS LKHS_date_valid_from_next
    FROM (SELECT SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
          FROM read_text('abfss://.../aktoer/aktoer_*.json')
         ) files
)
,CTE_FILE_LATEST AS (
    SELECT MAX(LKHS_filename)        AS LKHS_filename
    ,      MAX(LKHS_date_valid_from) AS LKHS_date_valid_from
    FROM   CTE_FILES
)
,CTE_BRONZE_INCL_LAG AS (
    SELECT CTE_BRONZE.*
    ,      CTE_FILES.LKHS_date_valid_from
    ,      LAG(CTE_BRONZE.LKHS_hash_value)                          -- LAG over hash per id
           OVER (PARTITION BY id ORDER BY CTE_BRONZE.LKHS_filename)
           AS LKHS_hash_value_previous
    FROM       CTE_BRONZE
    INNER JOIN CTE_FILES
    ON         CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
)
,CTE_ALL_ROWS AS
(
SELECT CTE_BRONZE_INCL_LAG.* EXCLUDE (LKHS_base_for_hash_value, LKHS_hash_value_previous, LKHS_date_valid_from)
,      CTE_BRONZE_INCL_LAG.LKHS_date_valid_from
,      CAST('2026-03-15 21:54:17.020' AS DATETIME) AS LKHS_date_inserted
,      CASE
           WHEN LKHS_hash_value_previous IS NULL THEN 'I'           -- first occurrence → Insert
           WHEN LKHS_hash_value != LKHS_hash_value_previous THEN 'U' -- hash changed → Update
       END AS LKHS_cdc_operation
FROM   CTE_BRONZE_INCL_LAG
WHERE  LKHS_cdc_operation IN ('I','U')
-- NOTE: no UNION ALL for deletes on incremental runs
-- Deletes are only detected on --full-refresh (see macro source)
)
SELECT CTE_ALL_ROWS.*
FROM   CTE_ALL_ROWS
WHERE  (CTE_ALL_ROWS.LKHS_filename >= (SELECT LKHS_filename_previous FROM main_silver.silver_aktoer_last_file)
        OR (SELECT LKHS_filename_previous FROM main_silver.silver_aktoer_last_file) IS NULL
       )
-- Deduplication: don't re-insert rows already in silver
AND NOT EXISTS (
    SELECT id FROM main_silver.silver_aktoer
    WHERE id = CTE_ALL_ROWS.id
    AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from
)
```

**Post-hook:** Same as the full extraction — drops and recreates `_last_file` with the latest file as bookmark.

</details>

> **Note:** For details on Dagster executor configuration and the DuckDB single-writer
> constraint, see the [Executor Concurrency Model](../README.md#executor-concurrency-model)
> section in the main README.
