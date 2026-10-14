# dbt Transformations: Models, Macros and the Silver CDC Logic

How the dbt project in [`dbt/`](../dbt/) turns raw extraction files into a
historised Silver layer and a star-schema Gold layer. It covers the layer
conventions, every macro, the change-detection (CDC) logic in depth, and the
operational rules that keep Silver correct.

For how the models are run (jobs, schedules, Metabase stop/start) see
[daily_pipeline_execution.md](daily_pipeline_execution.md); for the reasons
behind the design see [design_decisions.md](design_decisions.md).

---

## 1. Layers at a glance

| Layer | Materialization | Models | Built from |
| --- | --- | --- | --- |
| **Bronze** (`main_bronze`) | view | one view + one `_latest` view per entity, plus 3 utility views (`bronze_dates`, `bronze_dates_holidays`, `bronze_source_systems`) | the extraction files, read with `read_json_auto()` |
| **Silver** (`main_silver`) | incremental table + view | one CDC table + one `_cv` view per entity | Bronze |
| **Gold** (`main_gold`) | view | handcrafted dimensions/facts + generated `_cv` views | Silver `_cv` views and tables |
| **data_engineering** | views (2 tables) | Dagster run history as a small star schema | Dagster's SQLite + step-failure NDJSON |

- **Generated vs handwritten.** All Bronze and Silver model files and the Gold
  `_cv` views are written by `python -m ddd_python.ddd_dbt.generate_dbt_models`
  from the entity lists in `configuration_variables.py`. Each generated file is
  one macro call. The Gold dimensions and facts (`actor`, `case`, `meeting`,
  `vote`, `individual_votes`, `date`, `time`, …) are written by hand.
- **Names.** Bronze/Silver use `{layer}_{source}_{entity}`, with Danish
  characters normalised (ø→oe, æ→ae, å→aa): `Aktør` → `bronze_ddd_aktoer` →
  `silver_ddd_aktoer`. Gold uses English business names (`actor`, `vote`).
- **`LKHS_` columns** are added by the pipeline, never by a source:

| Column | Meaning |
| --- | --- |
| `LKHS_filename` | Bronze file the row came from (`<entity>_<YYYYMMDD_HHMMSS>.json`, `.jsonl` or `.json.jsonl`); for a `D` row the file without the key (§3.1) |
| `LKHS_source_system_code` | `DDD` or `RFAM` |
| `LKHS_hash_value` | SHA-256 of the business columns (change fingerprint) |
| `LKHS_date_valid_from` | SCD Type 2 validity start = timestamp in the file name |
| `LKHS_cdc_operation` | `I` insert, `U` update, `D` delete |
| `LKHS_date_inserted` | dbt run start (`run_started_at`) that wrote the row |
| `LKHS_date_inserted_src` | earliest source date for the key (the entity's date column, or the file timestamp when there is none) |

- **Storage.** In `SILVER_STORAGE_FORMAT=ducklake` mode the Silver tables are
  created in the attached `ducklake_catalog` database instead of the main
  `.duckdb` file. Bronze and Gold always stay in the main file, and `ref()`
  resolves across the two automatically.

---

## 2. Bronze

A Bronze model is two lines:

```sql
{{ config(tags=['ddd']) }}
{{ generate_model_bronze(this.name,'DDD','danish_parliament') }}
```

The macro reads the dbt source (defined in `models/bronze/__sources*.yml` as
`read_json_auto('<DATA_SOURCE>/<entity>/<entity>_*.json*', filename=True, union_by_name=true)`)
and returns every column except dlt's `_dlt_*` columns, plus `LKHS_filename`
and `LKHS_source_system_code`. `DISTINCT` removes exact duplicate rows within a
file.

Because Bronze models are **views**, a new extraction file is visible to Silver
immediately; re-running Bronze only redeploys the view definitions. Bronze
storage is wherever `DANISH_DEMOCRACY_DATA_SOURCE` / `RFAM_DATA_SOURCE` point:
the local filesystem or S3.

The **`_latest` view** reads only the newest file of an entity (the
lexicographically largest file name, which is also the newest timestamp). It is
used for delete detection on a Silver `--full-refresh` (§3.4).

---

## 3. Silver: CDC into SCD Type 2

Every Silver table is an append-only history: one row per version of each key,
with `LKHS_cdc_operation` saying how that version came about. The `_cv` view
returns the newest row per key:

```sql
SELECT src.* FROM silver_ddd_aktoer src
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code, src.id
                           ORDER BY src.LKHS_date_valid_from DESC) = 1
```

There are two Silver macros because the two extraction types deliver different
information:

| | Full-extract entities | Incremental entities |
| --- | --- | --- |
| Entities | 12 DDD lookups + 5 Rfam (`clan`, `clan_membership`, `author`, `literature_reference`, `dead_family`) | 6 DDD (`aktoer`, `moede`, `sag`, `sagstrin`, `sagstrinaktoer`, `stemme`) + 2 Rfam (`family`, `genome`) |
| Each Bronze file contains | **every** row | only rows changed in the lookback window (31 days DDD, 365 days Rfam by default) |
| Macro | `generate_model_silver_full_extraction` | `generate_model_silver_incr_extraction` |
| Insert | key not in the previous file | first time the key appears in any file, or first version after a `D` |
| Update | hash differs from the previous file | hash differs from the key's previous version |
| Delete | key missing from the next file, on every run | only on `--full-refresh`, against `_latest` |

### 3.1 Shared building blocks

**Run modes.** "Incremental" names a kind of entity here; for dbt's run modes
this document uses three terms:

| | First build | Normal run | Full refresh |
| --- | --- | --- | --- |
| When | the Silver table does not exist (also with `--full-refresh`) | `dbt build` / `run` on an existing table (`is_incremental()` is true) | `--full-refresh` on an existing table |
| `generate_pre_hook_silver` | creates an empty `_last_file` | creates `_last_file` only if it is missing | drops and recreates `_last_file` empty |
| `generate_pre_hook_silver_full_refresh` (incremental entities) | nothing | nothing | copies the newest version of every key and every `D` row to `_current_temp` |
| Bronze files read | all | all | all |
| Rows written | every version from all files | the versions from `LKHS_filename_previous` onwards that are not in the table yet | every version from all files |
| Full-extract deletes | from every file pair | from the file pairs in that range | from every file pair |
| Incremental deletes | none | none | carried `D` rows plus new deletes (§3.4) |
| `D` rows used to recognise returning keys (incremental) | none | from the table | from `_current_temp` |
| How the table is written | created | rows appended by column name; new source columns are added at the end | created anew, columns in Bronze order |
| Post-hooks | `_last_file` = newest file and its predecessor | same | same, and `_current_temp` is dropped |

**`CTE_BRONZE`** reads the Bronze view and adds `LKHS_hash_value =
sha256(<base_for_hash>)` and `LKHS_date_inserted_src`. Bronze selects `DISTINCT`
rows without dlt's `_dlt_*` columns, so an exact duplicate of a row in one file
is a single version.

**The hash.** `generate_base_for_hash` reads the Bronze view's columns from
`information_schema.columns` at compile time, in column order, and leaves out
the primary key and the columns in the dbt variable
`bronze_columns_to_exclude_in_silver_hash` (`LKHS_date_inserted`,
`LKHS_filename`). `LKHS_source_system_code` is included. Each column becomes
`COALESCE(col::VARCHAR, '<NULL>')` followed by the delimiter `]##[`, and
`LKHS_hash_value` is the SHA-256 of the concatenation. For `sagskategori` that
is:

```sql
sha256(CONCAT(COALESCE(kategori::VARCHAR, '<NULL>'), ']##[',
              COALESCE(opdateringsdato::VARCHAR, '<NULL>'), ']##[',
              COALESCE(LKHS_source_system_code::VARCHAR, '<NULL>'), ']##['))
```

A NULL and an empty string hash differently. A NULL and the literal text
`<NULL>` hash the same, and so do two adjacent values that differ only in where
`]##[` falls (`'x]##['`, `'y'` and `'x'`, `']##[y'`). A new source column is
picked up automatically and changes the hash of every key that has a value in
it (§3.9).

**Files and validity.** `CTE_FILES` lists the entity's files with
`read_text('<entity>_*.json*')`, which matches the `.json`, `.jsonl` and
`.json.jsonl` names that different dlt versions write, and uses `LAG()`/`LEAD()`
to know each file's predecessor and successor. `LKHS_date_valid_from` is the
`YYYYMMDD_HHMMSS` timestamp in the file name (`parse_filename_ts`), so files
must be named in timestamp order.

**`LKHS_` columns per kind of row:**

| Column | `I` / `U` row | Full-extract `D` | Incremental new `D` | Carried `D` (incremental full refresh) |
| --- | --- | --- | --- | --- |
| `LKHS_filename` | file of the version | first file without the key | newest file | file of its `LKHS_date_valid_from`; the stored name if that file was pruned |
| `LKHS_date_valid_from` | timestamp of that file | timestamp of the file without the key | timestamp of the newest file | unchanged |
| business columns, `LKHS_hash_value` | that version | the deleted version's | the deleted version's | unchanged |
| `LKHS_date_inserted` | start of the dbt run that wrote it; a full refresh rewrites it | same | start of the full refresh | unchanged |
| `LKHS_date_inserted_src` | earliest value of the entity's date column for the key, or the timestamp of the first file with the key when there is no date column; computed by the run that writes the row | the deleted version's | the deleted version's | unchanged |

**Bookmark.** `<model>_last_file` holds one row: the newest processed file and
its predecessor (`LKHS_filename_previous`, NULL after a first build with one
file). The post-hook rebuilds it from the files present at the end of the run.
A normal run still reads and hashes every Bronze file, because the comparisons
need each key's previous version, but only writes rows from
`LKHS_filename_previous` onwards. It re-evaluates `LKHS_filename_previous` and
the newest processed file so that a file arriving slightly late is still
processed (§3.8); that is also why the file before `LKHS_filename_previous`
must stay in Bronze (§3.6).

**Idempotency guard.** On a normal run, `NOT EXISTS (pk, LKHS_date_valid_from)`
against the existing table drops every re-evaluated version that is already
there, so no version is inserted twice.

### 3.2 Full-extract entities

Every file is a full snapshot, so consecutive files can be compared directly:

```text
file N-1 ──LEFT JOIN on pk──► file N ──LEFT JOIN on pk──► file N+1
  key not in N-1          → I            key in N but not in N+1 → D
  key in N-1, hash differs → U           (timestamped with file N+1)
```

Every key that was in the previous file but is missing from the newest file
therefore gets a `D` as soon as the newest file is processed, with its
timestamp and name. A key that is still present in the newest file can only be
deleted by a later file.

A key missing from several consecutive files gets one `D`, at the first file
without it; in the following pairs it is absent from both files. When it comes
back, it is not in the previous file, so it is an `I` again (`I1 D2 I4` for a
key missing from files 2 and 3). The `D` row carries the content of the version
it deletes. Because every delete is derived from Bronze file pairs, a
`--full-refresh` rebuilds all of them and needs no full extraction first.

### 3.3 Incremental entities

A file only contains changed rows, so "missing from the next file" means
nothing. Instead the macro takes the previous hash **for the same key** across
the whole file history:

```sql
LAG(LKHS_hash_value) OVER (PARTITION BY id ORDER BY LKHS_filename)
-- NULL → I;  different → U;  equal → no row
```

Because the lookback windows overlap, the same unchanged row appears in many
files; equal hashes produce no Silver row, so the overlap costs nothing.

**Keys that come back after a delete.** `D` rows of incremental entities are
written by a `--full-refresh` (§3.4) and are not in Bronze, so `LAG()` does not
see them. The macro therefore also reads the `D` rows already in Silver: from
the table itself on a normal run, from `_current_temp` on a full refresh. A
Bronze version is an `I` when Silver has a `D` for the key between the key's
previous Bronze version and this one, whatever its hash. A key can be deleted
and come back any number of times:

```text
F1 key present → I    F2 (complete) key absent, --full-refresh → D
F3 key back, same hash → I    F4 (complete) absent, --full-refresh → D    F5 back → I
```

### 3.4 Deletes of incremental entities (`--full-refresh` only)

A record deleted at the source simply stops appearing in incremental files, so
normal runs cannot see deletes. They are detected on a `--full-refresh`:

1. Pre-hook `generate_pre_hook_silver_full_refresh` copies the newest version
   of every key **and every `D` row** of the existing table into
   `<model>_current_temp`.
2. The table is rebuilt from the full Bronze history (all I/U rows, with the
   rule for returning keys from §3.3), plus two extra branches:
   - **Carried deletes:** every `D` row from `_current_temp`. Its
     `LKHS_filename` is set to the Bronze file of its `LKHS_date_valid_from`;
     if that file was pruned, the stored name is kept.
   - **New deletes:** per key, the newest version, rebuilt from Bronze or, for
     a key no longer in Bronze, taken from `_current_temp`. When that version
     is not a `D` and the key is **absent from `bronze_<entity>_latest`**, a
     `D` row is added with the newest file's name and timestamp, and the
     content of the deleted version.
3. The post-hook drops `_current_temp`.

All `D` rows are needed, not only the newest per key: each one is history that
cannot be rebuilt from Bronze, and each one marks the following return as an
`I`. The three branches are combined with `UNION ALL BY NAME`, because after a
new source column the existing Silver rows have that column at the end, while
the rebuilt rows have it in its Bronze position. Repeating a full refresh
reproduces the table (except `LKHS_date_inserted`).

> **Run a full extraction first.** `_latest` is a single file. After a normal
> daily run that file holds only the last 31 days of changes, and a
> `--full-refresh` would mark every other record as deleted (see
> [runbook §7](runbook.md#7-silver-needs-a-rebuild) for the repair). Before a full
> refresh of incremental entities, extract them with an early start date so the
> newest file is a complete snapshot:
>
> ```bash
> python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_danish_parliament_data --date_to_load_from 1900-01-01
> python -m ddd_python.ddd_dlt.dlt_run_extraction_pipelines_rfam --date_to_load_from 1900-01-01
> ```
>
> (In Dagster: set `date_to_load_from` in the Launchpad config.) A full refresh
> of a table that does not exist yet, such as after switching
> `SILVER_STORAGE_FORMAT`, skips the delete branch and is safe.

### 3.5 Batch and sequential runs give the same history

Extractions and Silver runs can be interleaved freely. Five files processed in
one Silver run produce exactly the rows five separate runs would have produced:
the full-extract macro evaluates every consecutive file pair independently, the
incremental macro's `LAG()` runs over the whole history, and the `NOT EXISTS`
guard drops anything already written. Example for one key in an incremental
entity:

```text
id=42 in F1: LAG = NULL     → I
id=42 in F2: LAG = hash_F1  → U   (changed)
id=42 in F3: LAG = hash_F2  → —   (unchanged)
id=42 in F4: LAG = hash_F2  → U   (changed again)
Silver: [I, U, U] — whether Silver ran once after F4 or after every file
```

### 3.6 Never delete Bronze files arbitrarily

Both macros compare each version with the key's previous version **in
Bronze**, not in Silver. That keeps Silver a function of the files on storage:
whatever happened, a `--full-refresh` rebuilds it (§3.4, §3.8). The price is
that Bronze files cannot be deleted at will. A deleted file is simply missing
from the comparisons: delete F2 and F3 from F1…F5, and F4's predecessor becomes
F1, so rows first seen in F2 look new in F4 and are inserted a second time.

**The rule (both macros).** Files can be deleted as long as the oldest
remaining file

1. is **complete**: any file of a full-extract entity, a full extraction
   (§3.4) for an incremental entity; and
2. is **older than `LKHS_filename_previous`** in the model's `_last_file`
   table.

**Why complete.** Every later comparison starts at the oldest remaining file.
A key that is not in it has its previous version in a deleted file, so its next
version is compared with nothing and becomes an `I`: an `I` instead of a `U`
when it changed, a second `I` with unchanged content when it did not. A
complete file contains every key.

**Why older than `LKHS_filename_previous`.** A normal run does not only write
rows from new files. It writes rows from `LKHS_filename_previous` onwards, so
that a file that arrives slightly late is still processed (§3.8). Evaluating a
file a second time is normally harmless: its versions are either in Silver
already (the `NOT EXISTS` guard drops them) or equal to the version before them
(no row). In the oldest remaining file, however, no key has a previous version
in Bronze, so every row is an `I`, and the guard does not stop it: Silver has
the key, but not at this file's `LKHS_date_valid_from`. The oldest remaining
file must therefore be outside the range a normal run evaluates again.

Example: F3 is a full extraction, F1 and F2 are deleted.

| Files processed after F3 | `_last_file` (newest, `LKHS_filename_previous`) | Next normal run writes rows from | Result |
| --- | --- | --- | --- |
| none | F3, F2 | F2, so F3 is evaluated again | an extra `I3` for every key |
| one (F4) | F4, F3 | F3 | an extra `I3` for every key |
| two (F4, F5) | F5, F4 | F4 | correct |

Hence "at least two files processed after the full extraction". Only processed
files count: a file that has been extracted but not yet run through Silver
does not move the bookmark. Check the bookmark before deleting anything (in
DuckLake mode the table is in `ducklake_catalog.main_silver`):

```sql
SELECT LKHS_filename, LKHS_filename_previous
FROM   main_silver.silver_ddd_stemme_last_file;
-- delete only files older than a complete file that is older than LKHS_filename_previous
```

In practice:

- **Full-extract entities:** every file is complete, so keep the newest
  processed file, `LKHS_filename_previous` and the file before it.
- **Incremental entities:** keep a full extraction after which at least two
  newer files have been processed, and everything after it.

Further points:

- **Deleting more** gives extra `I` rows. If the oldest remaining file is
  evaluated again, every key in it gets an `I` (1,253 of 1,253 keys in a test).
  If it is not complete, a key whose previous version was in a deleted file
  gets an `I` instead of a `U` when it changes, and a second `I` with unchanged
  content when it appears unchanged, for example in a later full extraction
  (1,251 of 1,253 keys in a test).
- **Silver becomes the only full history** once Bronze files are gone. Back it
  up (the platform backup does) before pruning Bronze.
- **Why not compare with Silver instead?** The newest Silver row per key plus
  a complete file would be enough for a normal run, and the rule above would
  disappear. But Silver would then be state rather than a result: a wrong row
  would be compared against from then on instead of being rebuilt away by a
  full refresh, and the stored hashes would have to stay comparable after
  source column changes (§3.9). The project keeps Bronze as the reference and
  accepts the pruning rule.
- **A `--full-refresh` rebuilds history only from the remaining files.**
  Versions that exist only in pruned files are lost, and a key's first version
  moves to the first remaining file that contains it: `I1 U2 U3` becomes
  `I2 U3` after pruning file 1, and a key unchanged since file 1 gets its `I`
  from the next complete file. For incremental entities the `D` rows survive,
  because they are carried forward from Silver. After pruning, avoid full
  refreshes, or restore the pruned files first; for incremental entities the
  full extraction rule of §3.4 still applies.

### 3.7 Why helper tables are database-qualified

`_last_file` and `_current_temp` are always written as
`{{ this.database }}.{{ this.schema }}.{{ this.name }}_…`. In DuckLake mode the
model lives in `ducklake_catalog`, and DuckDB forbids one transaction from
writing to two databases, so the helpers must sit next to the model. In `duckdb`
mode `this.database` is simply the main database.

### 3.8 Files that arrive late

Normal runs assume Bronze files arrive in timestamp order. A file whose
timestamp is older than the newest processed file, such as a restored or
re-copied extraction, is handled like this by both macros:

- **Newer than the bookmark's predecessor** (`LKHS_filename_previous` in
  `_last_file`): processed normally; the history is the same as if the file had
  arrived in order.
- **Older than that:** its own rows are skipped, but the next file is now
  compared with it, so Silver can get a `U` whose content equals the version
  before it while the real change is missing (`I1 U3` instead of `I1 U2 U3`).
  A `--full-refresh` rebuilds the correct history; for incremental entities run
  a full extraction first (§3.4).

### 3.9 Changes to the source columns

The hash covers every business column, so a column change in the source shows
up as new versions:

- **A new field:** the next file gives a `U` for every key that has a value in
  it; keys where it is NULL keep their hash. A normal run adds the column at the
  end of the Silver table (`on_schema_change='append_new_columns'`); a full
  refresh puts it in its Bronze position.
- **A field the source stops sending:** the column stays in Bronze
  (`union_by_name`) and is NULL in the new files, so the next file gives a `U`
  for every key that had a value.
- Renamed columns and type changes need a deliberate change and a
  `--full-refresh` (see ADR-2 in [design_decisions.md](design_decisions.md)).

### 3.10 All cases at a glance

Results per key. Full-extract entities behave the same on a normal run and on a
full refresh, except where noted.

| Case | Full-extract entities | Incremental, normal run | Incremental, `--full-refresh` |
| --- | --- | --- | --- |
| New key | `I` | `I` | `I` |
| Key unchanged (also in overlapping lookback windows) | no row | no row | no row |
| Key changed | `U` | `U` | `U` |
| Key missing from a file | `D` at the first file without it, with the content of the last version (§3.2) | no row; an incremental file only holds changes | `D` as of the newest file if the key is missing from it, with that file's name (§3.4) |
| Key still missing after its `D` | no row | no row | no row; the `D` is carried forward |
| Key back after a `D`, same or changed content | `I` | `I` (§3.3) | `I` (§3.3) |
| Deleted and back several times | every `D` and `I` | every return `I` | every `D` carried forward, every return `I` |
| Several new files in one run | same rows as one run per file (§3.5) | same | same |
| Repeated without new files | no new rows | no new rows | same table, except `LKHS_date_inserted` |
| First build (table does not exist) | all `I`, `U` and `D` from the Bronze files | all `I` and `U`; no deletes | same as normal run |
| Newest file is an incremental extract | – | – | false `D` for every key not in it; run a full extraction first (§3.4, [runbook §7](runbook.md#7-silver-needs-a-rebuild)) |
| Late file, newer than the bookmark's predecessor | processed in order (§3.8) | processed in order | rebuilt in order |
| Late file, older than the bookmark's predecessor | skipped; can add a `U` with unchanged content; a full refresh repairs it (§3.8) | same as full-extract | rebuilt correctly |
| Bronze files pruned | normal runs unaffected if the newest processed file, `LKHS_filename_previous` and the file before it are kept, otherwise extra `I` rows; a full refresh loses the history in the pruned files (§3.6) | unaffected if the remaining files start with a full extraction older than `LKHS_filename_previous`, otherwise extra `I` rows (§3.6) | history in the pruned files lost; `D` rows kept (§3.6) |
| Source adds a field | `U` for every key with a value in it (§3.9) | same | same |
| Source stops sending a field | `U` for every key that had a value (§3.9) | same | same |
| Same key twice in one file, different content | two rows with the same `(key, LKHS_date_valid_from)` when both differ from the previous version | can give two rows for the same `(key, LKHS_date_valid_from)` | same |

The source must deliver one row per key per file; duplicates are detected, not
handled. The grain test `silver_no_duplicate_id_date_valid_from` covers all 18
DDD Silver tables and a generic test covers `silver_rfam_family`; the other six
Rfam tables have no grain test (improvement 5 in
[design_decisions.md](design_decisions.md#known-limitations-and-possible-improvements)).

### 3.11 How this is verified

[`tests/test_dbt_silver.py`](../tests/test_dbt_silver.py) runs the real Silver
macros with dbt-oss on copies of the CI sample; every test gets its own Bronze
directory, DuckDB file and DuckLake catalog. It covers every row of §3.10 and
the building blocks of §3.1, for both macros:

| Area | Tests |
| --- | --- |
| Hash: columns, order, NULL token, delimiter | `test_hash_is_sha256_of_the_business_columns_in_bronze_order`, `test_hash_null_token_and_delimiter` |
| File names, bookmark, duplicate rows, `LKHS_` columns | `test_file_name_variants_and_bookmark`, `test_identical_rows_in_one_file_are_one_version`, `test_source_system_code_and_date_inserted`, `test_date_inserted_src_with_a_date_column`, `test_date_inserted_src_without_a_date_column` |
| Full-extract lifecycle (`sagskategori`): new, unchanged, changed, missing, missing twice in a row, back unchanged and changed, two delete cycles; one run per file, four files in one normal run, a first build with all files; DuckDB and DuckLake | `test_full_extract_*` |
| Incremental lifecycle (`stemme`, Rfam `family`): the same with deletes by full refresh; one run per file, two files in one full refresh, two files in one normal run; DuckDB and DuckLake | `test_incremental_*` |
| Incremental full refresh: new table, file name and content of new deletes, false deletes and their repair | `test_full_refresh_of_a_new_table_writes_no_deletes`, `test_new_delete_has_the_newest_file_and_the_deleted_content`, `test_false_deletes_and_their_repair` |
| Late files (§3.8) | `test_late_file_newer_than_the_bookmarks_predecessor`, `test_late_file_older_than_the_bookmarks_predecessor` |
| Pruning (§3.6) | `test_full_extract_pruning`, `test_incremental_pruning_down_to_a_full_extraction`, `test_incremental_pruning_without_a_full_extraction`, `test_full_refresh_after_pruning_keeps_only_the_remaining_history` |
| Source column changes (§3.9) | `test_new_source_column_then_full_refresh`, `test_dropped_source_field` |
| A key twice in one file | `test_same_key_twice_in_one_file` |

Besides the expected histories, the tests assert these invariants of a Silver
table: no two versions with the same `(key, LKHS_date_valid_from)`; the
timestamp in `LKHS_filename` equals `LKHS_date_valid_from`; a `D` carries the
hash of the version it deletes; a `U` differs from the version before it; no
`D` directly after a `D` and no `I` directly after an `I` or `U`; every key
starts with `I`; `LKHS_source_system_code` is correct; `_cv` holds exactly the
newest version of every key. The lifecycles also check that one run per file,
several files in one run and a full refresh give identical tables. Run them
with `DBT_INTEGRATION_TESTS=1 pytest tests/test_dbt_silver.py` (about four
minutes); CI runs them in the `dbt build (sample data)` job.

**Defects fixed in commit `498482d`.** The same tests run against the macros of
the commit before (`2dc3b83`), which differ only in
`generate_model_silver_incr_extraction` and
`generate_pre_hook_silver_full_refresh`: 66 pass and 25 fail. Every
full-extract test passes on both versions; every failure is in the delete
handling of incremental entities. Final histories of the incremental lifecycle
(`stemme`, one run per file):

| Key | Old macros | Current macros |
| --- | --- | --- |
| A: back unchanged after a delete | `I1 D2` | `I1 D2 I3` |
| B: back changed after a delete | `I1 U3` | `I1 D2 I3` |
| C: back, then deleted again | `I1 D2` | `I1 D2 I3 D4` |
| G: back unchanged, changed later | `I1 U5` | `I1 D2 I3 U5` |
| J: deleted and back twice | `I1 D2` | `I1 D2 I3 D4 I5` |
| D, E, H: no return | identical | identical |

The failures, by defect:

1. **A key back unchanged after a `D` stayed deleted** (A, C, G, J above). In
   `test_false_deletes_and_their_repair` the falsely deleted keys stay `D`
   after the full extraction (`I1 D2` instead of `I1 D2 I3`), so the repair in
   [runbook §7](runbook.md#7-silver-needs-a-rebuild) did not work.
2. **A full refresh dropped a `D` that was no longer a key's newest row** (B,
   G), so `test_incremental_repeat_run_and_full_refresh_change_nothing` failed:
   a second full refresh changed the table.
3. **A new `D` stored the previous version's file name**:
   `test_incremental_invariants` found 6 such rows after the first full
   refresh, and `test_new_delete_has_the_newest_file_and_the_deleted_content`
   got `stemme_20260102_000000.json` instead of `stemme_20260103_000000.json`.
4. **After a new source column, a full refresh shifted every `D` row by one
   column** (`test_new_source_column_then_full_refresh`, DuckDB and DuckLake):
   the `D` row had a wrong file name, hash and source system code, and `_cv`
   returned two rows for its key.

Besides fixing these, the current macros write `I` instead of `U` for a key that
comes back changed (B), as the full-extract macro does.

To repeat the comparison, export the old commit
(`git archive 2dc3b83 | tar -x -C <dir>`), copy `dbt/dbt_packages` and
`tests/test_dbt_silver.py` into it and run the module there with
`-o addopts=''` (the old `pyproject.toml` does not register the test marker).

---

## 4. Gold

Gold is a star schema of **views** over Silver.

- **Dimensions are SCD Type 2.** For example, `actor` reads every version in
  `silver_ddd_aktoer` and adds `LKHS_date_valid_to` (`LEAD` of the next
  `LKHS_date_valid_from`, `9999-12-31` for the newest) and `LKHS_row_version`.
- **Surrogate keys** are `cast_hash_to_bigint(source_system_code, id,
  LKHS_date_valid_from)`, so each version has its own key. A `_bk` column holds
  the business key (`DDD-<id>`).
- **Unknown member.** Every dimension has a row with key `0`; facts
  `COALESCE` missing keys to `0`.
- **`_cv` views** (generated) keep the newest version per business key, which
  is what most facts join to.
- **Facts.** `individual_votes` is one row per member per vote, built from the
  Silver `_cv` views of `stemme` (deleted votes excluded), `stemmetype` and
  `afstemning`, and joined to the `_cv` dimensions.
- **SCD2 start.** Every dimension makes its first version valid from
  `1900-01-01`, so a fact can never fall before the dimension's history.
- `date` is a date spine from `date_spine_start_date` (default `1950-01-01`)
  enriched with Danish public holidays (seed `publicholiday_dk.csv`); `time` is
  used only by the data_engineering facts.
- `actor` also parses the XML biography (`biografi`) into gender, birth date,
  party and party short name.

---

## 5. Macro reference

All macros live in [`dbt/macros/`](../dbt/macros/).

| Macro | Used by | What it does |
| --- | --- | --- |
| `generate_model_bronze(table_name, source_system_code, source_name)` | every Bronze model | `SELECT DISTINCT` all columns except `filename`/`_dlt_*`, plus `LKHS_filename` and `LKHS_source_system_code` |
| `generate_model_bronze_latest(file_name, source_system_code, source_name, data_source_env_var)` | every `_latest` model | same columns, newest file only, plus `LKHS_deleted_ind = 'N'` |
| `generate_base_for_hash(table_name, columns_to_exclude, primary_key_columns)` | Silver models | builds the `CONCAT(COALESCE(col::VARCHAR,'<NULL>'),']##[', …)` expression from `information_schema` |
| `generate_model_silver_full_extraction(file_name, bronze_table_name, primary_key_columns, date_column, base_for_hash, data_source_env_var)` | full-extract Silver models | file-pair CDC with I/U/D (§3.2) |
| `generate_model_silver_incr_extraction(…same…)` | incremental Silver models | per-key `LAG` CDC with I/U, where the first version after a Silver `D` is an I, plus the carried and new delete branches on `--full-refresh` (§3.3–3.4); adds the `_current_temp` pre/post hooks |
| `generate_pre_hook_silver(file_name, data_source_env_var)` | all Silver models | creates `_last_file` if missing; drops it first on `--full-refresh` so all Bronze files are reprocessed |
| `generate_post_hook_silver(file_name, data_source_env_var)` | all Silver models | rebuilds `_last_file` with the newest file and its predecessor |
| `generate_pre_hook_silver_full_refresh(primary_key_columns)` | incremental Silver models | on `--full-refresh` only, copies the newest version per key and every `D` row into `_current_temp` |
| `parse_filename_ts(col)` | Silver macros and hooks | `strptime(regexp_extract(col, '([0-9]{8}_[0-9]{6})', 1), '%Y%m%d_%H%M%S')` |
| `cast_hash_to_bigint(columns_to_hash)` | Gold surrogate keys | DuckDB `hash()` (a 64-bit `UBIGINT`) mapped to a signed `BIGINT`, because Power BI rejects `UBIGINT` |

Notes:

- `data_source_env_var` defaults to `DANISH_DEMOCRACY_DATA_SOURCE`; Rfam models
  pass `RFAM_DATA_SOURCE`.
- The file glob is `<entity>_*.json*`, which matches `.json` and the
  `.json.jsonl` names some dlt versions produced. `parse_filename_ts` uses a
  regex for the same reason.
- `cast_hash_to_bigint` maps values above 2^63−1 to `-(value − (2^63−1))`.
  That mapping is one-to-one, so the only collision risk is the 64-bit `hash()`
  itself, which is negligible at this volume. The surrogate key is **not** the
  SHA-256 change hash.

---

## Appendix: compiled SQL

The statements DuckDB runs after dbt compiles the macros, for an incremental
run (table already exists). Paths are shortened to `.../`, long column lists to
`...`, and the database prefix of Silver objects is omitted.

<details>
<summary><strong>Full-extract entity — <code>silver_ddd_aktoertype</code></strong></summary>

**Pre-hook** — create the bookmark table if it does not exist (empty):

```sql
CREATE TABLE IF NOT EXISTS main_silver.silver_ddd_aktoertype_last_file AS
SELECT processed_files.*
,      CAST('2026-03-16 06:43:00.302' AS DATETIME) AS LKHS_date_inserted
FROM
(
SELECT LKHS_filename
,      strptime(regexp_extract(LKHS_filename, '([0-9]{8}_[0-9]{6})', 1), '%Y%m%d_%H%M%S') AS LKHS_date_valid_from
,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
,      LAG(strptime(...)) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_previous
FROM    (SELECT SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
        FROM read_text('.../aktoertype/aktoertype_*.json*')
        ) files
WHERE 1 = 0
) processed_files
```

**Main query:**

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
FROM main_bronze.bronze_ddd_aktoertype src
)
,CTE_FILES AS (
    SELECT LKHS_filename
    ,      strptime(...)       AS LKHS_date_valid_from
    ,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename)  AS LKHS_filename_previous
    ,      LEAD(LKHS_filename) OVER (ORDER BY LKHS_filename)  AS LKHS_filename_next
    ,      LAG(strptime(...))  OVER (ORDER BY LKHS_filename)  AS LKHS_date_valid_from_previous
    ,      LEAD(strptime(...)) OVER (ORDER BY LKHS_filename)  AS LKHS_date_valid_from_next
    FROM (SELECT SUBSTRING(filename, ...) AS LKHS_filename
          FROM read_text('.../aktoertype/aktoertype_*.json*')) files
)
,CTE_BRONZE_INCL_LAG AS (
    SELECT CTE_BRONZE.*
    ,      CTE_FILES.LKHS_date_valid_from
    ,      CTE_BRONZE_PREVIOUS.LKHS_hash_value AS LKHS_hash_value_previous
    ,      CTE_BRONZE_PREVIOUS.id              AS LKHS_primary_key_previous
    FROM       CTE_BRONZE
    INNER JOIN CTE_FILES ON CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
    LEFT JOIN  CTE_BRONZE CTE_BRONZE_PREVIOUS                       -- previous file
    ON         CTE_FILES.LKHS_filename_previous = CTE_BRONZE_PREVIOUS.LKHS_filename
    AND        CTE_BRONZE.id = CTE_BRONZE_PREVIOUS.id
)
,CTE_ALL_ROWS AS (
-- Inserts and updates
SELECT CTE_BRONZE_INCL_LAG.* EXCLUDE (LKHS_base_for_hash_value, LKHS_hash_value_previous, LKHS_primary_key_previous, LKHS_filename, LKHS_date_valid_from)
,      CTE_BRONZE_INCL_LAG.LKHS_filename
,      CTE_BRONZE_INCL_LAG.LKHS_date_valid_from
,      CAST('2026-03-16 06:43:00.302' AS DATETIME) AS LKHS_date_inserted
,      CASE
           WHEN LKHS_primary_key_previous IS NULL THEN 'I'
           WHEN LKHS_primary_key_previous IS NOT NULL AND LKHS_hash_value != LKHS_hash_value_previous THEN 'U'
       END AS LKHS_cdc_operation
FROM   CTE_BRONZE_INCL_LAG
WHERE  LKHS_cdc_operation IN ('I','U')
UNION ALL
-- Deletes: key in file N but not in file N+1
SELECT CTE_BRONZE_INCL_LAG.* EXCLUDE (...)
,      CTE_FILES.LKHS_filename_next AS LKHS_filename
,      LKHS_date_valid_from_next    AS LKHS_date_valid_from
,      CAST('2026-03-16 06:43:00.302' AS DATETIME) AS LKHS_date_inserted
,      'D' AS LKHS_cdc_operation
FROM       CTE_BRONZE_INCL_LAG
INNER JOIN CTE_FILES ON CTE_BRONZE_INCL_LAG.LKHS_filename = CTE_FILES.LKHS_filename
LEFT JOIN  CTE_BRONZE_INCL_LAG CTE_BRONZE_INCL_LAG_NEXT           -- next file
ON         CTE_FILES.LKHS_filename_next = CTE_BRONZE_INCL_LAG_NEXT.LKHS_filename
AND        CTE_BRONZE_INCL_LAG.id = CTE_BRONZE_INCL_LAG_NEXT.id
WHERE      CTE_BRONZE_INCL_LAG_NEXT.id IS NULL
AND        CTE_FILES.LKHS_filename_next IS NOT NULL
)
SELECT CTE_ALL_ROWS.*
FROM   CTE_ALL_ROWS
WHERE  (CTE_ALL_ROWS.LKHS_filename >= (SELECT LKHS_filename_previous FROM main_silver.silver_ddd_aktoertype_last_file)
        OR (SELECT LKHS_filename_previous FROM main_silver.silver_ddd_aktoertype_last_file) IS NULL)
AND NOT EXISTS (SELECT id FROM main_silver.silver_ddd_aktoertype
                WHERE id = CTE_ALL_ROWS.id
                AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from)
```

**Post-hook** — rebuild the bookmark with the newest file:

```sql
DROP TABLE IF EXISTS main_silver.silver_ddd_aktoertype_last_file;
CREATE TABLE main_silver.silver_ddd_aktoertype_last_file AS
SELECT processed_files.*, CAST('2026-03-16 06:43:00.302' AS DATETIME) AS LKHS_date_inserted
FROM (
    SELECT LKHS_filename, strptime(...) AS LKHS_date_valid_from
    ,      LAG(LKHS_filename) OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
    ,      LAG(strptime(...)) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_previous
    FROM (SELECT SUBSTRING(filename, ...) AS LKHS_filename
          FROM read_text('.../aktoertype/aktoertype_*.json*')) files
    QUALIFY ROW_NUMBER() OVER (ORDER BY files.LKHS_filename DESC) = 1
) processed_files
```

</details>

<details>
<summary><strong>Incremental entity — <code>silver_ddd_aktoer</code></strong></summary>

Pre- and post-hooks are the same as above. The main query replaces the
previous-file join with a per-key `LAG()`, reads the `D` rows already in Silver
to recognise returning keys (§3.3), and has no delete branches on incremental
runs:

```sql
WITH CTE_BRONZE AS (
SELECT src.*
,sha256(CONCAT(
    COALESCE(typeid::VARCHAR,'<NULL>'),']##[',
    COALESCE(gruppenavnkort::VARCHAR,'<NULL>'),']##[',
    COALESCE(navn::VARCHAR,'<NULL>'),']##[',
    ...
    COALESCE(biografi::VARCHAR,'<NULL>'),']##[',
    COALESCE(LKHS_source_system_code::VARCHAR,'<NULL>'),']##['
)) AS LKHS_hash_value
,CONCAT(...) AS LKHS_base_for_hash_value
,CAST(MIN(opdateringsdato) OVER (PARTITION BY id) AS DATETIME) AS LKHS_date_inserted_src
FROM main_bronze.bronze_ddd_aktoer src
)
,CTE_FILES AS ( ... same as above, over '.../aktoer/aktoer_*.json*' ... )
,CTE_FILE_LATEST AS (
    SELECT MAX(LKHS_filename) AS LKHS_filename, MAX(LKHS_date_valid_from) AS LKHS_date_valid_from
    FROM   CTE_FILES
)
,CTE_BRONZE_INCL_LAG AS (
    SELECT CTE_BRONZE.*
    ,      CTE_FILES.LKHS_date_valid_from
    ,      LAG(CTE_BRONZE.LKHS_hash_value)
           OVER (PARTITION BY id ORDER BY CTE_BRONZE.LKHS_filename) AS LKHS_hash_value_previous
    ,      LAG(CTE_FILES.LKHS_date_valid_from)
           OVER (PARTITION BY id ORDER BY CTE_BRONZE.LKHS_filename) AS LKHS_date_valid_from_previous
    FROM       CTE_BRONZE
    INNER JOIN CTE_FILES ON CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
)
,CTE_SILVER_DELETES AS (          -- on --full-refresh: from silver_ddd_aktoer_current_temp
    SELECT id, LKHS_date_valid_from
    FROM   main_silver.silver_ddd_aktoer
    WHERE  LKHS_cdc_operation = 'D'
)
,CTE_BRONZE_INCL_DELETES AS (
    SELECT lagged.*
    ,      EXISTS (SELECT 1 FROM CTE_SILVER_DELETES del
                   WHERE  del.id = lagged.id
                   AND    del.LKHS_date_valid_from > lagged.LKHS_date_valid_from_previous
                   AND    del.LKHS_date_valid_from < lagged.LKHS_date_valid_from
                  ) AS LKHS_deleted_since_previous
    FROM   CTE_BRONZE_INCL_LAG lagged
)
,CTE_CHANGES AS (
SELECT CTE_BRONZE_INCL_DELETES.* EXCLUDE (LKHS_base_for_hash_value, LKHS_hash_value_previous, LKHS_date_valid_from,
                                          LKHS_date_valid_from_previous, LKHS_deleted_since_previous)
,      CTE_BRONZE_INCL_DELETES.LKHS_date_valid_from
,      CAST('2026-03-15 21:54:17.020' AS DATETIME) AS LKHS_date_inserted
,      CASE
           WHEN LKHS_hash_value_previous IS NULL THEN 'I'
           WHEN LKHS_deleted_since_previous THEN 'I'
           WHEN LKHS_hash_value != LKHS_hash_value_previous THEN 'U'
       END AS LKHS_cdc_operation
FROM   CTE_BRONZE_INCL_DELETES
WHERE  LKHS_cdc_operation IN ('I','U')
)
-- on --full-refresh only: CTE_CARRIED_DELETES, CTE_NEWEST_VERSION and CTE_NEW_DELETES (§3.4)
,CTE_ALL_ROWS AS (
SELECT * FROM CTE_CHANGES
-- on --full-refresh only: UNION ALL BY NAME the carried and the new 'D' rows
)
SELECT CTE_ALL_ROWS.*
FROM   CTE_ALL_ROWS
WHERE  (CTE_ALL_ROWS.LKHS_filename >= (SELECT LKHS_filename_previous FROM main_silver.silver_ddd_aktoer_last_file)
        OR (SELECT LKHS_filename_previous FROM main_silver.silver_ddd_aktoer_last_file) IS NULL)
AND NOT EXISTS (SELECT id FROM main_silver.silver_ddd_aktoer
                WHERE id = CTE_ALL_ROWS.id
                AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from)
```

</details>
