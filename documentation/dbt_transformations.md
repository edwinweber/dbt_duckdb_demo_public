# dbt Transformations: Models, Macros and the Silver CDC Logic

How the dbt project in [`dbt/`](../dbt/) turns raw extraction files into a
historised Silver layer and a star-schema Gold layer. It covers the layer
conventions, every macro, the change-detection (CDC) logic in depth, and the
operational rules that keep Silver correct.

For how the models are run (jobs, schedules) see
[daily_pipeline_execution.md](daily_pipeline_execution.md); for the reasons
behind the design see [design_decisions.md](design_decisions.md).

---

## 1. Layers at a glance

| Layer | Materialization | Models | Built from |
| --- | --- | --- | --- |
| **Bronze** (`main_bronze`) | view | one view per entity, plus 3 utility views (`bronze_dates`, `bronze_dates_holidays`, `bronze_source_systems`) | the extraction files, read with `read_json_auto()` |
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
`read_json_auto('<DATA_SOURCE>/<entity>/<entity>_*.json*', filename=True, union_by_name=true, timestampformat='iso')`)
and returns every column except dlt's `_dlt_*` columns, plus `LKHS_filename`
and `LKHS_source_system_code`. `DISTINCT` removes exact duplicate rows within a
file.

Because Bronze models are **views**, a new extraction file is visible to Silver
immediately; re-running Bronze only redeploys the view definitions. A view opens
every file in its folder, so processed files are archived from time to time
(§3.6). Bronze storage is wherever `DANISH_DEMOCRACY_DATA_SOURCE` /
`RFAM_DATA_SOURCE` point: the local filesystem or S3.

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

**Silver is the state.** A run reads only the Bronze files that are newer than
the model's bookmark and compares them with Silver's latest version of each key.
Processed Bronze files are never read again, so they can be archived (§3.6), and
the cost of a run does not grow with the number of files. Silver is never rebuilt
from Bronze: every Silver model sets `full_refresh=false`, so dbt ignores
`--full-refresh` for them (§3.7).

**Why.** The project first derived Silver from all Bronze files on every run:
each version was compared with the key's previous version in Bronze. That made
Silver reproducible from the files, but every run read and hashed the whole
history, and DuckDB 2.0 reserves 32 MiB of memory for every JSON file a view
opens. With Silver as the state, a run costs what its new files cost. The price
is that Silver cannot be rebuilt from Bronze by a refresh; repairs go through
backups or a replay (§3.7).

**One run, step by step:**

```text
compile    bookmark (newest processed file) ──► stop on a late file ──► list the newer Bronze files ──► log them as pending
run        read only those files ──► compare with Silver's latest version per key ──► stop on a mass delete ──► append I / U / D rows
post-hook  pending ──► bookmark, pending files ──► processed          (only when the run succeeded)
```

**What this guarantees:**

- No Bronze file is processed twice, and a run that fails leaves the bookmark
  where it was, so it is simply repeated.
- Several new files in one run give exactly the rows that one run per file
  gives (§3.5).
- A repeated run without new files writes nothing.
- A file that arrives while a run is going is left for the next run.
- A file that arrives late (older than the bookmark) stops the run instead of
  being skipped without notice (§3.7).
- A file that would delete too many keys, such as a truncated extraction, stops
  the run before anything is written (the delete guard, §3.1).

**What it does not guarantee.** Silver is exactly as complete as the extractions
it is given. Its current state equals what the Bronze files say, not necessarily
what the source holds today:

- **Versions between extractions are not seen.** A key that changed twice
  between two extractions gets one version, and `LKHS_date_valid_from` is the
  extraction time, not the moment the source changed.
- **Incremental entities only see changes inside a lookback window.** A run
  extracts the rows changed in the last 31 days (DDD) or 365 days (Rfam). When no
  extraction runs for longer than that, the changes in the gap are never
  extracted, and nothing warns about it. The next full extraction (`_full`
  file) repairs the state, dated at that extraction.
- **Deletes of incremental entities appear only at a full extraction** (§3.4).
- **A source row that changes without a new `opdateringsdato` (DDD) or `updated`
  (Rfam)** is not in any incremental file; only a full extraction shows it.
- **The first build of an incremental entity holds only the keys in its files.**
  Start an incremental entity with a full extraction, or it holds only the keys
  that changed recently.
- **One row per key per file** is assumed: duplicates are detected by the grain
  tests, not resolved (§3.10).

Full extractions of the incremental entities are therefore the reconciliation:
run them regularly, and after any gap in the daily runs. The runbook (§10) has
the queries that compare Silver with the Bronze files.

*Example from the development data (23 October 2026).* No extraction ran between
7 July and 10 October; the 10 October run looked back to 9 September. Silver's
current state matched every Bronze file exactly, but a fresh full extraction
showed what the gap had hidden:

| Entity | Source keys | In the source, not live in Silver | Live in Silver, deleted at the source | Different values |
| --- | --- | --- | --- | --- |
| `aktoer` | 18,424 | 2 | 1 | 86 |
| `moede` | 14,011 | 32 | 0 | 32 |
| `sagstrin` | 225,395 | 576 | 5 | 87,644 (a source-wide update in July) |

One normal Silver run over the three `_full` files brought all three tables to an
exact match with the source; the repaired versions carry 23 October as
`LKHS_date_valid_from`. The Rfam tables `family` and `genome` show the first-build
case: their first files were incremental, so Silver holds 540 of the 4,227
families until a full extraction.

There are two Silver macros because the two extraction types deliver different
information:

| | Full-extract entities | Incremental entities |
| --- | --- | --- |
| Entities | 12 DDD lookups + 5 Rfam (`clan`, `clan_membership`, `author`, `literature_reference`, `dead_family`) | 6 DDD (`aktoer`, `moede`, `sag`, `sagstrin`, `sagstrinaktoer`, `stemme`) + 2 Rfam (`family`, `genome`) |
| Each Bronze file contains | **every** row | only rows changed in the lookback window (31 days DDD, 365 days Rfam by default); a full extraction (`<entity>_<timestamp>_full.json`) holds every row |
| Macro | `generate_model_silver_full_extraction` | `generate_model_silver_incr_extraction` |
| Insert | key not in the previous snapshot (Silver's state for the first new file) | key not in Silver, or deleted there |
| Update | hash differs from the previous snapshot | hash differs from the key's latest version |
| Delete | key in the previous snapshot, missing from the next | key alive in Silver and missing from a `_full` file that is the only new file (§3.4) |

### 3.1 Shared building blocks

**Run modes.**

| | First build | Normal run |
| --- | --- | --- |
| When | the Silver table does not exist | the table exists (`is_incremental()` is true); also with `--full-refresh`, which is ignored |
| Bronze files read | all | the files newer than the bookmark |
| Compared with | an empty state | Silver's latest version of each key |
| Deletes | full-extract entities: between the files; incremental entities: none | full-extract entities: against the state and between the new files; incremental entities: from a `_full` file that is the only new file |
| How the table is written | created | rows appended by column name; new source columns are added at the end |

**The bookmark.** `<model>_last_file` holds one row: `LKHS_filename`, the newest
Bronze file the model has processed, and `LKHS_filename_pending`, the newest file
of the run in progress. When the model compiles, the macro `silver_files` lists
the entity's files newer than the bookmark (a `glob()`, no file is opened), notes
the newest of them as pending, and the model reads exactly those files: the
filters on the file name are constants, so DuckDB skips the other files. A file
that arrives during the run waits for the next one. The post-hook
`silver_bookmark_post_hook` moves pending to `LKHS_filename` only after a
successful run, so a failed run is simply repeated. The bookmark row is only
ever updated in place, so a failure while it is written cannot leave it empty. A
first build starts with an empty bookmark. On a normal run a missing or empty
bookmark stops the run with an error: without it every Bronze file would be
compared with Silver's state again.

**The file log.** `<model>_files` has one row per Bronze file:
`LKHS_filename`, `LKHS_status` and `LKHS_date_inserted`. The new files of a run
are logged as `pending` at compile time and become `processed` in the post-hook
`silver_files_post_hook`. A file that is skipped on purpose is `skipped`. A Silver
table built before the log existed gets one on its next run, with the files then
in the folder up to the bookmark as `seeded`. The log is the audit trail of what
Silver has read, and it is how late files are found.

**Files and validity.** `LKHS_date_valid_from` is the `YYYYMMDD_HHMMSS`
timestamp in the file name (`parse_filename_ts`), so files must be named in
timestamp order. The glob `<entity>_*.json*` matches the `.json`, `.jsonl` and
`.json.jsonl` names that different dlt versions write. A file whose name sorts
before the bookmark is never read. When such a file is in the folder but not in
the log, it arrived late and the run stops (§3.7).

**`CTE_BRONZE`** reads the new files through the Bronze view and adds
`LKHS_hash_value = sha256(<base_for_hash>)`. Bronze selects `DISTINCT` rows
without dlt's `_dlt_*` columns, so an exact duplicate of a row in one file is a
single version.

**The hash.** `generate_base_for_hash` reads the Bronze view's columns at compile
time with `DESCRIBE SELECT * FROM <Bronze view>`, in column order, and leaves out
the primary key and the columns in the dbt variable
`bronze_columns_to_exclude_in_silver_hash` (`LKHS_date_inserted`,
`LKHS_filename`). `LKHS_source_system_code` is included. `DESCRIBE` binds the view
against the files that are in its folder now; `information_schema` would still
list the columns the view had when it was last created, which after archiving can
include a column that no remaining file has (see §3.9). Each column becomes
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
`]##[` falls (`'x]##['`, `'y'` and `'x'`, `']##[y'`).

**Silver's state.** `CTE_SILVER_LATEST` is the latest version of every key in the
Silver table. Its hash is computed again with the same expression, over the
Bronze view's current columns (`silver_state_columns`): a column that Silver does
not have yet counts as NULL. The stored `LKHS_hash_value` is never compared, so a
new source column does not make every key look changed (§3.9).

**Column types.** `read_json_auto` infers each column's type from the files that
are in the folder at the time. Archiving can therefore change a type: a DOUBLE
column whose decimal values were all in archived files is read as BIGINT, and
`3` would no longer hash like Silver's `3.0`. On a normal run `CTE_BRONZE`
therefore casts every column that Silver already has to Silver's type
(`silver_bronze_columns`) before hashing, and dbt logs a warning that names the
columns whose types differ. A value that does not fit Silver's type (text in a
BIGINT column) stops the run: that is a real type change, handled by hand
(§3.9). With `timestampformat='iso'` the DDD dates are text in Bronze and Silver,
so they do not drift.

**`LKHS_` columns per kind of row:**

| Column | `I` / `U` row | `D` against Silver's state | `D` between two new files (full-extract) |
| --- | --- | --- | --- |
| `LKHS_filename` | file of the version | the new file without the key | the next file, without the key |
| `LKHS_date_valid_from` | timestamp of that file | timestamp of that file | timestamp of that file |
| business columns, `LKHS_hash_value` | that version | the latest Silver version's, as stored | the version in the previous new file |
| `LKHS_date_inserted` | start of the dbt run that wrote it | same | same |

**Idempotency guard.** `NOT EXISTS (pk, LKHS_date_valid_from)` against the
existing table drops any version that is already there.

**The delete guard.** A truncated extraction (a page of the API that failed
without an error, an empty export) looks like a mass delete. On a normal run
`CTE_DELETE_GUARD` counts, per new file, the keys it deletes and the live keys it
is compared with. The run stops when a file deletes every live key, or more than
`silver_delete_guard_min_rows` (10) keys that are also more than
`silver_delete_guard_fraction` (0.1) of the live keys. The check runs inside the
insert (DuckDB's `error()`), so nothing is written and the bookmark stays where
it was. The message names the file and the counts:

```text
silver_ddd_afstemning: afstemning_20261023_120000.json would delete 9536 of 10536 live keys
(silver_delete_guard_fraction 0.1, silver_delete_guard_min_rows 10). Check the file; if the
deletes are real, run this model once with --vars '{silver_delete_guard: false}'.
```

When the source really deleted that many rows, run Silver once with the guard
off: `--vars '{silver_delete_guard: false}'` for dbt,
`--no_silver_delete_guard` for `dbt_build_with_unique_logfile`, or
`delete_guard: false` in the Launchpad config of the Silver assets. The
thresholds are dbt variables in `dbt_project.yml`. A first build has no guard:
its deletes come from history that is already known.

### 3.2 Full-extract entities

Every file is a complete snapshot. The new files form a chain that starts at
Silver's state; each snapshot is compared with the one before it:

```text
Silver's state ──► new file 1 ──► new file 2 ──► …
  key not in the previous snapshot           → I   (also a key that comes back after a D)
  key in it, hash differs                     → U
  key in the previous snapshot, not in this   → D   (as of this file)
```

The state contains the keys whose latest version is not a `D`, so a key that
comes back after a delete is not in it and gets an `I`. A key missing from
several consecutive snapshots gets one `D`, at the first. A `D` carries the
content of the version it deletes. A first build starts from an empty state and
runs the same chain over all files.

Example: Silver holds keys A, B and C (all inserted on day 1); two new snapshots
arrive.

| | State | Day 2 | Day 3 | Rows written |
| --- | --- | --- | --- | --- |
| A | present | unchanged | changed | `U3` |
| B | present | missing | back | `D2`, `I3` |
| C | present | missing | missing | `D2` |
| N | – | new | unchanged | `I2` |

### 3.3 Incremental entities

A file only contains changed rows. A key's first version among the new files is
compared with its latest Silver version; a later version in the same run with
the version before it:

```text
key not in Silver, or its latest version is a D   → I
hash differs                                       → U
equal                                              → no row
```

Because the lookback windows overlap, the same unchanged row appears in many
files; equal hashes produce no Silver row, so the overlap costs nothing. A key
can be deleted (§3.4) and come back any number of times, in a plain or a
`_full` file:

```text
F1 key present → I    F2 (_full) key absent → D    F3 (plain) key back, same hash → I
F4 (_full) key absent → D    F5 key back → I
```

The rows in `silver_ddd_sag` for one key after five runs (hashes shortened):

| `LKHS_filename` | `LKHS_date_valid_from` | `LKHS_cdc_operation` | `titel` | `LKHS_hash_value` | Why |
| --- | --- | --- | --- | --- | --- |
| `sag_20260901_020000.json` | 2026-09-01 02:00 | `I` | Lov om A | `3f2a…` | new key |
| `sag_20260902_020000.json` | 2026-09-02 02:00 | `U` | Lov om A, ændret | `9c41…` | title changed |
| `sag_20260903_020000.json` | – | – | – | – | unchanged in the overlap: no row |
| `sag_20260915_020000_full.json` | 2026-09-15 02:00 | `D` | Lov om A, ændret | `9c41…` | missing from the full extraction; the content and hash of the version it deletes |
| `sag_20260920_020000.json` | 2026-09-20 02:00 | `I` | Lov om A, ændret | `9c41…` | back with the same content: `I`, not `U` |

`sag_cv` returns the last row. Gold dimensions read every row and derive
`LKHS_date_valid_to` from the next `LKHS_date_valid_from`.

### 3.4 Deletes of incremental entities (`_full` files)

A record deleted at the source simply stops appearing in incremental files, so
only a full extraction can show a delete. Full extractions of incremental
entities are marked in the file name:

```text
aktoer_20260901_020000.json         incremental, never deletes
aktoer_20260901_020000_full.json    full extraction, holds every key
```

The extraction scripts and the Dagster assets add `_full` when
`date_to_load_from` is `1900-01-01` or earlier (`FULL_EXTRACT_DATE` in
`string_utils.py`). Full-extract entities are not marked: each of their files is
complete, and their deletes come from the snapshots (§3.2).

**The procedure.** A `_full` file must be the only new file:

1. Process the waiting files first (a normal Silver run).
2. Run the full extraction (`--date_to_load_from 1900-01-01`; in Dagster, set
   `date_to_load_from` in the Launchpad config).
3. Run Silver again. Every key whose latest Silver version is not a `D` and that
   is missing from the file gets a `D`, with the file's name and timestamp and the
   content of that latest version. Keys in the file whose latest version is a `D`
   get an `I`.

If a `_full` file is new together with other files, the run stops with an error
that lists them: move the `_full` file aside, run Silver, put it back and run
again. Plain incremental files have no such limit (§3.5).

- A plain file never deletes, even when it happens to hold every key.
- A first build writes no deletes for incremental entities, also when a `_full`
  file is among the files.
- Run full extractions regularly: a deleted key is only seen at the next one.

> **Only a complete extraction may be named `_full`.** A `_full` file deletes every
> key it lacks. The next real full extraction brings those keys back as `I`, so
> `_cv` and Gold are correct again, but the false `D` rows and the `I` rows after
> them stay in the history. To remove them, restore the Silver backup (`duckdb`
> or `ducklake` target) from before that run, take `_full` out of the file name and
> run Silver again. Full extractions from before the `_full` naming have no marker;
> `python -m ddd_python.ddd_utils.mark_full_extracts` (a plan, then `--apply`) finds
> them in the extraction run log or by size. Renaming them has no effect on a
> Silver table that already processed them; it matters for a replay (§3.7).

### 3.5 Several files in one run

Extractions and Silver runs can be interleaved freely; only a `_full` file of an
incremental entity must be the only new file (§3.4). Five files processed in one
run give the same rows as five runs: the full-extract macro compares each new
snapshot with the one before it, starting at Silver's state, and the incremental
macro compares a key's first new version with Silver and the next ones with the
version before them. Example for one key in an incremental entity:

```text
id=42 in Silver: I, hash_0
id=42 in F1: compared with hash_0  → U   (changed)
id=42 in F2: compared with hash_F1 → —   (unchanged)
id=42 in F3: compared with hash_F1 → U   (changed again)
Silver: I, U1, U3 — whether Silver ran once after F3 or after every file
```

For a full-extract entity the chain does the same: in the example of §3.2, a run
over days 2 and 3 together writes `U3`, `D2`, `I3`, `D2` and `I2`, exactly what a
run after day 2 and another after day 3 write. The tests check both paths against
each other and against a first build over all files (§3.11).

### 3.6 Archiving Bronze

Silver reads only the files newer than its bookmark, so processed files are not
needed for any later run. They do cost: every Bronze view opens all files in its
folder, and DuckDB 2.0 reserves 32 MiB of its memory limit for each JSON file it
opens, whatever its size (the reason for the server's `QUACK_SERVER_MEMORY_LIMIT`).
Archiving processed files keeps the views small.

Archiving is a separate step that runs on demand, not in the daily schedule:
`python -m ddd_python.ddd_utils.archive_bronze`, or the Dagster job
`bronze_archive_job` (group `maintenance`).

- Per entity it archives every file older than the newest processed file (the
  bookmark) whose timestamp is older than `--keep-days` (default 7). The newest
  processed file always stays, so the Bronze views have a file to read and the
  bookmark points at a file that exists. Unprocessed files are never touched.
- The command line defaults to a dry run that prints the plan; `--execute` moves
  the files. The Dagster job moves them unless `dry_run` is set in its config.
- Each file is copied to `BRONZE_ARCHIVE_DIR/<source>/<entity>/` (optionally
  gzip-compressed), checked against its SHA-256 and listed in `MANIFEST.ndjson`
  before it is removed from Bronze. `--restore` copies archived files back,
  checked against the manifest.
- Silver now holds the only complete history outside the archive. Back up Silver
  (the platform backup does) and keep the archive off the server.

### 3.7 No rebuild from Bronze

A Silver table is the result of all runs so far, not of the files that happen
to be in Bronze, so it is never rebuilt automatically:

- `--full-refresh` is ignored by every Silver model (`full_refresh=false`).
- A wrong Silver table is repaired by restoring the `duckdb` (or `ducklake`)
  backup from before the problem and running Silver again over the Bronze files
  after it.
- A late file (older than the bookmark, not in the `_files` log) is never read,
  and the run stops with an error that lists it. Usually a later file covers its
  content: the next snapshot of a full-extract entity, or the overlapping
  lookback window of an incremental one. Then log it as skipped with one run with
  `--vars '{silver_skip_late_files: true}'` (`--skip_late_files` for
  `dbt_build_with_unique_logfile`, `skip_late_files: true` in the Launchpad), and
  later runs go on. To include it in the history instead, restore Silver from
  before its timestamp and run again with the file in place. The archive script
  leaves late files where they are.
- A deliberate rebuild is a replay: drop the Silver table, its `_last_file` and its `_files`,
  restore the archived files and run Silver again. Full-extract entities can
  process all files in one first build. Incremental entities must be replayed in
  segments, each `_full` file on its own (§3.4), or their deletes are not
  recreated. `LKHS_date_inserted` then holds the replay's run times.

### 3.8 Why the helper tables are database-qualified

`_last_file` and `_files` are always written as
`{{ this.database }}.{{ this.schema }}.{{ this.name }}_last_file` (and `_files`). In
DuckLake mode the model lives in `ducklake_catalog`, and DuckDB forbids one
transaction from writing to two databases, so the helpers must sit next to the
model. In `duckdb` mode `this.database` is simply the main database.

### 3.9 Changes to the source columns

The hash covers every business column. Silver's latest version is hashed again
with the current columns (§3.1), so:

- **A new field:** the next file gives a `U` only for keys that have a value in
  it; for the others both hashes count it as NULL. A normal run adds the column at
  the end of the Silver table (`on_schema_change='append_new_columns'`).
- **A field the source stops sending:** as long as older files with the field are
  in the Bronze folder, `union_by_name` keeps the column (NULL in the new files),
  and every key that had a value gets a `U`. Once those files are archived, the
  column leaves the Bronze view and is no longer compared; Silver keeps it. The
  same happens to a column that is NULL in every remaining file: dlt writes a
  column only into files where a row has a value (`folketingstidendeurl` of
  `sagstrin` after the first production archive). It comes back with the first
  file that has a value again.
- **A type that Bronze infers differently** (for example after archiving): Bronze
  is cast to Silver's type, so no key looks changed (§3.1).
- Renamed columns and real type changes need a deliberate change of the Silver
  table itself, for example with `ALTER TABLE`, because it is not rebuilt (see
  ADR-2 in [design_decisions.md](design_decisions.md)). A value that does not fit
  Silver's type stops the run until that is done.

### 3.10 All cases at a glance

Results per key.

| Case | Full-extract entities | Incremental entities |
| --- | --- | --- |
| New key | `I` | `I` |
| Key unchanged (also in overlapping lookback windows) | no row | no row |
| Key changed | `U` | `U` |
| Key missing from a file | `D` as of that file, with the content of the last version (§3.2) | no row from a plain file; `D` when a `_full` file that is the only new file lacks it, with the content of the latest Silver version (§3.4) |
| Key still missing after its `D` | no row | no row |
| Key back after a `D`, same or changed content | `I` | `I`, in a plain or a `_full` file (§3.3) |
| Deleted and back several times | every `D` and `I` | every `D` and every return `I` |
| Several new files in one run | same rows as one run per file (§3.5) | same, but a `_full` file must be the only new file (§3.4) |
| `_full` file new together with other files | – | the run stops with an error; nothing is written |
| Repeated without new files | no new rows | no new rows |
| `--full-refresh` | ignored (§3.7) | ignored |
| First build (table does not exist) | all `I`, `U` and `D` from the Bronze files | all `I` and `U`; no deletes |
| `_full` file that is not complete | – | false `D` for every key not in it; the next full extraction brings them back; restore the backup to remove them (§3.4) |
| A file deletes every live key, or more than 10 keys and 10 % of them | the run stops, nothing written; run once with `silver_delete_guard: false` when the deletes are real (§3.1) | same, for a `_full` file |
| Late file, older than the bookmark | the run stops; logged as skipped with `silver_skip_late_files: true`, never read (§3.7) | same |
| Processed Bronze files archived | no effect (§3.6) | no effect |
| Source adds a field | `U` only for keys with a value in it (§3.9) | same |
| Source stops sending a field | `U` for every key that had a value, while older files with the field are in Bronze (§3.9) | same |
| Bronze infers a column type differently | no row: Bronze is cast to Silver's type (§3.1) | same |
| A value that does not fit Silver's column type | the run stops; alter the Silver column (§3.9) | same |
| Same key twice in one file, different content | can give two rows with the same `(key, LKHS_date_valid_from)` | same |

The source must deliver one row per key per file; duplicates are detected, not
handled. The grain test `silver_no_duplicate_id_date_valid_from` covers all 18
DDD Silver tables and a generic test covers `silver_rfam_family`; the other six
Rfam tables have no grain test (improvement 5 in
[design_decisions.md](design_decisions.md#known-limitations-and-possible-improvements)).

### 3.11 How this is verified

[`tests/test_dbt_silver.py`](../tests/test_dbt_silver.py) runs the real Silver
macros with dbt (dbt-core) on copies of the CI sample; every test gets its own Bronze
directory, DuckDB file and DuckLake catalog. It covers every row of §3.10 and
the building blocks of §3.1, for both macros:

| Area | Tests |
| --- | --- |
| Hash: columns, order, NULL token, delimiter | `test_hash_is_sha256_of_the_business_columns_in_bronze_order`, `test_hash_null_token_and_delimiter` |
| File names, bookmark, file log, duplicate rows, `LKHS_` columns | `test_file_name_variants_and_bookmark`, `test_identical_rows_in_one_file_are_one_version`, `test_source_system_code_and_date_inserted` |
| Full-extract lifecycle (`sagskategori`): new, unchanged, changed, missing, missing twice in a row, back unchanged and changed, two delete cycles; one run per file, four files in one normal run, a first build with all files, an ignored full refresh; DuckDB and DuckLake | `test_full_extract_*` |
| Incremental lifecycle (`stemme`, Rfam `family`): the same with deletes from `_full` files, each processed on its own, a repeated run and an ignored full refresh; DuckDB and DuckLake | `test_incremental_*` |
| Incremental deletes: a first build writes none, a plain file never deletes, the file name and content of a delete, a key back before the next `_full` file, several plain files in one run with returning keys, a `_full` file new together with another file stops the run, a `_full` file marked by mistake | `test_a_first_build_writes_no_deletes`, `test_a_plain_file_never_deletes_even_when_it_is_complete`, `test_new_delete_has_the_full_file_and_the_deleted_content`, `test_a_key_that_is_back_before_the_next_full_file_is_not_deleted`, `test_several_plain_files_in_one_run`, `test_a_full_file_with_other_files_to_process_stops_the_run`, `test_a_full_file_marked_by_mistake_is_stopped_by_the_delete_guard` |
| Delete guard (§3.1): a truncated snapshot on its own and after a good file in the same run, the guard turned off | `test_a_truncated_snapshot_stops_the_run`, `test_a_full_file_marked_by_mistake_is_stopped_by_the_delete_guard` |
| Late files (§3.7): the run stops, then skips them with `silver_skip_late_files` | `test_a_late_file_stops_the_run_until_it_is_skipped` |
| A Silver table from an older version: old `_last_file` columns converted, the log seeded | `test_a_silver_table_from_an_older_version_is_migrated` |
| Column types (§3.1): a type Bronze infers differently after archiving, a value that does not fit | `test_a_type_bronze_infers_differently_after_archiving_gives_no_false_updates`, `test_a_value_that_does_not_fit_silvers_type_stops_the_run` |
| Archiving (§3.6): processed files removed, later files compared with Silver, deleted keys back with old or changed content, an ignored full refresh; DuckDB and DuckLake | `test_processed_files_can_be_archived` |
| Source column changes (§3.9), also in a `D` row read from Silver | `test_new_source_column_then_full_refresh`, `test_dropped_source_field`, `test_a_delete_lines_up_with_a_new_source_column` |
| A key twice in one file | `test_same_key_twice_in_one_file` |

Besides the expected histories, the tests assert these invariants of a Silver
table: no two versions with the same `(key, LKHS_date_valid_from)`; the
timestamp in `LKHS_filename` equals `LKHS_date_valid_from`; a `D` carries the
hash of the version it deletes; a `U` differs from the version before it; no
`D` directly after a `D` and no `I` directly after an `I` or `U`; every key
starts with `I`; `LKHS_source_system_code` is correct; `_cv` holds exactly the
newest version of every key. The full-extract lifecycle also checks that one
run per file, several files in one run and a first build give identical tables.
Run them with `DBT_INTEGRATION_TESTS=1 pytest tests/test_dbt_silver.py` (about
20 minutes); CI runs them in the `dbt build (sample data)` job.

**History.** Until this design, Silver was derived from all Bronze files on
every run: each version was compared with the key's previous version in Bronze,
a full refresh rebuilt the tables, and Bronze could only be pruned down to a
complete file. The test run of commit `498482d` from that time, and the defects
it found in the macros before it, are recorded in
[silver_test_results.md](silver_test_results.md); several test names there no
longer exist.

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
| `generate_base_for_hash(table_name, columns_to_exclude, primary_key_columns)` | Silver models | builds the `CONCAT(COALESCE(col::VARCHAR,'<NULL>'),']##[', …)` expression from the Bronze view's current columns (`DESCRIBE`) |
| `generate_model_silver_full_extraction(file_name, bronze_table_name, primary_key_columns, base_for_hash, data_source_env_var)` | full-extract Silver models | compares the new snapshots with Silver's state and with each other: I/U/D (§3.2); `full_refresh=false` |
| `generate_model_silver_incr_extraction(…same…)` | incremental Silver models | compares a key's first new version with its latest Silver version and later ones with the version before them: I/U; deletes from a `_full` file that is the only new file, and stops the run when it is not (§3.3–3.4); `full_refresh=false` |
| `silver_files(file_name, data_source_env_var)` | both Silver macros | at compile time: reads the bookmark, stops on late files, lists the newer Bronze files, notes the newest as pending and logs the files (§3.1) |
| `silver_bookmark_post_hook()`, `silver_files_post_hook()` | both Silver macros (post-hooks) | after a successful run: move the pending file to the bookmark, mark the pending files processed |
| `silver_bronze_columns(bronze_table_name)` | both Silver macros | select list over the Bronze view that casts the columns Silver already has to Silver's type (normal runs) |
| `silver_state_columns(bronze_table_name)` | both Silver macros | select list over the Silver table with the Bronze view's columns (missing ones as NULL), to hash Silver's latest version |
| `silver_delete_guard()` | both Silver macros | the condition that stops a normal run when a file deletes too many keys (§3.1) |
| `parse_filename_ts(col)` | Silver macros | `strptime(regexp_extract(col, '([0-9]{8}_[0-9]{6})', 1), '%Y%m%d_%H%M%S')` |
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

The statements DuckDB runs after dbt compiles the macros, for a normal run (the
table and its bookmark exist). Long column lists are shortened to `...`, file
names are examples, and the database prefix of Silver objects is omitted. Before
the SQL is generated, `silver_files` has listed the new files with `glob()` and
written the newest as pending into `_last_file` and logged the files as pending
in `_files`.

<details>
<summary><strong>Full-extract entity — <code>silver_ddd_aktoertype</code></strong></summary>

**Main query** (bookmark `aktoertype_20260101_000000.json`, new files up to
`aktoertype_20260103_000000.json`):

```sql
WITH CTE_FILES AS (                     -- the new files, in order
    SELECT LKHS_filename, <timestamp in the name> AS LKHS_date_valid_from
    ,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
    ,      LEAD(LKHS_filename) OVER (ORDER BY LKHS_filename) AS LKHS_filename_next
    ,      LEAD(<timestamp>)   OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_next
    FROM  (SELECT regexp_extract(file, '[^/]+$') AS LKHS_filename
           FROM   glob('.../aktoertype/aktoertype_*.json*')) files
    WHERE  LKHS_filename > 'aktoertype_20260101_000000.json'
    AND    LKHS_filename <= 'aktoertype_20260103_000000.json'
)
,CTE_BRONZE AS (
    SELECT src.*, sha256(CONCAT(COALESCE(type::VARCHAR,'<NULL>'),']##[', ...)) AS LKHS_hash_value
    FROM  (SELECT "id", "type", ...          -- a column whose Bronze type differs: CAST("col" AS <Silver type>)
           FROM   main_bronze.bronze_ddd_aktoertype
           WHERE  LKHS_filename > 'aktoertype_20260101_000000.json'
           AND    LKHS_filename <= 'aktoertype_20260103_000000.json') src
)
,CTE_SILVER_LATEST AS (
    SELECT * FROM main_silver.silver_ddd_aktoertype
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY LKHS_date_valid_from DESC) = 1
)
,CTE_STATE AS (                         -- live keys, hashed again with the current columns
    SELECT id, sha256(CONCAT(COALESCE(type::VARCHAR,'<NULL>'),']##[', ...)) AS LKHS_hash_value
    FROM  (SELECT "id", "type", ..., "LKHS_source_system_code"
           FROM   CTE_SILVER_LATEST WHERE LKHS_cdc_operation != 'D') state
)
,CTE_PREVIOUS AS (                      -- what each new file is compared with
    SELECT id, LKHS_hash_value, LKHS_filename FROM CTE_BRONZE
    UNION ALL
    SELECT id, LKHS_hash_value, '<silver>' AS LKHS_filename FROM CTE_STATE
)
,CTE_CHAIN AS (
    SELECT CTE_FILES.*, COALESCE(LKHS_filename_previous, '<silver>') AS LKHS_filename_compared_with
    FROM   CTE_FILES
)
,CTE_CHANGES AS (
    SELECT cur.* EXCLUDE (LKHS_filename), cur.LKHS_filename, chain.LKHS_date_valid_from
    ,      CAST('2026-03-16 06:43:00.302' AS DATETIME) AS LKHS_date_inserted
    ,      CASE WHEN prev.id IS NULL THEN 'I'
                WHEN cur.LKHS_hash_value != prev.LKHS_hash_value THEN 'U' END AS LKHS_cdc_operation
    FROM       CTE_BRONZE cur
    INNER JOIN CTE_CHAIN chain   ON chain.LKHS_filename = cur.LKHS_filename
    LEFT JOIN  CTE_PREVIOUS prev ON prev.LKHS_filename = chain.LKHS_filename_compared_with AND prev.id = cur.id
    WHERE      LKHS_cdc_operation IN ('I','U')
)
,CTE_DELETES AS (                       -- in a new file, not in the next one
    SELECT cur.* EXCLUDE (LKHS_filename), chain.LKHS_filename_next AS LKHS_filename
    ,      chain.LKHS_date_valid_from_next AS LKHS_date_valid_from
    ,      CAST('2026-03-16 06:43:00.302' AS DATETIME) AS LKHS_date_inserted, 'D' AS LKHS_cdc_operation
    FROM       CTE_BRONZE cur
    INNER JOIN CTE_CHAIN chain ON chain.LKHS_filename = cur.LKHS_filename
    WHERE      chain.LKHS_filename_next IS NOT NULL
    AND        NOT EXISTS (SELECT 1 FROM CTE_BRONZE nxt
                           WHERE nxt.LKHS_filename = chain.LKHS_filename_next AND nxt.id = cur.id)
)
,CTE_STATE_DELETES AS (                 -- live in Silver, not in the first new file
    SELECT latest.* EXCLUDE (LKHS_filename, LKHS_date_valid_from, LKHS_date_inserted, LKHS_cdc_operation)
    ,      first_file.LKHS_filename, first_file.LKHS_date_valid_from
    ,      CAST('2026-03-16 06:43:00.302' AS DATETIME) AS LKHS_date_inserted, 'D' AS LKHS_cdc_operation
    FROM       CTE_SILVER_LATEST latest
    CROSS JOIN (SELECT LKHS_filename, LKHS_date_valid_from FROM CTE_CHAIN
                WHERE LKHS_filename_previous IS NULL) first_file
    WHERE      latest.LKHS_cdc_operation != 'D'
    AND        NOT EXISTS (SELECT 1 FROM CTE_BRONZE b
                           WHERE b.LKHS_filename = first_file.LKHS_filename AND b.id = latest.id)
)
,CTE_ALL_ROWS AS (
    SELECT * FROM CTE_CHANGES
    UNION ALL BY NAME SELECT * FROM CTE_DELETES
    UNION ALL BY NAME SELECT * FROM CTE_STATE_DELETES
)
,CTE_DELETE_GUARD AS (                     -- per new file: deletes and the live keys before it
    SELECT chain.LKHS_filename, COALESCE(deletes.n, 0) AS LKHS_deletes, COALESCE(keys_before.n, 0) AS LKHS_keys_before
    FROM      CTE_CHAIN chain
    LEFT JOIN (SELECT LKHS_filename, COUNT(*) AS n FROM CTE_ALL_ROWS
               WHERE LKHS_cdc_operation = 'D' GROUP BY LKHS_filename) deletes
    ON        deletes.LKHS_filename = chain.LKHS_filename
    LEFT JOIN (SELECT LKHS_filename, COUNT(*) AS n FROM CTE_PREVIOUS GROUP BY LKHS_filename) keys_before
    ON        keys_before.LKHS_filename = chain.LKHS_filename_compared_with
)
SELECT CTE_ALL_ROWS.* FROM CTE_ALL_ROWS
WHERE NOT EXISTS (SELECT id FROM main_silver.silver_ddd_aktoertype
                  WHERE id = CTE_ALL_ROWS.id AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from)
AND (SELECT bool_and(CASE WHEN g.LKHS_deletes > 0
                           AND (g.LKHS_deletes >= g.LKHS_keys_before
                                OR (g.LKHS_deletes > 10 AND g.LKHS_deletes > 0.1 * g.LKHS_keys_before))
                          THEN error(printf('...: %s would delete %d of %d live keys ...', ...))
                          ELSE TRUE END)
     FROM CTE_DELETE_GUARD g) IS NOT FALSE
```

**Post-hooks** — the run succeeded, so the pending file becomes the bookmark and the
pending files are processed:

```sql
UPDATE main_silver.silver_ddd_aktoertype_last_file
SET    LKHS_filename = LKHS_filename_pending, LKHS_date_inserted = CAST('2026-03-16 06:43:00.302' AS DATETIME)
WHERE  LKHS_filename_pending IS NOT NULL AND LKHS_filename_pending != '';

UPDATE main_silver.silver_ddd_aktoertype_files
SET    LKHS_status = 'processed', LKHS_date_inserted = CAST('2026-03-16 06:43:00.302' AS DATETIME)
WHERE  LKHS_status = 'pending';
```

</details>

<details>
<summary><strong>Incremental entity — <code>silver_ddd_aktoer</code></strong></summary>

`CTE_FILES`, `CTE_BRONZE`, `CTE_SILVER_LATEST` and the post-hook are as above.
The delete branch is only generated when the only new file is a `_full` file (here
`aktoer_20261101_020000_full.json`); a `_full` file among other new files stops
the run before any SQL runs.

```sql
,CTE_NEW AS (                           -- each key's versions in the new files
    SELECT CTE_BRONZE.*, CTE_FILES.LKHS_date_valid_from
    ,      LAG(CTE_BRONZE.LKHS_hash_value) OVER (PARTITION BY id ORDER BY CTE_BRONZE.LKHS_filename) AS LKHS_hash_value_previous
    ,      ROW_NUMBER() OVER (PARTITION BY id ORDER BY CTE_BRONZE.LKHS_filename) AS LKHS_appearance
    FROM   CTE_BRONZE INNER JOIN CTE_FILES ON CTE_BRONZE.LKHS_filename = CTE_FILES.LKHS_filename
)
,CTE_STATE AS (                         -- every key's latest version, hashed with the current columns
    SELECT id, LKHS_cdc_operation AS LKHS_latest_operation
    ,      sha256(CONCAT(COALESCE(typeid::VARCHAR,'<NULL>'),']##[', ...)) AS LKHS_hash_value
    FROM  (SELECT "id", "typeid", ..., LKHS_cdc_operation FROM CTE_SILVER_LATEST) state
)
,CTE_CHANGES AS (
    SELECT n.* EXCLUDE (LKHS_hash_value_previous, LKHS_appearance, LKHS_date_valid_from), n.LKHS_date_valid_from
    ,      CAST('2026-03-15 21:54:17.020' AS DATETIME) AS LKHS_date_inserted
    ,      CASE
               WHEN n.LKHS_appearance > 1 THEN CASE WHEN n.LKHS_hash_value != n.LKHS_hash_value_previous THEN 'U' END
               WHEN state.id IS NULL OR state.LKHS_latest_operation = 'D' THEN 'I'
               WHEN n.LKHS_hash_value != state.LKHS_hash_value THEN 'U'
           END AS LKHS_cdc_operation
    FROM   CTE_NEW n LEFT JOIN CTE_STATE state ON state.id = n.id
    WHERE  LKHS_cdc_operation IN ('I','U')
)
,CTE_NEW_DELETES AS (                   -- only for a _full file that is the only new file
    SELECT latest.* EXCLUDE (LKHS_filename, LKHS_date_valid_from, LKHS_date_inserted, LKHS_cdc_operation)
    ,      'aktoer_20261101_020000_full.json' AS LKHS_filename, <its timestamp> AS LKHS_date_valid_from
    ,      CAST('2026-03-15 21:54:17.020' AS DATETIME) AS LKHS_date_inserted, 'D' AS LKHS_cdc_operation
    FROM   CTE_SILVER_LATEST latest
    WHERE  latest.LKHS_cdc_operation != 'D'
    AND    NOT EXISTS (SELECT 1 FROM CTE_BRONZE full_file
                       WHERE full_file.LKHS_filename = 'aktoer_20261101_020000_full.json' AND full_file.id = latest.id)
)
,CTE_ALL_ROWS AS (SELECT * FROM CTE_CHANGES UNION ALL BY NAME SELECT * FROM CTE_NEW_DELETES)
,CTE_DELETE_GUARD AS (
    SELECT 'aktoer_20261101_020000_full.json' AS LKHS_filename
    ,      (SELECT COUNT(*) FROM CTE_NEW_DELETES) AS LKHS_deletes
    ,      (SELECT COUNT(*) FROM CTE_SILVER_LATEST WHERE LKHS_cdc_operation != 'D') AS LKHS_keys_before
)
SELECT CTE_ALL_ROWS.* FROM CTE_ALL_ROWS
WHERE NOT EXISTS (SELECT id FROM main_silver.silver_ddd_aktoer
                  WHERE id = CTE_ALL_ROWS.id AND LKHS_date_valid_from = CTE_ALL_ROWS.LKHS_date_valid_from)
AND (SELECT bool_and(CASE WHEN g.LKHS_deletes > 0
                           AND (g.LKHS_deletes >= g.LKHS_keys_before
                                OR (g.LKHS_deletes > 10 AND g.LKHS_deletes > 0.1 * g.LKHS_keys_before))
                          THEN error(printf('...: %s would delete %d of %d live keys ...', ...))
                          ELSE TRUE END)
     FROM CTE_DELETE_GUARD g) IS NOT FALSE        -- only with a _full file
```

</details>
