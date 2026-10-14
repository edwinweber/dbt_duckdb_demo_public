# Silver Layer: Test Cases and Results

Every test of the Silver CDC logic, what it checks, the expected result and the
result of a run on 2026-10-13. The logic itself is described in
[dbt_transformations.md §3](dbt_transformations.md#3-silver-cdc-into-scd-type-2);
section numbers (§3.x) below refer to that document.

## Summary

| Module | What it runs | Tests | Current macros (`498482d`) | Macros before the fix (`2dc3b83`) |
| --- | --- | --- | --- | --- |
| [`test_dbt_silver.py`](../tests/test_dbt_silver.py) | the real macros, with dbt-oss on copies of the CI sample | 91 | **91 passed** | 66 passed, **25 failed** |
| [`test_integration_silver_cdc.py`](../tests/test_integration_silver_cdc.py) | hand-written copies of the macro SQL in DuckDB, no dbt | 32 | **32 passed** | not run (the mirrors follow the new SQL) |
| **Total** | | **123** | **123 passed** in 245 s | |

**Environment:** dbt-oss 2.0.5, DuckDB 1.5.6, Python 3.13.16, pytest 9.1.1, WSL2.
The macros in `dbt/macros/` are those of commit `498482d` (no local changes);
the test modules are the working-tree versions (`test_dbt_silver.py` is the
renamed and extended `test_dbt_silver_incremental_deletes.py`).

**Commands:**

```bash
DBT_INTEGRATION_TESTS=1 pytest tests/test_dbt_silver.py tests/test_integration_silver_cdc.py -v

# the same dbt tests against the macros before the fix
git archive 2dc3b83 | tar -x -C <dir>
cp -r dbt/dbt_packages <dir>/dbt/ && cp tests/test_dbt_silver.py <dir>/tests/
cd <dir> && DBT_INTEGRATION_TESTS=1 pytest tests/test_dbt_silver.py -o addopts=''
```

A passing test means the observed value equals the expected value in the
tables below; the tests compare them exactly.

## How to read the histories

Bronze files are named after their day in January 2026
(`stemme_20260103_000000.json` = day 3). A history lists every Silver row of a
key as operation + day of `LKHS_date_valid_from`:

- `I1 U2 U4`: inserted from file 1, updated from files 2 and 4.
- `I1 D2 I3`: inserted, deleted as of file 2, back from file 3.
- empty: the key has no Silver rows.

## Entities used

| Entity | Source | Kind | Primary key | How a test changes a row |
| --- | --- | --- | --- | --- |
| `sagskategori` | DDD | full extract | `id` | appends the text "(changed)" to `kategori` |
| `periode` | DDD | full extract | `id` | appends the text "(changed)" to `titel` |
| `author` | RFAM | full extract, no date column | `author_id` | appends the text "(changed)" to `name` |
| `stemme` | DDD | incremental | `id` | adds 100 to `typeid` |
| `family` | RFAM | incremental | `rfam_acc` | appends the text "(changed)" to `description` |

Every test starts from a copy of the CI sample in
[`tests/fixtures/bronze_sample`](../tests/fixtures/bronze_sample) and gets its
own Bronze directory, DuckDB file and DuckLake catalog.

## Silver invariants

Most tests also check that a Silver table breaks none of these rules (each
count must be 0):

| Invariant | Rows that break it |
| --- | --- |
| one row per `(key, LKHS_date_valid_from)` | duplicate versions |
| the timestamp in `LKHS_filename` equals `LKHS_date_valid_from` | rows with another file's name |
| a `D` carries the hash of the version it deletes | `D` with a different hash |
| a `U` differs from the version before it | `U` with an unchanged hash (except after a `D`) |
| no `D` directly after a `D` | double deletes |
| no `I` directly after an `I` or `U` | double inserts |
| every key starts with `I` | first version `U` or `D` |
| `LKHS_source_system_code` is `DDD` or `RFAM` as expected | wrong code |
| `_cv` holds exactly the newest version of every key | missing, extra or older rows in `_cv` |

---

## 1. Building blocks (§3.1)

| # | Test | Entity | What it checks | Expected | Current | Before fix |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `test_hash_is_sha256_of_the_business_columns_in_bronze_order` | sagskategori | `LKHS_hash_value` = `sha256(CONCAT(COALESCE(col::VARCHAR,'<NULL>'),']##[', …))` over the Bronze columns except the key, `LKHS_filename` and `LKHS_date_inserted` | hashed columns `kategori, opdateringsdato, LKHS_source_system_code`; 0 hash mismatches; one row per sample row | pass | pass |
| 2 | `test_hash_null_token_and_delimiter` | periode | NULL vs `''`, NULL vs the text `<NULL>`, the delimiter moved between two columns | `''`→NULL: `I1 U2`; NULL→`'<NULL>'`: `I1` (same hash); `'x]##['`,`'y'` → `'x'`,`']##[y'`: `I1` (same hash) | pass | pass |
| 3 | `test_file_name_variants_and_bookmark` | sagskategori | `.json`, `.jsonl` and `.json.jsonl` files are all read; `_last_file` | after build `_last_file` = (day 1, NULL); histories `I1 U2` and `I1 U3`; `U3` has file name `…_20260103_000000.json.jsonl`; `_last_file` = (day 3 `.json.jsonl`, day 2 `.jsonl`); no invariant broken | pass | pass |
| 4 | `test_identical_rows_in_one_file_are_one_version` | sagskategori | an exact duplicate row in a file (different `_dlt_id`) | `I1`, no extra version | pass | pass |
| 5 | `test_source_system_code_and_date_inserted` | sagskategori | `LKHS_date_inserted` = start of the dbt run; code | one value per run, the first run's value is kept; code `DDD` | pass | pass |
| 6 | `test_date_inserted_src_with_a_date_column` | sagskategori | `LKHS_date_inserted_src` with a date column | `I`, `U`; both rows have the earliest `opdateringsdato` of the key | pass | pass |
| 7 | `test_date_inserted_src_without_a_date_column` | author | `LKHS_date_inserted_src` without a date column | a key present from day 1 has day 1; a key that first appears on day 2 has `I` with day 2 | pass | pass |

## 2. Full-extract lifecycle (§3.2, §3.5)

Five complete files of `sagskategori`. Eight keys, named after their
behaviour:

| Key | What happens | Expected after day 1 | 2 | 3 | 4 | 5 |
| --- | --- | --- | --- | --- | --- | --- |
| A | unchanged | `I1` | `I1` | `I1` | `I1` | `I1` |
| B | missing on day 2, back unchanged on day 3 | `I1` | `I1 D2` | `I1 D2 I3` | `I1 D2 I3` | `I1 D2 I3` |
| C | missing on day 2, back changed on day 3 | `I1` | `I1 D2` | `I1 D2 I3` | `I1 D2 I3` | `I1 D2 I3` |
| D | missing from day 2 on | `I1` | `I1 D2` | `I1 D2` | `I1 D2` | `I1 D2` |
| E | changed on days 2 and 4 | `I1` | `I1 U2` | `I1 U2` | `I1 U2 U4` | `I1 U2 U4` |
| G | missing on days 3 and 4, back on day 5 | `I1` | `I1` | `I1 D3` | `I1 D3` | `I1 D3 I5` |
| J | missing on days 2 and 4, back on days 3 and 5 | `I1` | `I1 D2` | `I1 D2 I3` | `I1 D2 I3 D4` | `I1 D2 I3 D4 I5` |
| N | new on day 2 | – | `I2` | `I2` | `I2` | `I2` |

The files are processed in three ways, each in DuckDB and in DuckLake mode:

- **one run per file:** build on day 1, a normal run after each of days 2–5
  (histories checked after every day), then a repeat run without new files;
- **days 2–5 in one normal run:** build on day 1, then one run over four new files;
- **first build with all files:** one build with all five files present.

Each path ends with a `--full-refresh`.

| # | Test | What it checks | duckdb: per file | duckdb: 2–5 in one run | duckdb: first build | ducklake: per file | ducklake: 2–5 in one run | ducklake: first build |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 8 | `test_full_extract_histories` | histories as in the table above | pass | pass | pass | pass | pass | pass |
| 9 | `test_full_extract_invariants` | no invariant broken (after every day for "per file") | pass | pass | pass | pass | pass | pass |
| 10 | `test_full_extract_data_tests_pass` | the model's dbt data tests pass | pass | pass | pass | pass | pass | pass |

| # | Test | What it checks | duckdb | ducklake |
| --- | --- | --- | --- | --- |
| 11 | `test_full_extract_delete_row_takes_the_file_without_the_key` | D's `D` row: day 2, `LKHS_filename` = `sagskategori_20260102_000000.json` | pass | pass |
| 12 | `test_full_extract_cv_shows_deleted_keys` | `_cv` returns `D` for the deleted key | pass | pass |
| 13 | `test_full_extract_repeat_run_and_full_refresh_change_nothing` | a repeat run and a full refresh leave the table unchanged (all columns except `LKHS_date_inserted`) | pass | pass |
| 14 | `test_full_extract_paths_give_identical_tables` | the three paths give identical tables | pass | pass |

All 26 full-extract lifecycle tests also passed on the macros before the fix.

## 3. Incremental lifecycle (§3.3, §3.4, §3.5)

`stemme` and Rfam `family`. Day 1 is complete; day 2 is complete and
followed by a `--full-refresh`; day 3 is an incremental file (only changed or
returning keys) with a normal run; day 4 is complete with a `--full-refresh`;
day 5 is complete with a normal run. The run ends with another full refresh.

| Key | What happens | Expected after day 2 | 3 | 4 | 5 (and after the final full refresh) |
| --- | --- | --- | --- | --- | --- |
| A | missing on day 2, back unchanged on day 3 | `I1 D2` | `I1 D2 I3` | `I1 D2 I3` | `I1 D2 I3` |
| B | missing on day 2, back changed on day 3 | `I1 D2` | `I1 D2 I3` | `I1 D2 I3` | `I1 D2 I3` |
| C | missing on day 2, back on day 3, missing on day 4 | `I1 D2` | `I1 D2 I3` | `I1 D2 I3 D4` | `I1 D2 I3 D4` |
| D | missing from day 2 on | `I1 D2` | `I1 D2` | `I1 D2` | `I1 D2` |
| E | changed on day 3 | `I1` | `I1 U3` | `I1 U3` | `I1 U3` |
| G | missing on day 2, back on day 3, changed on day 5 | `I1 D2` | `I1 D2 I3` | `I1 D2 I3` | `I1 D2 I3 U5` |
| H | never changes | `I1` | `I1` | `I1` | `I1` |
| J | two delete/return cycles | `I1 D2` | `I1 D2 I3` | `I1 D2 I3 D4` | `I1 D2 I3 D4 I5` |

Three paths, for `stemme` in DuckDB and DuckLake and for `family` in DuckDB:

- **one run per file:** as above, with a repeat run after day 3;
- **days 3+4 in one full refresh:** files 3 and 4 arrive together and are
  processed by the day-4 full refresh;
- **days 3+4 in one normal run:** files 3 and 4 are processed by one normal
  run, then the full refresh.

Results, current macros / macros before the fix:

| # | Test | What it checks | duckdb-stemme | ducklake-stemme | duckdb-family |
| --- | --- | --- | --- | --- | --- |
| 15 | `test_incremental_histories` [per file] | histories after every step | pass / **fail** | pass / **fail** | pass / **fail** |
| 15 | `test_incremental_histories` [3+4 in one full refresh] | | pass / **fail** | pass / **fail** | pass / **fail** |
| 15 | `test_incremental_histories` [3+4 in one normal run] | | pass / **fail** | pass / **fail** | pass / **fail** |
| 16 | `test_incremental_invariants` [per file] | no invariant broken after every step | pass / **fail** | pass / **fail** | pass / **fail** |
| 16 | `test_incremental_invariants` [3+4 in one full refresh] | | pass / **fail** | pass / **fail** | pass / **fail** |
| 16 | `test_incremental_invariants` [3+4 in one normal run] | | pass / **fail** | pass / **fail** | pass / **fail** |
| 17 | `test_incremental_data_tests_pass` [each of the 3 paths] | the model's dbt data tests pass | pass / pass | pass / pass | pass / pass |
| 18 | `test_incremental_carried_deletes_keep_their_date_inserted` | a carried `D` row keeps the `LKHS_date_inserted` of the run that wrote it | pass / pass | pass / pass | pass / pass |
| 19 | `test_incremental_repeat_run_and_full_refresh_change_nothing` | a repeat run and a full refresh leave the table unchanged | pass / **fail** | pass / **fail** | pass / **fail** |
| 20 | `test_incremental_paths_give_identical_tables` | the three paths give identical tables | pass / pass | pass / pass | pass / pass |

Row 17 covers three tests per column; in total this section has 36 tests: 36
passed now, 21 failed before the fix.

## 4. Incremental full refresh details (§3.4)

| # | Test | Scenario | Expected | Current | Before fix |
| --- | --- | --- | --- | --- | --- |
| 21 | `test_full_refresh_of_a_new_table_writes_no_deletes` | first build with `--full-refresh`; the newest file lacks key k | `I1`, no `D` (there is no Silver to compare with) | pass | pass |
| 22 | `test_new_delete_has_the_newest_file_and_the_deleted_content` | k changed in incremental file 2, missing from complete file 3, full refresh | `I1 U2 D3`; `D` has file name `stemme_20260103_000000.json` and the hash of `U2` | pass | **fail**: file name was `stemme_20260102_000000.json` |
| 23 | `test_false_deletes_and_their_repair` | runbook §7: full refresh on an incremental file with 5 keys, then a full extraction and a normal run, then the false `D` rows are deleted and a full refresh runs | after the false refresh: `I1 D2` for every key not in the file; after the full extraction: `I1 D2 I3`, no `D` in `_cv`; after the repair: `I1`, no rows on day 3, no invariant broken | pass | **fail**: the key stayed `I1 D2` after the full extraction, so the repair did not work |

## 5. Files that arrive late (§3.8)

| # | Test | Scenario | Expected | sagskategori | stemme |
| --- | --- | --- | --- | --- | --- |
| 24 | `test_late_file_newer_than_the_bookmarks_predecessor` | files 1 and 3 processed (`_last_file` = 3, predecessor 1); file 2 with a change arrives late | `I1 U2 U3`, no invariant broken | pass | pass |
| 25 | `test_late_file_older_than_the_bookmarks_predecessor` | files 1, 3, 5 processed (`_last_file` = 5, predecessor 3); file 2 arrives | after the normal run `I1 U3`, exactly one `U` with an unchanged hash (the known limitation); after a full refresh `I1 U2 U3`, no invariant broken | pass | pass |

All four also passed before the fix.

## 6. Pruning Bronze files (§3.6)

| # | Test | Scenario | Expected | Current | Before fix |
| --- | --- | --- | --- | --- | --- |
| 26 | `test_full_extract_pruning` [delete file 1] | sagskategori, files 1–4 processed (`_last_file` = 4, predecessor 3); file 5 changes k | `I1 U5`: keeping files 2, 3, 4 is enough | pass | pass |
| 27 | `test_full_extract_pruning` [delete files 1, 2] | the same, but file 2 (the file before the predecessor) is also deleted | `I1 I3 U5`: every key gets an extra `I` on day 3 | pass | pass |
| 28 | `test_incremental_pruning_down_to_a_full_extraction` [full extraction older than the predecessor] | stemme: file 2 incremental, file 3 a full extraction, files 4 and 5 incremental; files 1 and 2 deleted; file 6 changes k and w | k `I1 U2 U6`, w `I1 U6`; no extra `I` on day 3 | pass | pass |
| 29 | `test_incremental_pruning_down_to_a_full_extraction` [full extraction is the predecessor] | the same with only file 4 after the full extraction | k `I1 U2 I3 U6`, w `I1 I3 U6`; every key gets an `I` on day 3 | pass | pass |
| 30 | `test_incremental_pruning_without_a_full_extraction` | stemme: files 2 and 3 incremental; file 1 deleted; file 4 changes z; file 5 is a full extraction | z `I1 I4` (`I` instead of `U`); w `I1 I5` (a second `I` with unchanged content); day 5 has an `I` for every key except the two in file 3 | pass | pass |
| 31 | `test_full_refresh_after_pruning_keeps_only_the_remaining_history` [sagskategori] | x changes on days 2 and 3, `deleted` missing from day 2, `gone` missing from day 3; files 1 and 2 deleted; full refresh | x `I3`, other keys `I3`; `deleted` and `gone` have no rows (their deletes came from pruned file pairs) | pass | pass |
| 32 | `test_full_refresh_after_pruning_keeps_only_the_remaining_history` [stemme] | the same for an incremental entity | x `I3`, other keys `I3`; `deleted` keeps only `D2` (carried from Silver); `gone` gets `D4` (missing from the newest file) | pass | pass |

## 7. Changes to the source columns (§3.9)

| # | Test | Scenario | Expected | Current | Before fix |
| --- | --- | --- | --- | --- | --- |
| 33 | `test_new_source_column_then_full_refresh` [stemme, duckdb] | file 2 adds `_test_marker` for 5 keys (normal run); file 3 complete without key m, full refresh; then a second full refresh | after file 2: column at the end of Silver; marked `I1 U2`, unmarked `I1`; after the full refresh: m `I1 D3`, column in Bronze position (before `LKHS_filename`), all hashes recomputed, no invariant broken; second full refresh unchanged; dbt data tests pass | pass | **fail**: 1 `D` row with the wrong file name, the wrong hash and the wrong source system code; `_cv` off by 2 rows |
| 34 | `test_new_source_column_then_full_refresh` [stemme, ducklake] | same in DuckLake mode | same | pass | **fail**: same as 33 |
| 35 | `test_new_source_column_then_full_refresh` [sagskategori, duckdb] | same for a full-extract entity | same | pass | pass |
| 36 | `test_dropped_source_field` [sagskategori] | file 2 lacks `kategori` | a `U` on day 2 for every key; a full refresh leaves the table unchanged; no invariant broken | pass | pass |
| 37 | `test_dropped_source_field` [stemme] | file 2 lacks `typeid` | same | pass | pass |

## 8. The same key twice in one file (§3.10)

The source must deliver one row per key per file; this is detected, not
handled.

| # | Test | Scenario | Expected | Current | Before fix |
| --- | --- | --- | --- | --- | --- |
| 38 | `test_same_key_twice_in_one_file` [stemme, both copies changed] | file 2 has two different changed copies of one key | 1 duplicate `(key, LKHS_date_valid_from)`; the dbt grain test of `silver_ddd_stemme` fails | pass | pass |
| 39 | `test_same_key_twice_in_one_file` [sagskategori, one copy unchanged] | one copy equals file 1, the other is changed | 0 duplicates (the unchanged copy gives no row) | pass | pass |
| 40 | `test_same_key_twice_in_one_file` [sagskategori, both copies changed] | two different changed copies | 1 duplicate | pass | pass |

---

## 9. The 25 failures on the macros before the fix

Every failure is in the delete handling of incremental entities; every
full-extract test passed on both versions. What the old macros produced:

| Defect | Failing tests | Observed with the old macros |
| --- | --- | --- |
| 1. A key back unchanged after a `D` stayed deleted | `test_incremental_histories` (9), `test_false_deletes_and_their_repair` | after day 3: A, C, G, J `I1 D2` instead of `I1 D2 I3`; after day 4: C and J `I1 D2` instead of `I1 D2 I3 D4`; in the repair test the key stayed `I1 D2` after the full extraction |
| (behaviour change) A key back changed after a `D` was a `U` | `test_incremental_histories` (9) | B `I1 D2 U3` instead of `I1 D2 I3` |
| 2. A full refresh dropped a `D` that was no longer the key's newest row | `test_incremental_repeat_run_and_full_refresh_change_nothing` (3) | a second full refresh changed the table |
| 3. A new `D` stored the previous version's file name | `test_incremental_invariants` (9), `test_new_delete_has_the_newest_file_and_the_deleted_content` | 6 rows whose file name timestamp differs from `LKHS_date_valid_from` after the first full refresh; `stemme_20260102_000000.json` instead of `…0103…` |
| 4. After a new source column, a full refresh shifted every `D` row by one column | `test_new_source_column_then_full_refresh` [stemme, duckdb and ducklake] (2) | 1 `D` row with the wrong file name, hash and source system code; `_cv` with 2 wrong rows |

9 + 1 + 3 + 9 + 1 + 2 = 25 (the histories tests count once).

---

## 10. SQL mirror tests (`test_integration_silver_cdc.py`)

These copy the macro SQL by hand into DuckDB and run it on small fixture files,
without dbt. They run in under a second and need no dbt binary. Because they
are copies, they show that the SQL logic is right, not that the macros contain
it; that is what `test_dbt_silver.py` is for.

| # | Class | Test | What it checks | Current |
| --- | --- | --- | --- | --- |
| M1 | `TestSilverFullExtractCDC` | `test_first_file_produces_inserts_only` | every row of the first file is an `I` | pass |
| M2 | | `test_changed_row_produces_update` | row 1 changed in file 2 → `U` | pass |
| M3 | | `test_unchanged_row_is_not_duplicated` | row 2 identical in files 1 and 2 → no `U` | pass |
| M4 | | `test_missing_row_produces_delete` | row 1 missing from file 3 → `D` | pass |
| M5 | | `test_date_valid_from_derived_from_filename` | `LKHS_date_valid_from` from the file name | pass |
| M6 | | `test_total_cdc_operations` | three files → exactly 2 `I`, 1 `U`, 1 `D` | pass |
| M7 | | `test_hash_detects_content_change` | the hash differs between the two versions of row 1 | pass |
| M8 | `TestSilverCurrentVersionView` | `test_cv_returns_one_row_per_pk` | `_cv` has one row per key | pass |
| M9 | | `test_cv_returns_latest_version` | `_cv` returns the `D` for row 1 | pass |
| M10 | | `test_cv_excludes_deleted_for_active_rows` | the Gold pattern (`_cv`, then drop `D`) keeps row 2 only | pass |
| M11 | `TestNotExistsDedup` | `test_not_exists_prevents_duplicate_insert` | the `NOT EXISTS` guard blocks an existing `(key, valid_from)` | pass |
| M12–14 | (module) | `test_parse_filename_ts_handles_all_dlt_extensions` [`.json`, `.jsonl`, `.json.jsonl`] | the timestamp parses for all three names | pass ×3 |
| M15 | `TestIncrementalSilverCDCDelete` | `test_deleted_pk_produces_d_row` | key missing from `_latest` → one `D` | pass |
| M16 | | `test_present_pk_not_marked_deleted` | keys still in `_latest` get no `D` | pass |
| M17 | | `test_d_row_timestamped_from_latest_file` | the `D` has the newest file's timestamp | pass |
| M18 | | `test_multiple_deletes_in_one_run` | two missing keys → two `D` rows | pass |
| M19 | | `test_prior_d_rows_carried_forward` | an existing `D` is carried, not deleted again | pass |
| M20 | | `test_idempotency_appending_to_silver` | a second full refresh adds no second `D` | pass |
| M21 | | `test_current_temp_reflects_latest_cv_state` | `_current_temp` holds the newest version per key | pass |
| M22 | | `test_no_delete_rows_when_all_pks_present` | no `D` when every key is in `_latest` | pass |
| M23 | `TestIncrementalSilverDeletedKeys` | `test_unchanged_key_after_delete_is_insert` | unchanged key after a Silver `D` → `I` | pass |
| M24 | | `test_without_delete_unchanged_key_gives_no_row` | unchanged key without a `D` → no row | pass |
| M25 | | `test_delete_before_previous_version_is_ignored` | a `D` older than the previous Bronze version does not count as a return | pass |
| M26 | | `test_changed_key_after_delete_is_insert_not_update` | changed key after a `D` → `I`, not `U` | pass |
| M27 | | `test_current_temp_keeps_every_delete` | a key deleted twice keeps both `D` rows in `_current_temp` | pass |
| M28 | `TestIncrementalSilverDeleteBranchDetails` | `test_new_delete_takes_the_latest_file_name` | a new `D` gets the newest file's name | pass |
| M29 | | `test_carried_delete_gets_the_file_name_of_its_timestamp` | a carried `D` gets the file of its `LKHS_date_valid_from` | pass |
| M30 | | `test_carried_delete_keeps_its_name_when_the_file_is_gone` | if that file was pruned, the stored name is kept | pass |
| M31 | | `test_key_revived_by_the_rebuild_is_deleted_again_when_missing` | a key whose old `D` is followed by a rebuilt `I` and that is missing from `_latest` gets a new `D` | pass |
| M32 | | `test_branches_are_combined_by_column_name` | after a new source column the `D` rows stay aligned (`UNION ALL BY NAME`) | pass |

## 11. Not covered by these tests

- The singular grain test `silver_no_duplicate_id_date_valid_from` needs all
  18 DDD tables, so the tests exclude it; CI runs it in the full sample build.
- Rfam tables other than `family` and `author`, and S3 raw storage, are not
  exercised here.
- Duplicate keys within one file are only detected (§8 above).
