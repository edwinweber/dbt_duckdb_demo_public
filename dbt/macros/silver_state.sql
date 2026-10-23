{#- Helpers for the Silver macros, which compare new Bronze files with Silver's current state. -#}

{#- The Bronze files a Silver run processes, decided once when the model compiles.

    <model>_last_file holds the bookmark: LKHS_filename is the newest Bronze file that is
    processed, LKHS_filename_pending the newest file of the run in progress.  This macro lists
    the files newer than the bookmark, notes the newest of them as pending and returns them; the
    model reads exactly those files (a file that arrives during the run waits for the next one),
    and its post-hooks move pending to LKHS_filename only after a successful run.

    <model>_files logs every Bronze file: LKHS_status is 'pending' (this run), 'processed',
    'seeded' (in the folder when the log was created, so processed before) or 'skipped' (a late
    file the admin chose to skip).  A file in the folder that is not newer than the bookmark and
    not in the log arrived late: it can no longer be merged into the history, so the run stops,
    unless the run is started with --vars '{silver_skip_late_files: true}'.

    A first build starts with an empty bookmark.  On a normal run a missing bookmark stops the run,
    because every Bronze file would otherwise be compared with Silver's state again.

    Returns {'processed': <bookmark or ''>, 'newest': <newest file to process>, 'new_files': [...]}. -#}
{%- macro silver_files(file_name, data_source_env_var) -%}
{%- if not execute -%}
{{ return({'processed': '', 'newest': '', 'new_files': []}) }}
{%- endif -%}
{%- set bookmark = this.database ~ '.' ~ this.schema ~ '.' ~ this.name ~ '_last_file' -%}
{%- set files_log = this.database ~ '.' ~ this.schema ~ '.' ~ this.name ~ '_files' -%}
{%- set source_files -%}
(SELECT regexp_extract(file, '[^/]+$') AS file_name FROM glob('{{ env_var(data_source_env_var) }}/{{ file_name }}/{{ file_name }}_*.json*'))
{%- endset -%}
{%- set processed = none -%}
{%- if is_incremental() -%}
{%- set relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name ~ '_last_file') -%}
{%- if relation is not none -%}
{%- do run_query('ALTER TABLE ' ~ bookmark ~ ' ADD COLUMN IF NOT EXISTS LKHS_filename_pending VARCHAR') -%}
{%- for obsolete in ['LKHS_date_valid_from', 'LKHS_filename_previous', 'LKHS_date_valid_from_previous'] -%}
{%- do run_query('ALTER TABLE ' ~ bookmark ~ ' DROP COLUMN IF EXISTS ' ~ obsolete) -%}
{%- endfor -%}
{%- set processed = run_query('SELECT MAX(LKHS_filename) FROM ' ~ bookmark).columns[0].values()[0] -%}
{%- endif -%}
{%- if not processed -%}
{{ exceptions.raise_compiler_error(this.name ~ ": the bookmark " ~ this.name ~ "_last_file is missing or empty. Restore it (the newest processed Bronze file) before running Silver; without it every Bronze file would be processed again.") }}
{%- endif -%}
{%- if adapter.get_relation(database=this.database, schema=this.schema, identifier=this.name ~ '_files') is none -%}
{%- do run_query('CREATE TABLE ' ~ files_log ~ ' (LKHS_filename VARCHAR, LKHS_status VARCHAR, LKHS_date_inserted TIMESTAMP)') -%}
{%- do run_query("INSERT INTO " ~ files_log ~ " SELECT file_name, 'seeded', CURRENT_TIMESTAMP FROM " ~ source_files ~ " WHERE file_name <= '" ~ processed ~ "'") -%}
{%- endif -%}
{%- do run_query("DELETE FROM " ~ files_log ~ " WHERE LKHS_status = 'pending'") -%}
{%- set late_files_query -%}
SELECT file_name FROM {{ source_files }}
WHERE  file_name <= '{{ processed }}'
AND    file_name NOT IN (SELECT LKHS_filename FROM {{ files_log }})
ORDER BY 1
{%- endset -%}
{%- set late_files = run_query(late_files_query).columns[0].values() | list -%}
{%- if late_files -%}
{%- if var('silver_skip_late_files', false) -%}
{%- do run_query("INSERT INTO " ~ files_log ~ " SELECT file_name, 'skipped', CURRENT_TIMESTAMP FROM (" ~ late_files_query ~ ")") -%}
{%- do exceptions.warn(this.name ~ ": skipped " ~ late_files | length ~ " late Bronze file(s): " ~ late_files | join(', ')) -%}
{%- else -%}
{{ exceptions.raise_compiler_error(this.name ~ ": " ~ late_files | length ~ " Bronze file(s) are not newer than the processed file " ~ processed ~ " but were never processed: " ~ late_files | join(', ') ~ ". Silver cannot merge them into its history. Check whether a later file covers their content (runbook: late Bronze files), then run this model once with --vars '{silver_skip_late_files: true}' to log them as skipped, or replay Silver.") }}
{%- endif -%}
{%- endif -%}
{%- else -%}
{%- do run_query('CREATE OR REPLACE TABLE ' ~ bookmark ~ ' (LKHS_filename VARCHAR, LKHS_filename_pending VARCHAR, LKHS_date_inserted TIMESTAMP)') -%}
{%- do run_query('CREATE OR REPLACE TABLE ' ~ files_log ~ ' (LKHS_filename VARCHAR, LKHS_status VARCHAR, LKHS_date_inserted TIMESTAMP)') -%}
{%- endif -%}
{%- set new_files_query -%}
SELECT file_name FROM {{ source_files }}
WHERE  file_name > '{{ processed or '' }}'
ORDER BY 1
{%- endset -%}
{%- set new_files = run_query(new_files_query).columns[0].values() | list -%}
{%- set newest = new_files[-1] if new_files else (processed or '') -%}
{#- The bookmark row is only ever updated in place: a failure here must never leave it empty. -#}
{%- if processed -%}
{%- do run_query("UPDATE " ~ bookmark ~ " SET LKHS_filename_pending = '" ~ newest ~ "'") -%}
{%- else -%}
{%- do run_query("INSERT INTO " ~ bookmark ~ " (LKHS_filename, LKHS_filename_pending, LKHS_date_inserted) VALUES (NULL, '" ~ newest ~ "', CURRENT_TIMESTAMP)") -%}
{%- endif -%}
{%- if new_files -%}
{%- do run_query("INSERT INTO " ~ files_log ~ " (LKHS_filename, LKHS_status, LKHS_date_inserted) VALUES ('" ~ new_files | join("', 'pending', NULL), ('") ~ "', 'pending', NULL)") -%}
{%- endif -%}
{{ return({'processed': processed or '', 'newest': newest, 'new_files': new_files}) }}
{%- endmacro -%}

{#- Post-hooks of every Silver model: the run succeeded, so its files are now processed. -#}
{%- macro silver_bookmark_post_hook() -%}
UPDATE {{ this.database }}.{{ this.schema }}.{{ this.name }}_last_file
SET    LKHS_filename      = LKHS_filename_pending
,      LKHS_date_inserted = CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}' AS DATETIME)
WHERE  LKHS_filename_pending IS NOT NULL AND LKHS_filename_pending != ''
{%- endmacro -%}

{%- macro silver_files_post_hook() -%}
UPDATE {{ this.database }}.{{ this.schema }}.{{ this.name }}_files
SET    LKHS_status        = 'processed'
,      LKHS_date_inserted = CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}' AS DATETIME)
WHERE  LKHS_status = 'pending'
{%- endmacro -%}

{#- Column names and types of the Bronze view, in Bronze order, as the view reads its files now.
    DESCRIBE binds the view again; information_schema would still list a column that only the
    archived files had, and the run would fail on it. -#}
{%- macro silver_bronze_column_types(bronze_table_name) -%}
{%- set query -%}
SELECT column_name, column_type AS data_type
FROM  (SELECT *, ROW_NUMBER() OVER () AS position FROM (DESCRIBE SELECT * FROM {{ ref(bronze_table_name) }}))
ORDER BY position
{%- endset -%}
{{ return(run_query(query)) }}
{%- endmacro -%}

{#- Column types of the Silver table, by lower-case name. -#}
{%- macro silver_column_types() -%}
{%- set types = {} -%}
{%- for column in adapter.get_columns_in_relation(this) -%}
{%- do types.update({column.name | lower: column.dtype | upper}) -%}
{%- endfor -%}
{{ return(types) }}
{%- endmacro -%}

{#- Select list over the Bronze view for a normal run: a column that Silver already has is cast
    to Silver's type.  read_json_auto infers the types from the files that are in the folder at
    the time, so archiving files can change them (a DOUBLE column whose decimal values were all in
    archived files is read as BIGINT); without the cast the text a hash is computed from (3 instead
    of 3.0) would change with them and every key would get a false U.  A first build reads Bronze as it is.  A cast that
    fails stops the run: the source really changed the type, and Silver's column must be altered
    by hand (runbook). -#}
{%- macro silver_bronze_columns(bronze_table_name) -%}
{%- if execute and is_incremental() -%}
{%- set silver_types = silver_column_types() -%}
{%- set drifted = [] -%}
{%- for row in silver_bronze_column_types(bronze_table_name) -%}
{%- set silver_type = silver_types.get(row['column_name'] | lower) -%}
{%- if silver_type and silver_type != row['data_type'] | upper -%}
{%- do drifted.append(row['column_name'] ~ ' ' ~ row['data_type'] ~ ' -> ' ~ silver_type) -%}
CAST("{{ row['column_name'] }}" AS {{ silver_type }}) AS "{{ row['column_name'] }}"
{%- else -%}
"{{ row['column_name'] }}"
{%- endif -%}
{{ ', ' if not loop.last }}
{%- endfor -%}
{%- if drifted -%}
{%- do exceptions.warn(this.name ~ ": Bronze column types differ from Silver; Bronze is cast to Silver's type: " ~ drifted | join(', ')) -%}
{%- endif -%}
{%- else -%}
*
{%- endif -%}
{%- endmacro -%}

{#- Select list over the Silver table with the Bronze view's columns, so that Silver's latest
    version of a key is hashed with the same expression as a Bronze row.  A Bronze column that
    Silver does not have yet (a new source column) is NULL, which keeps the hash of a key equal
    unless the key has a value in the new column. -#}
{%- macro silver_state_columns(bronze_table_name) -%}
{%- if execute -%}
{%- set silver_types = silver_column_types() -%}
{%- set bronze_columns = silver_bronze_column_types(bronze_table_name) -%}
{%- for row in bronze_columns if row['column_name'] != 'LKHS_filename' -%}
{%- if row['column_name'] | lower in silver_types -%}
"{{ row['column_name'] }}"
{%- else -%}
CAST(NULL AS {{ row['data_type'] }}) AS "{{ row['column_name'] }}"
{%- endif -%}
{{ ', ' if not loop.last }}
{%- endfor -%}
{%- endif -%}
{%- endmacro -%}

{#- Condition of the final SELECT of a normal run that stops the run when a new file would delete
    too many keys: a truncated or failed extraction looks like a mass delete.  CTE_DELETE_GUARD
    has one row per new file with LKHS_deletes and LKHS_keys_before (the live keys it is compared
    with).  The run stops when a file deletes every live key, or more than
    silver_delete_guard_min_rows keys that are more than silver_delete_guard_fraction of the live
    keys.  error() fails the insert, so nothing is written and the bookmark stays.  For a run with
    real mass deletes the guard is turned off with --vars '{silver_delete_guard: false}'. -#}
{%- macro silver_delete_guard() -%}
{%- set fraction = var('silver_delete_guard_fraction') | float -%}
{%- set min_rows = var('silver_delete_guard_min_rows') | int -%}
{%- if var('silver_delete_guard') -%}
(SELECT bool_and(CASE WHEN g.LKHS_deletes > 0
                       AND (g.LKHS_deletes >= g.LKHS_keys_before
                            OR (g.LKHS_deletes > {{ min_rows }} AND g.LKHS_deletes > {{ fraction }} * g.LKHS_keys_before))
                      THEN error(printf('{{ this.name }}: %s would delete %d of %d live keys (silver_delete_guard_fraction {{ fraction }}, silver_delete_guard_min_rows {{ min_rows }}). Check the file; if the deletes are real, run this model once with --vars ''{silver_delete_guard: false}''.', g.LKHS_filename, g.LKHS_deletes, g.LKHS_keys_before))
                      ELSE TRUE
                 END)
 FROM CTE_DELETE_GUARD g) IS NOT FALSE
{%- else -%}
TRUE
{%- endif -%}
{%- endmacro -%}
