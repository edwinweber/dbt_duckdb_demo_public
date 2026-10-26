{#- The body of the data_engineering view silver_load_history: one row per Silver table and dbt
    run that wrote to it or processed files for it, from Silver itself, so the history goes back
    to the first load and does not depend on Dagster.
      rows_inserted/updated/deleted  the table's rows with that LKHS_date_inserted (the start of
                                     the dbt run, the same for every row of a run), per operation
      files_processed                the Bronze files the run marked processed in <table>_files
                                     (same time); the only sign of a run that read files but
                                     wrote no row
    A run before the file log existed has files_processed 0.  A table without a file log yet (it
    gets one on its next run) is read without it.  Whether a log exists is asked from the
    database, not from dbt's relation cache: the Silver macros create the logs with run_query,
    so the cache of a dbt run that also built Silver does not list them.
    silver_tables: [(model name, source system code), ...], a literal in the model file, so that
    dbt sees the ref() dependencies at parse time. -#}
{%- macro silver_load_history(silver_tables) -%}
{%- set existing_logs = [] -%}
{%- if execute -%}
{%- set query -%}
SELECT lower(database_name || '.' || schema_name || '.' || table_name)
FROM   duckdb_tables()
WHERE  table_name LIKE '%\_files' ESCAPE '\'
{%- endset -%}
{%- set existing_logs = run_query(query).columns[0].values() | list -%}
{%- endif %}
WITH loads AS (
{%- for model, source_system_code in silver_tables %}
{%- set relation = ref(model) %}
{%- set files_name = (relation.database ~ '.' ~ relation.schema ~ '.' ~ relation.identifier ~ '_files') | lower %}
{%- set files_log = files_name if files_name in existing_logs else none %}
    SELECT '{{ model }}' AS silver_table
    ,      '{{ source_system_code }}' AS source_system_code
    ,      loaded_at
    ,      SUM(rows_inserted) AS rows_inserted
    ,      SUM(rows_updated) AS rows_updated
    ,      SUM(rows_deleted) AS rows_deleted
    ,      SUM(files_processed) AS files_processed
    FROM  (SELECT LKHS_date_inserted AS loaded_at
           ,      COUNT(*) FILTER (WHERE LKHS_cdc_operation = 'I') AS rows_inserted
           ,      COUNT(*) FILTER (WHERE LKHS_cdc_operation = 'U') AS rows_updated
           ,      COUNT(*) FILTER (WHERE LKHS_cdc_operation = 'D') AS rows_deleted
           ,      0 AS files_processed
           FROM   {{ relation }}
           GROUP BY 1
{%- if files_log is not none %}
           UNION ALL
           SELECT LKHS_date_inserted, 0, 0, 0, COUNT(*)
           FROM   {{ files_log }}
           WHERE  LKHS_status = 'processed'
           GROUP BY 1
{%- endif %}
          ) per_source
    GROUP BY loaded_at
{%- if not loop.last %}
    UNION ALL
{%- endif %}
{%- endfor %}
)
SELECT silver_table
,      source_system_code
,      loaded_at
,      CAST(loaded_at AS DATE) AS load_date_sk
,      EXTRACT(HOUR FROM loaded_at)::INTEGER * 100 + EXTRACT(MINUTE FROM loaded_at)::INTEGER AS load_time_sk
,      rows_inserted
,      rows_updated
,      rows_deleted
,      rows_inserted + rows_updated + rows_deleted AS rows_written
,      files_processed
FROM   loads
{%- endmacro -%}
