{%- macro generate_pre_hook_silver(file_name,data_source_env_var='DANISH_DEMOCRACY_DATA_SOURCE') -%}
{%- if is_incremental() == False -%}
DROP TABLE IF EXISTS {{ this.database }}.{{ this.schema }}.{{ this.name }}_last_file;
{%- endif -%}
CREATE TABLE IF NOT EXISTS {{ this.database }}.{{ this.schema }}.{{ this.name }}_last_file AS
SELECT processed_files.*
,      CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}' AS DATETIME) AS LKHS_date_inserted
FROM
(
SELECT LKHS_filename
,      {{ parse_filename_ts('LKHS_filename') }} AS LKHS_date_valid_from
,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
,      LAG({{ parse_filename_ts('LKHS_filename') }}) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_previous
FROM    (SELECT SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
        FROM read_text('{{ env_var(data_source_env_var) }}/{{ file_name }}/{{ file_name }}_*.json*')
        ) files
WHERE 1 = 0
) processed_files
{%- endmacro -%}