{%- macro generate_pre_hook_silver(file_name) -%}
CREATE TABLE IF NOT EXISTS {{ this.schema }}.{{ this.name }}_last_file AS
SELECT processed_files.*
,      CAST('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] }}' AS DATETIME) AS LKHS_date_inserted
FROM
(
SELECT LKHS_filename
,      strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),'%Y%m%d_%H%M%S') AS LKHS_date_valid_from
,      LAG(LKHS_filename)  OVER (ORDER BY LKHS_filename) AS LKHS_filename_previous
,      LAG(strptime(SUBSTRING(LKHS_filename, LENGTH(LKHS_filename) - 19, 15),'%Y%m%d_%H%M%S')) OVER (ORDER BY LKHS_filename) AS LKHS_date_valid_from_previous
FROM    (SELECT SUBSTRING(filename, LENGTH(filename) - POSITION('/' IN REVERSE(filename)) + 2) AS LKHS_filename
        FROM read_text('{{ var('danish_democracy_data_source') }}/{{ file_name }}/{{ file_name }}_*.json')
        ) files
WHERE 1 = 0
) processed_files
{%- endmacro -%}