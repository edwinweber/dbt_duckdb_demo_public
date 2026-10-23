{#- The hash expression over the Bronze view's columns, in column order.  The columns come from
    DESCRIBE, which binds the view against the files that are in its folder now:
    information_schema keeps the columns the view had when it was created, so after archiving
    files it can still list a column that no remaining file has (silver_bronze_column_types). -#}
{% macro generate_base_for_hash(table_name,columns_to_exclude,primary_key_columns) %}
    {%- set query -%}
        SELECT column_name
        FROM  (SELECT column_name, ROW_NUMBER() OVER () AS position
               FROM   (DESCRIBE SELECT * FROM {{ ref(table_name) }}))
        WHERE column_name NOT IN ( {{ columns_to_exclude }} )
        AND   column_name NOT IN ( {{ primary_key_columns }} )
        ORDER BY position
    {%- endset -%}
    {%- set results = run_query(query) -%}
    {%- set columns = [] -%}
    {%- for row in results -%}
        {%- do columns.append('COALESCE(' + row['column_name'] + '::VARCHAR,' + var('hash_null_replacement')  + '),' + var('hash_delimiter')) -%}
    {%- endfor -%}
    {%- set colstring = columns|join(',') -%}
    CONCAT( {{ colstring }} )
{% endmacro %}
