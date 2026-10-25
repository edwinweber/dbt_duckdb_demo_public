{#- Concatenation of columns as text, each followed by the delimiter, NULL as the NULL token:
    CONCAT( COALESCE(a::VARCHAR,'<NULL>'),']##[',COALESCE(b::VARCHAR,'<NULL>'),']##[' ).
    The change hash (generate_base_for_hash) and a primary key of several columns
    (generate_primary_key_column) are built with it, so both treat NULLs and the
    delimiter the same way (variables hash_null_replacement and hash_delimiter). -#}
{% macro concat_with_null_token(columns) %}
    {%- set parts = [] -%}
    {%- for column in columns -%}
        {%- do parts.append('COALESCE(' + column + '::VARCHAR,' + var('hash_null_replacement')  + '),' + var('hash_delimiter')) -%}
    {%- endfor -%}
    CONCAT( {{ parts | join(',') }} )
{%- endmacro %}

{#- The column LKHS_primary_key_1_column of a Bronze view for a primary key of several
    columns (a list); nothing for a key of one column. -#}
{% macro generate_primary_key_column(primary_key_columns) %}
    {%- if primary_key_columns is not string and primary_key_columns | length > 1 -%}
,      {{ concat_with_null_token(primary_key_columns) }} AS LKHS_primary_key_1_column
    {%- endif -%}
{%- endmacro %}

{#- The hash expression over the Bronze view's columns, in column order.  The columns come from
    DESCRIBE, which binds the view against the files that are in its folder now:
    information_schema keeps the columns the view had when it was created, so after archiving
    files it can still list a column that no remaining file has (silver_bronze_column_types).
    The primary key columns are left out: for a key of several columns these are its source
    columns and LKHS_primary_key_1_column. -#}
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
        {%- do columns.append(row['column_name']) -%}
    {%- endfor -%}
    {{ concat_with_null_token(columns) }}
{% endmacro %}
