{% macro generate_base_for_hash(table_name,columns_to_exclude,primary_key_columns) %}
    {%- set query -%}
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{{ table_name }}'
        AND column_name NOT IN ( {{ columns_to_exclude }} )
        AND column_name NOT IN ( {{ primary_key_columns }} )
        ORDER BY ordinal_position
    {%- endset -%}
    {%- set results = run_query(query) -%}
    {%- set columns = [] -%}
    {%- for row in results -%}
        {%- do columns.append('COALESCE(' + row['column_name'] + '::VARCHAR,' + var('hash_null_replacement')  + '),' + var('hash_delimiter')) -%}
    {%- endfor -%}
    {%- set colstring = columns|join(',') -%}
    CONCAT( {{ colstring }} )
{% endmacro %}
