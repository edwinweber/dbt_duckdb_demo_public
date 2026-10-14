{#- Extract the YYYYMMDD_HHMMSS timestamp from a dlt file name. A regex instead of a position
    from the last dot: dlt >= 1.30 writes '<name>.json.jsonl', which has two extensions. -#}
{% macro parse_filename_ts(col) %}
strptime(regexp_extract({{ col }}, '([0-9]{8}_[0-9]{6})', 1), '%Y%m%d_%H%M%S')
{% endmacro %}
