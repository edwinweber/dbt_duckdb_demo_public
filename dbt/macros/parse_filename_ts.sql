{% macro parse_filename_ts(col) %}
strptime(SUBSTRING({{ col }}, LENGTH({{ col }}) - POSITION('.' IN REVERSE({{ col }})) - 14, 15), '%Y%m%d_%H%M%S')
{% endmacro %}
