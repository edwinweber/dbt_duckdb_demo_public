-- Verify the SCD2 unique key constraint: no duplicate (id, LKHS_date_valid_from) pairs
-- across all DDD Silver tables.  Returns rows that violate the constraint (test passes if empty).
--
-- The table list is built from the dbt graph at compile time so it stays in sync
-- automatically when entities are added or renamed in configuration_variables.py.
-- Rfam Silver tables are excluded: their primary keys vary per table (rfam_acc, upid, …)
-- and the GROUP BY id would be incorrect for them.

{% set ns = namespace(silver_tables=[]) %}
{% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model'
          and node.name.startswith('silver_ddd_')
          and not node.name.endswith('_cv') %}
        {% set ns.silver_tables = ns.silver_tables + [node.name] %}
    {% endif %}
{% endfor %}
{% set silver_tables = ns.silver_tables | sort %}

{% if silver_tables | length == 0 %}
-- Graph not yet populated (dbt parse); test is a no-op so parse succeeds.
SELECT
    NULL AS table_name,
    NULL AS id,
    NULL AS lkhs_date_valid_from,
    0    AS cnt
WHERE 1 = 0
{% else %}
    {% for table in silver_tables %}
SELECT '{{ table }}' AS table_name, id, LKHS_date_valid_from, COUNT(*) AS cnt
FROM {{ ref(table) }}
GROUP BY id, LKHS_date_valid_from
HAVING COUNT(*) > 1
        {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
{% endif %}
