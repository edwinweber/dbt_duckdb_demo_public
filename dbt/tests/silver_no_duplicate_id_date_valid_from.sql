-- Verify the SCD2 unique key constraint: no duplicate (id, LKHS_date_valid_from) pairs
-- across all Silver tables.  Returns rows that violate the constraint (test passes if empty).

{% set silver_tables = [
    'silver_afstemning', 'silver_afstemningstype', 'silver_aktoer', 'silver_aktoertype',
    'silver_moede', 'silver_moedestatus', 'silver_moedetype', 'silver_periode',
    'silver_sag', 'silver_sagskategori', 'silver_sagsstatus', 'silver_sagstrin',
    'silver_sagstrinaktoer', 'silver_sagstrinsstatus', 'silver_sagstrinstype',
    'silver_sagstype', 'silver_stemme', 'silver_stemmetype'
] %}

{% for table in silver_tables %}
SELECT '{{ table }}' AS table_name, id, LKHS_date_valid_from, COUNT(*) AS cnt
FROM {{ ref(table) }}
GROUP BY id, LKHS_date_valid_from
HAVING COUNT(*) > 1
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
