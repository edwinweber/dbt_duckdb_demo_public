{%- macro cast_hash_to_bigint(columns_to_hash) -%}
{#- Map the 64-bit UBIGINT resulting from the DuckDB hash to a 64-bit BIGINT. -#}
{#- Overflows map to the negative value of the large value - the maximum of a BIGINT. -#}
{#- A BIGINT is accepted by Power BI, a UBIGINT is not! -#}
        CAST(
            CASE
                WHEN hash({{ columns_to_hash }}) > 9223372036854775807
                    THEN -1 * (hash({{ columns_to_hash }}) - 9223372036854775807)
                ELSE hash({{ columns_to_hash }})
            END
            AS BIGINT
            )
{%- endmacro -%}