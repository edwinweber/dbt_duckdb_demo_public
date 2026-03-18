{{ config( materialized='view' ) }}
SELECT src.*
FROM {{ ref('silver_moede') }} src
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from DESC) = 1
