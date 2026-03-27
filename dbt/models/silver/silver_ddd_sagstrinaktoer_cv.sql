{{ config( materialized='view',tags=['ddd'] ) }}
SELECT src.*
FROM {{ ref('silver_ddd_sagstrinaktoer') }} src
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from DESC) = 1
