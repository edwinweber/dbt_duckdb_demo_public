{{ config( materialized='view',tags=['rfam'] ) }}
SELECT src.*
FROM {{ ref('silver_rfam_author') }} src
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.author_id ORDER BY src.LKHS_date_valid_from DESC) = 1
