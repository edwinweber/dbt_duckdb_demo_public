{{ config( materialized='view',tags=['rfam'] ) }}
SELECT src.*
FROM {{ ref('silver_rfam_clan_membership') }} src
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.rfam_acc ORDER BY src.LKHS_date_valid_from DESC) = 1
