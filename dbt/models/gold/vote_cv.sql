SELECT  src.* EXCLUDE (LKHS_date_inserted_src,LKHS_date_valid_from,LKHS_date_valid_to,LKHS_row_version)
FROM {{ ref('vote') }} src
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.vote_bk ORDER BY src.LKHS_date_valid_from DESC) = 1
