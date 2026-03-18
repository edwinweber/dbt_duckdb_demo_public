WITH CTE_SRC AS
(SELECT {{ cast_hash_to_bigint('src.LKHS_source_system_code,src.id,src.LKHS_date_valid_from') }} AS LKHS_actor_type_id
,       src.*
,       LEAD(src.LKHS_date_valid_from,1,CAST('9999-12-31' AS DATETIME)) OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
,       ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
FROM {{ ref('silver_aktoertype') }} src
)
SELECT  CTE_SRC.LKHS_actor_type_id
,       CONCAT(CTE_SRC.LKHS_source_system_code,'-',CAST(CTE_SRC.id AS VARCHAR)) AS actor_type_bk
,       CTE_SRC.type AS actor_type_danish
,       CASE CTE_SRC.type
            WHEN 'Anden gruppe' THEN 'Other group'
            WHEN 'Folketingsgruppe' THEN 'Parliamentary group'
            WHEN 'Gruppe' THEN 'Group'
            WHEN 'Kommission' THEN 'Commission'
            WHEN 'Ministerium' THEN 'Ministry'
            WHEN 'Ministerområde' THEN 'Ministerial area'
            WHEN 'Ministertitel' THEN 'Ministerial title'
            WHEN 'Organisation' THEN 'Organization'
            WHEN 'Parlamentarisk forsamling' THEN 'Parliamentary Assembly'
            WHEN 'Person' THEN 'Person'
            WHEN 'Privatperson' THEN 'Individual'
            WHEN 'Tværpolitisk netværk' THEN 'Cross-party network'
            WHEN 'Udvalg' THEN 'Committee'
            ELSE CONCAT(CTE_SRC.type,' (not translated yet!)')
        END AS actor_type_english
,       CTE_SRC.LKHS_date_inserted_src
--      Make the first row_version valid from 1900-01-01.
,       CASE
            WHEN CTE_SRC.LKHS_row_version = 1
                THEN CAST('1900-01-01' AS DATETIME)
            ELSE CAST(CTE_SRC.LKHS_date_valid_from AS DATETIME)
        END AS LKHS_date_valid_from
,       CTE_SRC.LKHS_date_valid_to
,       CTE_SRC.LKHS_row_version
,       CTE_SRC.LKHS_source_system_code
FROM    CTE_SRC
-- The value of 0 for the 'Unknown'-entry is theoretically also a possible result of the DuckDB hash-function.
-- The chance of that is very, very small, but we check for uniqueness of the results in dbt.
-- If the test fails, we have found a collision!
UNION ALL
SELECT 0                              AS LKHS_actor_type_id
,      'Unknown'                      AS actor_type_bk
,      'Ukendt'                       AS actor_type_danish
,      'Unknown'                      AS actor_type_english
,      CAST('1900-01-01' AS DATETIME) AS LKHS_date_inserted_src
,      CAST('1900-01-01' AS DATETIME) AS LKHS_date_valid_from
,      CAST('9999-12-31' AS DATETIME) AS LKHS_date_valid_to
,      1                              AS LKHS_row_version
,      'LKHS'                         AS LKHS_source_system_code