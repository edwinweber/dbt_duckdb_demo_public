WITH
vote AS
(
SELECT {{ cast_hash_to_bigint('src.LKHS_source_system_code,src.id,src.LKHS_date_valid_from') }} AS LKHS_vote_id
,      src.*
,      LEAD(src.LKHS_date_valid_from,1,CAST('9999-12-31' AS DATETIME)) OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
,      ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
FROM {{ ref('silver_ddd_afstemning') }} src
)
,
vote_type AS (
    SELECT * FROM {{ ref('vote_type_cv') }}
)
SELECT  vote.LKHS_vote_id
,       vote_type.LKHS_vote_type_id
,       CONCAT(vote.LKHS_source_system_code
            ,'-'
            ,CAST(vote.id AS VARCHAR)) AS vote_bk
,       vote.nummer                    AS vote_number
,       vote.konklusion                AS conclusion
,       vote.vedtaget                  AS approved
,       vote.kommentar                 AS vote_comment
,       vote_type.vote_type_danish
,       vote_type.vote_type_english
--      Metadata
,       vote.LKHS_date_inserted_src
,       CASE
            WHEN vote.LKHS_row_version = 1
                THEN CAST('1900-01-01' AS DATETIME)
            ELSE CAST(vote.LKHS_date_valid_from AS DATETIME)
        END                            AS LKHS_date_valid_from
,       vote.LKHS_date_valid_to
,       vote.LKHS_row_version
,       vote.LKHS_source_system_code
FROM vote
LEFT JOIN vote_type
       ON CONCAT(vote.LKHS_source_system_code,'-',CAST(vote.typeid AS VARCHAR)) = vote_type.vote_type_bk
-- The value of 0 for the 'Unknown'-entry is theoretically also a possible result of the DuckDB hash-function.
-- The chance of that is very, very small, but we check for uniqueness of the results in dbt.
-- If the test fails, we have found a collision!
UNION ALL
SELECT  0                              AS LKHS_vote_id
,       0                              AS LKHS_vote_type_id
,       'Unknown'                      AS vote_bk
,       NULL                           AS vote_number
,       NULL                           AS conclusion
,       NULL                           AS approved
,       NULL                           AS vote_comment
,       'Ukendt'                       AS vote_type_danish
,       'Unknown'                      AS vote_type_english
--      Metadata
,       CAST('1900-01-01' AS DATETIME) AS LKHS_date_inserted_src
,       CAST('1900-01-01' AS DATETIME) AS LKHS_date_valid_from
,       CAST('9999-12-31' AS DATETIME) AS LKHS_date_valid_to
,       1                              AS LKHS_row_version
,       'LKHS'                         AS LKHS_source_system_code