WITH
case_step_type AS
(
SELECT src.*
,      LEAD(src.LKHS_date_valid_from,1,CAST('9999-12-31' AS DATETIME)) OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
,      ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
FROM {{ ref('silver_sagstrinstype_cv') }} src
),
case_step_status AS
(
SELECT src.*
,      LEAD(src.LKHS_date_valid_from,1,CAST('9999-12-31' AS DATETIME)) OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
,      ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
FROM {{ ref('silver_sagstrinsstatus_cv') }} src
),
case_type AS
(
SELECT src.*
,      LEAD(src.LKHS_date_valid_from,1,CAST('9999-12-31' AS DATETIME)) OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
,      ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
FROM {{ ref('silver_sagstype_cv') }} src
),
case_status AS
(
SELECT src.*
,      LEAD(src.LKHS_date_valid_from,1,CAST('9999-12-31' AS DATETIME)) OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
,      ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
FROM {{ ref('silver_sagsstatus_cv') }} src
),
case_category AS
(
SELECT src.*
,      LEAD(src.LKHS_date_valid_from,1,CAST('9999-12-31' AS DATETIME)) OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
,      ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
FROM {{ ref('silver_sagskategori_cv') }} src
),
case_src AS
(
SELECT {{ cast_hash_to_bigint('src.LKHS_source_system_code,src.id,src.LKHS_date_valid_from') }} AS LKHS_case_id
,      src.*
,      LEAD(src.LKHS_date_valid_from,1,CAST('9999-12-31' AS DATETIME)) OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
,      ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
FROM {{ ref('silver_sag_cv') }} src
),
case_step AS
(
SELECT {{ cast_hash_to_bigint('src.LKHS_source_system_code,src.id,src.LKHS_date_valid_from') }} AS LKHS_case_id
,      src.*
,      LEAD(src.LKHS_date_valid_from,1,CAST('9999-12-31' AS DATETIME)) OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
,      ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
FROM {{ ref('silver_sagstrin_cv') }} src
)
select  case_step.LKHS_case_id
,       CONCAT(case_src.LKHS_source_system_code
              ,'-'
              ,CAST(case_step.id AS VARCHAR)
              )                           AS case_bk
,       case_src.titel                    AS case_title
,       case_src.titelkort                AS case_short_title
,       case_src.offentlighedskode        AS case_public_code
,       case_src.nummer                   AS case_number
,       case_src.nummerprefix             AS case_number_prefix
,       case_src.nummernumerisk           AS case_number_numeric
,       case_src.nummerpostfix            AS case_number_postfix
,       case_src.resume                   AS case_summary
,       case_src.afstemningskonklusion    AS case_voting_conclusion
,       case_src.afg_relsesresultatkode   AS case_decision_result_code
,       case_src.opdateringsdato          AS case_updated_at
,       case_src.statsbudgetsag           AS case_state_budget
,       case_src.begrundelse              AS case_reason
,       case_src.paragrafnummer           AS case_paragraph_number
,       case_src.paragraf                 AS case_paragraph
,       case_src.afg_relsesdato           AS case_decision_date
,       case_src.afg_relse               AS case_decision
,       case_src.r_dsm_dedato            AS case_council_meeting_date
,       case_src.lovnummer                AS case_law_number
,       case_src.lovnummerdato            AS case_law_number_date
,       NULL                              AS case_law_url
,       case_src.fremsatundersagid        AS case_proposed_under_case_id
,       case_src.deltundersagid           AS case_shared_case_id
,       case_step.titel                   AS case_step_title
--      Status, type etc.
,       case_type.type                    AS case_type_danish
,       case_category.kategori            AS case_category_danish
,       case_status.status                AS case_status_danish
,       case_step_status.status           AS case_step_status_danish
,       case_step_type.type               AS case_step_type_danish
--      Metadata
,       case_src.LKHS_date_inserted_src
,       CASE
            WHEN case_src.LKHS_row_version = 1
                THEN CAST('1900-01-01' AS DATETIME)
            ELSE CAST(case_src.LKHS_date_valid_from AS DATETIME)
        END                               AS LKHS_date_valid_from
,       case_src.LKHS_date_valid_to
,       case_src.LKHS_row_version
,       case_src.LKHS_source_system_code
FROM case_step
LEFT JOIN case_step_type
       ON case_step.typeid = case_step_type.id
LEFT JOIN case_step_status
       ON case_step.statusid = case_step_status.id
LEFT JOIN case_src
       ON case_step.sagid = case_src.id
LEFT JOIN case_type
       ON case_src.typeid = case_type.id
LEFT JOIN case_status
       ON case_src.statusid = case_status.id
LEFT JOIN case_category
       ON case_src.kategoriid = case_category.id
-- The value of 0 for the 'Unknown'-entry is theoretically also a possible result of the DuckDB hash-function.
-- The chance of that is very, very small, but we check for uniqueness of the results in dbt.
-- If the test fails, we have found a collision!
UNION ALL
SELECT 0                              AS LKHS_case_id
,      'Unknown'                      AS case_bk
,      NULL                           AS case_title
,      NULL                           AS case_short_title
,      NULL                           AS case_public_code
,      NULL                           AS case_number
,      NULL                           AS case_number_prefix
,      NULL                           AS case_number_numeric
,      NULL                           AS case_number_postfix
,      NULL                           AS case_summary
,      NULL                           AS case_voting_conclusion
,      NULL                           AS case_decision_result_code
,      NULL                           AS case_updated_at
,      NULL                           AS case_state_budget
,      NULL                           AS case_reason
,      NULL                           AS case_paragraph_number
,      NULL                           AS case_paragraph
,      NULL                           AS case_decision_date
,      NULL                           AS case_decision
,      NULL                           AS case_council_meeting_date
,      NULL                           AS case_law_number
,      NULL                           AS case_law_number_date
,      NULL                           AS case_law_url
,      NULL                           AS case_proposed_under_case_id
,      NULL                           AS case_shared_case_id
,      NULL                           AS case_step_title
,      NULL                           AS case_type_danish
,      NULL                           AS case_category_danish
,      NULL                           AS case_status_danish
,      NULL                           AS case_step_status_danish
,      NULL                           AS case_step_type_danish
,      CAST('1900-01-01' AS DATETIME) AS LKHS_date_inserted_src
,      CAST('1900-01-01' AS DATETIME) AS LKHS_date_valid_from
,      CAST('9999-12-31' AS DATETIME) AS LKHS_date_valid_to
,      1                              AS LKHS_row_version
,      'LKHS'                         AS LKHS_source_system_code