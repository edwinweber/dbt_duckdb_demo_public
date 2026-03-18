WITH
meeting as (
SELECT iv.*
FROM
(SELECT {{ cast_hash_to_bigint('src.LKHS_source_system_code,src.id,src.LKHS_date_valid_from') }} AS LKHS_meeting_id
,       src.*
,       LEAD(src.LKHS_date_valid_from,1,CAST('9999-12-31' AS DATETIME)) OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
,       ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
FROM {{ ref('silver_moede') }} src
) iv
WHERE  LKHS_date_valid_to = '9999-12-31'
),
meeting_type AS (
    SELECT * FROM {{ ref('meeting_type_cv') }}
)
,meeting_status AS (
    SELECT * FROM {{ ref('meeting_status_cv') }}
)
SELECT  meeting.LKHS_meeting_id
,       meeting_type.LKHS_meeting_type_id
,       meeting_status.LKHS_meeting_status_id
,       CONCAT(meeting.LKHS_source_system_code
              ,'-'
              ,CAST(meeting.id AS VARCHAR)
              )                               AS meeting_bk
,       meeting.dagsordenurl                  AS agenda_url
,       meeting.dato                          AS meeting_date
,       meeting.lokale                        AS meeting_room
,       meeting.nummer                        AS meeting_number
,       meeting.offentlighedskode             AS public_code
,       meeting.opdateringsdato               AS meeting_updated_at
,       meeting.starttidsbem_rkning           AS meeting_start_time_note
,       meeting.titel                         AS meeting_title
,       meeting_type.meeting_type_danish
,       meeting_type.meeting_type_english
,       meeting_status.meeting_status_danish  AS meeting_status_danish
,       meeting_status.meeting_status_english AS meeting_status_english
--      Metadata
,       meeting.LKHS_date_inserted_src
,       CASE
            WHEN meeting.LKHS_row_version = 1
                THEN CAST('1900-01-01' AS DATETIME)
            ELSE CAST(meeting.LKHS_date_valid_from AS DATETIME)
        END                                   AS LKHS_date_valid_from
,       meeting.LKHS_date_valid_to
,       meeting.LKHS_row_version
,       meeting.LKHS_source_system_code
FROM meeting
LEFT JOIN meeting_type
       ON CONCAT(meeting.LKHS_source_system_code,'-',CAST(meeting.typeid AS VARCHAR)) = meeting_type.meeting_type_bk
LEFT JOIN meeting_status
       ON CONCAT(meeting.LKHS_source_system_code,'-',CAST(meeting.statusid AS VARCHAR)) = meeting_status.meeting_status_bk
UNION ALL
SELECT  0                              AS LKHS_meeting_id
,       0                              AS LKHS_meeting_type_id
,       0                              AS LKHS_meeting_status_id
,       'Unknown'                      AS meeting_bk
,       NULL                           AS agenda_url
,       NULL                           AS meeting_date
,       NULL                           AS meeting_room
,       NULL                           AS meeting_number
,       NULL                           AS public_code
,       NULL                           AS meeting_updated_at
,       NULL                           AS meeting_start_time_note
,       NULL                           AS meeting_title
,       'Ukendt'                       AS meeting_type_danish
,       'Unknown'                      AS meeting_type_english
,       'Ukendt'                       AS meeting_status_danish
,       'Unknown'                      AS meeting_status_english
--      Metadata
,       CAST('1900-01-01' AS DATETIME) AS LKHS_date_inserted_src
,       CAST('1900-01-01' AS DATETIME) AS LKHS_date_valid_from
,       CAST('9999-12-31' AS DATETIME) AS LKHS_date_valid_to
,       1                              AS LKHS_row_version
,       'LKHS'                         AS LKHS_source_system_code