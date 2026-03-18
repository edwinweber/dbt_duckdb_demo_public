WITH actor AS
(
SELECT {{ cast_hash_to_bigint('src.LKHS_source_system_code,src.id,src.LKHS_date_valid_from') }} AS LKHS_actor_id
,       src.*
,       LEAD(src.LKHS_date_valid_from,1,CAST('9999-12-31' AS DATETIME)) OVER (PARTITION BY src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_date_valid_to
,       ROW_NUMBER() OVER (PARTITION BY src.LKHS_source_system_code,src.id ORDER BY src.LKHS_date_valid_from) AS LKHS_row_version
FROM {{ ref('silver_aktoer') }} src
)
,
actor_type AS (
    SELECT * FROM {{ ref('actor_type_cv') }}
)
SELECT  actor.LKHS_actor_id
,       actor_type.LKHS_actor_type_id
,       CONCAT(actor.LKHS_source_system_code
              ,'-'
              ,CAST(actor.id AS VARCHAR)
              )                       AS actor_bk
,       actor_type.actor_type_danish  AS actor_type_danish
,       actor_type.actor_type_english AS actor_type_english
,       actor.gruppenavnkort          AS group_short_name
,       actor.navn                    AS full_name
,       actor.fornavn                 AS first_name
,       actor.efternavn               AS last_name
,       actor.biografi                AS biography
,       actor.periodeid               AS period_id
,       actor.opdateringsdato         AS updated_at
,       actor.startdato               AS valid_from
,       actor.slutdato                AS valid_to
,       substring(actor.biografi,position('<sex>' in biografi) + length('<sex>'),position('</sex>' in biografi) - position('<sex>' in biografi) - length('<sex>')) AS gender_danish
,       CASE substring(actor.biografi,position('<sex>' in biografi) + length('<sex>'),position('</sex>' in biografi) - position('<sex>' in biografi) - length('<sex>'))
            WHEN 'Mand' THEN 'Male'
            WHEN 'Kvinde' THEN 'Female'
            ELSE substring(actor.biografi,position('<sex>' in biografi) + length('<sex>'),position('</sex>' in biografi) - position('<sex>' in biografi) - length('<sex>'))
        END                           AS gender_english
,       try_strptime(substring(
            actor.biografi,
            position('<born>' in biografi) + length('<born>'),
            position('</born>' in biografi)
            - position('<born>' in biografi)
            - length('<born>')
            ), '%d-%m-%Y')            AS birth_date
,       try_strptime(substring(
            actor.biografi, position('<died>' in biografi) + length('<died>'),
            position('</died>' in biografi)
            - position('<died>' in biografi)
            - length('<died>')
        ), '%d-%m-%Y')                AS death_date
,       substring(
            actor.biografi,
            position('<party>' in biografi) + length('<party>'),
            position('</party>' in biografi)
            - position('<party>' in biografi)
            - length('<party>')
        )                             AS party_name
,       substring(
            actor.biografi,
            position('<partyShortname>' in biografi)
            + length('<partyShortname>'),
            position('</partyShortname>' in biografi)
            - position('<partyShortname>' in biografi)
            - length('<partyShortname>')
        )                             AS party_short_name
,       actor.LKHS_date_inserted_src
--      Make the first row_version valid from 1900-01-01.
,       CASE
            WHEN actor.LKHS_row_version = 1
                THEN CAST('1900-01-01' AS DATETIME)
            ELSE CAST(actor.LKHS_date_valid_from AS DATETIME)
        END                           AS LKHS_date_valid_from
,       actor.LKHS_date_valid_to
,       actor.LKHS_row_version
,       actor.LKHS_source_system_code
FROM      actor
LEFT JOIN actor_type
       ON CONCAT(actor.LKHS_source_system_code,'-',CAST(actor.typeid AS VARCHAR)) = actor_type.actor_type_bk
UNION ALL
select  0                              AS LKHS_actor_id
,       0                              AS LKHS_actor_type_id
,       'Unknown'                      AS actor_bk
,       'Ukendt'                       AS actor_type_danish
,       'Unknown'                      AS actor_type_english
,       NULL                           AS group_short_name
,       NULL                           AS full_name
,       NULL                           AS first_name
,       NULL                           AS last_name
,       NULL                           AS biography
,       NULL                           AS period_id
,       NULL                           AS updated_at
,       NULL                           AS valid_from
,       NULL                           AS valid_to
,       NULL                           AS gender_danish
,       NULL                           AS gender_english
,       NULL                           AS birth_date
,       NULL                           AS death_date
,       NULL                           AS party_name
,       NULL                           AS party_short_name
,       CAST('1900-01-01' AS DATETIME) AS LKHS_date_inserted_src
,       CAST('1900-01-01' AS DATETIME) AS LKHS_date_valid_from
,       CAST('9999-12-31' AS DATETIME) AS LKHS_date_valid_to
,       1                              AS LKHS_row_version
,       'LKHS'                         AS LKHS_source_system_code