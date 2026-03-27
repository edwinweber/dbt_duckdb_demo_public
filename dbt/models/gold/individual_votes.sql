WITH
actor AS (
    SELECT * FROM {{ ref('actor_cv') }}
),
case_gold AS (
    SELECT * FROM {{ ref('case_cv') }}
),
meeting AS (
    SELECT * FROM {{ ref('meeting_cv') }}
),
vote AS (
    SELECT * FROM {{ ref('vote_cv') }}
),
vote_silver AS
(
SELECT src.*
FROM {{ ref('silver_ddd_afstemning_cv') }} src
),
individual_voting_type AS 
(
SELECT src.*
FROM {{ ref('silver_ddd_stemmetype_cv') }} src
),
individual_vote AS 
(
SELECT src.*
FROM {{ ref('silver_ddd_stemme_cv') }} src
-- Only include rows that are not marked as deleted!
WHERE src.LKHS_cdc_operation != 'D'
)
SELECT  
--      surrogate keys
        COALESCE(vote.LKHS_vote_id,0)       AS LKHS_vote_id
,       COALESCE(actor.LKHS_actor_id,0)     AS LKHS_actor_id
,       COALESCE(meeting.LKHS_meeting_id,0) AS LKHS_meeting_id
,       COALESCE(case_gold.LKHS_case_id,0)  AS LKHS_case_id
--      The key of the date dimension is date_day, not a surrogate key
,       cast(meeting.meeting_date as date)  AS date_day
--      degenerate dimension
,       individual_voting_type.type         AS individual_voting_type
--      measures
,       1                                   AS individual_vote_count
FROM      individual_vote
LEFT JOIN individual_voting_type
       ON individual_vote.typeid = individual_voting_type.id
LEFT JOIN vote_silver
       ON individual_vote.afstemningid = vote_silver.id
LEFT JOIN meeting
       ON CONCAT(vote_silver.LKHS_source_system_code,'-',CAST(vote_silver.m_deid AS VARCHAR)) = meeting.meeting_bk
LEFT JOIN actor
       ON CONCAT(individual_vote.LKHS_source_system_code,'-',CAST(individual_vote.akt_rid AS VARCHAR)) = actor.actor_bk
LEFT JOIN case_gold
       ON CONCAT(vote_silver.LKHS_source_system_code,'-',CAST(vote_silver.sagstrinid AS VARCHAR)) = case_gold.case_bk
LEFT JOIN vote
       ON CONCAT(vote_silver.LKHS_source_system_code,'-',CAST(vote_silver.id AS VARCHAR)) = vote.vote_bk