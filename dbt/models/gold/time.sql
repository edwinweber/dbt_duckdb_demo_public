-- time.sql: Time dimension (grain = 1 minute)

WITH time_series AS (
    SELECT
        minute_of_day
    ,   LPAD(CAST(minute_of_day / 60 AS VARCHAR), 2, '0') || ':' || LPAD(CAST(minute_of_day % 60 AS VARCHAR), 2, '0') AS time_hhmm
    ,   LPAD(CAST(minute_of_day / 60 AS VARCHAR), 2, '0') || ':' || LPAD(CAST(minute_of_day % 60 AS VARCHAR), 2, '0') AS time_hhmi
    ,   LPAD(CAST(minute_of_day / 60 AS VARCHAR), 2, '0') || ':' || LPAD(CAST(minute_of_day % 60 AS VARCHAR), 2, '0') || ':00' AS time_hhmmss
    ,   minute_of_day / 60 AS hour
    ,   minute_of_day % 60 AS minute
    ,   (minute_of_day / 60) * 100 + (minute_of_day % 60) AS time_key
    ,   CASE
            WHEN minute_of_day BETWEEN 0 AND 359 THEN 'Night'
            WHEN minute_of_day BETWEEN 360 AND 719 THEN 'Morning'
            WHEN minute_of_day BETWEEN 720 AND 1079 THEN 'Afternoon'
            ELSE 'Evening'
        END AS period_of_day
    FROM (
        SELECT unnest(generate_series(0, 1439)) AS minute_of_day
    )
)
SELECT
    time_key
,   time_hhmm
,   time_hhmi
,   time_hhmmss
,   hour
,   minute
,   period_of_day
FROM time_series
ORDER BY time_key