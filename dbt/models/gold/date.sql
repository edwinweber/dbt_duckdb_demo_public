WITH
dates AS (
    SELECT * FROM {{ ref('bronze_dates') }}
),
holidays as (
    SELECT * FROM {{ ref('bronze_dates_holidays') }}
    -- remove one random of the overlapping holiday as we only want one of them
    qualify row_number() OVER (PARTITION BY holiday_date ORDER BY holiday_type DESC) = 1
),
final AS (
    SELECT
--      Date specific fields
        dates.date_day
--      Year specific fields
    ,   dates.date_year
    ,   dates.iso_year
    ,   dates.year_start_date
    ,   dates.year_end_date
    ,   dates.year_day_number
    ,   dates.year_offset
    ,   dates.year_completed
--      Quarter specific fields
    ,   dates.date_quarter
    ,   dates.quarter_start_date
    ,   dates.quarter_end_date
    ,   dates.quarter_day_number
    ,   dates.quarter_offset
    ,   dates.quarter_completed
--      Month specific fields
    ,   dates.date_month
    ,   dates.month_start_date
    ,   dates.month_end_date
    ,   dates.month_name
    ,   dates.month_name_short
    ,   dates.month_initial
    ,   dates.month_and_year
    ,   dates.month_and_year_number
    ,   dates.month_day_number
    ,   dates.month_offset
    ,   dates.month_completed
--      Week specific fields
    ,   dates.date_week
    ,   dates.iso_week_of_year
    ,   dates.week_start_date
    ,   dates.week_end_date
    ,   dates.week_day_number
    ,   dates.week_offset
    ,   dates.week_completed
    ,   dates.iso_week_year_number
--      Holiday specific fields
    ,   dates.has_53_iso_weeks
    ,   coalesce(holidays.holiday_name, 'Not a holiday') AS holiday_name
    ,   IF(holidays.holiday_date IS NULL, false, true)   AS is_holiday
    ,   coalesce(holidays.is_global_holiday, false)      AS is_global_holiday
    ,   coalesce(holidays.holiday_type, 'Not a holiday') AS holiday_type
    ,   coalesce(holidays.fixed, false)                  AS fixed
    --  Day flags
    ,   dates.is_after_today
    ,   dates.is_before_today
    ,   dates.is_weekend
    ,   dates.is_weekday
    FROM dates
    LEFT JOIN holidays
        ON dates.date_id = holidays.date_holidays_id
)
SELECT * FROM final