with date_spine as (

  {{ dbt_utils.date_spine(
      start_date = "cast('2014-01-01' as date)",
      datepart = "day",
      end_date = "cast(date_trunc('year', current_date + interval 1 year) as date)"
     )
  }}

),

create_date_table as (
    select
        date_day as date_id,
        cast(date_day as date) as date_day,
        date_part('year', date_day) as date_year,
        date_part('isoyear', date_day) as iso_year,
        date_trunc('year', date_day) as year_start_date,
        cast(date_trunc('year', date_day)
        + interval '1 year'
        - interval '1 day' as date) as year_end_date,
        datediff('day',date_trunc('year', date_day),date_day) + 1 as year_day_number,
        date_diff('year', current_date, date_day) as year_offset,
        if(
            date_diff('year', current_date, date_day) < 0, 1, 0
        ) as year_completed,
        date_part('quarter', date_day) as date_quarter,
        date_trunc('quarter', date_day) as quarter_start_date,
        cast(date_trunc('quarter', date_day)
        + interval '1 quarter'
        - interval '1 day' as date) as quarter_end_date,
        datediff('day',date_trunc('quarter', date_day),date_day) + 1 as quarter_day_number,
        date_diff('quarter', current_date, date_day) as quarter_offset,
        if(
            date_diff('quarter', current_date, date_day) < 0, 1, 0
        ) as quarter_completed,
        date_part('month', date_day) as date_month,
        date_trunc('month', date_day) as month_start_date,
        cast(date_trunc('month', date_day)
        + interval '1 month'
        - interval '1 day' as date) as month_end_date,
        monthname(date_day) as month_name,
        left(monthname(date_day), 3) as month_name_short,
        left(monthname(date_day), 1) as month_initial,
        concat(
            monthname(date_day), ' ', date_part('year', date_day)
        ) as month_and_year,
        yearweek(date_day) as month_and_year_number,
        datediff('day',date_trunc('month', date_day),date_day) + 1 as month_day_number,
        date_diff('month', current_date, date_day) as month_offset,
        if(
            date_diff('month', current_date, date_day) < 0, 1, 0
        ) as month_completed,
        date_part('week', date_day) as date_week,
        week(date_day) as iso_week_of_year,
        date_trunc('week', date_day) as week_start_date,
        cast(date_trunc('week', date_day)
        + interval '1 week'
        - interval '1 day' as date) as week_end_date,
        isodow(date_day) as week_day_number,
        date_diff('week', current_date, date_day) as week_offset,
        if(
            date_diff('week', current_date, date_day) < 0, 1, 0
        ) as week_completed,
        yearweek(date_day) as iso_week_year_number,
        if(date_day > current_date, 1, 0) as is_after_today,
        if(date_day < current_date, 1, 0) as is_before_today,
        if(date_part('isodow', date_day) in (6, 7), 1, 0) as is_weekend,
        if(date_part('isodow', date_day) not in (6, 7), 1, 0) as is_weekday,
        /*has 53 weeks*/
        if(
            week(
                date_trunc('year', date_day)
                + interval '1 year'
                - interval '1 day'
            )
            = 53,
            1,
            0
        ) as has_53_iso_weeks
    from date_spine


)

select * from create_date_table
