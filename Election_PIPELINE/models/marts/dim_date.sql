
{{config(schema='tweets_mart')}}


WITH date_spine as (
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2020-01-01' as date)",
    end_date="cast('2020-12-31' as date)"
) }}
)


SELECT 
    CAST(to_char(DATE_DAY , 'YYYYMMDD') as int) as date_key , 
    extract(year from DATE_DAY) as year,
    extract(quarter from DATE_DAY) as quarter,
    extract(month from DATE_DAY) as month,
    extract(week from DATE_DAY) as week,
    extract(day from DATE_DAY) as day,
    dayname(DATE_DAY) as day_name,
    monthname(DATE_DAY) as month_name

FROM date_spine
