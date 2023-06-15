--Query to create a dim_date table in our mart layer

create or replace table `analytics-exports-sandbox.dbt_msouare.dim_date`

as

with


dates_table as (

    select
        d as full_date,
        extract(year from d) as year,
        extract(week from d) as year_week,
        extract(day from d) as year_day,
        extract(year from d) as fiscal_year,
        format_date('%Q', d) as fiscal_qtr,
        extract(month from d) as month,
        format_date('%B', d) as month_name,
        format_date('%w', d) as week_day,
        format_date('%A', d) as day_name,
        (
            case
                when
                    format_date('%A', d) in ('Sunday', 'Saturday')
                    then 0
                else 1
            end
        ) as day_is_weekday
    from (
        select *
        from
            unnest(
                --start date is the min of the campaign start date
                generate_date_array('2022-01-16', current_date, interval 1 day) -- Start date is the min of the start date for campaigns
            ) as d
    )

),

final as (

    select
        sha256(cast(full_date as string)) as date_key   -- hashing the natural key to generate a surrogate key
        full_date,
        year,
        year_week,
        year_day,
        fiscal_year,
        fiscal_qtr,
        month,
        month_name,
        week_day,
        day_name,
        day_is_weekday
    from dates_table
)

select *
from final
