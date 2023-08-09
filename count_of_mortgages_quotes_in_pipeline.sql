/****************************************************************************************
 How many of our mortgage quotes haven't been completed yet?
******************************************************************************************/
-- Step 1 :Create a calendar table for 2023 up to current date. This will ensure to display the records even in the absence of quotes for the entire period considered.

with

dates_table as (

    select d as full_date
    from
        (
            select *
            from
                unnest(
                    generate_date_array(
                        '2023-01-01', current_date, interval 1 day 
                    ) -- Start date is the first day of 2023
                ) as d
        )

),

-- Step 2 : Join date table to mortgage journey  data. Where conditions nor met,the number of quotes in pipeline will show 0

quotes_in_pipeline as (
    select
        dates_table.full_date,
        coalesce(
            count(mortgage_journey.mortgage_journey_id), 0
        ) as number_of_quotes_in_pipeline  -- assuming here that mortgage_journey_id is PK and therefore unique in the table
    from dates_table
    left join
        mortgage_journey
        on dates_table.full_date = cast(mortgage_journey.step__quote_ts as date) -- converting timestamp to date
    where
        mortgage_journey.step__completed_ts is null
        and mortgage_journey.step__closed_lost_ts is null
    group by 1
    order by 1
)

-- Step 3: Simple select statement
select *
from quotes_in_pipeline
