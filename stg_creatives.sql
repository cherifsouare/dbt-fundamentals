-- Query to stage our creatives source table in a view named stg_creatives

create or replace view `analytics-exports-sandbox.dbt_msouare.stg_creatives`

as 

with

    source as (select * from `analytics-exports-sandbox.dbt_msouare.creatives` ),

    transformed as (

        select
            -- ids
            creative_id as creative_creativeid,

            -- dimensions
            content as content,
            call_to_action as call_to_action,
            url as url,
            image as image

        from source

    )

-- final select
select *
from transformed
