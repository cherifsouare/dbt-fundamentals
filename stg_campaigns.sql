-- Query to stage our campaign table in a view named stg_campaigns

create or replace view `analytics-exports-sandbox.dbt_msouare.stg_campaigns`

as 

with

    source as (select * from `analytics-exports-sandbox.dbt_msouare.campaigns` ),

    transformed as (

        select
            -- ids
            campaign_id as campaign_campaignid,

            -- dimensions
            status as status,
            country as country,

            -- date/times
            created as created_date

        from source

    )

-- final select
select *
from transformed
