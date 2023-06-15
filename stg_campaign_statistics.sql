-- Query to stage our campaign_statistics source table in a view named stg_campaign_statistics

create or replace view `analytics-exports-sandbox.dbt_msouare.stg_campaign_statistics`

as 

with

    source as (select * from `analytics-exports-sandbox.dbt_msouare.campaign_statistics` ),

    transformed as (

        select
            -- ids
            campaign_id as campaign_stats_campaignid,
            creative_id as campaign_stats_creativeid,

            -- measures
            impressions,
            cost,
            platform.google_ads.impressions as google_ads_impressions,
            platform.google_ads.cost as google_ads_cost,
            platform.facebook.cost as facebook_cost,
            platform.facebook.impressions as facebook_impressions,

            -- dates/time
           start_date 
        from source,
        unnest(platforms) AS platform

    )

-- final select
select *
from transformed
