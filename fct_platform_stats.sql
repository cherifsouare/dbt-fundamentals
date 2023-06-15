--Query to create fct_platform_stats table in or data mart layer

create or replace table `analytics-exports-sandbox.dbt_msouare.fact_platform_stats`

as

with

platforms_stats as (


    select
        1 as platform_id,
        'google_ads' as platform, -- platform for google_ads
        campaign_stats_campaignid,
        campaign_stats_creativeid,
        start_date,
        google_ads_cost as cost,
        google_ads_impressions as impressions
    from `analytics-exports-sandbox.dbt_msouare.stg_campaign_statistics`

    union all

    select
        2 as platform_id,
        'facebook' as platform, -- platform for facebook
        campaign_stats_campaignid,
        campaign_stats_creativeid,
        start_date,
        facebook_cost as cost,
        facebook_impressions as impressions
    from `analytics-exports-sandbox.dbt_msouare.stg_campaign_statistics`

),

platform_stats_with_sk as (

    select
        SHA256(
            CONCAT(
                CAST(platform_id as string),
                platform,
                campaign_stats_campaignid,
                campaign_stats_creativeid,
                CAST(start_date as string)
            )
        ) as platform_stats_key, -- hashing the natural keys to generate a surrogate key for the table
        -- FK to dim_creative
        SHA256(CONCAT(CAST(platform_id as string), platform)) as platform_key, -- FK to dim_platform
        SHA256(campaign_stats_campaignid) as campaign_key, -- FK to dim_campaign
        SHA256(campaign_stats_creativeid) as creative_key,-- FK to dim_creative
        SHA256(CAST(start_date as string)) as date_key, -- FK to dim_date
        cost,
        impressions
    from platforms_stats
),

final as (

    select
        platform_stats_key,
        platform_key,
        campaign_key,
        creative_key,
        date_key,
        impressions,
        cost
    from platform_stats_with_sk
)

select *
from final
