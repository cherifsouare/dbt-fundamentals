--Query to create fct_campaign_stats table in or data mart layer

create or replace table `analytics-exports-sandbox.dbt_msouare.fact_campaign_stats`

as

with

campaign_stats_with_sk as (


    select
        SHA256(
            CONCAT(
                campaign_stats_campaignid,
                campaign_stats_creativeid,
                CAST(start_date as string)
            )
        ) as campaign_stats_key, -- hashing the concatenated columns to create a surrogate key
        SHA256(campaign_stats_campaignid) as campaign_key, -- FK to dim_campaign
        SHA256(campaign_stats_creativeid) as creative_key, -- FK to dim_creative
        SHA256(CAST(start_date as string)) as date_key -- FK to dim_date
		cost,
		impressions
    from `analytics-exports-sandbox.dbt_msouare.stg_campaign_statistics`
),

final as (

    select
        campaign_stats_key,
        campaign_key,
        creative_key,
        date_key,
        cost,
        impressions
    from campaign_stats_with_sk
)

select *
from final
