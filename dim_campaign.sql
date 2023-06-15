--Query to create a dim_campaign table in our mart layer

create or replace table `analytics-exports-sandbox.dbt_msouare.dim_campaign`

as

with

campaign as (

    select
        *,
        SHA256(campaign_campaignid) as campaign_key, -- hashing the natural key to generate a surrogate key
    from `analytics-exports-sandbox.dbt_msouare.stg_campaigns`
),

final as (
    select
        campaign_key,
        campaign_campaignid,
        status,
        country
    from campaign
)

select *
from final
