-- Query to create dim_creative table in our mart layer

create or replace table `analytics-exports-sandbox.dbt_msouare.dim_creative`

as

with

creative as (

    select
        *,
        SHA256(creative_creativeid) as creative_key, -- hashing the natural key to generate a surrogate key

    from `analytics-exports-sandbox.dbt_msouare.stg_creatives`
),

final as (
    select
        creative_key,
        creative_creativeid,
        content,
        call_to_action,
        url,
        image
    from creative
)

select *
from final
