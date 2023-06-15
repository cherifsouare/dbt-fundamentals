--Query to create a dim_platform table in or data mart layer

create or replace table `analytics-exports-sandbox.dbt_msouare.dim_platform`

as

with

platforms as (


    select
        1 as platform_id,

        'google_ads' as platform -- platform for google_ads

    union all

    select
        2 as platform_id,
        'facebook' as platform -- platform for facebook

),

final as (

    select
        SHA256(concat(cast(platform_id as string), '-', platform)) as platform_key, -- hashing the concatenated columns to create a surrogate key
        platform_id,
        platform
    from platforms
)

select *
from final
