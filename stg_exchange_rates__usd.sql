

with source as (

    select * from {{ source('exchange_rate_yearly', 'exchange_rate_usd') }}

),

transformed as (

    select
        -- Create surrogate key to uniquely identify records
        
        {{
            dbt_utils.surrogate_key (
                   [
                       "currency_code",
                       "year"                 
                    ]
               )
        }} as surrogate_key,

        -- dimensions
        upper(currency_code) as currency_code,

        -- measures
        avg_exchange_rate,

        -- date/times
        cast(year as int) as exchange_rate_year

    from source

)

select * from transformed