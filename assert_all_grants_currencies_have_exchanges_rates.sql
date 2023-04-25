-- This is a singular test tests our assumptions that all the currencies in the grants data have teir corresponding USD exchange rates in the exchange rates data source.

{{
    config(
        severity='warn'
    )
}}

select 
   original_funding_currency,
   count(distinct original_funding_currency) as total_currencies
from {{ ref("int_grants_only__joigned") }}
where
    original_funding_currency is not null
    and original_funding_currency <> 'USD'
    and original_funding_amount is not null
    and funding_amount_usd is null
    and is_current_funding = 1
group by 1
having not (total_currencies = 0)
