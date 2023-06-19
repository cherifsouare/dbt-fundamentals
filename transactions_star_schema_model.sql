/****************************************************************************************
part 2: write the necessary sql to produce a single data model from the sources supplied in
supplement 2 that would allow a user of a modern bi software tool to answer any of the
questions in part 1.
******************************************************************************************/

/****************************************************************************************
 dim_product
 create sql code to create the product dimension table
******************************************************************************************/

create or replace dim_product 
as 
select 
    farm_fingerprint(cast(product_id as string)) as product_key,  -- hashing the natural key to create a surrogate key
    product_id,
    product_code,
    split(product_code, '_')[safe_offset(0)] as country,
    split(product_code, '_')[safe_offset(1)] as vertical,
    split(product_code, '_')[safe_offset(2)] as product_type,
    split(product_code, '_')[safe_offset(3)] as vehicle_type,
    is_billed_upfront
from product


/****************************************************************************************
 dim_policy
 create sql code to create the policy dimension table
******************************************************************************************/

create or replace dim_policy 

as 

with

policy_transactions as (

    select distinct
        policy_transaction.inception_policy_id,
        policy_transaction.adjustment_policy_id,
        transaction_type.transaction_type_id,
        transaction_type.transaction_type,
        transaction_type.is_adjustment,
        cast(policy_transaction.change_number as string) -- transforming this field into a dimension field
    from transaction_type
    left join
        policy_transaction
        on transaction_type.transaction_type_id
        = policy_transaction.transaction_type_id
),

final as (
    select distinct
        farm_fingerprint(
            concat(
                cast(policy_transactions.inception_policy_id as string),
                cast(policy_transactions.adjustment_policy_id as string),
                cast(policy_transactions.transaction_type_id as string)
            )
        ) as policy_key,  -- hashing the 3 keys to create a surrogate key
        policy_transactions.inception_policy_id,
        policy_transactions.adjustment_policy_id,
        policy_transactions.transaction_type_id,
        policy_transactions.transaction_type,
        policy_transactions.is_adjustment,
        policy_transactions.change_number
    from policy_transactions
)

-- simple select statement  

select *
from final


/****************************************************************************************
 dim_user
 create sql code to create the user table
******************************************************************************************/

create or replace dim_user 
as 
select 
    farm_fingerprint(user_id) as user_key,  -- hashing the natural key to create a surrogate key
    user_id,
    given_names,
    last_names,
    concat(given_names,' ',last_names) as full_name,
    address_1,
    address_2,
    city,
    postcode,
    country as user_country
from user


/****************************************************************************************
 dim_date
 create sql code to create the date dimension table
******************************************************************************************/

create or replace table dim_date

as

with

dates_table as (

    select
        d as full_date,
        extract(year from d) as year,
        extract(week from d) as year_week,
        extract(day from d) as year_day,
        extract(year from d) as fiscal_year,
        format_date('%q', d) as fiscal_qtr,
        extract(month from d) as month,
        format_date('%b', d) as month_name,
        format_date('%w', d) as week_day,
        format_date('%a', d) as day_name,
        (
            case
                when
                    format_date('%a', d) in ('sunday', 'saturday')
                    then 0
                else 1
            end
        ) as day_is_weekday
    from (
        select *
        from
            unnest(
                generate_date_array('2023-01-01', current_date, interval 1 day)
            ) as d
    )

),

final as (

    select
        cast(format_date('%y%m%d', full_date) as int64) as date_key -- converting the full_date in int64 to create the surrogate_key
        full_date,
        year,
        year_week,
        year_day,
        fiscal_year,
        fiscal_qtr,
        month,
        month_name,
        format_date('%y-%m', months) as month_year
        week_day,
        day_name,
        day_is_weekday
    from dates_table
)

-- simple select statement
select *
from final


/****************************************************************************************
 fact_policy_transaction
 create sql code to create the fact_policy_transaction table
******************************************************************************************/

create or replace fact_policy_transaction 

as

with 

transactions as (
      select
          user_id,
          inception_policy_id,
          adjustment_policy_id,
          product_id,
          transaction_type_id,
          effective_start_date,
          effective_end_date,
          transaction_gwp,
          policy_total_gwp,
          tbr.transaction_breakdown.transaction.tax / tbr.transaction_breakdown.transaction.premium  as tax_rate_paid
      from policy_transaction,
           unnest([policy_transaction.transaction_breakdown]) as tbr 

),

final as (

   select 
       farm_fingerprint(transactions.user_id) as user_key,-- FK to dim_user
       farm_fingerprint(
            concat(
                cast(transactions.inception_policy_id as string),
                cast(transactions.adjustment_policy_id as string),
                cast(transactions.transaction_type_id as string)
            )
        ) as policy_key, -- FK to dim_policy
        farm_fingerprint(cast(transactions.product_id as string)) as product_key,  -- fk to dim_product
        cast(format_date('%y%m%d', transactions.effective_start_date) as int64) as start_date_key, -- fk to dim_date
        cast(format_date('%y%m%d', transactions.effective_end_date) as int64) as end_date_key, --fk to dim_date
        transactions.transaction_gwp,
        transactions.policy_total_gwp,
        transactions.tax_rate_paid
    from transactions
)

--Simple select statement
select *
from final


