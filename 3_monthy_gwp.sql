/****************************************************************************************
 3. Extract the monthly GWP in 2023 to date by vehicle type, ensuring there is a row for
    each vehicle type and month even if no transactions took place.
******************************************************************************************/


-- Create a calendar table for 2023 up to current date. This will ensure to display the records even in the absence of transactions for the entire period considered.


with

    calendar_months as (
        select
            months,
            extract(month from months) as month,
            extract(year from months) as year,
            format_date('%Y-%m', months) as month_year
        from
            unnest(
                generate_date_array(
                    date('2023-01-01'), current_date(), interval 1 month
                )
            ) as months
    ),
    monthly_gwp as (

        select
            calendar_months.month_year,
            split(product.product_code, '_')[safe_offset(3)] as vehicle_type,    -- extracting the vehicule type from the product code
            transaction_policy.transaction_gwp
        from calendar_months
        left join
            policy_transaction
            on calendar_months.month
            = extrat(month from policy_transaction.effective_start_date)
            and calendar_months.year
            = extrat(year from policy_transaction.effective_start_date)
        left join product on policy_transaction.product_id = product.product_id
        left join
            transaction_type
            on transaction_policy.transaction_type_id
            = transaction_type.transaction_type_id
        where transaction_type.transaction_type_description = 'new_policy' -- considering the new policy traqnsaction type only
    )

    final as (
        select
            monthly_gwp.month_year,
            monthly_gwp.vehicle_type,
            coalesce(sum(monthly_gwp.transaction_gwp)) as total_monthly_gwp
        from monthly_gwp
        order by monthly_gwp.month_year,
                 monthly_gwp.vehicle_type
    )

-- Simple select statement
select *
from final
