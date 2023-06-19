/****************************************************************************************
 1. Count the number of users who purchased at least one policy of the fixed type,
making sure that the overall transaction value for each user is greater than zero.
******************************************************************************************/
with

fixed_users as (

    select
        user.user_id,
        policy_transaction.transaction_gwp
    from user
    inner join policy_transaction on user.user_id = policy_transaction.user_id
    inner join product on transaction_policy.product_id = product.product_id
    where
        product.product_code like '%fixed%'    -- ensure we are only returning results for the fixed type product
        and product.is_billed_upfront is true  -- assumption here is that it's an annual product therefore users are billed upfront

),

fixed_users_value as (

    select
        fixed_users.user_id,
        coalesce(sum(fixed_users.transaction_gwp),0) as overall_transaction_value
    from fixed_users
    group by fixed_users.user_id

),

final as (

    select count(user_id)
    from fixed_users_value
    where overall_transaction_value > 0
)

-- Simple select statement
select *
from final
