/****************************************************************************************
 4. Add a column to policy_transaction that shows whether the previous transaction was
    a mid-term adjustment.
******************************************************************************************/

-- Note: Conceptually we could alter the table, add the column and update. However, BigQuery doesn't support CTEs in Update statement.

create or replace transaction_policy as

-- Step 1 Given that a policy is always linked a to a user, let's find the previous start date of each policy

with

    previous_policy_date as (
        select 
            policy_transaction.user_id,
            policy_transaction.inception_policy_id,
            policy_transaction.effective_start_date,
            lag(policy_transaction.effective_start_date) over (
                partition by
                    policy_transaction_user_id, policy_transaction.inception_policy_id
                order by policy_transaction.effective_start_date
            ) as previous_start_date
        from policy_transaction
    ),

-- Step 2 Add a flag in the original policy_transaction table using the results in previous step
   
   final as (

        select
            policy_transaction.*,  -- Pulling all columns from the original table
            case
                when
                    previous_policy_date.previous_start_date
                    < policy_transaction.effective_start_date
                    and transaction_type.transaction_type_description
                    = 'mid_term_adjustment'
                then true
                else false
           end as is_previous_transaction_adjustment
        from policy_transaction
        left join
            previous_policy_date
            on policy_transaction.user_id = previous_policy_date.user_id
            and policy_transaction.inception_policy_id = previous_policy_date.inception_policy_id
            and policy_transaction.effective_start_date = previous_policy_date.effective_start_date
        left join product on policy_transaction.product_id = product.product_id
        left join
            transaction_type
            on transaction_policy.transaction_type_id
            = transaction_type.transaction_type_id
    )

-- Simple select statement
select *
from final
