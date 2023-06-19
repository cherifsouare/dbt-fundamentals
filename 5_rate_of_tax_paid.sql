/****************************************************************************************
 5. Add a column to policy_transaction that shows the rate of tax paid.
******************************************************************************************/

-- Step 1 Let's add the column to the table first

ALTER TABLE policy_transaction
ADD COLUMN tax_rate_paid DECIMAL(5, 2);


-- Step 2 Update the column value by unnesting the JSON (struct) data type column transaction breakdown and access the relevant elements

UPDATE policy_transaction AS ptr
SET tax_rate_paid = (
  SELECT tbr.transaction_breakdown.transaction.tax / tbr.transaction_breakdown.transaction.premium         
  FROM UNNEST([ptr.transaction_breakdown]) AS tbr                   --first convert the column as an array of struct and unnest it to get individual values
)
WHERE ptr.transaction_breakdown IS NOT NULL;                       -- Ignoring records in which no revenue is generated yet.
