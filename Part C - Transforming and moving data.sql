/****** Part C - Moving and transforming data ******/

/****************************************************************************************
 1. Create Table As Select statement for the denormalized table user_orders 
******************************************************************************************/

CREATE TABLE user_orders AS
SELECT
	U.user_id
   ,U.name
   ,U.date_of_birth
   ,O.order_id
   ,O.shop_id
   ,O.number_of_items
   ,O.transaction_amount
FROM USERS AS U
LEFT JOIN ORDERS AS O             -- Using a left join will ensure that all the users are accounted for
	ON U.user_id = O.user_id;


/****************************************************************************************
 2. Create SQL code for an incremental update of the denormalized table user_orders 
******************************************************************************************/

MERGE user_orders AS target USING (
	-- Let's look for the new data for update in our source tables
	SELECT
		USERS.user_id
	   ,USERS.name
	   ,USERS.date_of_birth
	   ,ORDERS.order_id
	   ,ORDERS.shop_id
	   ,ORDERS.number_of_items
	   ,ORDERS.transaction_amount
	FROM USERS
	INNER JOIN ORDERS
		ON USERS.user_id = ORDERS.user_id
	WHERE ORDERS.order_id > (SELECT
			MAX(order_id)             -- If there are records in our source ORDERS table, they will have an order_id greater than the latest order_id in user_orders
		FROM user_orders)) AS source
ON target.order_id = source.order_id

-- Update matched rows
WHEN MATCHED
	THEN UPDATE
		SET target.name = source.name
		   ,target.date_of_birth = source.date_of_birth
		   ,target.shop_id = source.shop_id
		   ,target.number_of_items = source.number_of_items
		   ,target.transaction_amount = source.transaction_amount -- when new rows matched then update the rows columns in user_orders

-- Insert new rows
WHEN NOT MATCHED BY target
	THEN INSERT (user_id, name, date_of_birth, order_id, shop_id, number_of_items, transaction_amount)
			VALUES (source.user_id, source.name, source.date_of_birth, source.order_id, source.shop_id, source.number_of_items, source.transaction_amount); --- add new records that are new into user_orders


/****************************************************************************************
 3. Query to to store the first initial of the name followed by ‘***’
******************************************************************************************/

UPDATE user_orders
SET name = CONCAT(SUBSTRING(name, 1, 1), '***')  -- extracting the first initial of the name and adding the second string ***