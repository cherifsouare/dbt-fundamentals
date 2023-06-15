/****** Part B - Select Queries ******/

/****************************************************************************************
 1.All the users with the number of orders they have including those who haven't ordered anything yet. 
******************************************************************************************/


SELECT
	U.user_id
   ,U.name
   ,COALESCE(COUNT(DISTINCT O.order_id),0) AS Number_of_Orders   ---Using the distinct function assuming that an order can have many different items and can go through different shops
FROM USERS AS U
LEFT JOIN ORDERS AS O                      --- Using the left join, we want to ensure that all our users are returned regardless wether they have ordered or not
	ON U.user_id = O.user_id
GROUP BY U.user_id
		,U.name
		
	

	

/****************************************************************************************
 2.Users that haven’t got any orders.
 We are leveraging the query we compiled in part 1 and using it as a CTE to make it simpler
******************************************************************************************/

WITH 

Users_Orders AS (

SELECT
	U.user_id
   ,U.name
   ,COALESCE(COUNT(DISTINCT O.order_id),0) AS Number_of_Orders   ---Using the distinct function assuming that an order can have many different items and can go through different shops
FROM USERS AS U
LEFT JOIN ORDERS AS O                      
	ON U.user_id = O.user_id
GROUP BY U.user_id
		,U.name

)

SELECT 
    UO.user_id
   ,UO.name
FROM Users_Orders AS UO
WHERE Number_of_Orders = 0



/****************************************************************************************
 3. Only users that have at least one order of amount greater than 100. Include the total
number of orders (including those below 100 too)
******************************************************************************************/

SELECT
	U.user_id
   ,U.name
   ,COALESCE(COUNT(DISTINCT O.order_id),0) AS Number_of_Orders      ---Using the distinct function assuming that an order  can have many different items and can go through different shops
FROM USERS AS U
INNER JOIN ORDERS AS O                                              -- Using iiner join to return only natching records
	ON U.user_id = O.user_id
WHERE O.transaction_amount > 100                                    -- Filtering for only users that have one order with ammount > 1000
GROUP BY U.user_id
		,U.name