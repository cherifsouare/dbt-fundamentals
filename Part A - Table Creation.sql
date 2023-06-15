/****** Part A - Table Creation ******/

-- Create the Users table

CREATE TABLE [USERS] (
	[user_id] INT IDENTITY (10, 1) NOT NULL  -- user_id will start with 10 and increment by 1 automatically for a new record
   ,[name] VARCHAR(400) -- with the assumption that a full name can take up to 400 characters
   ,[date_of_birth] DATE
   ,CONSTRAINT [PK_USERS_user_id] PRIMARY KEY CLUSTERED (user_id) --Define the primary key on the user_id column
)
;

-- Create the Orders table
CREATE TABLE ORDERS (
	[order_id] INT IDENTITY (1000, 1) NOT NULL -- order_id will start with 1000 and increment by 1 automatically for a new record
   ,[user_id] INT NOT NULL
   ,[shop_id] INT
   ,[number_of_items] INT
   ,[transaction_amount] DECIMAL(10, 2)
   ,CONSTRAINT [PK_ORDERS_order_id] PRIMARY KEY CLUSTERED (order_id) --Define the primary key on the order_id column
);

-- Let's now create a foreign key in the Orders table that refernces user_id in the Users table
ALTER TABLE ORDERS
ADD CONSTRAINT FK_OrdersUsers FOREIGN KEY (user_id)
REFERENCES USERS (user_id)
ON DELETE CASCADE
ON UPDATE CASCADE --ensure that referenced rows are updated when it's the case in Users
;