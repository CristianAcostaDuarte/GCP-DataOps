#!/bin/bash

dataset='retail'

table_1='orders'
table_2='order_items'
table_3='products'


region='US'

# First we are goign to create the dataset
bq mk --data_location=$region $dataset

# Now create the orders table
bq mk --table $dataset.$table_1 order_id:INTEGER,order_date:TIMESTAMP,order_customer_id:INTEGER,order_status:STRING

# Now create the order_items table
bq mk --table $dataset.$table_2 order_item_id:INTEGER,order_item_order_id:INTEGER,order_item_product_id:INTEGER,order_item_quantity:INTEGER,order_item_subtotal:FLOAT,order_item_product_price:FLOAT

# Now create the order_items table
bq mk --table $dataset.$table_3 product_id:INTEGER,product_cateogry_id:INTEGER,product_name:STRING,product_description:STRING,product_price:FLOAT,product_image:STRING

