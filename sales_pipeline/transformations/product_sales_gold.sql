-- Gold layer - Order line items with product details

CREATE OR REPLACE MATERIALIZED VIEW order_line_items_gold
COMMENT "Gold layer - Detailed order line items enriched with product information"
AS 
WITH exploded_products AS (
  SELECT 
    customer_id,
    customer_name,
    order_number,
    order_date,
    order_hour,
    order_timestamp,
    sales_person_id,
    explode(ordered_products) AS product_array
  FROM LIVE.sales_stream_silver
),
parsed_products AS (
  SELECT 
    customer_id,
    customer_name,
    order_number,
    order_date,
    order_hour,
    order_timestamp,
    sales_person_id,
    product_array[0] AS currency,
    product_array[1] AS product_id,
    product_array[2] AS product_name_from_order,
    CAST(product_array[3] AS DECIMAL(10,2)) AS price,
    CAST(product_array[4] AS INT) AS quantity,
    product_array[5] AS unit
  FROM exploded_products
)
SELECT 
  -- Order info
  a.order_number,
  a.customer_id,
  a.customer_name,
  a.order_date,
  a.order_hour,
  a.order_timestamp,
  a.sales_person_id,
  
  -- Product info from products table
  b.product_id,
  b.product_name,
  b.product_category,
  b.sales_price AS catalog_price,
  
  -- Line item details from order
  a.price AS order_price,
  a.quantity,
  a.currency,
  a.unit,
  
  -- Calculated fields
  (a.price * a.quantity) AS line_total
  
FROM parsed_products a
LEFT JOIN LIVE.product_silver b 
  ON a.product_id = b.product_id