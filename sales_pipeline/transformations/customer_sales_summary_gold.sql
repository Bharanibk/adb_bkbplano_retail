
CREATE OR REPLACE MATERIALIZED VIEW customer_sales_summary_gold AS
SELECT 
  a.customer_id,
  MAX(b.customer_name) AS customer_name,
  MAX(b.state) AS state,
  MAX(b.city) AS city,
  MAX(b.loyalty_segment) AS loyalty_segment,
  
  -- Aggregated sales metrics
  COUNT(DISTINCT a.order_number) AS total_orders,
  SUM(a.num_line_items) AS total_line_items,
  SUM(a.num_ordered_products) AS total_products_ordered,
  MIN(a.order_date) AS first_order_date,
  MAX(a.order_date) AS last_order_date
  
FROM LIVE.sales_stream_silver a
LEFT JOIN LIVE.customer_silver b 
  ON a.customer_id = b.customer_id

GROUP BY a.customer_id
