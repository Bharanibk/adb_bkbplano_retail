-- Gold layer - Hourly sales summary for business reporting
-- Aggregates silver data by date and hour

CREATE OR REPLACE MATERIALIZED VIEW sales_hourly_summary_gold
  COMMENT "Gold layer - Hourly aggregated sales metrics"
AS SELECT
  order_date,
  order_hour,
  
  -- Aggregated metrics
  COUNT(order_number) AS total_orders,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT sales_person_id) AS unique_sales_people,
  
  -- Product metrics
  SUM(num_line_items) AS total_line_items,
  SUM(num_ordered_products) AS total_products_ordered,
  SUM(num_clicked_items) AS total_items_clicked,
  
  -- Averages
  AVG(num_line_items) AS avg_line_items_per_order,
  AVG(num_ordered_products) AS avg_products_per_order,
  
  -- Time range for the hour
  MIN(order_timestamp) AS first_order_time,
  MAX(order_timestamp) AS last_order_time

FROM LIVE.sales_stream_silver

GROUP BY order_date, order_hour
ORDER BY order_date, order_hour