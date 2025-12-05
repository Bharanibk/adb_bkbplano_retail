-- Silver layer - Cleaned and transformed sales stream data
-- Reads from Bronze streaming table and applies transformations

CREATE MATERIALIZED VIEW sales_stream_silver
COMMENT "Silver layer - Cleaned sales data with parsed timestamps and calculated fields"
AS SELECT
  -- Original fields
  customer_id,
  customer_name,
  order_number,
  
  -- Parse timestamp from Unix epoch
  from_unixtime(order_datetime) AS order_timestamp,
  to_date(from_unixtime(order_datetime)) AS order_date,
  hour AS order_hour,
  minute AS order_minute,
  
  -- Cast and clean numeric fields
  CAST(number_of_line_items AS INT) AS num_line_items,
  CAST(sales_person AS INT) AS sales_person_id,
  
  -- Calculate array sizes
  size(ordered_products) AS num_ordered_products,
  size(clicked_items) AS num_clicked_items,
  
  -- Keep other fields
  ship_to_address,
  ordered_products,
  clicked_items,
  
  -- Metadata from Bronze
  ingestion_timestamp

FROM LIVE.sales_stream_bronze

WHERE order_number IS NOT NULL  -- Basic quality check
  AND customer_id IS NOT NULL;



-- Silver layer - Cleaned customer data with parsed timestamps and proper types

-- Silver layer - Cleaned customer data with parsed timestamps and proper types

CREATE MATERIALIZED VIEW customer_silver
COMMENT "Silver layer - Cleaned customer data with parsed dates and coordinates"
AS SELECT
  -- IDs
  customer_id,
  tax_id,
  tax_code,
  customer_name,
  
  -- Location fields
  state,
  city,
  postcode,
  street,
  number,
  unit,
  region,
  district,
  
  -- Coordinates (already proper type from Bronze)
  lon,
  lat,
  
  -- Full address
  ship_to_address,
  
  -- Parse Unix timestamps to readable dates
  from_unixtime(valid_from) AS valid_from_timestamp,
  to_date(from_unixtime(valid_from)) AS valid_from_date,
  from_unixtime(valid_to) AS valid_to_timestamp,
  to_date(from_unixtime(valid_to)) AS valid_to_date,
  
  -- Add this to your SELECT:
  CASE 
    WHEN valid_to > unix_timestamp(current_timestamp()) THEN 'Active'
    ELSE 'Inactive'
  END AS customer_status,

  -- Business metrics
  units_purchased,
  loyalty_segment,
  
  -- Metadata from Bronze
  ingestion_timestamp,
  batch_id

FROM LIVE.customer_bronze

WHERE customer_id IS NOT NULL  -- Basic quality check
  AND customer_name IS NOT NULL;





-- Silver layer - Cleaned product data

CREATE MATERIALIZED VIEW product_silver
COMMENT "Silver layer - Cleaned product data with validated prices"
AS SELECT
  -- IDs
  product_id,
  
  -- Product details
  product_category,
  product_name,
  product_unit,
  
  -- Price (already decimal from Bronze)
  sales_price,
  
  -- Barcodes
  EAN13,
  EAN5,
  
  -- Metadata from Bronze
  ingestion_timestamp,
  batch_id

FROM LIVE.product_bronze

WHERE product_id IS NOT NULL  -- Basic quality check
  AND product_name IS NOT NULL
  AND sales_price IS NOT NULL
  AND sales_price > 0  -- Price should be positive;