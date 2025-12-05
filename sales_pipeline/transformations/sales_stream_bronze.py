from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import *
import uuid


@dp.table(
    comment="Bronze layer - Raw sales stream data"
)
def sales_stream_bronze():
    """
    Reads raw sales_stream JSON data as batch.
    """
    # Generate unique batch_id for this run
    batch_id = str(uuid.uuid4())
    
    # Define schema
    schema = StructType([
        StructField("5minutes", StringType(), True),
        StructField("clicked_items", ArrayType(ArrayType(StringType())), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("datetime", StringType(), True),
        StructField("hour", LongType(), True),
        StructField("minute", LongType(), True),
        StructField("number_of_line_items", StringType(), True),
        StructField("order_datetime", StringType(), True),
        StructField("order_number", LongType(), True),
        StructField("ordered_products", ArrayType(ArrayType(StringType())), True),
        StructField("sales_person", DoubleType(), True),
        StructField("ship_to_address", StringType(), True)
    ])
    
    return (
        spark.read
            .schema(schema)
            .format("json")
            .load("/databricks-datasets/retail-org/sales_stream/sales_stream.json/")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("batch_id", lit(batch_id))
    )



@dp.table(comment="bronze layer - raw customer data")
def customer_bronze():
    batch_id = str(uuid.uuid4())

    schema = StructType([
    StructField("customer_id", StringType(), True),           # ID - keep as string
    StructField("tax_id", StringType(), True),                # ID - keep as string
    StructField("tax_code", StringType(), True),              # Code - keep as string
    StructField("customer_name", StringType(), True),         # Name - string
    StructField("state", StringType(), True),                 # State code - string
    StructField("city", StringType(), True),                  # City - string
    StructField("postcode", StringType(), True),              # Zip code - string (can have leading zeros)
    StructField("street", StringType(), True),                # Street - string
    StructField("number", StringType(), True),                # Street number - string (can be "123A")
    StructField("unit", StringType(), True),                  # Unit - string
    StructField("region", StringType(), True),                # Region - string
    StructField("district", StringType(), True),              # District - string
    StructField("lon", DoubleType(), True),                   # Longitude - decimal
    StructField("lat", DoubleType(), True),                   # Latitude - decimal
    StructField("ship_to_address", StringType(), True),       # Address - string
    StructField("valid_from", LongType(), True),              # Timestamp - Unix epoch (like order_datetime)
    StructField("valid_to", LongType(), True),                # Timestamp - Unix epoch
    StructField("units_purchased", IntegerType(), True),      # Count - integer
    StructField("loyalty_segment", IntegerType(), True)       # Segment ID - integer
    ]) 

    return (
        spark.read
            .schema(schema)
            .format("csv")
            .option("header", "true")  # ADD THIS LINE!
            .load("/databricks-datasets/retail-org/customers/customers.csv")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("batch_id", lit(batch_id))
    )


@dp.table(comment="bronze layer - raw product data")
def product_bronze():
    batch_id = str(uuid.uuid4())

    schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("sales_price", DecimalType(10, 2), True),  # Price with 2 decimals
        StructField("EAN13", StringType(), True),              # Barcode
        StructField("EAN5", StringType(), True),               # Barcode
        StructField("product_unit", StringType(), True)
    ])

    return (
        spark.read
            .format("csv")
            .option("header", "true")
            .option("delimiter", ";")
            .load("/databricks-datasets/retail-org/products/products.csv")
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("batch_id", lit(batch_id))
    )








