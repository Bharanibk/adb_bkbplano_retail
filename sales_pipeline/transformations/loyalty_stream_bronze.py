from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit, max as spark_max
import uuid

@dp.table(comment="Bronze layer - Incremental loyalty segments")
def loyalty_segments_bronze():
    """
    Dynamically loads next loyalty segment based on max existing ID.
    """
    batch_id = str(uuid.uuid4())
    
    # Read full source directly from file
    source_df = spark.read.format("csv").option("header", "true").load("/databricks-datasets/retail-org/loyalty_segments/*.csv")
    
    # Try to get current max loyalty_segment_id from bronze
    try:
        existing_bronze = spark.table("workspace.retail.loyalty_segments_bronze")
        max_id = existing_bronze.agg(spark_max(col("loyalty_segment_id"))).collect()[0][0]
        next_id = str(int(max_id) + 1) if max_id is not None else "0"
    except:
        # Table doesn't exist yet, start with 0
        next_id = "0"
    
    # Load only the next row
    incremental_df = source_df.filter(col("loyalty_segment_id") == next_id)
    
    return (
        incremental_df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("batch_id", lit(batch_id))
            .withColumn("load_sequence", lit(next_id))
    )