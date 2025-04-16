# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DecimalType



# Configuration
bucket = "edwinraws-de-retail-project-bucket"
stage = "bronze"
table = "order_items"
date = "20250301"
file_path = f"s3://{bucket}/{stage}/dev/{table}/{date}-*.csv"

appname = "bronze_to_silver_order_items"

target_stage = "silver"
target_path = f"s3://{bucket}/{target_stage}/dev/{table}"



# Initialize Spark Session
spark = SparkSession.builder \
    .appName(appname) \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()



# Read bronze data
bronze_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(file_path)



def transform_order_items(df):
    # Standardize columns and data types
    silver_df = df.select(
        F.col("order_item_id"),
        F.col("order_id"),
        F.col("product_id"),
        F.col("quantity").cast("integer"),
        F.col("unit_price").cast(DecimalType(18, 2)),
        F.col("discount_pct").cast(DecimalType(5, 2)),
        F.col("subtotal").cast(DecimalType(18, 2)),
        # Calculate derived fields
        (F.col("unit_price") * F.col("quantity") * (1 - F.col("discount_pct")/100)).cast(DecimalType(18, 2)).alias("net_amount"),
        F.current_timestamp().alias("processing_timestamp"),
        F.lit(date).alias("partition_date")  # Using the date from configuration
    )
    
    # # Data quality checks
    # silver_df = silver_df.filter(
    #     F.col("order_item_id").isNotNull() &
    #     F.col("order_id").isNotNull() &
    #     F.col("product_id").isNotNull() &
    #     (F.col("quantity") > 0) &
    #     (F.col("unit_price") >= 0) &
    #     (F.col("discount_pct").between(0, 100)) &
    #     (F.col("subtotal") >= 0)
    # )
    
    return silver_df



# Apply transformations
silver_df = transform_order_items(bronze_df)



# Write to silver layer as Parquet with partitioning
silver_df.write \
    .partitionBy("partition_date") \
    .format("parquet") \
    .mode("append") \
    .option("compression", "snappy") \
    .save(target_path)

# Verify write operation
print(f"Successfully wrote order_items data to silver layer at: {target_path}")
print(f"Total records processed: {silver_df.count()}")



