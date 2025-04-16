# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DecimalType



# Configuration
bucket = "edwinraws-de-retail-project-bucket"
stage = "bronze"
table = "orders"
date = "20250302"
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



# Transformation function
def transform_orders(df):
    # Standardize columns and data types
    silver_df = df.select(
        F.col("order_id"),
        F.col("customer_id"),
        F.to_timestamp("order_date").alias("order_date"),
        F.col("status"),
        F.col("payment_method"),
        F.col("shipping_cost").cast(DecimalType(18, 2)).alias("shipping_cost"),
        F.col("order_total").cast(DecimalType(18, 2)).alias("order_amount"),
        F.date_format("order_date", "yyyyMMdd").alias("partition_date")
    )
    
    # # Data quality checks
    # silver_df = silver_df.filter(
    #     F.col("order_key").isNotNull() &
    #     F.col("customer_key").isNotNull() &
    #     F.col("order_timestamp").isNotNull() &
    #     (F.col("order_amount") > 0)
    # )
    
    # Add derived columns
    silver_df = silver_df.withColumn(
        "order_year_month",
        F.date_format("order_date", "yyyy-MM")
    ).withColumn(
        "order_day_of_week",
        F.date_format("order_date", "EEEE")
    ).withColumn(
        "processing_timestamp",
        F.current_timestamp()
    )
    
    return silver_df




# Apply transformations
silver_df = transform_orders(bronze_df)



# Write to silver layer as Parquet with partitioning
silver_df.write \
    .partitionBy("partition_date") \
    .format("parquet") \
    .mode("append") \
    .option("compression", "snappy") \
    .save(target_path)

# Verify write operation
print(f"Successfully wrote orders data to silver layer at: {target_path}")
print(f"Total records processed: {silver_df.count()}")



