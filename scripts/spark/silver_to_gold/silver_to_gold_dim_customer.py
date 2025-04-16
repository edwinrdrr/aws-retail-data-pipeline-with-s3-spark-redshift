# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



# Configuration
bucket = "edwinraws-de-retail-project-bucket"
source_stage = "silver"
source_table = "customers"
source_path = f"s3://{bucket}/{source_stage}/dev/{source_table}"

appname = "silver_to_gold_dim_customer"

target_stage = "gold"
target_table = "dim_customer"
target_path = f"s3://{bucket}/{target_stage}/dev/{target_table}"



# Initialize Spark Session
spark = SparkSession.builder.appName(appname).getOrCreate()



# Read silver data (only current records)
silver_customers = spark.read.parquet(source_path).filter(F.col("is_current") == True)

# Transform to dimension table with explicit schema casting
dim_customer = silver_customers.select(
    F.col("customer_sk").alias("customer_key").cast("string"),
    F.col("customer_id").alias("customer_natural_key").cast("string"),
    F.col("name").alias("customer_name").cast("string"),
    F.col("email").cast("string"),
    F.col("phone").cast("string"),
    F.col("address").cast("string"),
    F.col("city").cast("string"),
    F.col("valid_from").alias("row_effective_date").cast("timestamp"),
    F.col("valid_to").alias("row_expiration_date").cast("timestamp"),
    F.col("is_current").alias("current_row_flag").cast("boolean"),
    F.current_timestamp().alias("etl_processed_date").cast("timestamp")
)



# # completely clean the target directory before writing new files
# dbutils.fs.rm(target_path, recurse=True)

# Then write as before
(dim_customer
 .repartition(1)
 .write
 .format("parquet")  # Explicitly specify format
 .mode("overwrite")
 .option("compression", "snappy")
 .parquet(target_path)
)

print(f"Dimension table {target_table} created successfully at {target_path}")
print(f"Total customer records: {dim_customer.count()}")



