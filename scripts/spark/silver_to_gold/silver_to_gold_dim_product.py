# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType



# Configuration
bucket = "edwinraws-de-retail-project-bucket"
source_stage = "silver"
source_table = "products"
source_path = f"s3://{bucket}/{source_stage}/dev/{source_table}"

appname = "silver_to_gold_dim_product"

target_stage = "gold"
target_table = "dim_product"
target_path = f"s3://{bucket}/{target_stage}/dev/{target_table}"



# Read silver data (only current records)
silver_products = spark.read.parquet(source_path).filter(F.col("is_current") == True)

# Transform to dimension table with explicit schema enforcement
dim_product = silver_products.select(
    F.col("product_sk").cast("string").alias("product_key"),
    F.col("product_id").cast("string").alias("product_natural_key"),
    F.col("product_name").cast("string"),
    F.col("price").cast(DecimalType(18,2)).alias("current_price"),
    F.col("stock_quantity").cast("integer"),
    F.col("category").cast("string"),
    F.col("valid_from").cast("timestamp").alias("row_effective_date"),
    F.col("valid_to").cast("timestamp").alias("row_expiration_date"),
    F.col("is_current").cast("boolean").alias("current_row_flag"),
    F.current_timestamp().cast("timestamp").alias("etl_processed_date")
)



# Write to gold layer with Redshift-compatible settings
(dim_product
 .repartition(1)  # Single file for small datasets, adjust as needed
 .write
 .mode("overwrite")
 .option("compression", "snappy")
 # Additional Redshift-compatible options
 .option("parquet.enable.dictionary", "true")
 .option("parquet.page.size", "1048576")  # 1MB page size
 .parquet(target_path)
)

print(f"Dimension table {target_table} created successfully at {target_path}")
print(f"Total product records: {dim_product.count()}")



