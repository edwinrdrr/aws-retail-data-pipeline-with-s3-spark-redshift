# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, lag, row_number, coalesce, to_date, lower, trim, sha2, concat_ws, to_timestamp, current_timestamp
from pyspark.sql.window import Window
from datetime import datetime



# Configuration
bucket = "edwinraws-de-retail-project-bucket"
stage = "bronze"
table = "products"  # Changed from customers to products
date = "20250306"
file_path = f"s3://{bucket}/{stage}/dev/{table}/{date}-*.csv"

appname = "bronze_to_silver_products"  # Updated app name

target_stage = "silver"
target_path = f"s3://{bucket}/{target_stage}/dev/{table}"



# Initialize Spark
spark = SparkSession.builder.appName(appname).getOrCreate()



# 1. Read new bronze data
new_data = spark.read.csv(file_path, header=True, inferSchema=True)

# 1.1 Clean and standardize product data 
new_data = new_data.withColumn("product_name", lower(trim(col("product_name")))) \
    .withColumn("category", lower(trim(col("category")))) \
    .withColumn("hash_key", sha2(concat_ws("|", 
        col("product_name"), 
        col("price"), 
        col("stock_quantity"), 
        col("category")), 256))



# 2. Check if silver table exists
try:
    # Read existing silver data
    existing_silver = spark.read.parquet(target_path)

    # Get current records
    current_records = existing_silver.filter(col("is_current") == True)
    
    # Debugging prints
    print("Current records count:", current_records.count())
    print("Current records schema:")
    current_records.printSchema()
    print("Product IDs in current_records:", current_records.select("product_id").distinct().count())
    
    # Detect changes using hash
    current_records_alias = current_records.alias("current")
    new_data_alias = new_data.alias("new")

    changed_records = new_data_alias.join(
        current_records_alias,
        (col("new.product_id") == col("current.product_id")) & 
        (col("new.hash_key") != col("current.hash_key")),
        "inner"
    ).select(
        col("new.*"),
        col("current.product_sk"),
        col("current.is_current"),
        col("current.valid_from"),
        col("current.valid_to")
    )
    
    # Debugging prints
    print("Changed records count:", changed_records.count())
    print("Changed records schema:")
    changed_records.printSchema()
    print("Product IDs in changed_records:", changed_records.select("product_id").distinct().count())

    # 3. Prepare SCD2 updates
    # 3.1 Identify NEW products (not in current_records)
    new_products = new_data.join(
        current_records.select("product_id"),
        "product_id",
        "left_anti"
    )
    
    print(f"New products count: {new_products.count()}")

    # 3.2 Prepare SCD2 updates for changed records
    expired_records = current_records.join(
        changed_records.select("product_id"), 
        "product_id"
    ).withColumn("is_current", lit(False)) \
     .withColumn("valid_to", current_timestamp())

    # 3.3 Create updated records for changed products
    window = Window.orderBy("product_id")
    max_sk = existing_silver.agg({"product_sk": "max"}).collect()[0][0] or 0
    
    updated_records = changed_records.withColumn(
        "product_sk",
        row_number().over(window) + max_sk
    ).withColumn("is_current", lit(True)) \
     .withColumn("valid_from", current_timestamp()) \
     .withColumn("valid_to", to_timestamp(lit("9999-12-31 23:59:59")))

    # 3.4 Create records for new products
    new_products_sk = new_products.withColumn(
        "product_sk",
        row_number().over(window) + max_sk + changed_records.count()
    ).withColumn("is_current", lit(True)) \
     .withColumn("valid_from", current_timestamp()) \
     .withColumn("valid_to", to_timestamp(lit("9999-12-31 23:59:59")))

    # 3.5 Get unchanged current records
    unchanged_current_records = current_records.join(
        changed_records.select("product_id"),
        "product_id",
        "left_anti"
    )

    # 4. Combine all records
    final_silver = (
        existing_silver.filter(col("is_current") == False)
        .unionByName(expired_records)
        .unionByName(updated_records)
        .unionByName(new_products_sk)
        .unionByName(unchanged_current_records)
    )
    
except Exception as e:
    print(f"Error occurred: {str(e)}")
    print("First load - all records are new")

    # First load - all records are new
    window = Window.orderBy("product_id")
    final_silver = new_data.withColumn("product_sk", row_number().over(window)) \
        .withColumn("is_current", lit(True)) \
        .withColumn("valid_from", current_timestamp()) \
        .withColumn("valid_to", to_timestamp(lit("9999-12-31 23:59:59")))



# Write to silver layer
final_silver.write.parquet(target_path, mode="overwrite")



