# Databricks notebook source
# Import necessary Spark functions and libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, lag, row_number, coalesce, to_date, lower, trim, sha2, concat_ws, to_timestamp, current_timestamp
from pyspark.sql.window import Window
from datetime import datetime



# Configuration section - sets up paths and parameters for our job
bucket = "edwinraws-de-retail-project-bucket"  # S3 bucket name
stage = "bronze"  # Current stage of the data (bronze is typically raw data)
table = "customers"  # Table we're processing
date = "20250303"  # Date of the data we're processing
file_path = f"s3://{bucket}/{stage}/dev/{table}/{date}-*.csv"  # Path to source CSV files

appname = "bronze_to_silver_customers"  # Spark application name

target_stage = "silver"  # Target stage (silver is typically cleaned/processed data)
target_path = f"s3://{bucket}/{target_stage}/dev/{table}"  # Where to save processed data



# Initialize Spark session - the entry point to Spark functionality
spark = SparkSession.builder.appName(appname).getOrCreate()



# 1. Read new bronze data (raw data)
# Read CSV files with header row and automatic schema inference
new_data = spark.read.csv(file_path, header=True, inferSchema=True)



# 1.1 Clean and standardize data 
new_data = new_data.withColumn("name", lower(trim(col("name"))))\
    .withColumn("email", lower(trim(col("email"))))\
    .withColumn("phone", lower(trim(col("phone"))))\
    .withColumn("address", lower(trim(col("address"))))\
    .withColumn("city", lower(trim(col("city"))))\
    .withColumn("hash_key", sha2(concat_ws("|", col("name"), col("email"), col("phone"), col("city"), col("phone")), 256)) # Create a hash key to detect changes in records by combining key fields



# 2. Check if silver table exists (try to read existing processed data)
try:
    # Read existing silver data (processed data from previous runs)
    existing_silver = spark.read.parquet(target_path)

    print(1)  # Simple debug print

    # Get current records (only the most recent version of each record)
    current_records = existing_silver.filter(col("is_current") == True)

    print(2)  # Simple debug print

    # Debugging prints to help understand the data
    print("Current records count:", current_records.count())
    print("Current records schema:")
    current_records.printSchema()

    # Check how many distinct customer IDs we have
    print("Customer IDs in current_records:", current_records.select("customer_id").distinct().count())
    
    # Detect changes between new data and existing data using hash comparison
    # We alias the dataframes to make column references clearer
    current_records_alias = current_records.alias("current")
    new_data_alias = new_data.alias("new")

    # Find records where customer_id matches but hash_key differs (meaning data changed)
    changed_records = new_data_alias.join(
        current_records_alias,
        # JOIN CONDITION:
        # 1. Customer IDs must match (same customer)
        # 2. Hash keys must differ (data has changed)
        (col("new.customer_id") == col("current.customer_id")) & 
        (col("new.hash_key") != col("current.hash_key")),
        "inner"  # Only keep matching records from both datasets
    ).select(
        # SELECT CLAUSE - What columns to keep in the result:
        col("new.*"),          # All columns from the NEW data version
        col("current.customer_sk"),  # Keep the existing surrogate key (important for tracking)
        col("current.is_current"),   # Current status from existing record
        col("current.valid_from"),   # Original valid_from date
        col("current.valid_to")      # Original valid_to date
    )

    print(3)  # Simple debug print
    
    print("Changed records count:", changed_records.count())
    print("Changed records schema:")
    changed_records.printSchema()
    print("Customer IDs in changed_records:", changed_records.select("customer_id").distinct().count())

    # 3. Prepare SCD2 updates (Slowly Changing Dimension Type 2 - keeps history)
    # 3.1 Identify NEW customers (not in current_records)
    new_customers = new_data.join(
        current_records.select("customer_id"),
        "customer_id",
        "left_anti"  # Anti-join: only keep records in new_data that DON'T exist in current_records
    )
    
    print(f"New customers count: {new_customers.count()}")

    # 3.2 Prepare SCD2 updates for changed records
    # For records that changed, we need to expire the old version
    expired_records = current_records.join(changed_records.select("customer_id"), "customer_id")\
    .withColumn("is_current", lit(False)) \
    .withColumn("valid_to", current_timestamp())  # Set end date to now

    # 3.3 Create updated records for changed customers
    # We need to assign new surrogate keys (SK) to the changed records
    window = Window.orderBy("customer_id")
    # Find the maximum existing SK to start new SKs after it
    max_sk = existing_silver.agg({"customer_sk": "max"}).collect()[0][0] or 0
    
    # Create updated records with new SKs
    updated_records = changed_records.withColumn(
        "customer_sk",
        row_number().over(window) + max_sk  # Generate new sequential SKs
    ).withColumn("is_current", lit(True))\
    .withColumn("valid_from", current_timestamp())\
    .withColumn("valid_to", to_timestamp(lit("9999-12-31 23:59:59")))  # Far future end date

    # 3.4 Create records for new customers
    new_customers_sk = new_customers.withColumn(
        "customer_sk",
        row_number().over(window) + max_sk + changed_records.count()  # Ensure unique SKs
    ).withColumn("is_current", lit(True)) \
     .withColumn("valid_from", current_timestamp()) \
     .withColumn("valid_to", to_timestamp(lit("9999-12-31 23:59:59")))

    # 3.5 Identify unchanged records (to keep as-is)
    unchanged_current_records = current_records.join(
        changed_records.select("customer_id"),
        "customer_id",
        "left_anti"  # Current records with no changes
    )

    # 4. Combine all records into final dataset
    final_silver = (
        existing_silver.filter(col("is_current") == False)  # Keep all historical records
        .unionByName(expired_records)  # Add newly expired records
        .unionByName(updated_records)  # Add updated versions
        .unionByName(new_customers_sk)  # Add new customers
        .unionByName(unchanged_current_records)  # Add unchanged current records
    )
    
except Exception as e:
    # If anything fails (likely first run when silver layer doesn't exist)
    print(f"Error occurred: {str(e)}")
    print("First load - all records are new")

    # First load - all records are new
    window = Window.orderBy("customer_id")
    final_silver = new_data.withColumn("customer_sk", row_number().over(window))\
    .withColumn("is_current", lit(True)) \
    .withColumn("valid_from", current_timestamp()) \
    .withColumn("valid_to", to_timestamp(lit("9999-12-31 23:59:59")))  # Far future end date



# Write final processed data to silver layer in Parquet format (columnar storage)
final_silver.write.parquet(target_path, mode="overwrite")



