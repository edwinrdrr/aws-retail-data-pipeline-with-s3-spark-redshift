# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime



# Configuration
bucket = "edwinraws-de-retail-project-bucket"
target_stage = "gold"
target_table = "dim_date"
target_path = f"s3://{bucket}/{target_stage}/dev/{target_table}"

appname = "silver_to_gold_dim_date"

# Date range parameters
start_date = "2010-01-01"
end_date = "2030-12-31"



# Initialize Spark Session
spark = SparkSession.builder.appName(appname).getOrCreate()



# Create date range using Spark native functions (memory efficient)
# Create date range (2010-2030)
date_df = spark.sql("""
    SELECT 
        date_format(dt, 'yyyyMMdd')::int AS date_key,
        dt AS full_date,
        dayofweek(dt) AS day_of_week,
        date_format(dt, 'EEEE') AS day_name,
        dayofmonth(dt) AS day_of_month,
        dayofyear(dt) AS day_of_year,
        weekofyear(dt) AS week_of_year,
        month(dt) AS month,
        date_format(dt, 'MMMM') AS month_name,
        quarter(dt) AS quarter,
        year(dt) AS year,
        dayofweek(dt) IN (1,7) AS is_weekend,
        false AS is_holiday,
        current_timestamp() AS etl_processed_date
    FROM (
        SELECT explode(sequence(
            to_date('2010-01-01'), 
            to_date('2030-12-31'), 
            interval 1 day
        )) AS dt
    )
""")



# # Transform to dimension table with explicit casting
# dim_date = date_df.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast(IntegerType())) \
#     .withColumn("full_date", F.col("date").cast(DateType())) \
#     .withColumn("day_of_week", F.dayofweek("date").cast(IntegerType())) \
#     .withColumn("day_name", F.date_format("date", "EEEE").cast(StringType())) \
#     .withColumn("day_of_month", F.dayofmonth("date").cast(IntegerType())) \
#     .withColumn("day_of_year", F.dayofyear("date").cast(IntegerType())) \
#     .withColumn("week_of_year", F.weekofyear("date").cast(IntegerType())) \
#     .withColumn("month", F.month("date").cast(IntegerType())) \
#     .withColumn("month_name", F.date_format("date", "MMMM").cast(StringType())) \
#     .withColumn("quarter", F.quarter("date").cast(IntegerType())) \
#     .withColumn("year", F.year("date").cast(IntegerType())) \
#     .withColumn("is_weekend", F.when(F.dayofweek("date").isin(1, 7), True).otherwise(False).cast(BooleanType())) \
#     .withColumn("is_holiday", F.lit(False).cast(BooleanType())) \
#     .withColumn("etl_processed_date", F.current_timestamp().cast(TimestampType()))



# Write with explicit schema matching Redshift
(date_df
 .repartition(1)
 .write
 .mode("overwrite")
 .option("compression", "snappy")
 .parquet(target_path)
)

print(f"Dimension table {target_table} created successfully at {target_path}")
# print(f"Total date records: {dim_date.count()}")



