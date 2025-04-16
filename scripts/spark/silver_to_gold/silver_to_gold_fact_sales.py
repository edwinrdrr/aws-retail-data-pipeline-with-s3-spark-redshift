# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



# Configuration
bucket = "edwinraws-de-retail-project-bucket"
orders_source = f"s3://{bucket}/silver/dev/orders"
order_items_source = f"s3://{bucket}/silver/dev/order_items"

appname = "silver_to_gold_fact_sales"

target_stage = "gold"
target_table = "fact_sales"
target_path = f"s3://{bucket}/{target_stage}/dev/{target_table}"



# Initialize Spark Session
spark = SparkSession.builder.appName(appname).getOrCreate()



# Read silver data
silver_orders = spark.read.parquet(orders_source)
silver_order_items = spark.read.parquet(order_items_source)



# Join and transform - include order_date_key twice (once for data, once for partition)
fact_sales = (
    silver_order_items.join(silver_orders, "order_id", "inner")
    .select(
        F.monotonically_increasing_id().cast("long").alias("sales_key"),
        F.col("order_id").cast("string").alias("order_natural_key"),
        F.col("order_item_id").cast("string").alias("order_item_natural_key"),
        # Include as regular column
        F.date_format("order_date", "yyyyMMdd").cast("int").alias("order_date_key"), 
        F.col("customer_id").cast("string").alias("customer_natural_key"),
        F.col("product_id").cast("string").alias("product_natural_key"),
        F.col("quantity").cast("integer"),
        F.col("unit_price").cast("decimal(18,2)"),
        F.col("discount_pct").cast("decimal(5,2)"),
        F.col("subtotal").cast("decimal(18,2)"),
        F.col("net_amount").cast("decimal(18,2)"),
        F.col("shipping_cost").cast("decimal(18,2)"),
        F.col("order_amount").cast("decimal(18,2)").alias("total_amount"),
        F.col("status").cast("string").alias("order_status"),
        F.col("payment_method").cast("string"),
        F.current_timestamp().alias("etl_processed_date"),
        # Duplicate for partitioning (will be removed from files)
        F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_partition") # for partition only, this will not be in our parquet 
    )
)



# Write to gold layer as Parquet with partitioning
# Write with partitioning
fact_sales.write.partitionBy("order_date_key") \
    .parquet(
        path=target_path,
        mode="overwrite",
        compression="snappy"
    )

print(f"Fact table {target_table} created successfully at {target_path}") 
print(f"Total sales records: {fact_sales.count()}")



# Read back to verify
df_test = spark.read.parquet(target_path)
print(f"Columns in files: {len(df_test.columns)}")  # Should show 16
df_test.printSchema()

# Check counts
print(f"Total records: {df_test.count()}")