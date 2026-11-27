from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

# Connect to Databricks Serverless (delete this line if running in a Databricks Notebook)
spark = DatabricksSession.builder.serverless().getOrCreate()

CATALOG = "ecommerce_lakehouse"

# 1. CUSTOMERS (CSV)
customers_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/raw/checkpoints/bronze/customers/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(f"/Volumes/{CATALOG}/raw/landing/customers")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    customers_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"/Volumes/{CATALOG}/raw/checkpoints/bronze/customers/checkpoint")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.bronze.customers")
    .awaitTermination()
)

# 2. PRODUCTS (JSON)
products_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/raw/checkpoints/bronze/products/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(f"/Volumes/{CATALOG}/raw/landing/products")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    products_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"/Volumes/{CATALOG}/raw/checkpoints/bronze/products/checkpoint")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.bronze.products")
    .awaitTermination()
)

# 3. ORDERS (JSON)
orders_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"/Volumes/{CATALOG}/raw/checkpoints/bronze/orders/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(f"/Volumes/{CATALOG}/raw/landing/orders")
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    orders_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"/Volumes/{CATALOG}/raw/checkpoints/bronze/orders/checkpoint")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.bronze.orders")
    .awaitTermination()
)

# 4. VERIFY COUNTS
print("customers:", spark.table(f"{CATALOG}.bronze.customers").count())
print("products :", spark.table(f"{CATALOG}.bronze.products").count())
print("orders   :", spark.table(f"{CATALOG}.bronze.orders").count())