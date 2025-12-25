# Databricks notebook source
print("Hello Databricks")

# COMMAND ----------

name = "Yohans Samuel"
print(name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Data Ingestion & Exploration with PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action 3.1: Prepare and Upload Mock Data

# COMMAND ----------

# sales_df = spark.read.option("header", "true").option("inferSchema", "true").csv(
#     "file:/Workspace/Users/yohanssamuel2014@outlook.com/mock_sales_data.csv"
# )
# display(sales_df)

# COMMAND ----------

# Path to your uploaded CSV file in DBFS

file_path = "file:/Workspace/Users/yohanssamuel2014@outlook.com/mock_sales_data.csv"

# Read the CSV file into a DataFrame

# Infer schema and specify that the first row is the header

sales_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

# Display the DataFrame (shows top 20 rows by default)

sales_df.show()

# Print the schema of the DataFrame

sales_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action 3.2: Load CSV Data into a PySpark DataFrame

# COMMAND ----------

# Count the total number of rows
sales_df.count()

# COMMAND ----------

# Display the first 5 rows: 
sales_df.head(5)
sales_df.limit(5).show()

# COMMAND ----------

# Get a summary of statistics for numerical columns: 
sales_df.describe().show()

# COMMAND ----------

# Select and display specific columns (e.g., TransactionID, Product, SaleAmount):
sales_df.select("TransactionID", "Product", "SaleAmount").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Basic Data Transformation & Cleansing with Spark SQL & PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action 4.1: Using Spark SQL for Exploration

# COMMAND ----------

# To use Spark SQL directly on your DataFrame, you first need to create a temporary view.
# Create a temporary view from the DataFrame
sales_df.createOrReplaceTempView("sales_data_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_data_view LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select distinct product categories: 
# MAGIC SELECT DISTINCT ProductCategory FROM sales_data_view;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the total sales amount per product:
# MAGIC -- SELECT ProductCategory, SUM(SaleAmount) FROM sales_data_view GROUP BY ProductCategory;
# MAGIC SELECT Product, SUM(SaleAmount) as TotalSales
# MAGIC FROM sales_data_view
# MAGIC GROUP BY Product
# MAGIC ORDER BY TotalSales DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Filter sales for a specific CustomerID (choose one from your data).
# MAGIC
# MAGIC -- SELECT * from sales_data_view
# MAGIC SELECT CustomerID, Product, SaleAmount 
# MAGIC FROM sales_data_view
# MAGIC WHERE CustomerID = "CUST085"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action 4.2: Data Type Conversion (PySpark)

# COMMAND ----------

sales_df.printSchema() 
# Notice if TransactionDate is a string. We need to convert it to a date or timestamp type for proper date operations.

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp
# Assuming TransactionDate is in 'yyyy-MM-dd HH:mm:ss' format
# If it's just date 'yyyy-MM-dd', use to_date and adjust format if needed.

sales_df_transformed = sales_df.withColumn("TransactionTimestamp", to_timestamp(col("TransactionDate"), "yyyy-MM-dd HH:mm:ss"))
sales_df_transformed.printSchema()
sales_df_transformed.select("TransactionID", "TransactionDate", "TransactionTimestamp").show(5, truncate=False)

# COMMAND ----------

# If SaleAmount or Quantity were inferred as strings, convert them to appropriate numeric types (e.g., DoubleType, IntegerType).
from pyspark.sql.types import DoubleType, IntegerType

# Example: if SaleAmount was a string
sales_df_transformed = sales_df_transformed.withColumn("SaleAmount", col("SaleAmount").cast(DoubleType()))
sales_df_transformed = sales_df_transformed.withColumn("Quantity", col("Quantity").cast(IntegerType()))

sales_df_transformed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action 4.3: Handling Missing Values (PySpark)

# COMMAND ----------

# Check if there are any null values in PaymentMethod (as per mock data spec).
from pyspark.sql.functions import col, count, when

sales_df.select(
    count(when(col("PaymentMethod").isNull(), True)).alias("null_count")
).show()

# COMMAND ----------

# Use fillna() to replace nulls in PaymentMethod with a default value like 'Unknown'.
sales_df_transformed = sales_df_transformed.fillna({"PaymentMethod": "Unknown"})

# Verify by showing rows where PaymentMethod might have been null
sales_df_transformed.filter(col("PaymentMethod") == "Unknown").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action 4.4: Creating New Columns (PySpark)

# COMMAND ----------

from pyspark.sql.functions import round
sales_df_transformed = sales_df_transformed.withColumn("UnitPrice", round((col("SaleAmount") / col("Quantity")), 2))
sales_df_transformed.select("Product", "Quantity", "SaleAmount", "UnitPrice").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action 4.5: Filtering and Ordering (PySpark)

# COMMAND ----------

# Filter the DataFrame to show only sales made via "Credit Card".
# Order the results by SaleAmount in descending order.
credit_card_sales_df = sales_df_transformed.filter(col("PaymentMethod") == "Credit Card") \
.orderBy(col("SaleAmount").desc())

credit_card_sales_df.show(10)

# COMMAND ----------

final_sales_df = sales_df_transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Storing Processed Data & Version Control Basics

# COMMAND ----------

# MAGIC %md
# MAGIC ### Action 5.1: Save Transformed DataFrame to DBFS (Parquet format)

# COMMAND ----------

# Define the output path in DBFS
output_path_parquet = "file:/Workspace/Users/yohanssamuel2014@outlook.com/processed_sales_parquet"

# Write the DataFrame to Parquet format
# "overwrite" mode will replace data if it already exists

final_sales_df.write.mode("overwrite").parquet(output_path_parquet)

print(f"DataFrame saved to Parquet at: {output_path_parquet}")

# COMMAND ----------

# MAGIC %fs ls 
# MAGIC /Workspace/Users/yohanssamuel2014@outlook.com//processed_sales_parquet/

# COMMAND ----------

