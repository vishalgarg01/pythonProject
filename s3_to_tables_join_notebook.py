# Databricks notebook source
# COMMAND ----------

# DBTITLE 1,Setup SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a dropdown widget for filtering options
dbutils.widgets.dropdown("filter_type", "all", ["all", "success", "failure"], "Filter Type")

# Get the selected filter value
filter_type = dbutils.widgets.get("filter_type")

# The SparkSession is already created in Databricks notebooks as 'spark'

# COMMAND ----------

# DBTITLE 1,Define S3 Paths
# Define S3 paths
s3_bucket = "crm-nightly-new-cp-validations"
s3_prefix = "Join-test/"
original_file_path = f"s3a://{s3_bucket}/{s3_prefix}original.csv"
split_id_maping_file_path = f"s3a://{s3_bucket}/{s3_prefix}splitId.csv"
response_file_path = f"s3a://{s3_bucket}/{s3_prefix}response.csv"

print(f"Using filter type: {filter_type}")

# COMMAND ----------

# DBTITLE 1,Read Data from S3
# Databricks automatically uses the IAM role attached to the cluster for S3 access
# Adjust file format as needed (.csv, .parquet, .json, etc.)

# Read first file - original data
df1 = spark.read.option("header", "true").csv(original_file_path)
print(f"Loaded data from {original_file_path}")
display(df1.limit(5))

# COMMAND ----------

# Read second file - split ID mapping
df2 = spark.read.option("header", "true").csv(split_id_maping_file_path)
print(f"Loaded data from {split_id_maping_file_path}")
display(df2.limit(5))

# COMMAND ----------

# Read third file - response data
df3 = spark.read.option("header", "true").csv(response_file_path)
print(f"Loaded data from {response_file_path}")
display(df3.limit(5))

# COMMAND ----------

# DBTITLE 1,Create Tables from DataFrames
# Create database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS connectplus")

# Create temporary tables
df1.createOrReplaceTempView("originalTable")
df2.createOrReplaceTempView("splitTable")
df3.createOrReplaceTempView("responseTable")

print("Created temporary tables: originalTable, splitTable, responseTable")

# COMMAND ----------

# DBTITLE 1,Create Permanent Tables (Optional)
# Uncomment to create permanent tables in the metastore
# Update the database name if needed

# df1.write.mode("overwrite").saveAsTable("default.table1")
# df2.write.mode("overwrite").saveAsTable("default.table2")
# df3.write.mode("overwrite").saveAsTable("default.table3")
# print("Created permanent tables in the metastore")

# COMMAND ----------

# DBTITLE 1,Join Tables with Filtering
# Build SQL query with conditional filtering based on user input
filter_condition = ""
if filter_type == 'success':
    filter_condition = "WHERE t3.API_error_code = 200"
elif filter_type == 'failure':
    filter_condition = "WHERE t3.API_error_code != 200"

# Join the tables with conditional filtering
query = f"""
    SELECT 
        t1.*, 
        t3.*
    FROM 
        originalTable t1
    JOIN 
        splitTable t2 ON t1.LINE_NO = t2.LINE_NO
    JOIN 
        responseTable t3 ON t2.split_id = t3.split_id
    {filter_condition}
"""

joined_df = spark.sql(query)
joined_df = joined_df.drop("LINE_NO")
joined_df = joined_df.drop("split_id")

# Preview the joined data
print(f"Joined data preview (filter: {filter_type}):")
display(joined_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Alternative Join Using DataFrame API
# Alternative join using DataFrame API
# Uncomment if you prefer this approach

# joined_df = df1.join(df2, "id").join(df3, "id")
# display(joined_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Write Joined Data to a New Table
# Output table name includes the filter type
table_name = f"joined_table_{filter_type}"

# Write the joined data to a new table
joined_df.write.mode("overwrite").saveAsTable(table_name)

print(f"Process completed: Tables joined with '{filter_type}' filter and saved to '{table_name}'")

# COMMAND ----------

# DBTITLE 1,Verify Data
# Verify the data in the new table
result_df = spark.sql(f"SELECT * FROM {table_name} LIMIT 10")
display(result_df) 