from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("S3DataJoin").getOrCreate()

# Define S3 paths
s3_bucket = "crm-nightly-new-cp-validations"
s3_prefix = "Join-test/"
original_file_path = f"s3a://{s3_bucket}/{s3_prefix}original.csv"
split_id_maping_file_path = f"s3a://{s3_bucket}/{s3_prefix}splitId.csv"
response_file_path = f"s3a://{s3_bucket}/{s3_prefix}response.csv"


df1 = spark.read.option("header", "true").csv(original_file_path)
df2 = spark.read.option("header", "true").csv(split_id_maping_file_path)
df3 = spark.read.option("header", "true").csv(response_file_path)

spark.sql(f"CREATE DATABASE IF NOT EXISTS connectplus")
# Create temporary tables
df1.createOrReplaceTempView("originalTable")
df2.createOrReplaceTempView("splitTable")
df3.createOrReplaceTempView("responseTable")

# Join the tables
# Modify the join conditions based on your actual data schema
joined_df = spark.sql("""
    SELECT 
        t1.*, 
        t3.*
    FROM 
        originalTable t1
    JOIN 
        splitTable t2 ON t1.LINE_NO = t2.LINE_NO
    JOIN 
        responseTable t3 ON t2.split_id = t3.split_id
""")
joined_df = joined_df.drop("LINE_NO")
joined_df = joined_df.drop("split_id")

# Write the joined data to a new table
joined_df.write.mode("overwrite").saveAsTable("joined_table")

print("Process completed: 3 tables loaded and joined successfully") 