# Databricks notebook source
import json
# Configuration

storage_account_name = "storagessample"
container_name = "intermediatedata"
storage_account_key = "ogqJCBwZUE5xb1H6bmjvYH3xSQeULXfqf8EwbfRj3UP8JLHeVPMNbosEUPrQw3YmZmhBqjdN9u8p+ASthYAyMg=="

# Set Spark configuration for accessing the storage account
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key)

newoutput_directory = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/new_delta_tablesfinal/"
permanent_table_locationfinal= newoutput_directory + "sales_activity_monthlyn15"
parquet_output_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/intermediate_parquetfinal/sales_activityfinal.parquet"

# Read the Delta table
df = spark.read.format("delta").load(permanent_table_locationfinal)

# Write DataFrame to Parquet
df.write.mode("overwrite").parquet(parquet_output_path)

# Debugging: Verify the Parquet file is created
files = dbutils.fs.ls(parquet_output_path)
if len(files) == 0:
    raise Exception("Parquet file was not created")
#/container_name/filepath/filename.parquet
# Output the Parquet file path
parquet_file_path = f"/{container_name}/intermediate_parquetfinal/sales_activityfinal.parquet"
dbutils.notebook.exit(json.dumps({"parquetOutputPath": parquet_file_path}))


# COMMAND ----------


