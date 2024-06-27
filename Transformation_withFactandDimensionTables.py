# Databricks notebook source
# Configuration
storage_account_name = "storagessample"
container_name = "intermediatedata"
file_name = "car_prices.parquet"  # This should match the path where ADF copied the Parquet file

# Using Storage Account Key
storage_account_key = "ogqJCBwZUE5xb1H6bmjvYH3xSQeULXfqf8EwbfRj3UP8JLHeVPMNbosEUPrQw3YmZmhBqjdN9u8p+ASthYAyMg=="
file_location = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{file_name}"

# Set Spark configuration for accessing the storage account
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key)

# Read the Parquet file into DataFrame
df = spark.read.parquet(file_location)
display(df)

# COMMAND ----------

# Create a temporary view
temp_table_name = "car_prices_parquet"
df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Query the created temp table in a SQL cell
# MAGIC
# MAGIC SELECT * FROM car_prices_parquet

# COMMAND ----------

# Save DataFrame as a permanent table
permanent_table_name = "car_prices_parquettn15"
df.write.format("parquet").mode("overwrite").saveAsTable(permanent_table_name)

# COMMAND ----------

# Query the permanent table
df1 = spark.sql("SELECT * FROM car_prices_parquettn15")
display(df1)

# COMMAND ----------

from pyspark.sql.functions import substring

# Extract the part before the timezone information
df1 = df1.withColumn("saledate2", substring(df1["saledate"], 5, 20))

# Display the DataFrame
display(df1)

# COMMAND ----------

from pyspark.sql.functions import to_date

# Convert the saledate column from string format to date format
df1 = df1.withColumn("saledate3", to_date(df1["saledate2"], "MMM dd yyyy HH:mm:ss"))

# Display the DataFrame
display(df1)

# COMMAND ----------

from pyspark.sql.functions import date_format

# Add a new column YearMonth in format yyyy-MM
df1 = df1.withColumn("YearMonth", date_format(df1["saledate3"], "yyyy-MM"))

# Display the DataFrame
display(df1)

# COMMAND ----------

from pyspark.sql.functions import col
df1 = df1.withColumn('year', col('year').cast('int'))
df1 = df1.withColumn('condition', col('condition').cast('int'))
df1 = df1.withColumn('odometer', col('odometer').cast('int'))
df1 = df1.withColumn('mmr', col('mmr').cast('int'))
df1 = df1.withColumn('sellingprice', col('sellingprice').cast('int'))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import col

# Initialize an empty dictionary to store null counts
NULL_COUNTS = {}

# Iterate over each column
for col_name in df1.columns:
    # Calculate the count of null values for the current column
    NULL_COUNTSvar = df1.where(col(col_name).isNull()).count()
    
    # If null values are found, add to NULL_COUNTS dictionary
    if NULL_COUNTSvar > 0:
        NULL_COUNTS[col_name] = NULL_COUNTSvar

# Print null counts for each column
for col_name, count in NULL_COUNTS.items():
    print(f"Column '{col_name}': {count} null values")

# COMMAND ----------

from pyspark.sql.functions import when, sum,col,avg

# Replace null values in 'sellingprice' column with zero
df1 = df1.withColumn("sellingprice", when(df1["sellingprice"].isNull(), 0).otherwise(df1["sellingprice"]))
df1 = df1.withColumn("YearMonth", when(df1["YearMonth"].isNull(), '999912').otherwise(df1["YearMonth"]))
df1 = df1.withColumn("make", when(df1["make"].isNull(), 'XYZ').otherwise(df1["make"]))
df1 = df1.withColumn("color", when(df1["color"].isNull(), 'NA').otherwise(df1["color"]))
df1 = df1.withColumn("body", when(df1["body"].isNull(), 'na').otherwise(df1["body"]))
df1 = df1.withColumn("condition", when(df1["condition"].isNull(), '0').otherwise(df1["condition"]))
df1 = df1.withColumn('sellingprice', col('sellingprice').cast('double'))
# Group by 'make' and 'YearMonth', and aggregate the sum of 'sellingprice'
df_monthly = df1.groupBy("make", "YearMonth","color","body","condition" ).agg(sum("sellingprice").alias("total_sellingprice"),avg("mmr").alias("mmr"))

# Display the resulting DataFrame
display(df_monthly)

# COMMAND ----------

# Required import
from pyspark.sql import functions as F

# Group by and aggregate to create DimCar DataFrame
df_dimcar1 = df1.groupBy("vin").agg(
    F.first("make").alias("make"),
    F.first("model").alias("model"),
    F.first("color").alias("color"),
    F.first("body").alias("body")
)

# Display the DimCar DataFrame
display(df_dimcar1)

# COMMAND ----------

# Required import
from pyspark.sql import functions as F

# Create FactSales DataFrame
df_factsales = df1.groupBy("vin", "YearMonth").agg(
    F.sum("sellingprice").alias("total_selling_price"),
    F.avg("mmr").alias("mmr")
)

display(df_factsales)

# COMMAND ----------

from pyspark.sql import SparkSession

# Assuming spark is your SparkSession
jdbc_url = "jdbc:sqlserver://adiserver1234.database.windows.net:1433;database=SQLDatabase"
connection_properties = {
    "user": "MyBoiAdi",
    "password": "Thefe23$",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Use JDBC to execute database-specific commands
def execute_sql_server_command(command):
    try:
        db_connection = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, connection_properties["user"], connection_properties["password"])
        statement = db_connection.createStatement()
        statement.execute(command)
        statement.close()
        db_connection.close()
    except Exception as e:
        print(f"Error executing command: {e}")

# Example commands to drop FOREIGN KEY constraints that reference dbo.DimCar
# Replace 'FOREIGN_KEY_CONSTRAINT_NAME' with the actual name of the constraint
# and 'REFERENCING_TABLE' with the name of the table that has the FOREIGN KEY constraint.
execute_sql_server_command("ALTER TABLE dbo.DimCar DROP CONSTRAINT PK__DimCar__4C9A0DB3813BDEBE")       

# Drop FOREIGN KEY constraint
execute_sql_server_command("ALTER TABLE dbo.FactSales DROP CONSTRAINT PK__FactSale__17A06D843390D48D")  
execute_sql_server_command("ALTER TABLE dbo.FactSales DROP CONSTRAINT FK__FactSales__car_i__5EBF139D")  

# Note: There's no need to drop the table in SQL Server before overwriting it with Spark, as the .mode("overwrite") option in the DataFrame write operation will handle this.

# Example: Writing a DataFrame to SQL Server
df_dimcar1.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.DimCar") \
    .option("user", "MyBoiAdi") \
    .option("password", "Thefe23$") \
    .mode("overwrite") \
    .save()

df_factsales.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.FactSales") \
    .option("user", "MyBoiAdi") \
    .option("password", "Thefe23$") \
    .mode("overwrite") \
    .save()

# COMMAND ----------

df_monthly.createOrReplaceTempView('df_monthly')
display(df_monthly)

# COMMAND ----------

permanent_table_name = "sales_activity_monthlyn"
df_monthly.write.format("parquet").mode("overwrite").saveAsTable(permanent_table_name)

# COMMAND ----------

display(spark.sql("SELECT * from sales_activity_monthlyn15"))

# COMMAND ----------

df_update = df_monthly.alias('df_update')
df_update = df_update.where("YearMonth like '2015%'")
df_update = df_update.withColumn("total_sellingprice", when(col("make") == 'Lexus', 0).otherwise(col("total_sellingprice")))
display(df_update)
df_update.createOrReplaceTempView("df_update")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA sales_activity_monthlyn15;
# MAGIC
# MAGIC SELECT * FROM sales_activity_monthlyn15
# MAGIC WHERE YearMonth like '2015-03' and total_sellingprice >500000 ;

# COMMAND ----------

# Configuration
storage_account_name = "storagessample"
container_name = "intermediatedata"
newoutput_directory = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/new_delta_tablesfinal/"

# Save DataFrame as a Delta table to external location
permanent_table_locationfinal = newoutput_directory + "sales_activity_monthlyn15"

# Assuming df_monthly is the transformed DataFrame
df_monthly.write.format("delta").mode("overwrite").save(permanent_table_locationfinal)

# COMMAND ----------


