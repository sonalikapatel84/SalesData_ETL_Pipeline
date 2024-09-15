# Databricks notebook source
# List files in the directory where you uploaded the CSV files
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

#import PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum

#Create SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# Read the CSV files into DataFrames
accounts_df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/accounts.csv")
skus_df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/skus.csv")
invoices_df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/invoices.csv")
invoice_line_items_df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/invoice_line_items.csv")

# Print schema of all DataFrames
accounts_df.printSchema()
skus_df.printSchema()
invoices_df.printSchema()
invoice_line_items_df.printSchema()


# COMMAND ----------

#display data from the dataframe
accounts_df.show()
skus_df.show()
invoices_df.show()
invoice_line_items_df.show()

# COMMAND ----------

# Join accounts_df with invoices_df on account_id
accounts_invoices_df = accounts_df.join(invoices_df, "account_id")

# Display the joined DataFrame
display(accounts_invoices_df)

# COMMAND ----------

# Join the resulting DataFrame with invoice_line_items_df on invoice_id
accounts_invoices_items_df = accounts_invoices_df.join(invoice_line_items_df, "invoice_id")

# Display the joined DataFrame
display(accounts_invoices_items_df)

# COMMAND ----------

# Join the resulting DataFrame with skus_df on sku_id
final_df = accounts_invoices_items_df.join(skus_df, "item_id")

# Select relevant columns

# Display the final DataFrame
display(final_df)
