# Databricks notebook source
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, IntegerType
source_path = "abfss://pei@vjdatabricksdevstg.dfs.core.windows.net/landing/"

# COMMAND ----------

def write_source_file_to_bronze(df,table_name):
  try:
    (df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"bronze.{table_name}"))
    return True
  except Exception as e:
    print(f"Error: {e}")
    return False

# COMMAND ----------

def read_products_file(source_path, source_file):
    products_schema = (StructType([
                                StructField("product_id", StringType(), True),
                                 StructField("category", StringType(), True), 
                                 StructField("sub_category", StringType(), True), 
                                 StructField("product_name", StringType(), True), 
                                 StructField("state", StringType(), True), 
                                 StructField("price_per_product", FloatType(), True)
                                ]))
    try:
        df = (spark.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("quote", '"')
        .option("multiLine", True)
        .option("escape", '"')
        .schema(products_schema)
        .load(f"{source_path}/{source_file}"))
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None
    

# COMMAND ----------

def read_products_data():
    products_df = read_products_file(source_path, "Products.csv")
    write_source_file_to_bronze(products_df, "products")

# COMMAND ----------

def read_customers_file(source_path, source_file):
    #!pip install openpyxl
    import pandas as pd
    customer_schema = (StructType([
                                StructField("customer_id", StringType(), True),
                                 StructField("customer_name", StringType(), True), 
                                 StructField("email", StringType(), True), 
                                 StructField("phone", StringType(), True), 
                                 StructField("address", StringType(), True), 
                                 StructField("segment", StringType(), True),
                                 StructField("country", StringType(), True),
                                 StructField("city", StringType(), True), 
                                 StructField("state", StringType(), True),
                                 StructField("postal_code", IntegerType(), True),
                                 StructField("region", StringType(), True)
                                ]))
    
    try:
        pdf = pd.read_excel(source_path + source_file, sheet_name="Worksheet")
        customer_df = spark.createDataFrame(pdf,schema=customer_schema)
        return customer_df
    except Exception as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

def read_customers_data():
    xlsx_path = "/Volumes/databricks_dev_ws/default/xlsx/"
    customer_df = read_customers_file(xlsx_path, "Customer.xlsx")
    write_source_file_to_bronze(customer_df, "customers")

# COMMAND ----------

def read_orders_file(source_path, source_file):
    try:
        orders_df = (spark.read.format("json")
             .option("multiLine", True)
             .load(f"{source_path}/{source_file}")
             )
        orders_df = orders_df.toDF(*[col.replace(" ", "_").lower() for col in orders_df.columns])
        return orders_df
    except Exception as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

def read_orders_data():
    orders_df = read_orders_file(source_path, "Orders.json")
    write_source_file_to_bronze(orders_df, "orders")

# COMMAND ----------

def main():
    read_products_data()
    read_customers_data()
    read_orders_data()
    

# COMMAND ----------

dbutils.widgets.text("RUNNING_FROM_PARENT", "true") 
if dbutils.widgets.get("RUNNING_FROM_PARENT") == "false":
    main()