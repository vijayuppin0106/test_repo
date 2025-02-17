# Databricks notebook source
def read_delta_table_to_df(schema,table_name):
  try:
    df = spark.read.format("delta").table(f"{schema}.{table_name}")
    return df
  except Exception as e:
    print(f"Error: {e}")
    return None

# COMMAND ----------

def orders_data_agg(df):
    from pyspark.sql.functions import year, col, sum, round
    try:
        df = df.withColumn("year", year(col("order_date")))\
            .groupBy("year","product_category","product_sub_category","customer_name")\
                .agg(sum("profit").alias("total_profit")).withColumn("total_profit", col("total_profit").cast("decimal(20,2)"))

        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

def write_gold_agg(df,table_name,schema):
    try:
        df.write.format("delta")\
        .mode("overwrite")\
        .saveAsTable(f"{schema}.{table_name}")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

# COMMAND ----------

def main():
    orders_agg = read_delta_table_to_df("silver","orders_information")
    orders_agg_df = orders_data_agg(orders_agg)
    status = write_gold_agg(df=orders_agg_df,table_name="profit_agg",schema="gold")
    if status:
        print("Data written to gold table successfully")
    else:
        print("Error writing data to gold table")

# COMMAND ----------

dbutils.widgets.text("RUNNING_FROM_PARENT", "true") 
if dbutils.widgets.get("RUNNING_FROM_PARENT") == "false":
    main()
