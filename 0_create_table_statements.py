# Databricks notebook source
def create_database(db_name):
    spark.sql(f"create schema if not exists {db_name}")

# COMMAND ----------

create_database("bronze")
create_database("silver")
create_database("gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze.products (
# MAGIC   product_id STRING,
# MAGIC   category STRING,
# MAGIC   sub_category STRING,
# MAGIC   product_name STRING,
# MAGIC   state STRING,
# MAGIC   price_per_product FLOAT)
# MAGIC USING delta;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.customers (
# MAGIC   customer_id STRING,
# MAGIC   customer_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   address STRING,
# MAGIC   segment STRING,
# MAGIC   country STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   postal_code INT,
# MAGIC   region STRING)
# MAGIC USING delta;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.orders (
# MAGIC   customer_id STRING,
# MAGIC   discount DOUBLE,
# MAGIC   order_date STRING,
# MAGIC   order_id STRING,
# MAGIC   price DOUBLE,
# MAGIC   product_id STRING,
# MAGIC   profit DOUBLE,
# MAGIC   quantity BIGINT,
# MAGIC   row_id BIGINT,
# MAGIC   ship_date STRING,
# MAGIC   ship_mode STRING)
# MAGIC USING delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.products (
# MAGIC   product_id STRING,
# MAGIC   category STRING,
# MAGIC   sub_category STRING,
# MAGIC   product_name STRING,
# MAGIC   state STRING,
# MAGIC   price_per_product DECIMAL(10,2),
# MAGIC   last_updated_timestamp TIMESTAMP
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (category);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.customers (
# MAGIC   customer_id STRING,
# MAGIC   customer_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   address STRING,
# MAGIC   segment STRING,
# MAGIC   country STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   postal_code INT,
# MAGIC   region STRING,
# MAGIC   valid_from TIMESTAMP,
# MAGIC   valid_to TIMESTAMP,
# MAGIC   is_current INT
# MAGIC )
# MAGIC USING delta
# MAGIC PARTITIONED BY (region);
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.orders_information(
# MAGIC   customer_id STRING,
# MAGIC   discount DOUBLE,
# MAGIC   order_date DATE,
# MAGIC   order_id STRING,
# MAGIC   price DOUBLE,
# MAGIC   product_id STRING,
# MAGIC   profit DECIMAL(20,2),
# MAGIC   quantity BIGINT,
# MAGIC   row_id BIGINT,
# MAGIC   ship_date DATE,
# MAGIC   ship_mode STRING,
# MAGIC   product_category STRING,
# MAGIC   product_sub_category STRING,
# MAGIC   customer_name STRING,
# MAGIC   customer_country STRING,
# MAGIC   record_creation_timestamp TIMESTAMP  )
# MAGIC USING delta
# MAGIC PARTITIONED BY (order_date);

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS gold.profit_agg
# MAGIC (
# MAGIC   year INT,
# MAGIC   product_category STRING,
# MAGIC   product_sub_category STRING,
# MAGIC   customer_name STRING,
# MAGIC   total_profit DECIMAL(20,2)
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (year);