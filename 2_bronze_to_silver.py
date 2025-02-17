# Databricks notebook source
# MAGIC %md
# MAGIC Products processing

# COMMAND ----------

def read_bronze_table_to_df(table_name):
  try:
    df = spark.read.format("delta").table(f"bronze.{table_name}")
    return df
  except Exception as e:
    print(f"Error: {e}")
    return None

# COMMAND ----------

def read_silver_table_to_df(table_name):
  try:
    df = spark.read.format("delta").table(f"silver.{table_name}")
    return df
  except Exception as e:
    print(f"Error: {e}")
    return None

# COMMAND ----------

def read_silver_table(table_name):
    from delta.tables import DeltaTable
    try:
        silver_table = DeltaTable.forName(spark, f"silver.{table_name}")
        return silver_table
    except Exception as e:
        raise Exception(f"Table '{table_name}' does not exist. Error: {str(e)}")
        return False

# COMMAND ----------

def read_bronze_table(table_name):
    from delta.tables import DeltaTable
    try:
        bronze_table = DeltaTable.forName(spark, f"bronze.{table_name}")
        return bronze_table
    except Exception as e:
        raise Exception(f"Table '{table_name}' does not exist. Error: {str(e)}")
        return None
    

# COMMAND ----------

def upsert_products_to_silver(df, silver_table):
    from pyspark.sql.functions import current_timestamp
    try:
        df = df.dropDuplicates(["product_id"])
        (silver_table.alias("tgt")
        .merge( df.alias("src"), "tgt.product_id = src.product_id" )
        .whenMatchedUpdate(set={"tgt.category": "src.category", 
                                "tgt.sub_category": "src.sub_category", 
                                "tgt.product_name": "src.product_name", 
                                "tgt.state": "src.state", 
                                "tgt.price_per_product": "src.price_per_product",
                                "tgt.last_updated_timestamp": current_timestamp() }
            ).whenNotMatchedInsert(values={
            "product_id": "src.product_id",
            "category": "src.category",
            "sub_category": "src.sub_category",
            "product_name": "src.product_name",
            "state": "src.state",
            "price_per_product": "src.price_per_product",
            "last_updated_timestamp": current_timestamp()
        }).execute()
        )
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False
    

# COMMAND ----------

def process_products_information():
    products_silver_df = read_bronze_table_to_df("products")
    products_silver_table = read_silver_table("products")
    merge = upsert_products_to_silver(products_silver_df, products_silver_table)
    if merge:
        print("Product data processed successful")
    else:
        print("Product data process failed")

# COMMAND ----------

# MAGIC %md
# MAGIC <h4> Processing Customer data

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, col, trim, length, concat, substring, when

# COMMAND ----------

def clean_email(df):
    try:
        df = df.withColumn("email", regexp_replace("email", "\n", ""))
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

def clean_address(df):
    try:
        df = df.withColumn(
            "address", 
            regexp_replace("address", "\\n+", " ")
        )
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

def clean_name(df):
    remove_extra_spaces = r"[\s\u00A0]+"
    remove_special_chars = r'[^a-zA-Z\s\']'                                                
    remove_leading_specials = r"^[^a-zA-Z]+"
    remove_space_inside_name = r"\s+(?=[a-z])"

    try:
        df = df.withColumn(
            "customer_name", 
            trim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                col("customer_name"), 
                                remove_special_chars, 
                                ""
                            ), 
                            remove_extra_spaces, 
                            " "
                        ), 
                        remove_leading_specials, 
                        ""
                    ), 
                    remove_space_inside_name, 
                    ""
                )
            )
        ).filter(
        (col("customer_name").isNotNull()) & (col("customer_name") != 'NaN')
    )
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

def clean_phone(df):
    from pyspark.sql.functions import col, regexp_replace, length, when, concat, substring, lit
    try:
        df = df.withColumn(
            "phone",
            regexp_replace(col("phone"), "[^0-9]", "")
        ).withColumn("phone_val_len", length(col("phone"))).withColumn(
            "phone",  
            when(
                length(col("phone")) == 10, 
                concat(
                    substring(col("phone"), 1, 3), lit("-"),
                    substring(col("phone"), 4, 3), lit("-"),
                    substring(col("phone"), 7, 4)
                )
            ).when(
                length(col("phone")) > 10,
                concat(
                    substring(col("phone"), 1, 3), lit("-"),
                    substring(col("phone"), 4, 3), lit("-"),
                    substring(col("phone"), 7, 4),lit("x"),
                    substring(col("phone"), 11, 20)
            )
        ).otherwise("Invalid phone")).drop("phone_val_len")
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

def upsert_customers_to_silver(df, silver_table):
    df.createOrReplaceTempView("bronze_customers")
    try:
        merge_sql = f""" MERGE INTO silver.customers AS tgt 
        USING (SELECT bronze.customer_id as merge_key, bronze.*
        FROM bronze_customers AS bronze
        UNION ALL
        SELECT null AS merge_key, bronze.*
        FROM bronze_customers AS bronze
        INNER JOIN silver.customers AS silver
        ON bronze.customer_id = silver.customer_id
        WHERE (bronze.customer_name <> silver.customer_name 
        OR bronze.email <> silver.email
        OR bronze.phone <> silver.phone
        OR bronze.address <> silver.address
        OR bronze.segment <> silver.segment
        OR bronze.country <> silver.country
        OR bronze.city <> silver.city
        OR bronze.state <> silver.state
        OR bronze.postal_code <> silver.postal_code
        OR bronze.region <> silver.region) AND is_current = 1
        )src ON src.merge_key = tgt.customer_id
        WHEN MATCHED AND (src.customer_name <> tgt.customer_name OR src.email <> tgt.email OR src.phone <> tgt.phone OR src.address <> tgt.address OR src.segment <> tgt.segment OR src.country <> tgt.country OR src.city <> tgt.city OR src.state <> tgt.state OR src.postal_code <> tgt.postal_code OR src.region <> tgt.region)
        THEN UPDATE SET valid_to = current_timestamp(), is_current = 0
        WHEN NOT MATCHED THEN INSERT (customer_id, customer_name, email, phone, address, segment, country, city, state, postal_code, region, valid_from, valid_to, is_current) VALUES (src.customer_id, src.customer_name, src.email, src.phone, src.address, src.segment, src.country, src.city, src.state, src.postal_code, src.region, current_timestamp(),'2099-12-31 23:59:59', 1)
        """
        spark.sql(merge_sql)
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False      

# COMMAND ----------

def process_customers_information():
    customers_sdf = read_bronze_table_to_df("customers")
    customers_sdf = clean_address(customers_sdf)
    customers_sdf = clean_name(customers_sdf)
    customers_sdf = clean_email(customers_sdf)
    customers_sdf = clean_phone(customers_sdf)
    merge = upsert_customers_to_silver(customers_sdf, "silver.customers")
    if merge:
        print("Customers data processed successful")
    else:
        print("Customers data process failed")


# COMMAND ----------

# MAGIC %md
# MAGIC Processing Orders Data

# COMMAND ----------

def format_order_date(df):
    from pyspark.sql.functions import col, to_date, lit, date_format, expr
    try:
        df = df.withColumn("order_date",
        when(col("order_date").rlike(r'^\d{1,2}/\d{1,2}/\d{4}$'),
            to_date(expr("concat(split(order_date, '/')[2], '-', lpad(split(order_date, '/')[1],2,'0'), '-', lpad(split(order_date, '/')[0],2,'0'))"), "yyyy-MM-dd")
        )
        )
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

def format_ship_date(df):
    from pyspark.sql.functions import col, to_date, lit, date_format, expr
    try:
        df = df.withColumn(
        "ship_date",
        when(
            col("ship_date").rlike(r'^\d{1,2}/\d{1,2}/\d{4}$'),
            to_date(expr("concat(split(ship_date, '/')[2], '-', lpad(split(ship_date, '/')[1],2,'0'), '-', lpad(split(ship_date, '/')[0],2,'0'))"), "yyyy-MM-dd")
            )
        )
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

def add_current_date(df):
    from pyspark.sql.functions import current_timestamp
    try:
        df = df.withColumn("record_creation_timestamp", current_timestamp())
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

def write_silver_table(table_name, df):
    try:
        df.write.format("delta").mode("overwrite").saveAsTable(f"silver.{table_name}")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

# COMMAND ----------

def enrich_orders_information(orders_df, products_df, customers_df):
    from pyspark.sql.functions import broadcast
    try:
        df = orders_df.join(broadcast(products_df), orders_df.product_id == products_df.product_id, "left")\
            .join(broadcast(customers_df), orders_df.customer_id == customers_df.customer_id, "left")\
                .select(orders_df.customer_id, orders_df.discount, orders_df.order_date, orders_df.order_id, orders_df.price, orders_df.product_id, orders_df.profit, orders_df.quantity, orders_df.row_id, orders_df.ship_date, orders_df.ship_mode, products_df.category.alias('product_category'), products_df.sub_category.alias('product_sub_category'), customers_df.customer_name, customers_df.country.alias('customer_country'))\
                    .fillna("unknown", subset=["customer_country","customer_name","product_sub_category","product_category"])
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None    

# COMMAND ----------

def format_profit_value(df):
    from pyspark.sql.functions import col
    try:
        df = df.withColumn("profit",col("profit").cast("decimal(20,2)"))
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None

# COMMAND ----------

def process_order_information():
    orders_sdf = read_bronze_table_to_df("orders")
    products_sdf = read_silver_table_to_df('products')
    customers_sdf = read_silver_table_to_df('customers')
    orders_sdf = enrich_orders_information(orders_sdf, products_sdf, customers_sdf)
    orders_sdf = format_order_date(orders_sdf)
    orders_sdf = format_ship_date(orders_sdf)
    orders_sdf = add_current_date(orders_sdf)
    orders_sdf = format_profit_value(orders_sdf)
    status = write_silver_table("orders_information", orders_sdf)
    if status:
        print("Orders data processed successful")
    else:
        print("Orders data process failed")

# COMMAND ----------

def main():
    process_products_information()
    process_customers_information()
    process_order_information()


# COMMAND ----------

dbutils.widgets.text("RUNNING_FROM_PARENT", "true") 
if dbutils.widgets.get("RUNNING_FROM_PARENT") == "false":
    main()
