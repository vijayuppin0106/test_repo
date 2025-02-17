# Databricks notebook source
# MAGIC %run "/Workspace/Users/vijayakumaruppin@vijayakumaruppingmail.onmicrosoft.com/pei/2_bronze_to_silver"

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import TimestampType
from unittest.mock import patch, MagicMock

class TestSilver(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("TestBronzeToSilver").getOrCreate()
        print("Starting tests.")

    def test_clean_emailg(self):
        data = [("pei1@gmail.com",), ("fintech\n@gmail.com",)]
        columns = ["email"]
        df = spark.createDataFrame(data, columns)
        result_df = clean_email(df)
        results = result_df.select(col("email"))\
            .rdd.map(lambda row: row[0]).collect()
        expected = ["pei@example.com", "fintech@example.com"]  
        self.assertEqual(results, expected)           

    def test_clean_address(self):
        data = [("123 Main \n\nSt",), ("456 Elm \nSt",)]
        columns = ["address"]
        df = spark.createDataFrame(data, columns)
        result_df = clean_address(df)
        results = result_df.select(col("address"))\
            .rdd.map(lambda row: row[0]).collect()
        expected = ["123 Main  St","456 Elm  St"]
        self.assertEqual(results, expected)
    
    def test_customer_name(self):
        data = [("John Doe",), ("Ja33ne S%66mith",)]
        columns = ["customer_name"]
        df = spark.createDataFrame(data, columns)
        result_df = clean_name(df)
        results = result_df.select(col("customer_name"))\
            .rdd.map(lambda row: row[0]).collect()
        expected = ["John Doe", "Jane Smith"]
        self.assertEqual(results, expected)
    
    def test_clean_phone(self):
        data = [("123-456-7890",), ("(123) 456-7890",), ("123456-7890x6453",)]
        columns = ["phone"]
        df = spark.createDataFrame(data, columns)
        result_df = clean_phone(df)
        results = result_df.select(col("phone"))\
            .rdd.map(lambda row: row[0]).collect()
        expected = ["123-456-7890", "123-456-7890","123-456-7890x6453"]
        self.assertEqual(results, expected)

    def test_format_order_date(self):
        data = [("01/1/2025",), ("9/11/2024",), ("19/11/2014",)]
        columns = ["order_date"]
        df = spark.createDataFrame(data, columns)
        result_df = format_order_date(df)
        results = result_df.select(col("order_date"))\
            .rdd.map(lambda row: row[0]).collect()
        expected = [
            datetime.strptime("2025-01-01", "%Y-%m-%d").date(),
            datetime.strptime("2024-11-09", "%Y-%m-%d").date(),
            datetime.strptime("2014-11-19", "%Y-%m-%d").date()
        ]
        self.assertEqual(results, expected)

    def test_format_ship_date(self):
        data = [("01/1/2025",), ("9/11/2024",), ("19/11/2014",)]
        columns = ["ship_date"]
        df = spark.createDataFrame(data, columns)
        result_df = format_ship_date(df)
        results = result_df.select(col("ship_date"))\
            .rdd.map(lambda row: row[0]).collect()
        expected = [
            datetime.strptime("2025-01-01", "%Y-%m-%d").date(),
            datetime.strptime("2024-11-09", "%Y-%m-%d").date(),
            datetime.strptime("2014-11-19", "%Y-%m-%d").date()
        ]
        self.assertEqual(results, expected)

    def test_add_current_date(self):
        data = [("CA-2014-140662",),("CA-2014-999099",)]
        columns = ["order_id"]
        df = spark.createDataFrame(data, columns)
        result_df = add_current_date(df)
        self.assertIn("record_creation_timestamp", result_df.columns)
        self.assertIsInstance(result_df.schema["record_creation_timestamp"].dataType, TimestampType)
    
    def test_format_profit_value(self):
        data = [("100.6007778",), ("200.088990",), ("300.24567800",)]
        columns = ["profit"]
        df = spark.createDataFrame(data, columns)
        result_df = format_profit_value(df)
        results = result_df.select(col("profit"))\
        .rdd.map(lambda row: str(row[0])).collect()
        expected = ["100.60", "200.09", "300.25"]
        self.assertEqual(results, expected)

    def test_enrich_orders_information(self):
        orders_data = [("ME-17320", 0, "2014-01-05", "CA-2014-167199",6.54,"OFF-PA-10000955",3.01,1,6294,"2014-01-10","Standard Class")]
        orders_columns = ["customer_id", "discount", "order_date", "order_id", "price", "product_id", "profit", "quantity", "row_id", "ship_date", "ship_mode"]
        
        products_data = [("OFF-PA-10000955","Office Supplies","Paper")]
        products_columns = ["product_id", "category", "sub_category"]

        customers_data = [("ME-17320","Maria Etezadi","United States")]     
        customers_columns = ["customer_id", "customer_name" ,"country"]

        orders_df = self.spark.createDataFrame(orders_data, orders_columns)
        products_df = self.spark.createDataFrame(products_data, products_columns)
        customers_df = self.spark.createDataFrame(customers_data, customers_columns)

        result_df = enrich_orders_information(orders_df, products_df, customers_df)

        expected_data = [("ME-17320",0,"2014-01-06","CA-2014-167199",6.54,"OFF-PA-10000955",3.01,1,6294,"2014-01-10","Standard Class","Office Supplies","Paper","Maria Etezadi","United States","2025-02-16T17:33:12.276+00:00")]
        expected_columns = ["customer_id", "discount", "order_date", "order_id", "price", "product_id", "profit", "quantity", "row_id", "ship_date", "ship_mode", "product_category", "product_sub_category", "customer_name", "customer_country", "record_creation_timestamp"]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        self.assertEqual(result_df.count(), expected_df.count())
        self.assertEqual(result_df.select("order_id", "product_id", "customer_id", "customer_name", "customer_country","product_category", "product_sub_category").collect(), expected_df.select("order_id", "product_id", "customer_id", "customer_name", "customer_country","product_category", "product_sub_category").collect())

    @patch("pyspark.sql.DataFrameReader.format")
    def test_read_bronze_table_to_df(self, mock_format):
        mock_df = MagicMock()
        mock_format.return_value.table.return_value = mock_df
        result_df = read_bronze_table_to_df("customers")
        mock_format.assert_called_once_with("delta")
        mock_format.return_value.table.assert_called_once_with("bronze.customers")
        self.assertEqual(result_df, mock_df)

    @patch("pyspark.sql.DataFrameReader.format")
    def test_read_silver_table_to_df(self, mock_format):
        mock_df = MagicMock()
        mock_format.return_value.table.return_value = mock_df
        result_df = read_silver_table_to_df("customers")
        mock_format.assert_called_once_with("delta")
        mock_format.return_value.table.assert_called_once_with("silver.customers")
        self.assertEqual(result_df, mock_df)
    
    @patch('delta.tables.DeltaTable.forName')
    def test_read_silver_table(self, mock_for_name):
        mock_silver_table = MagicMock()
        mock_for_name.return_value = mock_silver_table
        result = read_silver_table("customers")
        mock_for_name.assert_called_once_with(spark, "silver.customers")
        self.assertEqual(result, mock_silver_table)

    @patch('delta.tables.DeltaTable.forName')
    def test_read_bronze_table(self, mock_for_name):
        mock_bronze_table = MagicMock()
        mock_for_name.return_value = mock_bronze_table
        result = read_bronze_table("customers")
        mock_for_name.assert_called_once_with(spark, "bronze.customers")
        self.assertEqual(result, mock_bronze_table)

if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)

# COMMAND ----------

