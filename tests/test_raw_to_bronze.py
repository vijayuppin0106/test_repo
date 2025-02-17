# Databricks notebook source
# MAGIC %run "/Workspace/Users/vijayakumaruppin@vijayakumaruppingmail.onmicrosoft.com/pei/src/1_ingest_raw_to_bronze"

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import TimestampType
from unittest.mock import patch, MagicMock

class TestRawToBronze(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("TestRawToBronze").getOrCreate()
        print("Starting tests.")

    @patch("pyspark.sql.SparkSession.read")
    def test_read_products_file(self, mock_read):
        mock_df = MagicMock()
        mock_read.format.return_value = mock_read
        mock_read.option.return_value = mock_read
        mock_read.schema.return_value = mock_read
        mock_read.load.return_value = mock_df

        result_df = read_products_file("/pei_source_path", "products.csv")

        mock_read.format.assert_called_once_with("csv")
        mock_read.option.assert_any_call("header", "true")
        mock_read.option.assert_any_call("delimiter", ",")
        mock_read.schema.assert_called()
        mock_read.load.assert_called_once_with("/pei_source_path/products.csv")

        self.assertEqual(result_df, mock_df)

    @patch("pandas.read_excel")
    @patch("pyspark.sql.SparkSession.createDataFrame")
    def test_read_customers_file_success(self, mock_create_df, mock_read_excel):
        source_path = "/path/to/source/"
        source_file = "customers.xlsx"
        mock_customer_df = MagicMock()
        mock_read_excel.return_value = mock_customer_df
        mock_spark_df = MagicMock()
        mock_create_df.return_value = mock_spark_df

        result_df = read_customers_file(source_path, source_file)
        mock_read_excel.assert_called_once_with(source_path + source_file, sheet_name="Worksheet")
    
        expected_schema = StructType([
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
        ])
        
        mock_create_df.assert_called_once_with(mock_customer_df, schema=expected_schema)  
        
        self.assertEqual(result_df, mock_spark_df)

    @patch("pyspark.sql.SparkSession.read")
    def test_read_orders_file_success(self, mock_read):
        source_path = "/pei_source_path"
        source_file = "orders.json"
        
        mock_ordes_df = MagicMock()
        mock_read.format.return_value = mock_read
        mock_read.option.return_value = mock_read
        mock_read.load.return_value = mock_ordes_df
        
        mock_ordes_df.toDF.return_value = mock_ordes_df
        result_df = read_orders_file(source_path, source_file)
        mock_read.format.assert_called_once_with("json")
        mock_read.option.assert_called_once_with("multiLine", True)
        mock_read.load.assert_called_once_with(f"{source_path}/{source_file}")
        mock_ordes_df.toDF.assert_called_once() 
        self.assertEqual(result_df, mock_ordes_df)

if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)