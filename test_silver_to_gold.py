# Databricks notebook source
# MAGIC %run "/Workspace/Users/vijayakumaruppin@vijayakumaruppingmail.onmicrosoft.com/pei/3_silver_to_gold_agg"

# COMMAND ----------

import unittest
from pyspark.sql import SparkSession
from unittest.mock import patch, MagicMock

class TestGold(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
        print("Starting tests.")

    def test_orders_data_agg(self):
        df = self.spark.createDataFrame(
            [('2025-02-14', 'Paper', 'Office Supplies', 'Maria Jane', 120.0),('2025-02-14', 'Paper', 'Office Supplies', 'Maria Jane', 180.0)],
            ["order_date", "product_sub_category", "product_category", "customer_name", "profit"]
        )
        result_df = orders_data_agg(df)
        result = result_df.select("total_profit").rdd.map(lambda row: row[0]).collect()
        expected = [300.00]
        
        self.assertEqual(result, expected)
    
    @patch('pyspark.sql.DataFrame.write')
    def test_write_gold_agg(self, mock_write):
        mock_df = MagicMock()
        mock_df.write = mock_write
        mock_write.format.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.saveAsTable.return_value = None 
        result = write_gold_agg(mock_df, "profit_agg", "gold")

        mock_write.format.assert_called_once_with("delta")
        mock_write.mode.assert_called_once_with("overwrite")
        mock_write.saveAsTable.assert_called_once_with("gold.profit_agg")
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main(argv=[''], exit=False)