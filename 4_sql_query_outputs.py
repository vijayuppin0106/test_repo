# Databricks notebook source
# MAGIC %md
# MAGIC # **## 5.	Using SQL output the following aggregates**

# COMMAND ----------

# MAGIC %md
# MAGIC **a.	Profit by Year**

# COMMAND ----------

# MAGIC %sql
# MAGIC select year, sum(total_profit) as total_profit from gold.profit_agg
# MAGIC group by year order by year

# COMMAND ----------

# MAGIC %md
# MAGIC **b.	Profit by Year + Product Category**

# COMMAND ----------

# MAGIC %sql
# MAGIC select year, product_category,sum(total_profit) as total_profit from gold.profit_agg
# MAGIC group by year, product_category order by year,product_category

# COMMAND ----------

# MAGIC %md
# MAGIC ### c.	Profit by Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_name,sum(total_profit) as total_profit from gold.profit_agg
# MAGIC group by customer_name order by customer_name

# COMMAND ----------

# MAGIC %md
# MAGIC **d.	Profit by Customer + Year**

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_name,year,sum(total_profit) as total_profit from gold.profit_agg
# MAGIC group by customer_name, year order by customer_name,year