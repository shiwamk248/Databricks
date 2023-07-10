# Databricks notebook source
print("Shiwam")

# COMMAND ----------

df = spark.read.parquet("dbfs:/FileStore/tables/alltypes_dictionary.parquet")
df.show()
df.count()

# COMMAND ----------

