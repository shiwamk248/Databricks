# Databricks notebook source
df= spark.read.csv("dbfs:/FileStore/tables/Superstore_orders.csv", header = True)
display(df.head(20))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

