# Databricks notebook source
df = spark.read.csv(path = "dbfs:/FileStore/tables/Trips.csv",header = True)
df.show()

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

df = spark.read.csv(path = "dbfs:/FileStore/tables/employee1.csv", header = True)
df.show()

# COMMAND ----------

df = spark.read.csv(path = ["dbfs:/FileStore/tables/employee1.csv","dbfs:/FileStore/tables/employee2.csv"], header = True)
df.show()

# COMMAND ----------

