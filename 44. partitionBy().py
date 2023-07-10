# Databricks notebook source
data = [(1,'shiwam','male','IT'),(2,'ritu','female','Research'),(3,'sonu','male','IT')]
schema = ['id','name','gender','dep']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

help(df.write.parquet)

# COMMAND ----------

df.write.parquet(path = "dbfs:/FileStore/employees",mode = 'overwrite',partitionBy='dep')

# COMMAND ----------

spark.read.parquet("dbfs:/FileStore/employees/*").show()

# COMMAND ----------

spark.read.parquet("dbfs:/FileStore/employees").show()

# COMMAND ----------

spark.read.parquet("dbfs:/FileStore/employees/dep=IT").show()

# COMMAND ----------

spark.read.parquet("dbfs:/FileStore/employees/dep=Research").show()

# COMMAND ----------

