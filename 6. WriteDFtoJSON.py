# Databricks notebook source
from pyspark.sql.types import *
data = [(1,'Shiwam'),(2,'Sonu')]
schema = StructType().add(field = 'id', data_type = IntegerType())\
                     .add(field = 'Name', data_type = StringType())

df = spark.createDataFrame(data = data, schema = schema)
df.show()
df.printSchema()

# COMMAND ----------

df.write.json(path = 'dbfs:/FileStore/jsondata/emps', mode = 'ignore')

# COMMAND ----------

display(spark.read.json( path = 'dbfs:/FileStore/jsondata/emps'))

# COMMAND ----------

help(df.write)