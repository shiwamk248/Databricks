# Databricks notebook source
from pyspark.sql.types import *
data = [(1,'Shiwam'),(2,'Sonu')]
schema = StructType([StructField(name= 'id', dataType = IntegerType()),
                     StructField(name= 'name', dataType = StringType())])
df = spark.createDataFrame(data= data, schema= schema)
df.show()
df.printSchema()

# COMMAND ----------

data = [
  {'id' : 1, 'name': 'Shiwam'},
  {'id' : 2, 'name': 'Sonu'}
]
df = spark.createDataFrame(data= data)
df.show()
df.printSchema()

# COMMAND ----------

