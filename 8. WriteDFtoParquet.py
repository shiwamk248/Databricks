# Databricks notebook source
from pyspark.sql.types import *

data = [(1,'Shiwam'),(2,'Sonu')]
# schema = StructType().add(field = 'ID', data_type = IntegerType())\
#                            .add(field = 'Name', data_type = StringType())

schema = ['id','name']
  
df = spark.createDataFrame( data = data, schema = schema)
df.show()
df.printSchema()


# COMMAND ----------

df.write.parquet(path = 'dbfs:/FileStore/tables/parquet_outputt', mode = 'overwrite')


# COMMAND ----------

df1 = spark.read.parquet(path = '/dbfs/FileStore/tables/parquet_outputt')
display(df1)