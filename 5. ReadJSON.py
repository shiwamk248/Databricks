# Databricks notebook source
from pyspark.sql.types import *

schema = StructType().add(field = 'id', data_type = IntegerType())\
                      .add(field = 'gender', data_type = StringType())\
                      .add(field = 'name', data_type = StringType())\
                      .add(field = 'salary', data_type = IntegerType())

df = spark.read.json(path = "dbfs:/FileStore/tables/demojson.json", schema = schema)
display(df)
df.printSchema()

# COMMAND ----------

df1= spark.read.json(path = "dbfs:/FileStore/tables/demojsonML.json",multiLine=True)
display(df1)
df1.printSchema()