# Databricks notebook source
data = [('Shiwam','{"eye":"black","hair":"brown", "skin": "fair"}'),
       ('Shiwam','{"eye":"brown","hair":"black", "skin": "white"}')]

schema = ['name','property']

df = spark.createDataFrame(data, schema)
df.show(truncate= False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import json_tuple

df1 = df.select(df.name,json_tuple(df.property,'hair','skin').alias('hair','skin'))
df1.show(truncate=False)
df1.printSchema()

# COMMAND ----------

