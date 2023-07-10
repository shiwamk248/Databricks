# Databricks notebook source
data = [('abc',"{'eye' : 'brown', 'hair' : 'black'}")]
schema = ['name','specification']

df = spark.createDataFrame(data, schema)
df.show(truncate = False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.types import MapType, StringType

df1 = df.withColumn('prop_name', from_json(df.specification,MapType(StringType(),StringType())))
df1.show(truncate= False)
df1.printSchema()

# COMMAND ----------

df2 = df1.withColumn('eye',df1.prop_name.eye)
df2.show(truncate= False)


# COMMAND ----------

