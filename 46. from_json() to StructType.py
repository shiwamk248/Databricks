# Databricks notebook source
data = [('abc',"{'eye' : 'brown', 'hair' : 'black'}")]
schema = ['name','specification']

df = spark.createDataFrame(data, schema)
df.show(truncate = False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType

structtypefield = StructType([StructField('eye',StringType()),
                             StructField('hair',StringType())])
df1 = df.withColumn('prop_name', from_json(df.specification,structtypefield))
df1.show(truncate= False)
df1.printSchema()

# COMMAND ----------

