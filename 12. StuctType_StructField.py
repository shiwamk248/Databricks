# Databricks notebook source
from pyspark.sql.types import *
data = [(1,'Sonu',98787),(2,'Shiwam',89009)]
# schema = ['id', 'name','salary']
schema = StructType([StructField( name = 'id', dataType = IntegerType()),
                           StructField( name = 'name', dataType = StringType()),
                           StructField( name = 'salary', dataType = IntegerType())])

df = spark.createDataFrame(data = data, schema = schema)
df.show()
df.printSchema()

# COMMAND ----------

data = [(1,('Sonu','Kumar'),98787),(2,('Shiwam','Prasad'),89009)]
# schema = ['id', 'name','salary']
nested_field = StructType([StructField( name = 'first_name', dataType = StringType()),
                          StructField( name = 'last_name', dataType = StringType())])

schema = StructType([StructField( name = 'id', dataType = IntegerType()),\
                     StructField( name = 'name', dataType = nested_field),\
                     StructField( name = 'salary', dataType = IntegerType())])

df = spark.createDataFrame(data = data, schema = schema)
df.show()
display(df)
df.printSchema()