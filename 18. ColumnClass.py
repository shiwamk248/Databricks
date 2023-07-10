# Databricks notebook source
from pyspark.sql.functions import lit

col1 = lit("abc")
print(type(col1))

# COMMAND ----------

data = [('Shiwam',22,2000),('Sonu',24,4000)]
schema = ['name','age','sal']

df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

df1 = df.withColumn('newColumn',lit("newColumnValue"))
df1.show()
df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df1.select(df.name).show()
df1.select(df['name']).show()
df1.select(col('name')).show()

# COMMAND ----------

from pyspark.sql.types import  StringType, IntegerType, StructType, StructField
from pyspark.sql.functions import lit,col


data = [('Shiwam',22,2000,('black','brown')),('Sonu',24,4000,('black','blue'))]
nested_field = StructType([StructField(name = 'hair', dataType = StringType()),
                    StructField(name = 'eye', dataType = StringType())])

schema = StructType([StructField(name = 'name', dataType = StringType()),
                    StructField(name = 'age', dataType = IntegerType()),
                    StructField(name = 'salary', dataType = StringType()),
                    StructField(name = 'properties', dataType = nested_field)])

df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

df.select(df.properties).show()
df.select(df.properties.eye).show()
df.select(df['properties.eye']).show()
df.select(col('properties.eye')).show()



# COMMAND ----------

