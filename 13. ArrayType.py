# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import col
data = [("abc", [1, 2]), ("pqr", [3, 4]), ("xyz", [5, 6])]
schema = ["id", "number"]


df = spark.createDataFrame(data=data, schema=schema)
df.show()
df.display()
df.printSchema()

# COMMAND ----------

data = [("abc", [1, 2]), ("pqr", [3, 4]), ("xyz", [5, 6])]
# schema = ["id", "number"]

schema = StructType([StructField(  'id',StringType()),
                     StructField( 'number',ArrayType(IntegerType()))])

df = spark.createDataFrame(data=data, schema=schema)
df.show()
df.display()
df.printSchema()

# COMMAND ----------

df.withColumn('First_array_value',df.number[0]).show()

# COMMAND ----------

df.withColumn('First_array_value',col('number')[0]).show()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, array

data = [(1,2), (3,4)]
schema = ['num1', 'num2']

df = spark.createDataFrame(data, schema)
df.show()
df1 = df.withColumn('number', array(df.num1,df.num2))
df2 = df.withColumn('number', array(col('num1'),col('num2')))


df1.show()
df2.show()

# COMMAND ----------

