# Databricks notebook source
data = [('Shiwam',{'eye': 'brown', 'hair': 'black'})]
schema = ['name','property']

df = spark.createDataFrame(data,schema)
df.show(truncate= False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_json
from pyspark.sql.types import StructType, StructField, StringType

df1 = df.withColumn('propToString',to_json(df.property))
df1.show(truncate= False)
df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_json
from pyspark.sql.types import StructType, StructField, StringType
data = [('Shiwam', ('brown', 'blue'))]
schema = StructType(
    [
        StructField('name', StringType()),
      StructField('property',
        StructType(
            [StructField('eye', StringType()), StructField('hair', StringType())]
        ))
    ]
)


df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

df1 = df.withColumn('propToSting',to_json(df.property))
df1.show(truncate=False)
df1.printSchema()
display(df1)

# COMMAND ----------

