# Databricks notebook source
data = [(1,'xyz',['python','gcp','sql']),(2,'abc',['dotnet','C#','angularJS'])]
schema = ['id','name','skill']

df = spark.createDataFrame(data, schema)
df.show(truncate = False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import transform, upper

df.select('id','name',transform('skill',lambda x: upper(x)).alias('skill')).show(truncate = False)

# COMMAND ----------

def convertSkillToUpper(df):
  return upper(df)

# COMMAND ----------

df.select('id','name',transform('skill',convertSkillToUpper)
         .alias('skill')).show(truncate = False)

# COMMAND ----------

df.show()

# COMMAND ----------

