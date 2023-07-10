# Databricks notebook source
data = [('abc','male',2000),('pqr','male',3000),('pqr','male',3000),('xyz','female',4000)]
schema = ['name','gender','salary']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.distinct().show()
df.drop_duplicates().show()

# COMMAND ----------

df.drop_duplicates(['gender']).show()
df.drop_duplicates(['gender','salary']).show()

# COMMAND ----------

