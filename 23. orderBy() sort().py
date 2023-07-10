# Databricks notebook source
data = [('abc','male',2000),('pqr','male',3000),('suv','male',15000),('xyz','female',4000)]
schema = ['name','gender','salary']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.orderBy(['gender','salary'],ascending = [1,0]).show()

# COMMAND ----------

df.sort(df.gender.desc()).show()
df.sort(df.gender.asc()).show()
df.sort(df.gender,df.salary.desc()).show()

# COMMAND ----------

