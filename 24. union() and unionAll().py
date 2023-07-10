# Databricks notebook source
data1 = [('abc','male',2000),('pqr','male',3000),('suv','male',15000),('xyz','female',4000)]
data2 = [('abc','male',5454),('pqr','male',3200),('suv','male',15000),('xyz','female',32321)]

schema = ['name','gender','salary']

df1 = spark.createDataFrame(data1, schema)
df2 = spark.createDataFrame(data2, schema)

df1.show()
df2.show()

# COMMAND ----------

df1.union(df2).show()

# COMMAND ----------

df3 = df2.unionAll(df1)
df3.show()

# COMMAND ----------

df3.distinct().show()

# COMMAND ----------

