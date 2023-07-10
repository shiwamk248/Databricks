# Databricks notebook source
data = [(1,'abc'),(2,'xyz')]
rdd = spark.sparkContext.parallelize(data)
print(type(rdd))
print(rdd.collect())

# COMMAND ----------

df = rdd.toDF()
df.show()

# COMMAND ----------

df = rdd.toDF(schema = ['id','name'])
df.show()

# COMMAND ----------

df1 =spark.createDataFrame(rdd, schema = ['id','name'])
df1.show()

# COMMAND ----------

