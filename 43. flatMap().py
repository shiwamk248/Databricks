# Databricks notebook source
data = [('Shiwam Prasad'),('Sonu Kumar')]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

# COMMAND ----------

for item in rdd.collect():
  print(item)

# COMMAND ----------

rdd1 = rdd.flatMap(lambda x: x.split(' '))
for item in rdd1.collect():
  print(item)

# COMMAND ----------

