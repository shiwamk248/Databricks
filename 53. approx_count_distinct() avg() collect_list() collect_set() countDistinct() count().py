# Databricks notebook source
from pyspark.sql.functions import approx_count_distinct,avg, collect_list, collect_set,countDistinct, count
data = [('abc','HR',1500),('pqr','IT',2000),('xyz','HR',1500)]
schema = ['name','dept','sal']
df = spark.createDataFrame(data, schema)

df.show()

# COMMAND ----------

df.select(approx_count_distinct(df.sal)).show()


# COMMAND ----------

df.select(avg(df.sal)).show()


# COMMAND ----------

df.select(collect_list(df.dept)).show()


# COMMAND ----------

df.select(collect_set(df.dept)).show()


# COMMAND ----------

df.select(countDistinct(df.dept)).show()


# COMMAND ----------

df.select(count(df.name)).show()


# COMMAND ----------

