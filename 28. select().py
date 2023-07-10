# Databricks notebook source
from pyspark.sql.functions import col
data = [(1,'Shiwam','male',2000),(2,'Diksha','female',3000),(3,'Sonu','male',4000)]
schema = ['id','name','gender','salary']

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

df.select('id','name').show()

# COMMAND ----------

df.select(df['id'],df['name']).show()

# COMMAND ----------

df.select(df.id,df.name).show()

# COMMAND ----------

df.select(col('id'),col('name')).show()

# COMMAND ----------

df.select(['id','name']).show()

# COMMAND ----------

df.select("*").show()

# COMMAND ----------

df.select([col for col in df.columns]).show()

# COMMAND ----------

