# Databricks notebook source
data = [(1,'Shiwam',2000),(2,'Diksha',4000),(3,'Sonu',3000)]
schema = ['id','name','sal']

df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(df['id'].alias('emp_id'),df.name.alias('emp_name'),col('sal').alias('emp_sal')).show()

# COMMAND ----------

df.sort(df.name.asc()).show()
df.sort(df.id.desc()).show()

# COMMAND ----------

df.printSchema()
df1 = df.select(df.id.cast('int'),df.name,df.sal.cast('int'))
df1.show()
df1.printSchema()

# COMMAND ----------

df1.filter(df1.name.like('S%')).show()

# COMMAND ----------

