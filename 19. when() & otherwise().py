# Databricks notebook source
data = [(1,'Shiwam','M',2000),(2,'Diksha','F',4000),(3,'Sonu','',3000)]
schema = ['id','name','sex','sal']

df = spark.createDataFrame(data,schema)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import when
df1 = df.select(df.id, df.name, when(condition = df.sex == 'M', value = 'Male')
        .when(condition = df.sex == 'F', value = 'Female')
        .otherwise('unkonwn').alias('sex'), df.sal)
df1.show()

# COMMAND ----------

from pyspark.sql.functions import when
df2 = df.select(df.id, df.name, df.sex,when( df.sex == 'M', 'Male')
        .when( df.sex == 'F', 'Female')
        .otherwise('unkonwn').alias('sex'), df.sal)
df2.show()

# COMMAND ----------

