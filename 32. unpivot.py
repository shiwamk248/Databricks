# Databricks notebook source
data = [('IT',8,5),('HR',3,2),('Payroll',3,4)]
schema = ['dept','male','female']

df = spark.createDataFrame(data,schema)

df.show()


# COMMAND ----------

from pyspark.sql.functions import expr
unpivotDf = df.select('dept',expr("stack(2,'Male',male,'Female',female) as (gender,count)"))
unpivotDf.show()


# COMMAND ----------

unpivotDf.groupBy('dept').pivot('gender').show()

# COMMAND ----------

