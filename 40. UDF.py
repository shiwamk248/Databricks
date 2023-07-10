# Databricks notebook source
data = [(1,'abc',2000,500),(2,'xyz',4000,1000)]
schema = ['id','name','salary','bonus']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
df.select('*').show()

# COMMAND ----------

def totalSal(s,b):
  return s+b

# COMMAND ----------

TotalPayment = udf(lambda s,b: totalSal(s, b),IntegerType())

df.withColumn('TotalSalary',totalSal(col('salary'),col('bonus'))).show()
df.withColumn('TotalSalary',TotalPayment(col('salary'),col('bonus'))).show()

# COMMAND ----------

@udf(returnType= IntegerType())
def totalSal(s,b):
  return s+b

df.withColumn('TotalSalary',totalSal(col('salary'),col('bonus'))).show()


# COMMAND ----------

df.select('*',totalSal(col('salary'),col('bonus')).alias('TotalSalary')).show()


# COMMAND ----------

df.createTempView('Emp')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from Emp
# MAGIC select *,(salary + bonus) as TotalSal from Emp
# MAGIC

# COMMAND ----------

def totalSal(s,b):
  return s+b
spark.udf.register(name = 'TotalSalary', f = totalSal, returnType = IntegerType())

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,TotalSalary(salary,bonus) as TotalSalary from Emp
# MAGIC

# COMMAND ----------

