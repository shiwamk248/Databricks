# Databricks notebook source
data = [(1,'abc','male',2000,'IT'),(2,'pqr',None,5000,'Payroll'),(3,'xyz','male',4000,None)]
schema = ['id','name','gender','salary','dep']

df = spark.createDataFrame(data,schema)
df.show()


# COMMAND ----------

df.fillna('UNKNOWN',['dep']).show()

# COMMAND ----------

df.fillna('UNKNOWN',['dep','gender']).show()

# COMMAND ----------

df.fillna('UNKNOWN').show()

# COMMAND ----------

