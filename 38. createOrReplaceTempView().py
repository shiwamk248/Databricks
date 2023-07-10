# Databricks notebook source
data = [(1,'xyz',2000),(2,'abc',3000)]
schema = ['id','name','salary']

df= spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.createOrReplaceTempView('Employees')
df1 = spark.sql("select id, name from Employees")
df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select id,upper(name),salary from Employees

# COMMAND ----------

df.createOrReplaceGlobalTempView('Emp')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.Emp

# COMMAND ----------

# spark.catalog.dropTempView('employee')