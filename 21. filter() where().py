# Databricks notebook source
data = [('1','abc','male',2000),('2','pqr','female',4000),('3','xyz','male',3000)]
schema = ['id','name','gender','salary']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.filter( df.gender == 'male').show()

# COMMAND ----------

df.filter( "gender <> 'female'").show()

# COMMAND ----------

df.filter( "gender = 'female'").show()

# COMMAND ----------

df.where("gender = 'female'").show()

# COMMAND ----------

df.where("gender <> 'female' and id=3").show()

# COMMAND ----------

df.filter( (df.gender == 'male') & (df.salary == 2000)).show()

# COMMAND ----------

