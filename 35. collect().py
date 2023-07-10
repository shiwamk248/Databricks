# Databricks notebook source
data = [(1,'abc',2000),(2,'xyz',3000)]
schema = ['id','name','sal']

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

listRows = df.collect()
print(listRows)

# COMMAND ----------

print(listRows[1])

# COMMAND ----------

print(listRows[1][2])

# COMMAND ----------

