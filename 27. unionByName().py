# Databricks notebook source
data1 = [(1,'Shiwam','male')]
schema1 = ['id','name','gender']

data2 = [(1,'Shiwam',3000)]
schema2 = ['id','name','salary']

df1 = spark.createDataFrame(data1,schema1)
df2 = spark.createDataFrame(data2,schema2)


df1.show()
df2.show()

# COMMAND ----------

df1.union(df2).show()
df2.union(df1).show()

# COMMAND ----------

df1.unionByName(df2,allowMissingColumns= True).show()
# help(df1.unionByName)


# COMMAND ----------

