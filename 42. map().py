# Databricks notebook source
data = [('Shiwam','Kumar'),('Sonu','Prasad')]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())
rdd1 = rdd.map(lambda x: x + (x[0] + ' ' +x[1],))
print(rdd1.collect())

# COMMAND ----------

data = [('Shiwam','Kumar'),('Sonu','Prasad')]
df = spark.createDataFrame(data, ['fn','ln'])
df.show()
rdd1 = df.rdd.map(lambda x: x + (x[0] + ' ' +x[1],))
df1 = rdd1.toDF(['fn','ln','fullname'])
print(rdd1.collect())
df1.show()

# COMMAND ----------

def fullname(x):
  return x + (x[0] + ' ' +x[1],)

data = [('Shiwam','Kumar'),('Sonu','Prasad')]
df = spark.createDataFrame(data, ['fn','ln'])
df.show()
rdd1 = df.rdd.map(lambda x: fullname(x))
df1 = rdd1.toDF(['fn','ln','fullname'])
print(rdd1.collect())
df1.show()

# COMMAND ----------

