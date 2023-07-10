# Databricks notebook source
data = [(1,'abc',2000),(2,'xyz',3000)]
schema = ['id','name','sal']

df = spark.createDataFrame(data, schema)
df.show()


# COMMAND ----------

from pyspark.sql.functions import upper
df.withColumn('name',upper('name')).show()
df.show()

# COMMAND ----------

from pyspark.sql.functions import upper
def convertNameToUpper(df):
  return df.withColumn('name',upper('name'))


# COMMAND ----------

df1 =df.transform(convertNameToUpper)
df1.show()
df.show()

# COMMAND ----------

def doubleSalary(df):
  return df.withColumn('sal',df.sal *2)

# COMMAND ----------

df2 = df.transform(doubleSalary)
df2.show()
df.show()

# COMMAND ----------

df3 =df.transform(convertNameToUpper).transform(doubleSalary)
df3.show()
df.show()

# COMMAND ----------

