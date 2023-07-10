# Databricks notebook source
from pyspark.sql.functions import col,lit

# COMMAND ----------

data = [(1,'Shiwam','3000'),(4,'Sonu','4000')]
columns = ['id', 'name', 'salary']
df = spark.createDataFrame(data = data, schema = columns)
df.show()
df.printSchema()

# COMMAND ----------

df1 = df.withColumn(colName= 'salary', col = col('salary').cast('Integer'))
df1 = df1.withColumn('id', col('id').cast('Integer'))
df1.show()
df1.printSchema()

# COMMAND ----------

df2 = df1.withColumn('salary',col('salary') * 2)
df2.show()

# COMMAND ----------

df3 = df2.withColumn('country', lit('India'))
df3.show()

# COMMAND ----------

df4 =df3.withColumn('copiedsalary',col('salary'))
df4.show()

# COMMAND ----------

