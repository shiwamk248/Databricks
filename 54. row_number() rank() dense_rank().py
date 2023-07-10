# Databricks notebook source
from pyspark.sql.functions import row_number, rank, dense_rank
from pyspark.sql.window import Window
data = [('maheer','HR',2000),('Wafa','IT',3000),('Asi','HR',1500),
        ('Annu','PAYROLL',3500),('Shakti','IT',3000),('Pradeep','IT',400)
       ,('Kranthi','PAYROLL',2000),('Himanshu','IT',2000),('Bhargava','IT',2000),('Martin','HR',2500)]
schema = ['name','dept','sal']
df = spark.createDataFrame(data, schema)

df.show()

# COMMAND ----------

df.sort(df.dept).show()

# COMMAND ----------

window = Window.partitionBy(df.dept).orderBy(df.sal)

# COMMAND ----------

df.withColumn('rownumber',row_number().over(window)).show()

# COMMAND ----------

df.withColumn('rank',rank().over(window)).show()

# COMMAND ----------

df.withColumn('denserank',dense_rank().over(window)).show()

# COMMAND ----------

