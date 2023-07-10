# Databricks notebook source
from pyspark.sql.functions import current_timestamp,to_timestamp, hour,minute,second, date_format,lit

df = spark.range(2)
df1 = df.withColumn('date',lit('1993-07-27'))
df1.show()

df1.printSchema()

# COMMAND ----------

df2 = df1.withColumn('currentdate',current_timestamp())\
   .withColumn('totimestamp',to_timestamp(df1.date))
df2.show(truncate=False)

# COMMAND ----------

df3 = df2.withColumn('hour',hour(df2.currentdate))\
          .withColumn('minute',minute(df2.currentdate))\
          .withColumn('second',second(df2.currentdate))
df3.show(truncate=False)


# COMMAND ----------

