# Databricks notebook source
df = spark.range(3)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_date, current_timestamp
df1 = df.withColumn('current_date',current_date())\
        .withColumn('current_timestamp',current_timestamp())
df1.show(truncate=False)
df1.printSchema()

# COMMAND ----------

from pyspark.sql.functions import date_format
df2 = df1.withColumn('changedDataFormat',date_format(df1.current_date,'yyyy.MM.dd'))
df2.show(truncate=False)
df2.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date
df3 = df2.withColumn('Convert changedDataFormat to String',to_date(df2.changedDataFormat,'yyyy.MM.dd'))
df3.show(truncate=False)
df3.printSchema()

# COMMAND ----------

