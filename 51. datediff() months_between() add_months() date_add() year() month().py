# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import to_date,current_date

data = [('abc','1999-05-22'),('xyz','2000-04-17')]
# schema = StructType([StructField('name',StringType()),
#                     StructField('dob',date()])

schema = ['name','dob']
df = spark.createDataFrame(data,schema)
df1 = df.withColumn('todaydate',current_date())
df1.show()
df1.printSchema()

# COMMAND ----------

df2 = df1.select('name',to_date(df1.dob).alias('dob'),'todaydate')
df2.show()
df2.printSchema()

# COMMAND ----------

from pyspark.sql.functions import datediff

df3 = df2.withColumn('noofday',datediff(df2.todaydate,df2.dob))
df3.show()

# COMMAND ----------

from pyspark.sql.functions import months_between

df4 = df3.withColumn('noofmonths',months_between(df3.todaydate,df3.dob))
df4.show()

# COMMAND ----------

from pyspark.sql.functions import add_months

df5 = df4.withColumn('after4months',add_months(df4.todaydate,4))
df5.show()

# COMMAND ----------

from pyspark.sql.functions import date_add

df6 = df5.withColumn('after15days',date_add(df5.todaydate,15))
df6.show()

# COMMAND ----------

from pyspark.sql.functions import year,month

df7= df6.withColumn('year',year(df6.todaydate))\
        .withColumn('month',month(df6.todaydate))
df7.show()

# COMMAND ----------

