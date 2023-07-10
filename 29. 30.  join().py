# Databricks notebook source
from pyspark.sql.functions import col
data1 = [(1,'Shiwam',2000,2),(2,'Diksha',3000,1),(3,'Sonu',4000,4)]
schema1 = ['id','name','salary','dep']

data2 = [(1,'IT'),(2,'HR'),(3,'Payroll')]
schema2 = ['id','name']

df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)
df1.show()
df2.show()


# COMMAND ----------

df1.join(df2, df1.dep == df2.id, 'outer').select('*').sort(df1.id, ascending = [0]).show()

# COMMAND ----------

df1.join(df2, df1.dep == df2.id, 'inner').select('*').sort(df1.id, ascending = [0]).show()

# COMMAND ----------

df1.join(df2, df1.dep == df2.id, 'left').select('*').sort(df1.id, ascending = [0]).show()

# COMMAND ----------

df1.join(df2, df1.dep == df2.id, 'right').select('*').sort(df1.id, ascending = [0]).show()

# COMMAND ----------

# default``inner``. Must be one of: ``inner``, ``cross``, ``outer``,
#         ``full``, ``fullouter``, ``full_outer``, ``left``, ``leftouter``, ``left_outer``,
#         ``right``, ``rightouter``, ``right_outer``, ``semi``, ``leftsemi``, ``left_semi``,
#         ``anti``, ``leftanti`` and ``left_anti``.
      
df1.join(df2, df1.dep == df2.id, 'full').select('*').sort(df1.id, ascending = [0]).show()

# COMMAND ----------

# default``inner``. Must be one of: ``inner``, ``cross``, ``outer``,
#         ``full``, ``fullouter``, ``full_outer``, ``left``, ``leftouter``, ``left_outer``,
#         ``right``, ``rightouter``, ``right_outer``, ``semi``, ``leftsemi``, ``left_semi``,
#         ``anti``, ``leftanti`` and ``left_anti``.
      
df1.join(df2, df1.dep == df2.id, 'cross').select('*').sort(df1.id, ascending = [0]).show()

# COMMAND ----------

help(df1.pivot)

# COMMAND ----------

