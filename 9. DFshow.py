# Databricks notebook source
from pyspark.sql.types import *

data = [(1, 'ndcksdjvbkhvbdsvjksvnadjl'),
        (2, 'ndjbdc jkbdovhvoifvhidovn'),
        (3, 'cbdciduvichvuehufhifhuifs'),
        (4, 'cdsnhvinudhodnsjcnjcdbvvj')]

schema = StructType().add(field = 'ID', data_type = IntegerType())\
                     .add(field = 'Comments', data_type = StringType())

df = spark.createDataFrame(data = data, schema = schema)
df.show()

# COMMAND ----------

df.show(n=2)

# COMMAND ----------

df.show(truncate = False)

# COMMAND ----------

df.show(truncate = 8)

# COMMAND ----------

df.show(vertical = True)

# COMMAND ----------

df.show(n=2, truncate = False, vertical = True)