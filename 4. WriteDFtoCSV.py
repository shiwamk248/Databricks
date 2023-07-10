# Databricks notebook source
from pyspark.sql.types import *
#help(DataFrameWriter)

# COMMAND ----------

data = [(1,'Shiwam'),(2,'Sonu')]
schema = ['id','Name']

df = spark.createDataFrame(data = data, schema = schema)
display(df)
df.printSchema()

# COMMAND ----------

data = [(1,'Shiwam'),(2,'Sonu')]
schema = StructType([StructField(name= 'id', dataType = IntegerType()),
                     StructField(name= 'name', dataType = StringType())])

df = spark.createDataFrame(data = data, schema = schema)
display(df)
df.printSchema()

# COMMAND ----------

help(StructType)

# COMMAND ----------

df.write.csv(path = 'dbfs:/temp/emps', header = True, mode = 'append')
# ``append``: Append contents of this :class:`DataFrame` to existing data.
#         * ``overwrite``: Overwrite existing data.
#         * ``ignore``: Silently ignore this operation if data already exists.
#         * ``error`` or ``errorifexists`` (default case): Throw an exception if data already \
#             exists.

# COMMAND ----------

display(spark.read.csv(path = 'dbfs:/temp/emps',header = True))

# COMMAND ----------

list = [float(x) for x in input("Enter multiple value: ").split(" ")]
sum = 0
for i in list:
  sum = sum + i
  
print(sum)

# COMMAND ----------

schema = StructType([StructField(name = "id" , dataType = IntegerType()),
                    StructField(name = "Name", dataType = StringType())])