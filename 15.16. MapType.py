# Databricks notebook source
from pyspark.sql.types import MapType,StringType,IntegerType,StructType,StructField

data = [("Shiwam", {"hair": "brown", "eyes" : "balck"}),
       ("Sonu", {"hair": "curly", "eyes" : "brown"})]

schema = StructType([StructField("name",StringType()),
                    StructField("characteristic",MapType(StringType(),StringType()))])

df = spark.createDataFrame(data, schema)
display(df)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

df1 = df.withColumn('eyes', df.characteristic["eyes"])
df1.show(truncate = False)

# COMMAND ----------

df2 = df1.withColumn('hair', df.characteristic.getItem("hair"))
df2.show(truncate = False)

# COMMAND ----------

from pyspark.sql.functions import explode,col

df3 = df.select("name", "characteristic", explode(df.characteristic))
df3.show(truncate = False)

# COMMAND ----------

from pyspark.sql.functions import explode,col,map_keys

df4 = df.select("name", "characteristic", map_keys(df.characteristic))
df4.show(truncate = False)

# COMMAND ----------

from pyspark.sql.functions import explode,col,map_keys,map_values

df5 = df.select("name", "characteristic", map_values(df.characteristic))
df5.show(truncate = False)