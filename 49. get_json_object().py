# Databricks notebook source
data = [('Shiwam','{"address" : { "city" : "banglore", "state": "Bengaluru"}, "gender" : "male"}'),
       ('Sonu','{"address" : { "city" : "kolkata", "state": "West Bengal"}, "hair" : "black"}')]

schema = ['name','prop']

df = spark.createDataFrame(data, schema)
df.show(truncate= False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import get_json_object

df1 = df.select('name',get_json_object('prop','$.address').alias('address'))
df1.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import get_json_object

df1 = df.select('name',get_json_object('prop','$.gender').alias('gender'))
df1.show(truncate=False)



# COMMAND ----------

from pyspark.sql.functions import get_json_object

df1 = df.select('name',get_json_object('prop','$.gender').alias('gender'),get_json_object('prop','$.hair').alias('hair'))
df1.show(truncate=False)


# COMMAND ----------


df1 = df.select('name',get_json_object('prop','$.address.city').alias('city'),get_json_object('prop','$.address.state').alias('state'))
df1.show(truncate=False)

# COMMAND ----------

