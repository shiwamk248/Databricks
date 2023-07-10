# Databricks notebook source
from pyspark.sql import Row

row1 = Row('Shiwam',2000)
row2 = Row(name = 'Sonu', sal = 4000)

print(row1[0] + ' ' + str(row1[1]))
print(row2.name + ' ' + str(row2.sal))

# COMMAND ----------

row1 = Row(name = 'Shiwam', sal = 2400)
row2 = Row(name = 'Sonu', sal = 4000)
data = [row1,row2]

df = spark.createDataFrame(data)
df.show()
df.printSchema()

# COMMAND ----------

Person = Row('name', 'sal')

p1 = Person('Shiwam',2400)
p2 = Person('Sonu',3000)

print(p1.name + " " + str(p1.sal) + " " + p2.name + " " + str(p2.sal))
df = spark.createDataFrame([p1,p2])
df.show()

# COMMAND ----------

data = [Row(name = "Shiwam", properties = Row(hair = 'Black', eyes = 'Brown')),
        Row(name = "Sonu", properties = Row(hair = 'Curly', eyes = 'Black'))]

df = spark.createDataFrame(data)
df.show()
df.printSchema()

# COMMAND ----------

