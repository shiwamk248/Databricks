# Databricks notebook source
# DBTITLE 1,Explode function in Array
from pyspark.sql.types import *
from pyspark.sql.functions import col,array,lit,explode

data = [ (1,'Sonu',['DotNet', 'C#']), (2,'Shiwam',['Python', 'Sql'])]
# schema = ['id', 'name', 'skills']
schema = StructType([StructField('id',IntegerType()),
                    StructField('name',StringType()),
                    StructField('skill',ArrayType(StringType()))])

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()
df.withColumn('explodeCol',explode(col('skill'))).show()

# COMMAND ----------

# DBTITLE 1,Split Function in Array
from pyspark.sql.functions import split

data = [(1,'Sonu','DotNet,C#'), (2,'Shiwam','Python,sql')]
schema = StructType([StructField('id',IntegerType()),
                    StructField('name',StringType()),
                    StructField('skill',StringType())])

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()
df1= df.withColumn('splitCol',split(col('skill'),','))
df1.show()
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,array() 
from pyspark.sql.functions import array

data = [(1,'Sonu','DotNet','C#'), (2,'Shiwam','Python5','sql')]
schema = StructType([StructField('id',IntegerType()),
                    StructField('name',StringType()),
                    StructField('PrimarySkill',StringType()),
                    StructField('SecondarySkill',StringType())])

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()
df1= df.withColumn('arraycol',array(col('PrimarySkill'),col('SecondarySkill')))
df1.show()
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,array_contains()
from pyspark.sql.types import *
from pyspark.sql.functions import col,array_contains

data = [ (1,'Sonu',['DotNet', 'C#']), (2,'Shiwam',['Python', 'Sql'])]
# schema = ['id', 'name', 'skills']
schema = StructType([StructField('id',IntegerType()),
                    StructField('name',StringType()),
                    StructField('skill',ArrayType(StringType()))])

df = spark.createDataFrame(data, schema)
display(df)
df.printSchema()
df1 = df.withColumn('find_skill', array_contains(col('skill'),'C#'))
df1.show()