# Databricks notebook source
data = [(1,'abc','M',5000,'IT'),\
        (2,'def','M',6000,'IT'),\
       (3,'ijk','F',2500,'HR'),\
       (4,'lmn','M',4000,'Payroll'),\
       (5,'opq','F',2000,'Payroll'),\
       (6,'rst','M',2000,'HR'),\
       (7,'xyz','F',3000,'IT')]

schema = ['id','name','gender','salary','dept']

df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df1 = df.groupBy(df.dept).count()
df1.show()

# COMMAND ----------

df1 = df.groupBy(df.dept).sum('salary')
df1.show()

# COMMAND ----------

df1 = df.groupBy(df.gender).sum('salary')
df1.show()

# COMMAND ----------

df1 = df.groupBy(df.gender,df.dept).sum('salary').
df1.show()

# COMMAND ----------

df1 = df.groupBy(df.gender,df.dept).sum('salary')
df1.orderBy(df.dept,df.gender.desc()).show()

# COMMAND ----------

