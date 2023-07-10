# Databricks notebook source
data = [(1,'Sonu',98787),(2,'Shiwam',89009)]
schema = ['id', 'name','salary']

df = spark.createDataFrame(data = data, schema = schema)
df1 = df.withColumnRenamed('salary', 'salary_amount')
df1.show()