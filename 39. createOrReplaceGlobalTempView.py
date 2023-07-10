# Databricks notebook source
# MAGIC %sql
# MAGIC select * from global_temp.Emp
# MAGIC
# MAGIC --Here we have declare global temp table in 38. createOrReplaceTempView() notebook
# MAGIC --df.createOrReplaceGlobalTempView('Emp')
# MAGIC

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

spark.catalog.listTables('global_temp')

# COMMAND ----------

# spark.catalog.dropGlobalTempView('Emp')