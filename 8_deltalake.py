# Databricks notebook source
# MAGIC %md
# MAGIC # **UNIFORM**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE deltalakeansh.default.unitbl
# MAGIC USING DELTA TBLPROPERTIES(
# MAGIC   'delta.enableIcebergCompatV2' = 'true',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO deltalakeansh.default.unitbl