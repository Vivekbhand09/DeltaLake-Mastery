# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # **Create Delta Table**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Using SQL API**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE workspace.default.first_table
# MAGIC (
# MAGIC   id INT NOT NULL,
# MAGIC   salary INT UNIQUE
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Using Delta API**

# COMMAND ----------

from delta.tables import DeltaTable, IdentityGenerator

# COMMAND ----------

DeltaTable.createIfNotExists(spark) \
  .tableName("workspace.default.firstdeltaapi") \
  .addColumn("id", "INT") \
  .addColumn("salary", "INT") \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE workspace.default.firstdeltaapi

# COMMAND ----------

# MAGIC %md
# MAGIC # **GENERATED COLUMNS**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Identity Columns**

# COMMAND ----------

DeltaTable.create(spark)\
  .tableName("workspace.default.firstdeltaapi")\
  .addColumn("id_col", dataType=LongType(), generatedAlwaysAs=IdentityGenerator())\
  .addColumn("salary", dataType = IntegerType())\
  .addColumn("name", dataType = StringType())\
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Computed Columns**

# COMMAND ----------

DeltaTable.create(spark)\
  .tableName("workspace.default.firstdeltaapi")\
  .addColumn("salaryAfterTax", dataType=LongType(), generatedAlwaysAs = "CAST((salary * 0.7) AS BIGINT)")\
  .addColumn("salary", dataType = IntegerType())\
  .addColumn("name", dataType = StringType())\
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.default.firstdeltaapi(salary,name)
# MAGIC VALUES 
# MAGIC (100,'aa'),
# MAGIC (200,'bb')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.firstdeltaapi

# COMMAND ----------

