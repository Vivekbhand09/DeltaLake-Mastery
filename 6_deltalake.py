# Databricks notebook source
# MAGIC %md
# MAGIC # **TABLE UTILITY COMMANDS**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.default.first_table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED workspace.default.first_table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/deltalakeansh/default/deltavol/dmlsink/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/deltalakeansh/default/deltavol/dmlsink/`
# MAGIC TIMESTAMP AS OF '2025-07-05T01:12:36.000+00:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE delta.`/Volumes/deltalakeansh/default/deltavol/dmlsink/` 
# MAGIC TO VERSION AS OF 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES delta.`/Volumes/deltalakeansh/default/deltavol/dmlsink`

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM delta.`/Volumes/deltalakeansh/default/deltavol/dmlsink/` RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE deltalakeansh.default.clonetblshallow
# MAGIC SHALLOW CLONE deltalakeansh.default.first_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM deltalakeansh.default.clonetbl

# COMMAND ----------

