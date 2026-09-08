# Databricks notebook source
# MAGIC %sql
# MAGIC ALTER TABLE deltalakeansh.default.clonetbl
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM deltalakeansh.default.clonetbl

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO deltalakeansh.default.clonetbl
# MAGIC VALUES (10, 100, 'zz',1)

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE deltalakeansh.default.clonetbl
# MAGIC SET name = 'hi bro' where cust_id = 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM deltalakeansh.default.clonetbl
# MAGIC WHERE cust_id = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY deltalakeansh.default.clonetbl

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('deltalakeansh.default.clonetbl',1,3)

# COMMAND ----------

