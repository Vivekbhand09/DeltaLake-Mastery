# Databricks notebook source
df = spark.read.table("deltalakeansh.default.clonetbl")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
        .save("/Volumes/deltalakeansh/default/deltavol/optimization/")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/deltalakeansh/default/deltavol/optimization/`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/Volumes/deltalakeansh/default/deltavol/optimization/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/deltalakeansh/default/deltavol/optimization/`
# MAGIC WHERE cust_id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`/Volumes/deltalakeansh/default/deltavol/optimization/` ZORDER BY (cust_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE deltalakeansh.default.clonetbl
# MAGIC CLUSTER BY AUTO

# COMMAND ----------

