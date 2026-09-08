# Databricks notebook source
# MAGIC %md
# MAGIC # **SCHEMA ENFORCEMENT**

# COMMAND ----------

data = [(1,100,'aa',1),(2,200,'bb',1),(3,300,'cc',1),(4,400,'dd',1)]

df = spark.createDataFrame(data, ['id','salary','name','sus_col'])

display(df)

# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
        .save("/Volumes/workspace/default/filestore/rawcsvsink/demo1/")

# COMMAND ----------

# MAGIC %md
# MAGIC # **SCHEMA EVOLUTION**

# COMMAND ----------

df.write.format("delta")\
        .mode("append")\
        .option("mergeSchema",True)\
        .save("/Volumes/workspace/default/filestore/rawcsvsink/demo2/")

# COMMAND ----------

# MAGIC %md
# MAGIC # READ DELTA DATA

# COMMAND ----------

# MAGIC %md
# MAGIC ### **TABLE**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM deltalakeansh.default.firstdeltaapi

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DATA LAKE**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/filestore/rawcsvsink/demo2/`

# COMMAND ----------

# MAGIC %md
# MAGIC # **SCHEMA OVERWRITE**

# COMMAND ----------

data = [(1,100,'aa',1),(2,200,'bb',1),(3,300,'cc',1),(4,400,'dd',1)]

df = spark.createDataFrame(data, ['cust_id','income','name','tip'])

display(df)

# COMMAND ----------

df.write.format("delta")\
        .mode("overwrite")\
        .option("overwriteSchema",True)\
        .save("/Volumes/workspace/default/filestore/rawcsvsink/demo3/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/Volumes/workspace/default/filestore/rawcsvsink/demo3/`