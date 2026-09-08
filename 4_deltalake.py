# Databricks notebook source
# MAGIC %md
# MAGIC # **DML**

# COMMAND ----------

data = [(5,100,'aa',1),(6,200,'bb',1),(7,300,'cc',1),(8,400,'dd',1)]

df = spark.createDataFrame(data, ['cust_id','income','name','tip'])

df.write.format("delta")\
        .mode("append")\
        .save("/Volumes/workspace/default/filestore/dmlsink/")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/filestore/dmlsink/`

# COMMAND ----------

# MAGIC %md
# MAGIC ### **update**

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/Volumes/workspace/default/filestore/dmlsink/`
# MAGIC SET income = 1000 WHERE cust_id = 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/filestore/dmlsink/`

# COMMAND ----------

# MAGIC %md
# MAGIC # **UPSERT**

# COMMAND ----------

data = [(1,100,'xyz',1),(9,200,'bb',1),(10,300,'cc',1)]

df = spark.createDataFrame(data, ['cust_id','income','name','tip'])

display(df)


# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

dlt_obj = DeltaTable.forPath(spark,"/Volumes/workspace/default/filestore/dmlsink/")

dlt_obj.alias("trg").merge(df.alias("src"),"trg.cust_id = src.cust_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/workspace/default/filestore/dmlsink/`

# COMMAND ----------

