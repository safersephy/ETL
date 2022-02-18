# Databricks notebook source
# MAGIC 
# MAGIC %run ./Classroom-Setup

# COMMAND ----------

dbutils.fs.rm(userhome + "/streaming-demo", True)

(spark.read
  .format("text")
  .load("/mnt/training/definitive-guide/data/activity-data-with-geo.json/")
  .withColumnRenamed("value", "body")
  .write
  .mode("overwrite")
  .format("delta")
  .save(userhome + "/streaming-demo"))


activityStreamDF = (spark.readStream
  .format("delta")
  .option("maxFilesPerTrigger", 1)
  .load(userhome + "/streaming-demo"))


