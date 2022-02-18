# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Structured Streaming with Azure EventHubs 
# MAGIC 
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you should be able to:
# MAGIC * Establish a connection with Event Hubs in Spark
# MAGIC * Subscribe to and configure an Event Hubs stream
# MAGIC * Parse JSON records from Event Hubs
# MAGIC 
# MAGIC ## Library Requirements
# MAGIC 
# MAGIC The Maven library with coordinate `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.17`
# MAGIC 
# MAGIC ## Resources
# MAGIC - [Docs for Azure Event Hubs connector](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/streaming-event-hubs)
# MAGIC - [Documentation on how to install Maven libraries](https://docs.azuredatabricks.net/user-guide/libraries.html#maven-or-spark-package)
# MAGIC - [Spark-EventHub debugging FAQ](https://github.com/Azure/azure-event-hubs-spark/blob/master/FAQ.md)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Started
# MAGIC 
# MAGIC Run the following cell to configure our classroom and set up a local streaming file read that we'll be writing to Event Hubs.

# COMMAND ----------

# MAGIC %run ./Includes/Streaming-Demo-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Azure Event Hubs</h2>
# MAGIC 
# MAGIC Microsoft Azure Event Hubs is a fully managed, real-time data ingestion service.
# MAGIC You can stream millions of events per second from any source to build dynamic data pipelines and immediately respond to business challenges.
# MAGIC It integrates seamlessly with a host of other Azure services.
# MAGIC 
# MAGIC Event Hubs can be used in a variety of applications such as
# MAGIC * Anomaly detection (fraud/outliers)
# MAGIC * Application logging
# MAGIC * Analytics pipelines, such as clickstreams
# MAGIC * Archiving data
# MAGIC * Transaction processing
# MAGIC * User telemetry processing
# MAGIC * Device telemetry streaming
# MAGIC * <b>Live dashboarding</b>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Define Connection Strings and Create Configuration Object
# MAGIC 
# MAGIC This cell uses a connection string to create a simple `EventHubsConf` object, which will be used to connect.
# MAGIC 
# MAGIC Note that the code below uses DB Utils secrets to load in the Event Hub connection string previously loaded into Azure Key Vault.
# MAGIC 
# MAGIC To run this notebook, you'll need to configure Event Hubs and provide the relavent information in the following format:
# MAGIC ```
# MAGIC Endpoint=sb://<event_hubs_namespace>.servicebus.windows.net/;SharedAccessKeyName=<key_name>;SharedAccessKey=<signing_key>=;EntityPath=<event_hubs_instance>
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream to Event Hub to Produce Stream
# MAGIC 
# MAGIC Below, we configure a streaming write to Event Hubs. Refer to the docs for additional ways to [write data to Event Hubs](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md#writing-data-to-eventhubs).

# COMMAND ----------

checkpointPath = userhome + "/event-hub/write-checkpoint"
dbutils.fs.rm(checkpointPath,True)

connectionString = "Endpoint=sb://sienneventhub.servicebus.windows.net/;SharedAccessKeyName=master;SharedAccessKey=mTC+GIY/i2wotXa0JZykBbtwcJWHgzGpsud+aegN5/A=;EntityPath=sienneh"

ehConf = {}

#depending on the version of the Maven library (2.3.15 version and above), you might need to encrypt the connectionString. 
#ehConf['eventhubs.connectionString'] = connectionString 

ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

(activityStreamDF
  .writeStream
  .format("eventhubs")
  .outputMode("update")
  .options(**ehConf)
  .trigger(processingTime="25 seconds")
  .option("checkpointLocation", checkpointPath)
  .start())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Event Hubs Configuration
# MAGIC 
# MAGIC Above, a simple `EventHubsConf` object is used to write data. There are [numerous additional options for configuration](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md#eventhubsconf). Below, we specify an `EventPosition` ([docs](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/structured-streaming/streaming-event-hubs#eventposition)) and limit our throughput by setting `MaxEventsPerTrigger`.

# COMMAND ----------

from datetime import datetime as dt
import json
# Start from beginning of stream
startOffset = "-1"

# End at the current time. This datetime formatting creates the correct string format from a python datetime object
endTime = dt.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")


# Create the positions
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}

endingEventPosition = {
  "offset": None,           #not in use
  "seqNo": -1,              #not in use
  "enqueuedTime": endTime,
  "isInclusive": True
}



connectionString = "Endpoint=sb://sienneventhub.servicebus.windows.net/;SharedAccessKeyName=master;SharedAccessKey=mTC+GIY/i2wotXa0JZykBbtwcJWHgzGpsud+aegN5/A=;EntityPath=sienneh"

eventHubsConf = {}
eventHubsConf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
eventHubsConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
eventHubsConf["eventhubs.endingPosition"] = json.dumps(endingEventPosition)
eventHubsConf["maxEventsPerTrigger"] = 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### READ Stream using EventHub
# MAGIC 
# MAGIC The `readStream` method is a <b>transformation</b> that outputs a DataFrame with specific schema specified by `.schema()`. 

# COMMAND ----------

eventStreamDF = (spark.readStream
  .format("eventhubs")
  .options(**eventHubsConf)
  .load())

# COMMAND ----------

display(eventStreamDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Most of the fields in this response are metadata describing the state of the Event Hubs stream. We are specifically interested in the `body` field, which contains our JSON payload.
# MAGIC 
# MAGIC Noting that it's encoded as binary, as we select it, we'll cast it to a string.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Each line of the streaming data becomes a row in the DataFrame once an <b>action</b> such as `writeStream` is invoked.
# MAGIC 
# MAGIC Notice that nothing happens until you engage an action, i.e. a `display()` or `writeStream`.

# COMMAND ----------

from pyspark.sql.functions import col
bodyDF = eventStreamDF.select(col("body").cast("string"))
                              
display(bodyDF, streamName= "bodyDF")                              

# COMMAND ----------

# MAGIC %md
# MAGIC While we can see our JSON data now that it's cast to string type, we can't directly manipulate it.
# MAGIC 
# MAGIC Before proceeding, stop this stream. We'll continue building up transformations against this streaming DataFrame, and a new action will trigger an additional stream.

# COMMAND ----------

for stream in spark.streams.active:
  if stream.name == "bodyDF":
    stream.stop()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## <img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Parse the JSON payload
# MAGIC 
# MAGIC The EventHub acts as a sort of "firehose" (or asynchronous buffer) and displays raw data in the JSON format.
# MAGIC 
# MAGIC If desired, we could save this as raw bytes or strings and parse these records further downstream in our processing.
# MAGIC 
# MAGIC Here, we'll directly parse our data so we can interact with the fields.
# MAGIC 
# MAGIC The first step is to define the schema for the JSON payload.
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Both time fields are encoded as `LongType` here because of non-standard formatting.

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType

schema = StructType([
  StructField("Arrival_Time", LongType()),
  StructField("Creation_Time", LongType()),
  StructField("Device", StringType()),
  StructField("Index", LongType()),
  StructField("Model", StringType()),
  StructField("User", StringType()),
  StructField("gt", StringType()),
  StructField("x", DoubleType()),
  StructField("y", DoubleType()),
  StructField("z", DoubleType()),
  StructField("geolocation", StructType([
    StructField("PostalCode", StringType()),
    StructField("StateProvince", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType())])),
  StructField("id", StringType())])

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Parse the data
# MAGIC 
# MAGIC Next we can use the function `from_json` to parse out the full message with the schema specified above.
# MAGIC 
# MAGIC When parsing a value from JSON, we end up with a single column containing a complex object.

# COMMAND ----------

from pyspark.sql.functions import from_json

parsedEventsDF = bodyDF.select(
  from_json("body", schema).alias("json"))
parsedEventsDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we can further parse this to flatten the schema entirely and properly cast our time fields.

# COMMAND ----------

from pyspark.sql.functions import from_unixtime, col

flatSchemaDF = (parsedEventsDF
  .select(from_unixtime(col("json.Arrival_Time")/1000).alias("Arrival_Time").cast("timestamp"),
          (col("json.Creation_Time")/1E9).alias("Creation_Time").cast("timestamp"),
          col("json.Device").alias("Device"),
          col("json.Index").alias("Index"),
          col("json.Model").alias("Model"),
          col("json.User").alias("User"),
          col("json.gt").alias("gt"),
          col("json.x").alias("x"),
          col("json.y").alias("y"),
          col("json.z").alias("z"),
          col("json.id").alias("id"),
          col("json.geolocation.country").alias("country"),
          col("json.geolocation.city").alias("city"),
          col("json.geolocation.PostalCode").alias("PostalCode"),
          col("json.geolocation.StateProvince").alias("StateProvince"))
               )

# COMMAND ----------

# MAGIC %md
# MAGIC This flat schema provides us the ability to view each nested field as a column.

# COMMAND ----------

display(flatSchemaDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Stop all active streams

# COMMAND ----------

for stream in spark.streams.active:
  stream.stop()

# COMMAND ----------


        
    

# COMMAND ----------



# COMMAND ----------

# MAGIC 
# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
