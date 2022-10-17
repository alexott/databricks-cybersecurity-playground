# Databricks notebook source
import os
from typing import List, Dict, Any, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

# Variables...
base_dir = "/mnt/cyberdata"
secret_scope = "..."
evhub_secret_key = "..."
evhub_ns_name = "..."
evhub_topic_name = "iocs"

# COMMAND ----------

# MAGIC %run "./IoCs Common"

# COMMAND ----------

num_executors = sc._jsc.sc().getExecutorMemoryStatus().size()-1
num_cores = sum(sc.parallelize((("")*num_executors), num_executors).mapPartitions(lambda p: [os.cpu_count()]).collect())

# COMMAND ----------

spark.sql(f"set spark.sql.shuffle.partitions = {num_cores}")

# COMMAND ----------

import datetime

readConnectionString = dbutils.secrets.get(secret_scope, evhub_secret_key)
eh_sasl = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{readConnectionString}";'
kafka_options = {"kafka.bootstrap.servers": f"{evhub_ns_name}.servicebus.windows.net:9093",
                 "kafka.sasl.mechanism": "PLAIN",
                 "kafka.security.protocol": "SASL_SSL",
                 "kafka.request.timeout.ms": "60000",
                 "kafka.session.timeout.ms": "30000",
                 "startingOffsets": "earliest",
                 "minPartitions": num_cores,
                 "kafka.sasl.jaas.config": eh_sasl,
                 "subscribe": evhub_topic_name,
                }

df = spark.readStream\
  .format("kafka")\
  .options(**kafka_options)\
  .load()\
  .withColumn("value", F.col("value").cast("string"))

# COMMAND ----------

partial_schema = "`@timestamp` timestamp, fileset struct<name:string>, service struct<type:string>, message string"
df2 = df.select("*", F.from_json("value", partial_schema).alias("jsn")) \
  .withColumnRenamed("timestamp", "kafka_ts") \
  .selectExpr("*", "jsn.`@timestamp` as timestamp", "jsn.fileset.name as dataset", 
              "jsn.service.type as service", "sha2(jsn.message, 256) as msg_hash") \
  .drop("jsn", "timestampType") \
  .withColumn("date", F.col("timestamp").cast("date"))
#display(df2)

# COMMAND ----------

def perform_foreach_batch(df: DataFrame, epoch):
  return drop_duplicates_with_merge(df, primary_key_columns=["msg_hash"], 
                                    path=f"{base_dir}/bronze/threatintel/",
                                    partitionby=["date"], opts={"mergeSchema": "true"},
                                    additional_merge_cond="update.date >= current_date()-10"
                                   )

# COMMAND ----------

checkpoint = f"{base_dir}/checkpoints/threatintel-bronze/"

# COMMAND ----------

df2.writeStream \
  .option("checkpointLocation", checkpoint) \
  .trigger(availableNow=True) \
  .foreachBatch(perform_foreach_batch) \
  .start()
