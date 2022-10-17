# Databricks notebook source
import os
from typing import List, Dict, Any, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

# COMMAND ----------

# Variables...
base_dir = "/mnt/cyberdata"
iocs_table_name = "cyber.iocs"

# COMMAND ----------

# MAGIC %run "./IoCs Common"

# COMMAND ----------

df = spark.readStream\
  .format("delta")\
  .option("ignoreChanges", "true") \
  .load(f"{base_dir}/bronze/threatintel/")

# COMMAND ----------

# MAGIC %md ## TODOs
# MAGIC 
# MAGIC * \[X\] Extract `first_seen` timestamp either from data, or based on the first timestamp from the threat intel feed
# MAGIC * \[ \] Think how to handle `last_seen` over the time... For example, set last_seen for IPs to "first_seen + N days" ?

# COMMAND ----------

# DBTITLE 1,Do the base decode that will be used for all pipelines
message_df = df.select("dataset", "timestamp", "msg_hash", F.from_json("value", "message string").alias("json")) \
        .select("*", "json.message").drop("json")

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# TODO: instead of dropping duplicates, also add condition on the first/last seen
def process_single_source(input_df: DataFrame, service: str, transform_func):
  checkpoint = f"{base_dir}/checkpoints/threatintel-silver-{service}/"
  transform_func(input_df).writeStream.format("delta") \
    .option("checkpointLocation", checkpoint) \
    .trigger(availableNow=True) \
    .foreachBatch(lambda df, epoch: drop_duplicates_with_merge(df, primary_key_columns=["ioc_type", "ioc"], 
                                    path=f"{base_dir}/silver/threatintel/", table_name=iocs_table_name,
                                    partitionby=["ioc_type"], opts={"mergeSchema": "true"})) \
    .start().awaitTermination()

# COMMAND ----------

# MAGIC %md ## Handle OTX (OpenThread Exchange) feed

# COMMAND ----------

def handle_otx(df: DataFrame) -> DataFrame:
  otx_schema = 'STRUCT<content: STRING, description: STRING, id: BIGINT, indicator: STRING, title: STRING, type: STRING>'
  otx_df = df.filter("dataset = 'otx'").select("*", F.from_json("message", otx_schema).alias("jsn")).select("*", "jsn.*").drop("jsn", "message")
  otx_df = otx_df.withColumnRenamed("type", "ioc_type") \
    .withColumnRenamed("indicator", "ioc") \
    .withColumn("ioc_id", F.col("id").cast("string")) \
    .withColumnRenamed("description", "ioc_description") \
    .withColumnRenamed("content", "ioc_content") \
    .withColumnRenamed("title", "ioc_title") \
    .withColumn("first_seen", F.col("timestamp"))
  return otx_df

# COMMAND ----------

process_single_source(message_df, "otx", handle_otx)

# COMMAND ----------

# MAGIC %md ## Handle AbuseURL feed

# COMMAND ----------

def handle_abuseurl(df: DataFrame) -> DataFrame:
  abuseurl_schema = 'STRUCT<blacklists: STRUCT<spamhaus_dbl: STRING, surbl: STRING>, date_added: STRING, host: STRING, id: STRING, larted: STRING, reporter: STRING, tags: ARRAY<STRING>, threat: STRING, url: STRING, url_status: STRING, urlhaus_reference: STRING>'
  abuseurl_df = df.filter("dataset = 'abuseurl'").select("*", F.from_json("message", abuseurl_schema).alias("abuseurl")) \
    .selectExpr("*", "abuseurl.url as ioc", "abuseurl.id as ioc_id", "'URL' as ioc_type",
               "cast(abuseurl.date_added as timestamp) as first_seen").drop("message")
  return abuseurl_df

# COMMAND ----------

process_single_source(message_df, "abuseurl", handle_abuseurl)

# COMMAND ----------

# MAGIC %md ## Handle Malware Bazaar feed

# COMMAND ----------

def handle_malwarebazaar(df: DataFrame) -> DataFrame:
  malwarebazaar_schema = "STRUCT<anonymous: BIGINT, code_sign: ARRAY<STRING>, dhash_icon: STRING, file_name: STRING, file_size: BIGINT, file_type: STRING, file_type_mime: STRING, first_seen: STRING, imphash: STRING, intelligence: STRUCT<clamav: STRING, downloads: STRING, mail: STRING, uploads: STRING>, last_seen: STRING, md5_hash: STRING, origin_country: STRING, reporter: STRING, sha1_hash: STRING, sha256_hash: STRING, sha3_384_hash: STRING, signature: STRING, ssdeep: STRING, tags: ARRAY<STRING>, telfhash: STRING, tlsh: STRING>"
  
  mb_df = df.filter("dataset = 'malwarebazaar'").select("*", F.from_json("message", malwarebazaar_schema).alias("malwarebazaar")) \
            .select("malwarebazaar.file_name", "malwarebazaar.file_size", "malwarebazaar.file_type", "malwarebazaar.file_type_mime",  
                 F.expr("cast(malwarebazaar.first_seen as timestamp) as first_seen"),
                 F.expr("cast(malwarebazaar.last_seen as timestamp) as last_seen"),
                 F.posexplode(F.create_map(F.lit('FileHash-MD5'), "malwarebazaar.md5_hash", 
                                   F.lit('FileHash-ImpHash'), "malwarebazaar.imphash", 
                                   F.lit('FileHash-SHA1'), "malwarebazaar.sha1_hash", 
                                   F.lit('FileHash-SHA256'), "malwarebazaar.sha256_hash", 
                                   F.lit('FileHash-SHA384'), "malwarebazaar.sha3_384_hash", 
                                   F.lit('FileHash-SSDEEP'), "malwarebazaar.ssdeep",
                                   F.lit('FileHash-TElfHash'), "malwarebazaar.telfhash", 
                                   F.lit('FileHash-TLSH'), "malwarebazaar.tlsh").alias("_map_")),
                "*").drop("pos", "message").withColumnRenamed("key", "ioc_type").withColumnRenamed("value", "ioc").filter("ioc is not null")
  return mb_df

# COMMAND ----------

process_single_source(message_df, "malwarebazaar", handle_malwarebazaar)

# COMMAND ----------

# MAGIC %md ## Handle Abuse Malware feed

# COMMAND ----------

def handle_abusemalware(df: DataFrame) -> DataFrame:
  abusemalware_schema = "STRUCT<file_size: STRING, file_type: STRING, firstseen: STRING, imphash: STRING, md5_hash: STRING, sha256_hash: STRING, signature: STRING, ssdeep: STRING, tlsh: STRING, urlhaus_download: STRING, virustotal: STRING>"
  am_df = df.filter("dataset = 'abusemalware'").select("*", F.from_json("message", abusemalware_schema).alias("abusemalware")) \
            .select(F.expr("cast(abusemalware.file_size as long) as file_size"), "abusemalware.file_type", 
                 F.expr("cast(abusemalware.firstseen as timestamp) as first_seen"),
                 F.posexplode(F.create_map(F.lit('FileHash-MD5'), "abusemalware.md5_hash", 
                                   F.lit('FileHash-ImpHash'), "abusemalware.imphash", 
                                   F.lit('FileHash-SHA256'), "abusemalware.sha256_hash", 
                                   F.lit('FileHash-SSDEEP'), "abusemalware.ssdeep",
                                   F.lit('FileHash-TLSH'), "abusemalware.tlsh").alias("_map_")),
                "*").drop("pos", "message").withColumnRenamed("key", "ioc_type").withColumnRenamed("value", "ioc").filter("ioc is not null")
  return am_df

# COMMAND ----------

process_single_source(message_df, "abusemalware", handle_abusemalware)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- do only after the table is created
# MAGIC -- CREATE BLOOMFILTER INDEX ON TABLE cyber.iocs FOR COLUMNS( ioc )
