# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ATT&CK loader — MITRE ATT&CK STIX bundles, upsert attack_raw
# MAGIC Looks up the requested ATT&CK version of each domain (`domains`, default
# MAGIC Enterprise, Mobile and ICS) in attack-stix-data's `index.json`; if the fetch log
# MAGIC already has exactly these versions, stops. Otherwise downloads the STIX bundles
# MAGIC (~55 + 6 + 5 MB) to memory, keeps one object per STIX id (groups, software and
# MAGIC data sources are shared by domains), stages one JSON line per object on a scratch
# MAGIC Volume, then does a full-snapshot MERGE into `attack_raw` on `stix_id`: update
# MAGIC when the STIX `modified` advanced, insert new objects, and soft-delete objects
# MAGIC absent from the bundles (`date_removed`). Revoked and deprecated objects stay in
# MAGIC the bundles with their flags, so removals are rare.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "index_url": str, "domains": str, "attack_version": str})
DOMAINS = [d.strip() for d in p["domains"].split(",") if d.strip()]
VOLUME_PATH = volume_path(p, "attack")
RAW_TABLE = table_name(p, "attack_raw")
FETCH_LOG_TABLE = table_name(p, "attack_fetch_log")

# COMMAND ----------

# DBTITLE 1,Resolve the versions; stop if already loaded
import datetime, json
from attack.helpers import bundle_objects, extract_raw_data, resolve_versions, versions_label
from common.fetch import fetch_with_retries
from common.fetch_log import append_fetch_log
from common.merge import dedupe_latest
from common.staging import clear_parts, write_text_parts
from delta.tables import DeltaTable
from pyspark.sql import functions as F


def download(url: str) -> bytes:
    data = fetch_with_retries(url, timeout=120, strip=False)
    if data is None:
        raise RuntimeError(f"not found: {url}")
    return data


versions = resolve_versions(json.loads(download(p["index_url"])), DOMAINS, p["attack_version"])
label = versions_label(versions)
print(f"ATT&CK {label}")
if spark.catalog.tableExists(FETCH_LOG_TABLE) and spark.catalog.tableExists(RAW_TABLE):
    last = spark.sql(f"SELECT versions FROM {FETCH_LOG_TABLE} ORDER BY fetch_time DESC LIMIT 1").first()
    if last is not None and last["versions"] == label:
        dbutils.notebook.exit(f"up to date ({label})")

# COMMAND ----------

# DBTITLE 1,Download the bundles and stage one JSON line per object
bundles = {d: (v, json.loads(download(url))) for d, (v, url) in versions.items()}
objects = bundle_objects(bundles)
del bundles
print(f"{len(objects)} STIX objects")

clear_parts(VOLUME_PATH)
n_records, n_parts = write_text_parts(
    (json.dumps({"raw": json.dumps(o), "attack_version": v}).encode("utf-8") for v, o in objects),
    VOLUME_PATH, 20000)
print(f"staged {n_records} records in {n_parts} parts")

# COMMAND ----------

# DBTITLE 1,Read staged JSONL, parse raw VARIANT
staged = spark.read.json(f"{VOLUME_PATH}/part-*.jsonl.gz")   # columns: attack_version, raw (string)
src = extract_raw_data(staged.selectExpr("try_parse_json(raw) as raw", "attack_version")).filter("stix_id IS NOT NULL")
# STIX ids are unique after bundle_objects; the dedupe guards the MERGE.
src = (dedupe_latest(src, "stix_id", "modified")
       .withColumn("date_removed", F.lit(None).cast("timestamp")))

# COMMAND ----------

# DBTITLE 1,Create attack_raw if needed (CDF on) and upsert on stix_id with soft-delete
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    raw VARIANT, stix_id STRING, type STRING, attack_id STRING, modified TIMESTAMP,
    attack_version STRING, date_removed TIMESTAMP
  ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
""")
(DeltaTable.forName(spark, RAW_TABLE).alias("t")
 .merge(src.alias("s"), "t.stix_id = s.stix_id")
 # Refresh when the object changed or a removed object reappears; an object in the
 # bundles is, by definition, listed.
 .whenMatchedUpdateAll(
     condition="s.modified > t.modified OR t.modified IS NULL OR t.date_removed IS NOT NULL")
 .whenNotMatchedInsertAll()
 # Object no longer in the bundles: stamp the removal once.
 .whenNotMatchedBySourceUpdate(
     condition="t.date_removed IS NULL",
     set={"date_removed": F.current_timestamp()})
 .execute())

# COMMAND ----------

# DBTITLE 1,Fetch-log row
n = spark.table(RAW_TABLE).count()
n_active = spark.table(RAW_TABLE).filter("date_removed IS NULL").count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), ",".join(DOMAINS), label, len(objects), n, n_active),
    "fetch_time timestamp, domains string, versions string, "
    "n_objects int, n_rows_after long, n_active_after long")
print(f"attack_raw rows: {n} ({n_active} active, {n - n_active} removed)")
