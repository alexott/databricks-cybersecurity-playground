# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # OSV full load — pinned all.zip snapshot, upsert osv_raw
# MAGIC Downloads one pinned version (GCS `generation`) of
# MAGIC `gs://osv-vulnerabilities/all.zip` (~2.6 GB, ~2M records, one file per id,
# MAGIC all ecosystems) to the scratch Volume, writes every record as one JSON line into
# MAGIC gzip parts, then `MERGE`s into `osv_raw` on `id` gated by `modified`. Ids that
# MAGIC are no longer in the snapshot are deleted. Used for the initial load, and as the
# MAGIC fallback when `Incremental load` sees too many changes.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "bucket_url": str, "part_records": int,
                         "min_snapshot_ratio": float})
ALL_ZIP_URL = f"{p['bucket_url']}/all.zip"
VOLUME_PATH = volume_path(p, "osv")
FULL_STAGING = f"{VOLUME_PATH}/full"
RAW_TABLE = table_name(p, "osv_raw")
FETCH_LOG_TABLE = table_name(p, "osv_fetch_log")
PART_RECORDS = p["part_records"]
MIN_SNAPSHOT_RATIO = p["min_snapshot_ratio"]

# COMMAND ----------

# DBTITLE 1,Resolve and download the pinned all.zip
import shutil, zipfile, urllib.request, datetime
from email.utils import parsedate_to_datetime
from osv.helpers import pinned_url, iter_zip_records
from osv.transform import extract_raw_data
from common.staging import write_text_parts, clear_parts
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log
from delta.tables import DeltaTable

# all.zip is rebuilt every ~15 minutes: pin one generation so the whole download
# comes from the same object.
with urllib.request.urlopen(urllib.request.Request(ALL_ZIP_URL, method="HEAD")) as r:
    generation = r.headers["x-goog-generation"]
    size = int(r.headers["Content-Length"])
    # Everything modified before the snapshot was built is in it: this is the
    # watermark for the next incremental run.
    snapshot_time = parsedate_to_datetime(r.headers["Last-Modified"])
print(f"all.zip generation={generation} size={size} built={snapshot_time}")

os.makedirs(VOLUME_PATH, exist_ok=True)
zip_path = f"{VOLUME_PATH}/all.zip"
with urllib.request.urlopen(pinned_url(ALL_ZIP_URL, generation)) as r, open(zip_path, "wb") as out:
    shutil.copyfileobj(r, out, length=16 * 1024 * 1024)
assert os.path.getsize(zip_path) == size, "incomplete all.zip download"
print("downloaded")

# COMMAND ----------

# DBTITLE 1,Write records as gzip JSON-line parts on the Volume
clear_parts(FULL_STAGING)
with zipfile.ZipFile(zip_path) as zf:
    n_records, n_parts = write_text_parts(iter_zip_records(zf), FULL_STAGING, PART_RECORDS)
os.remove(zip_path)
print(f"wrote {n_records} records into {n_parts} parts")

# COMMAND ----------

# DBTITLE 1,Read parts, parse raw VARIANT, extract id columns
parsed = extract_raw_data(spark.read.text(f"{FULL_STAGING}/part-*.jsonl.gz"))
# One pass for both counts (no .cache() on serverless).
n_unparsed, n_src = parsed.selectExpr("count_if(id IS NULL)", "count(DISTINCT id)").first()
if n_unparsed:
    print(f"WARNING: {n_unparsed} records did not parse and are skipped")
# One row per id (keep the newest) so the MERGE never sees several source rows for
# one target. all.zip has one file per id, so this is a defensive guard.
src = dedupe_latest(parsed.filter("id IS NOT NULL"), "id", "date_modified")

# COMMAND ----------

# DBTITLE 1,Create osv_raw if needed (CDF on) and merge the snapshot
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    raw VARIANT, id STRING, cve_id STRING,
    date_published TIMESTAMP, date_modified TIMESTAMP, date_withdrawn TIMESTAMP
  ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
""")
n_before = spark.table(RAW_TABLE).count()
if n_before and n_src < MIN_SNAPSHOT_RATIO * n_before:
    raise RuntimeError(f"snapshot has {n_src} ids but {RAW_TABLE} has {n_before}: "
                       "refusing to merge (and delete missing ids) from a partial snapshot")

(DeltaTable.forName(spark, RAW_TABLE).alias("t")
 .merge(src.alias("s"), "t.id = s.id")
 .whenMatchedUpdateAll(condition="t.date_modified IS NULL OR s.date_modified > t.date_modified")
 .whenNotMatchedInsertAll()
 .whenNotMatchedBySourceDelete()
 .execute())

# COMMAND ----------

# DBTITLE 1,Fetch-log row
n_after = spark.table(RAW_TABLE).count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), "full", generation, snapshot_time, n_records, n_unparsed, n_after),
    "fetch_time timestamp, mode string, source_generation string, watermark timestamp, "
    "n_records long, n_unparsed long, n_rows_after long")
print(f"osv_raw rows: {n_before} -> {n_after}")
