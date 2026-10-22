# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # OSV incremental load — modified_id.csv since the watermark, upsert osv_raw
# MAGIC Streams `gs://osv-vulnerabilities/modified_id.csv` (newest first) and stops at
# MAGIC the stored watermark minus a small overlap. Changed records are fetched one by
# MAGIC one (`<ecosystem>/<id>.json`) and `MERGE`d into `osv_raw` gated by `modified`.
# MAGIC With more than `full_reload_threshold` changes it sets the task value
# MAGIC `needs_full_load=true` so the job runs `Full load` next. Needs one `Full load`
# MAGIC run first (initial-load job). Idempotent and safe to schedule.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "bucket_url": str, "part_records": int,
                         "full_reload_threshold": int, "watermark_overlap_hours": float,
                         "fetch_threads": int})
BUCKET_URL = p["bucket_url"]
MODIFIED_CSV_URL = f"{BUCKET_URL}/modified_id.csv"
INCREMENTAL_STAGING = volume_path(p, "osv") + "/incremental"
RAW_TABLE = table_name(p, "osv_raw")
FETCH_LOG_TABLE = table_name(p, "osv_fetch_log")
PART_RECORDS = p["part_records"]
FULL_RELOAD_THRESHOLD = p["full_reload_threshold"]
FETCH_THREADS = p["fetch_threads"]

import datetime
# Re-read changes this far before the stored watermark: the exporter writes
# continuously, so a record can show up in modified_id.csv a bit late. The MERGE is
# gated by `modified`, so re-reading is idempotent.
WATERMARK_OVERLAP = datetime.timedelta(hours=p["watermark_overlap_hours"])
dbutils.jobs.taskValues.set(key="needs_full_load", value="false")

# COMMAND ----------

# DBTITLE 1,Find records changed since the watermark
import io, urllib.request
from concurrent.futures import ThreadPoolExecutor
from osv.helpers import changed_since, record_url, to_single_line
from osv.transform import extract_raw_data
from common.staging import write_text_parts, clear_parts
from common.fetch import fetch_with_retries
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log, read_watermark
from delta.tables import DeltaTable

watermark = read_watermark(spark, FETCH_LOG_TABLE)
if watermark is None:
    raise RuntimeError(f"no watermark in {FETCH_LOG_TABLE}: run the OSV initial-load job first")
since = watermark - WATERMARK_OVERLAP
print(f"watermark={watermark}, reading changes since {since}")

# The CSV is ~130 MB but sorted newest first: read it as a stream and stop early.
with urllib.request.urlopen(MODIFIED_CSV_URL) as r:
    changes, newest, exceeded = changed_since(
        io.TextIOWrapper(r, encoding="utf-8"), since, FULL_RELOAD_THRESHOLD)
print(f"{len(changes)}{'+' if exceeded else ''} changed records, newest modified={newest}")

# COMMAND ----------

# DBTITLE 1,Too many changes -> full load; no changes -> done
if exceeded:
    print(f"more than {FULL_RELOAD_THRESHOLD} changes: the job runs Full load next")
    dbutils.jobs.taskValues.set(key="needs_full_load", value="true")
    dbutils.notebook.exit("full load")
if not changes:
    dbutils.notebook.exit("no changes")

# COMMAND ----------

# DBTITLE 1,Fetch changed records and write gzip JSON-line parts
def fetch(path: str) -> bytes | None:
    """One record as a JSON line; None if it is gone (deleted after the CSV was written)."""
    data = fetch_with_retries(record_url(BUCKET_URL, path))
    return None if data is None else to_single_line(data)


clear_parts(INCREMENTAL_STAGING)
with ThreadPoolExecutor(FETCH_THREADS) as pool:
    # Stream results into the parts instead of keeping up to 100K records in memory.
    records = (x for x in pool.map(fetch, changes.values()) if x is not None)
    n_records, n_parts = write_text_parts(records, INCREMENTAL_STAGING, PART_RECORDS)
print(f"wrote {n_records} records into {n_parts} parts, {len(changes) - n_records} not found")

# COMMAND ----------

# DBTITLE 1,Parse and merge into osv_raw (no deletes: withdrawn records stay, flagged)
n_unparsed = 0
if n_records:
    parsed = extract_raw_data(spark.read.text(f"{INCREMENTAL_STAGING}/part-*.jsonl.gz"))
    n_unparsed = parsed.filter("id IS NULL").count()
    if n_unparsed:
        print(f"WARNING: {n_unparsed} records did not parse and are skipped")
    src = dedupe_latest(parsed.filter("id IS NOT NULL"), "id", "date_modified")
    (DeltaTable.forName(spark, RAW_TABLE).alias("t")
     .merge(src.alias("s"), "t.id = s.id")
     .whenMatchedUpdateAll(condition="t.date_modified IS NULL OR s.date_modified > t.date_modified")
     .whenNotMatchedInsertAll()
     .execute())

# COMMAND ----------

# DBTITLE 1,Fetch-log row
# The CSV can lag behind the all.zip snapshot, so never move the watermark back.
new_watermark = max(watermark, newest) if newest else watermark
n_after = spark.table(RAW_TABLE).count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), "incremental", None, new_watermark, n_records, n_unparsed, n_after),
    "fetch_time timestamp, mode string, source_generation string, watermark timestamp, "
    "n_records long, n_unparsed long, n_rows_after long")
print(f"osv_raw rows: {n_after}, watermark -> {new_watermark}")
