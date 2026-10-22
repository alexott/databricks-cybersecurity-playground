# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # CVE v5 incremental load — deltaLog.json since the watermark, upsert cves_v5_raw
# MAGIC Reads `cves/deltaLog.json`, takes the CVEs new/updated after the stored
# MAGIC watermark (minus a small overlap), downloads each record and `MERGE`s it (gated by `date_updated`).
# MAGIC Sets the task value `needs_full_load=true` instead when there is no watermark
# MAGIC yet, when deltaLog no longer reaches back to the watermark, or when more than
# MAGIC `full_reload_threshold` CVEs changed; the job then runs `Full load`.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "delta_log_url": str, "full_reload_threshold": int,
                         "fetch_threads": int, "part_records": int,
                         "watermark_overlap_minutes": float})
INCREMENTAL_STAGING = volume_path(p, "cve_v5") + "/incremental"
RAW_TABLE = table_name(p, "cves_v5_raw")
FETCH_LOG_TABLE = table_name(p, "cves_v5_fetch_log")
dbutils.jobs.taskValues.set(key="needs_full_load", value="false")

# COMMAND ----------

# DBTITLE 1,Plan: changed CVEs since the watermark
import datetime, json, urllib.request
from concurrent.futures import ThreadPoolExecutor
from cve_v5.helpers import plan_incremental, raw_github_url, extract_raw_data
from common.staging import to_single_line, write_text_parts, clear_parts
from common.fetch import fetch_with_retries
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log, read_watermark
from delta.tables import DeltaTable

watermark = read_watermark(spark, FETCH_LOG_TABLE)
with urllib.request.urlopen(p["delta_log_url"]) as r:
    delta_log = json.load(r)
# Records come from a CDN-cached URL: re-read recent entries in case one was stale.
overlap = datetime.timedelta(minutes=p["watermark_overlap_minutes"])
changes, newest, needs_full = plan_incremental(delta_log, watermark, p["full_reload_threshold"], overlap)
print(f"watermark={watermark}: {len(changes)} changed CVEs, newest fetchTime={newest}, full={needs_full}")

if needs_full:
    dbutils.jobs.taskValues.set(key="needs_full_load", value="true")
    dbutils.notebook.exit("full load")
if not changes:
    dbutils.notebook.exit("no changes")

# COMMAND ----------

# DBTITLE 1,Fetch changed records and write gzip JSON-line parts
def fetch(link: str) -> bytes | None:
    data = fetch_with_retries(raw_github_url(link))
    return None if data is None else to_single_line(data)


clear_parts(INCREMENTAL_STAGING)
with ThreadPoolExecutor(p["fetch_threads"]) as pool:
    records = (x for x in pool.map(fetch, changes.values()) if x is not None)
    n_records, n_parts = write_text_parts(records, INCREMENTAL_STAGING, p["part_records"])
print(f"wrote {n_records} records into {n_parts} parts, {len(changes) - n_records} not found")

# COMMAND ----------

# DBTITLE 1,Parse and merge into cves_v5_raw
n_unparsed = 0
if n_records:
    parsed = extract_raw_data(spark.read.text(f"{INCREMENTAL_STAGING}/part-*.jsonl.gz")
                              .selectExpr("try_parse_json(value) as raw"))
    n_unparsed = parsed.filter("cve_id IS NULL").count()
    if n_unparsed:
        print(f"WARNING: {n_unparsed} records did not parse and are skipped")
    src = dedupe_latest(parsed.filter("cve_id IS NOT NULL"), "cve_id", "date_updated")
    (DeltaTable.forName(spark, RAW_TABLE).alias("t")
     .merge(src.alias("s"), "t.cve_id = s.cve_id")
     .whenMatchedUpdateAll(condition="t.date_updated IS NULL OR s.date_updated > t.date_updated")
     .whenNotMatchedInsertAll()
     .execute())

# COMMAND ----------

# DBTITLE 1,Fetch-log row
new_watermark = max(watermark, newest)   # never move the watermark back
n_after = spark.table(RAW_TABLE).count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), "incremental", new_watermark, n_records, n_unparsed, n_after),
    "fetch_time timestamp, mode string, watermark timestamp, n_records long, n_unparsed long, n_rows_after long")
print(f"cves_v5_raw rows: {n_after}, watermark -> {new_watermark}")
