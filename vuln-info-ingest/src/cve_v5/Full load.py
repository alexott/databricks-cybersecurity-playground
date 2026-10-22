# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # CVE v5 full load — cvelistV5 tarball snapshot, upsert cves_v5_raw
# MAGIC Streams the `CVEProject/cvelistV5` main-branch tarball (no git clone), writes
# MAGIC every CVE record as one JSON line into gzip parts on the scratch Volume, then
# MAGIC `MERGE`s into `cves_v5_raw` on `cve_id`, updating rows only when `date_updated`
# MAGIC advanced. Nothing is deleted: rejected CVEs stay with `state = 'REJECTED'`.
# MAGIC Used for the initial load and as the fallback of `Incremental load`.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "tarball_url": str, "delta_log_url": str,
                         "part_records": int})
FULL_STAGING = volume_path(p, "cve_v5") + "/full"
RAW_TABLE = table_name(p, "cves_v5_raw")
FETCH_LOG_TABLE = table_name(p, "cves_v5_fetch_log")

# COMMAND ----------

# DBTITLE 1,Watermark, then stream the tarball into gzip JSON-line parts
import datetime, json, urllib.request
from cve_v5.helpers import iter_tar_records, newest_fetch_time, extract_raw_data
from common.staging import write_text_parts, clear_parts
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log
from delta.tables import DeltaTable

# Taken before the download: everything in deltaLog up to here is in the tarball,
# later changes are re-fetched by the next incremental run (the MERGE is idempotent).
started = datetime.datetime.now(datetime.UTC)
with urllib.request.urlopen(p["delta_log_url"]) as r:
    watermark = newest_fetch_time(json.load(r)) or started
print(f"watermark={watermark}")

clear_parts(FULL_STAGING)
with urllib.request.urlopen(p["tarball_url"]) as r:
    n_records, n_parts = write_text_parts(iter_tar_records(r), FULL_STAGING, p["part_records"])
print(f"wrote {n_records} records into {n_parts} parts")

# COMMAND ----------

# DBTITLE 1,Parse, dedupe, create cves_v5_raw if needed (CDF on) and merge
parsed = extract_raw_data(spark.read.text(f"{FULL_STAGING}/part-*.jsonl.gz")
                          .selectExpr("try_parse_json(value) as raw"))
n_unparsed = parsed.filter("cve_id IS NULL").count()
if n_unparsed:
    print(f"WARNING: {n_unparsed} records did not parse and are skipped")
src = dedupe_latest(parsed.filter("cve_id IS NOT NULL"), "cve_id", "date_updated")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    raw VARIANT, data_type STRING, data_version STRING, cve_id STRING, cve_year INT,
    date_published TIMESTAMP, date_updated TIMESTAMP
  ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
""")
(DeltaTable.forName(spark, RAW_TABLE).alias("t")
 .merge(src.alias("s"), "t.cve_id = s.cve_id")
 .whenMatchedUpdateAll(condition="t.date_updated IS NULL OR s.date_updated > t.date_updated")
 .whenNotMatchedInsertAll()
 .execute())

# COMMAND ----------

# DBTITLE 1,Fetch-log row
n_after = spark.table(RAW_TABLE).count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), "full", watermark, n_records, n_unparsed, n_after),
    "fetch_time timestamp, mode string, watermark timestamp, n_records long, n_unparsed long, n_rows_after long")
print(f"cves_v5_raw rows: {n_after}")
