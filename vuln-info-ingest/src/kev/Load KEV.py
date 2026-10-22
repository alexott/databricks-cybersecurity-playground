# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # KEV loader — fetch the CISA Known Exploited Vulnerabilities catalog, upsert kev_raw
# MAGIC Downloads the single ~1.8 MB catalog JSON to ephemeral /tmp, stages one JSON
# MAGIC line per vulnerability on a scratch Volume, then does a full-snapshot MERGE
# MAGIC into `kev_raw` on `cve_id`. KEV carries no per-record modified timestamp, so
# MAGIC the catalog version is the freshness token: matched rows refresh only when the
# MAGIC catalog version advanced (or a previously removed cveID reappears). CISA
# MAGIC removals are rare data-quality corrections (a CVE added in error or later
# MAGIC REJECTED), so we soft-delete — cveIDs absent from the snapshot keep their row
# MAGIC with `date_removed` set rather than being physically deleted.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "catalog_url": str})
CATALOG_URL = p["catalog_url"]
VOLUME_PATH = volume_path(p, "kev")
RAW_TABLE = table_name(p, "kev_raw")
FETCH_LOG_TABLE = table_name(p, "kev_fetch_log")

# COMMAND ----------

# DBTITLE 1,Fetch the catalog JSON to /tmp
import glob, json, urllib.request, datetime
from kev.helpers import extract_raw_data
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log
from delta.tables import DeltaTable
from pyspark.sql import functions as F

local = "/tmp/kev.json"
urllib.request.urlretrieve(CATALOG_URL, local)
with open(local, "r", encoding="utf-8") as f:
    catalog = json.load(f)
catalog_version = catalog["catalogVersion"]
date_released = catalog["dateReleased"]
vulns = catalog["vulnerabilities"]
print(f"catalog {catalog_version} released {date_released}: {len(vulns)} vulnerabilities")

# COMMAND ----------

# DBTITLE 1,Stage one JSON line per vulnerability on the Volume
# Remove only our own staging parts — never wipe the whole Volume, which may hold
# other state (e.g. a streaming checkpoint) we must not destroy.
os.makedirs(VOLUME_PATH, exist_ok=True)
for _f in glob.glob(f"{VOLUME_PATH}/part-*.jsonl"):
    os.remove(_f)
with open(f"{VOLUME_PATH}/part-00000.jsonl", "w", encoding="utf-8") as out:
    out.write("\n".join(json.dumps({"raw": json.dumps(v)}) for v in vulns))
print(f"staged {len(vulns)} records")

# COMMAND ----------

# DBTITLE 1,Read staged JSONL, parse raw VARIANT, stamp catalog columns
staged = spark.read.json(VOLUME_PATH)   # column: raw(string)
src = (staged.selectExpr("try_parse_json(raw) as raw")
       .withColumn("catalog_version", F.lit(catalog_version))
       .withColumn("date_released", F.lit(date_released).cast("timestamp")))
src = extract_raw_data(src).filter("cve_id IS NOT NULL")
# One row per cve_id so the MERGE never sees multiple source rows for one target
# (Delta errors on that). KEV ids are unique per catalog, so this is a guard.
src = (dedupe_latest(src, "cve_id", "date_added")
       .withColumn("date_removed", F.lit(None).cast("timestamp")))

# COMMAND ----------

# DBTITLE 1,Create kev_raw if needed (CDF on) and upsert on cve_id with soft-delete
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    raw VARIANT, cve_id STRING, date_added DATE,
    catalog_version STRING, date_released TIMESTAMP, date_removed TIMESTAMP
  ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
""")
(DeltaTable.forName(spark, RAW_TABLE).alias("t")
 .merge(src.alias("s"), "t.cve_id = s.cve_id")
 # Refresh only when the catalog advanced or a removed cveID reappears; clears
 # date_removed because a cveID present in the snapshot is, by definition, listed.
 .whenMatchedUpdateAll(
     condition="s.catalog_version <> t.catalog_version OR t.date_removed IS NOT NULL")
 .whenNotMatchedInsertAll()
 # cveID no longer in the catalog: stamp the removal once (keep the first version).
 .whenNotMatchedBySourceUpdate(
     condition="t.date_removed IS NULL",
     set={"date_removed": F.lit(date_released).cast("timestamp")})
 .execute())

# COMMAND ----------

# DBTITLE 1,Fetch-log row
n = spark.table(RAW_TABLE).count()
n_active = spark.table(RAW_TABLE).filter("date_removed IS NULL").count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), catalog_version,
     datetime.datetime.fromisoformat(date_released.replace("Z", "+00:00")),
     len(vulns), n, n_active),
    "fetch_time timestamp, catalog_version string, date_released timestamp, "
    "n_vulnerabilities int, n_rows_after int, n_active_after int")
print(f"kev_raw rows: {n} ({n_active} active, {n - n_active} removed)")
