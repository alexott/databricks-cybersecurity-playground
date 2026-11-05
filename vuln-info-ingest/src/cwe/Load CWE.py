# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # CWE loader — fetch the MITRE CWE catalog, upsert cwe_raw
# MAGIC Downloads the zipped catalog XML (~2 MB, ~19 MB unzipped) to memory, converts
# MAGIC every Weakness, Grouping and View entry to JSON (`cwe.helpers`), stages one JSON
# MAGIC line per entry on a scratch Volume, then does a full-snapshot MERGE into
# MAGIC `cwe_raw` on `cwe_id`. Entries carry no reliable per-record modified time (a
# MAGIC schema change rewrites entries without a Content_History modification), so the
# MAGIC catalog version is the freshness token: matched rows refresh only when the
# MAGIC catalog version changed (or a removed id reappears). CWE never drops ids —
# MAGIC obsolete entries stay with Status 'Deprecated' — so absent ids are soft-deleted
# MAGIC with `date_removed` rather than physically deleted.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "catalog_url": str})
CATALOG_URL = p["catalog_url"]
VOLUME_PATH = volume_path(p, "cwe")
RAW_TABLE = table_name(p, "cwe_raw")
FETCH_LOG_TABLE = table_name(p, "cwe_fetch_log")

# COMMAND ----------

# DBTITLE 1,Fetch and parse the catalog
import datetime, glob, io, json, zipfile
from cwe.helpers import extract_raw_data, parse_catalog
from common.fetch import fetch_with_retries
from common.fetch_log import append_fetch_log
from delta.tables import DeltaTable
from pyspark.sql import functions as F

data = fetch_with_retries(CATALOG_URL, strip=False)
if data is None:
    raise RuntimeError(f"catalog not found: {CATALOG_URL}")
with zipfile.ZipFile(io.BytesIO(data)) as z:
    xml_names = [n for n in z.namelist() if n.endswith(".xml")]
    if len(xml_names) != 1:
        raise RuntimeError(f"expected one XML file in the catalog zip, got {xml_names}")
    catalog_version, catalog_date, entries = parse_catalog(z.read(xml_names[0]))
print(f"CWE {catalog_version} ({catalog_date}): {len(entries)} entries")

# COMMAND ----------

# DBTITLE 1,Stage one JSON line per entry on the Volume
# Remove only our own staging parts — never wipe the whole Volume, which may hold
# other state (e.g. a streaming checkpoint) we must not destroy.
os.makedirs(VOLUME_PATH, exist_ok=True)
for _f in glob.glob(f"{VOLUME_PATH}/part-*.jsonl"):
    os.remove(_f)
with open(f"{VOLUME_PATH}/part-00000.jsonl", "w", encoding="utf-8") as out:
    out.write("\n".join(json.dumps({"raw": json.dumps(e), "entry_type": t}) for t, e in entries))
print(f"staged {len(entries)} records")

# COMMAND ----------

# DBTITLE 1,Read staged JSONL, parse raw VARIANT, stamp catalog columns
staged = spark.read.json(VOLUME_PATH)   # columns: entry_type, raw (string)
src = (staged.selectExpr("try_parse_json(raw) as raw", "entry_type")
       .withColumn("catalog_version", F.lit(catalog_version))
       .withColumn("catalog_date", F.lit(catalog_date).cast("date")))
src = extract_raw_data(src).filter("cwe_id IS NOT NULL")
# CWE ids are unique across Weaknesses, Groupings and Views; fail rather than let
# the MERGE pick one of several rows.
n_dupes = src.groupBy("cwe_id").count().filter("count > 1").count()
if n_dupes:
    raise RuntimeError(f"{n_dupes} CWE ids occur more than once in the catalog")
src = src.withColumn("date_removed", F.lit(None).cast("timestamp"))

# COMMAND ----------

# DBTITLE 1,Create cwe_raw if needed (CDF on) and upsert on cwe_id with soft-delete
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    raw VARIANT, cwe_id STRING, entry_type STRING,
    catalog_version STRING, catalog_date DATE, date_removed TIMESTAMP
  ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
""")
(DeltaTable.forName(spark, RAW_TABLE).alias("t")
 .merge(src.alias("s"), "t.cwe_id = s.cwe_id")
 # Refresh only when the catalog version changed or a removed id reappears; clears
 # date_removed because an id present in the snapshot is, by definition, listed.
 .whenMatchedUpdateAll(
     condition="s.catalog_version <> t.catalog_version OR t.date_removed IS NOT NULL")
 .whenNotMatchedInsertAll()
 # id no longer in the catalog: stamp the removal once (keep the first time).
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
    (datetime.datetime.now(datetime.UTC), catalog_version, datetime.date.fromisoformat(catalog_date),
     len(entries), n, n_active),
    "fetch_time timestamp, catalog_version string, catalog_date date, "
    "n_entries int, n_rows_after int, n_active_after int")
print(f"cwe_raw rows: {n} ({n_active} active, {n - n_active} removed)")
