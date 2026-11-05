# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ATLAS loader — MITRE ATLAS release YAML, upsert atlas_raw
# MAGIC Looks up the requested release (`atlas_version`, `latest` or e.g. `2026.10`) in
# MAGIC atlas-data's `dist/manifest.yaml`; if the fetch log already has it, stops.
# MAGIC Otherwise downloads the release's format-6 YAML (~1 MB), flattens it into one
# MAGIC record per object and per relationship (`atlas.helpers`), stages one JSON line
# MAGIC per record on a scratch Volume, then does a full-snapshot MERGE into
# MAGIC `atlas_raw` on `atlas_id`: update when the content hash changed (relationships
# MAGIC have no modified date), insert new records, soft-delete records absent from the
# MAGIC release (`date_removed`).

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "manifest_url": str, "atlas_version": str})
VOLUME_PATH = volume_path(p, "atlas")
RAW_TABLE = table_name(p, "atlas_raw")
FETCH_LOG_TABLE = table_name(p, "atlas_fetch_log")

# COMMAND ----------

# DBTITLE 1,Resolve the release; stop if already loaded
import datetime, glob, json
import yaml
from atlas.helpers import extract_raw_data, parse_atlas, resolve_release
from common.fetch import fetch_with_retries
from common.fetch_log import append_fetch_log
from delta.tables import DeltaTable
from pyspark.sql import functions as F


def download(url: str) -> bytes:
    data = fetch_with_retries(url, strip=False)
    if data is None:
        raise RuntimeError(f"not found: {url}")
    return data


release, release_date, path = resolve_release(yaml.safe_load(download(p["manifest_url"])), p["atlas_version"])
release_url = f"{p['manifest_url'].rsplit('/', 1)[0]}/{path}"
print(f"ATLAS {release} ({release_date}): {release_url}")
if spark.catalog.tableExists(FETCH_LOG_TABLE) and spark.catalog.tableExists(RAW_TABLE):
    last = spark.sql(f"SELECT release FROM {FETCH_LOG_TABLE} ORDER BY fetch_time DESC LIMIT 1").first()
    if last is not None and last["release"] == release:
        dbutils.notebook.exit(f"up to date ({release})")

# COMMAND ----------

# DBTITLE 1,Download, flatten and stage one JSON line per record
collection_version, records = parse_atlas(yaml.safe_load(download(release_url)))
if collection_version != release:
    raise RuntimeError(f"{release_url} is ATLAS {collection_version}, expected {release}")
print(f"{len(records)} records")

# Remove only our own staging parts — never wipe the whole Volume, which may hold
# other state (e.g. a streaming checkpoint) we must not destroy.
os.makedirs(VOLUME_PATH, exist_ok=True)
for _f in glob.glob(f"{VOLUME_PATH}/part-*.jsonl"):
    os.remove(_f)
with open(f"{VOLUME_PATH}/part-00000.jsonl", "w", encoding="utf-8") as out:
    out.write("\n".join(json.dumps(dict(r, raw=json.dumps(r["raw"]))) for r in records))

# COMMAND ----------

# DBTITLE 1,Read staged JSONL, parse raw VARIANT, stamp the release
staged = spark.read.schema(
    "atlas_id string, object_type string, modified string, content_hash string, raw string").json(VOLUME_PATH)
src = (extract_raw_data(staged.withColumn("atlas_release", F.lit(release)))
       .filter("atlas_id IS NOT NULL")
       .withColumn("date_removed", F.lit(None).cast("timestamp")))
n_dupes = src.groupBy("atlas_id").count().filter("count > 1").count()
if n_dupes:
    raise RuntimeError(f"{n_dupes} ATLAS ids occur more than once in release {release}")

# COMMAND ----------

# DBTITLE 1,Create atlas_raw if needed (CDF on) and upsert on atlas_id with soft-delete
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    raw VARIANT, atlas_id STRING, object_type STRING, modified DATE, content_hash STRING,
    atlas_release STRING, date_removed TIMESTAMP
  ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
""")
(DeltaTable.forName(spark, RAW_TABLE).alias("t")
 .merge(src.alias("s"), "t.atlas_id = s.atlas_id")
 # Refresh when the record changed or a removed record reappears; a record in the
 # release is, by definition, listed.
 .whenMatchedUpdateAll(condition="s.content_hash <> t.content_hash OR t.date_removed IS NOT NULL")
 .whenNotMatchedInsertAll()
 # Record no longer in the release: stamp the removal once.
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
    (datetime.datetime.now(datetime.UTC), release, datetime.date.fromisoformat(release_date),
     len(records), n, n_active),
    "fetch_time timestamp, release string, release_date date, "
    "n_records int, n_rows_after long, n_active_after long")
print(f"atlas_raw rows: {n} ({n_active} active, {n - n_active} removed)")
