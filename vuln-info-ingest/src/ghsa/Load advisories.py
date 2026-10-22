# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GHSA loader — fetch GitHub Advisory Database, upsert ghsa_raw
# MAGIC Full tarball fetch to ephemeral /tmp (no persistent clone, no git pull),
# MAGIC consolidated-JSONL staging on a scratch Volume, idempotent MERGE into
# MAGIC `ghsa_raw` on `ghsa_id` gated by `date_modified`.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "tarball_url": str, "part_records": int})
TARBALL_URL = p["tarball_url"]
BATCH = p["part_records"]
VOLUME_PATH = volume_path(p, "ghsa")
RAW_TABLE = table_name(p, "ghsa_raw")
FETCH_LOG_TABLE = table_name(p, "ghsa_fetch_log")

# COMMAND ----------

# DBTITLE 1,Fetch tarball to /tmp and extract
import glob, json, shutil, tarfile, urllib.request, datetime
from ghsa.helpers import github_reviewed_from_path
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log
from delta.tables import DeltaTable
from pyspark.sql import functions as F

WORK = "/tmp/ghsa_src"
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK, exist_ok=True)
tgz = "/tmp/ghsa.tar.gz"
urllib.request.urlretrieve(TARBALL_URL, tgz)
with tarfile.open(tgz, "r:gz") as t:
    t.extractall(WORK)
print("extracted tarball")

# COMMAND ----------

# DBTITLE 1,Consolidate advisory JSON into batched JSONL on the Volume
# Remove only our own staging parts — never wipe the whole Volume, which may hold
# other state (e.g. a streaming checkpoint) we must not destroy.
os.makedirs(VOLUME_PATH, exist_ok=True)
for _f in glob.glob(f"{VOLUME_PATH}/part-*.jsonl"):
    os.remove(_f)
paths = glob.glob(f"{WORK}/**/advisories/**/GHSA-*.json", recursive=True)
print(f"{len(paths)} advisory files")
batch, bno = [], 0

def flush():
    global batch, bno
    if not batch:
        return
    with open(f"{VOLUME_PATH}/part-{bno:05d}.jsonl", "w", encoding="utf-8") as out:
        out.write("\n".join(batch))
    bno += 1
    batch = []

for p in paths:
    with open(p, "r", encoding="utf-8") as f:
        raw = f.read()
    batch.append(json.dumps({"github_reviewed": github_reviewed_from_path(p), "raw": raw}))
    if len(batch) >= BATCH:
        flush()
flush()
print(f"wrote {bno} JSONL parts")

# COMMAND ----------

# DBTITLE 1,Read staged JSONL, parse raw VARIANT, extract id columns
staged = spark.read.json(VOLUME_PATH)   # columns: github_reviewed, raw(string)
src = (staged.selectExpr("github_reviewed", "try_parse_json(raw) as raw")
       .withColumn("ghsa_id", F.expr("try_variant_get(raw, '$.id', 'string')"))
       .withColumn("cve_id", F.expr(
           "get(filter(try_variant_get(raw,'$.aliases','ARRAY<STRING>'), x -> upper(x) like 'CVE-%'), 0)"))
       .withColumn("date_published", F.expr("try_variant_get(raw, '$.published', 'timestamp')"))
       .withColumn("date_modified", F.expr("try_variant_get(raw, '$.modified', 'timestamp')"))
       .filter("ghsa_id IS NOT NULL"))
# One row per ghsa_id (keep the newest) so the MERGE never sees multiple source
# rows for one target. GHSA ids are unique per repo file, so this is a guard.
src = dedupe_latest(src, "ghsa_id", "date_modified")

# COMMAND ----------

# DBTITLE 1,Create ghsa_raw if needed (CDF on) and upsert on ghsa_id gated by modified
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    raw VARIANT, ghsa_id STRING, cve_id STRING,
    date_published TIMESTAMP, date_modified TIMESTAMP, github_reviewed BOOLEAN
  ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
""")
(DeltaTable.forName(spark, RAW_TABLE).alias("t")
 .merge(src.alias("s"), "t.ghsa_id = s.ghsa_id")
 .whenMatchedUpdateAll(condition="s.date_modified > t.date_modified")
 .whenNotMatchedInsertAll()
 .execute())

# COMMAND ----------

# DBTITLE 1,Fetch-log row
n = spark.table(RAW_TABLE).count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), "refs/heads/main", len(paths), n),
    "fetch_time timestamp, source_ref string, n_advisories int, n_rows_after int")
print(f"ghsa_raw rows: {n}")
