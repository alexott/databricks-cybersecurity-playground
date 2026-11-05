# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EPSS current scores — newest day of epss_scores_history into epss_scores
# MAGIC When the history has a newer score date than `epss_scores`, `MERGE`s that day's
# MAGIC scores in on `cve_id` and deletes CVEs no longer scored (mostly REJECTED CVEs;
# MAGIC the history keeps their rows). Refuses to merge a day with fewer than
# MAGIC `min_snapshot_ratio` of the current rows. The scores are already typed, so
# MAGIC there is no raw VARIANT / change-feed step as for the other sources.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name

p = get_params(dbutils, {**COMMON_PARAMS, "min_snapshot_ratio": float})
HISTORY_TABLE = table_name(p, "epss_scores_history")
CURRENT_TABLE = table_name(p, "epss_scores")

# COMMAND ----------

# DBTITLE 1,Create current table with column comments
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {CURRENT_TABLE} (
    cve_id STRING NOT NULL
      COMMENT 'CVE identifier (CVE-YYYY-NNNN). Primary key and the join key to cves_v5, github_advisories, osv_vulnerabilities and kev_vulnerabilities.',
    score_date DATE
      COMMENT 'Date of the daily EPSS scores file the score comes from (the same for all rows).',
    epss DOUBLE
      COMMENT 'EPSS score: estimated probability [0-1] that the CVE is exploited in the wild in the 30 days after score_date.',
    percentile DOUBLE
      COMMENT 'Share [0-1] of all scored CVEs with the same or a lower EPSS score, e.g. 0.95 = top 5%.',
    model_version STRING
      COMMENT 'EPSS model version that produced the score (e.g. ''v2026.06.15'').'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Latest FIRST EPSS (Exploit Prediction Scoring System, https://www.first.org/epss/) scores: one row per scored CVE with the probability that it is exploited in the wild in the next 30 days, and its percentile among all CVEs. Refreshed daily; CVEs no longer scored are removed. LEFT JOIN on cve_id to cves_v5 / github_advisories / osv_vulnerabilities to prioritize, alongside kev_vulnerabilities (known exploitation). Daily history is in epss_scores_history.'
""")
print(f"Table {CURRENT_TABLE} ready")

# COMMAND ----------

# DBTITLE 1,Merge the newest score date
from delta.tables import DeltaTable
from common.merge import dedupe_latest

newest = spark.sql(f"SELECT max(score_date) FROM {HISTORY_TABLE}").first()[0]
current = spark.sql(f"SELECT max(score_date) FROM {CURRENT_TABLE}").first()[0]
if newest is None or (current is not None and newest <= current):
    dbutils.notebook.exit(f"up to date ({current})")

snapshot = spark.table(HISTORY_TABLE).filter(f"score_date = DATE'{newest}'")
n_snapshot, n_current = snapshot.count(), spark.table(CURRENT_TABLE).count()
if n_snapshot < p["min_snapshot_ratio"] * n_current:
    raise RuntimeError(f"{newest} has {n_snapshot} scores, less than {p['min_snapshot_ratio']} "
                       f"of the {n_current} current rows: refusing to merge")
# CVE ids are unique per day; the dedupe guards the MERGE (Delta errors on duplicates).
(DeltaTable.forName(spark, CURRENT_TABLE).alias("t")
 .merge(dedupe_latest(snapshot, "cve_id", "epss").alias("s"), "t.cve_id = s.cve_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .whenNotMatchedBySourceDelete()
 .execute())
print(f"{CURRENT_TABLE}: {spark.table(CURRENT_TABLE).count()} rows from {newest} (was {current})")
