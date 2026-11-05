# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # EPSS loader — daily score files into epss_scores_history
# MAGIC Finds the score dates in the last `lookback_days` that are missing from
# MAGIC `epss_scores_history`, downloads their daily `epss_scores-YYYY-MM-DD.csv.gz`
# MAGIC files in parallel to a scratch Volume, and replaces those dates in the table
# MAGIC (`replaceWhere`), `batch_days` dates at a time. A date that is not published yet
# MAGIC is skipped and picked up by a later run. The same notebook does the daily load
# MAGIC and, with a large `lookback_days`, the one-time backfill since 2021-04-14; a
# MAGIC failed backfill resumes from the dates still missing. Idempotent.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "base_url": str, "lookback_days": int,
                         "batch_days": int, "fetch_threads": int})
BASE_URL = p["base_url"].rstrip("/")
VOLUME_PATH = volume_path(p, "epss")
HISTORY_TABLE = table_name(p, "epss_scores_history")
FETCH_LOG_TABLE = table_name(p, "epss_fetch_log")

# COMMAND ----------

# DBTITLE 1,Create history table with column comments
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
    cve_id STRING NOT NULL
      COMMENT 'CVE identifier (CVE-YYYY-NNNN). Join key to cves_v5, github_advisories, osv_vulnerabilities and kev_vulnerabilities.',
    score_date DATE NOT NULL
      COMMENT 'Date of the daily EPSS scores file this row comes from. One row per (cve_id, score_date).',
    epss DOUBLE
      COMMENT 'EPSS score: estimated probability [0-1] that the CVE is exploited in the wild in the 30 days after score_date.',
    percentile DOUBLE
      COMMENT 'Share [0-1] of all CVEs scored that day with the same or a lower EPSS score. NULL before 2021-09 (not published then).',
    model_version STRING
      COMMENT 'EPSS model version that produced the score (e.g. ''v2025.03.14''). NULL before 2022-02-04, when the files had no version line (EPSS v1). Scores shift sharply when the model changes.'
  )
  USING DELTA
  CLUSTER BY (score_date, cve_id)
  COMMENT 'Daily history of FIRST EPSS (Exploit Prediction Scoring System, https://www.first.org/epss/) scores since 2021-04-14: one row per CVE per day with the probability of exploitation in the next 30 days and its percentile. Use for score trends; epss_scores holds the latest day only. Compare scores across model_version changes with care (v2 2022-02-04, v3 2023-03-07, v4 2025-03-17).'
""")

# COMMAND ----------

# DBTITLE 1,Find the score dates to load
import datetime, glob
from concurrent.futures import ThreadPoolExecutor
from epss.helpers import FIRST_SCORE_DATE, batches, dates_to_load, parse_header, score_filename, score_url
from epss.transform import read_scores, to_history
from common.fetch import fetch_with_retries
from common.fetch_log import append_fetch_log

today = datetime.datetime.now(datetime.UTC).date()
start = max(FIRST_SCORE_DATE, today - datetime.timedelta(days=p["lookback_days"]))
loaded = [r[0] for r in spark.sql(
    f"SELECT DISTINCT score_date FROM {HISTORY_TABLE} WHERE score_date >= DATE'{start}'").collect()]
dates = dates_to_load(today, p["lookback_days"], loaded)
print(f"{len(dates)} dates missing since {start}: {dates[:1]} .. {dates[-1:]}")

# COMMAND ----------

# DBTITLE 1,Download and load in batches
def fetch(d: datetime.date) -> tuple[datetime.date, str | None] | None:
    """Stage one daily file; (date, model version), or None if not published.
    The CDN is S3 behind CloudFront: a missing key is 403, not 404."""
    data = fetch_with_retries(score_url(BASE_URL, d), missing_codes=(403, 404), strip=False)
    if data is None:
        return None
    model_version, _ = parse_header(data)   # also validates the column layout
    with open(f"{VOLUME_PATH}/{score_filename(d)}", "wb") as f:
        f.write(data)
    return d, model_version


os.makedirs(VOLUME_PATH, exist_ok=True)
model_versions: dict[datetime.date, str | None] = {}
n_rows = 0
for batch in batches(dates, p["batch_days"]):
    # Remove only our own staged files, never other state on the Volume.
    for _f in glob.glob(f"{VOLUME_PATH}/epss_scores-*.csv.gz"):
        os.remove(_f)
    with ThreadPoolExecutor(p["fetch_threads"]) as pool:
        got = dict(x for x in pool.map(fetch, batch) if x is not None)
    if not got:
        continue
    in_list = ", ".join(f"DATE'{d}'" for d in sorted(got))
    (to_history(read_scores(spark, VOLUME_PATH), got)
     .write.mode("overwrite").option("replaceWhere", f"score_date IN ({in_list})")
     .saveAsTable(HISTORY_TABLE))
    metrics = spark.sql(f"DESCRIBE HISTORY {HISTORY_TABLE} LIMIT 1").first()["operationMetrics"]
    n_rows += int(metrics.get("numOutputRows", 0))
    model_versions.update(got)
    print(f"loaded {len(got)} dates {min(got)} .. {max(got)}, {n_rows} rows so far")

# COMMAND ----------

# DBTITLE 1,Fetch-log row
newest = max(model_versions) if model_versions else None
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), p["lookback_days"], len(dates), len(model_versions),
     min(model_versions) if model_versions else None, newest,
     model_versions[newest] if newest else None, n_rows),
    "fetch_time timestamp, lookback_days int, n_dates_missing int, n_dates_loaded int, "
    "first_date_loaded date, last_date_loaded date, model_version string, n_rows_loaded long")
print(f"loaded {len(model_versions)} of {len(dates)} missing dates, {n_rows} rows")
