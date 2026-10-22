# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path
from common.silver import stream_cdf_to_silver
from osv.transform import transform_vulnerabilities

p = get_params(dbutils, COMMON_PARAMS)
RAW_TABLE = table_name(p, "osv_raw")
SILVER_TABLE = table_name(p, "osv_vulnerabilities")
# Checkpoint lives outside the loaders' staging directory, so the staging cleanup
# never destroys the stream's offsets.
CHECKPOINT = volume_path(p, "osv_checkpoints") + "/silver"

# COMMAND ----------

# DBTITLE 1,Create silver table with column comments
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    id STRING NOT NULL
      COMMENT 'OSV record identifier (e.g. GHSA-xxxx-xxxx-xxxx, PYSEC-2024-1, GO-2024-1, RUSTSEC-2024-1, CVE-2024-1 for the GIT ecosystem, UBUNTU-CVE-2024-1, DSA-5000-1). Primary key.',
    id_prefix STRING
      COMMENT 'Leading letters of id (GHSA, PYSEC, GO, RUSTSEC, CVE, UBUNTU, DSA, CGA, MAL, ...). Identifies the source database; use it to filter or group by source.',
    cve_id STRING
      COMMENT 'CVE id for the record: id itself if it is a CVE, else the first CVE among aliases, else the first CVE in upstream. NULL if none. Join key to cves_v5.',
    aliases ARRAY<STRING>
      COMMENT 'Ids of the same vulnerability in other databases (OSV `aliases`).',
    upstream ARRAY<STRING>
      COMMENT 'Ids this record is derived from (OSV `upstream`), e.g. a distro record pointing to its CVE.',
    related ARRAY<STRING>
      COMMENT 'Ids of related, but not identical, records (OSV `related`).',
    summary STRING
      COMMENT 'Short one-line summary.',
    details STRING
      COMMENT 'Full Markdown details.',
    severity ARRAY<STRUCT<type: STRING, score: STRING>>
      COMMENT 'All OSV severity entries. type is CVSS_V2/CVSS_V3/CVSS_V4 (score = vector string) or a source-specific type such as ''Ubuntu'' (score = label like ''medium'').',
    cvss_version STRING
      COMMENT 'Type of the newest CVSS entry in severity (CVSS_V4 > CVSS_V3 > CVSS_V2); NULL if there is no CVSS entry.',
    cvss_vector STRING
      COMMENT 'CVSS vector string of that entry (OSV stores the vector, not a numeric base score).',
    cwe_ids ARRAY<STRING>
      COMMENT 'CWE identifiers from database_specific.cwe_ids. Only some sources set it (GHSA, CVE-derived GIT records).',
    ecosystems ARRAY<STRING>
      COMMENT 'Distinct base ecosystems across affected packages (version suffix removed: ''Ubuntu:22.04:LTS'' -> ''Ubuntu''). GIT-only records have an empty array.',
    affected_packages ARRAY<STRUCT<
      ecosystem: STRING, package: STRING, purl: STRING, repo: STRING, range_type: STRING,
      introduced: STRING, fixed: STRING, last_affected: STRING>>
      COMMENT 'Affected edges, one struct per (package, version range). ecosystem is the full OSV value (e.g. ''Debian:12''); ecosystem+package identify the component. repo is set for GIT ranges, which often have no package. introduced/fixed/last_affected are the first of each event in the range (range_type ECOSYSTEM/SEMVER/GIT); raw keeps multi-segment ranges. Packages with only an explicit versions list keep a struct with null version bounds.',
    references ARRAY<STRUCT<type: STRING, url: STRING>>
      COMMENT 'Reference URLs with their OSV type (ADVISORY, WEB, PACKAGE, FIX, REPORT, ...).',
    date_published TIMESTAMP
      COMMENT 'OSV `published` timestamp.',
    date_modified TIMESTAMP
      COMMENT 'OSV `modified` timestamp; the value that gates upserts.',
    date_withdrawn TIMESTAMP
      COMMENT 'OSV `withdrawn` timestamp; non-NULL means the record was retracted. Exclude withdrawn records for active analysis.',
    schema_version STRING
      COMMENT 'OSV schema version of the source record.',
    raw VARIANT
      COMMENT 'Raw VARIANT of the original OSV record. Use for fields not in the silver layer (ecosystem_specific, database_specific.*, versions, credits, multi-segment ranges).'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Silver-layer Delta table of the OSV (Open Source Vulnerabilities) database from gs://osv-vulnerabilities, all ecosystems. One row per OSV id: ids (id, id_prefix, cve_id, aliases, upstream, related), summary/details, severity entries with the newest CVSS vector, CWE ids where the source has them, the package-level affected_packages edge (ecosystem, package, purl, repo, version range), references, lifecycle timestamps and the raw VARIANT. A superset of github_advisories (GHSA records are included). Join cve_id to cves_v5. Filter date_withdrawn IS NULL to exclude retracted records.'
""")
print(f"Table {SILVER_TABLE} ready")

# COMMAND ----------

# DBTITLE 1,Stream raw CDF into silver table (upserts and deletes)
# The full load deletes ids gone from the snapshot; those deletes propagate to silver.
stream_cdf_to_silver(
    spark, RAW_TABLE, SILVER_TABLE, "id",
    lambda df: transform_vulnerabilities(df, keep=["_change_type"]),
    CHECKPOINT, apply_deletes=True)
