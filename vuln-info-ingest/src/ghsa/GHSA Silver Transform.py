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
from ghsa.transform import transform_advisories

p = get_params(dbutils, COMMON_PARAMS)
RAW_TABLE = table_name(p, "ghsa_raw")
SILVER_TABLE = table_name(p, "github_advisories")
# Checkpoint lives outside the loader's staging directory, so the per-run staging
# cleanup never destroys the stream's offsets.
CHECKPOINT = volume_path(p, "ghsa_checkpoints") + "/silver"

# COMMAND ----------

# DBTITLE 1,Create silver table (inline comments mirrored in github_advisories_comments.sql)
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    ghsa_id STRING NOT NULL
      COMMENT 'GitHub Security Advisory identifier (GHSA-xxxx-xxxx-xxxx). Primary key.',
    cve_id STRING
      COMMENT 'First CVE id among aliases (NULL if none). Join key to cves_v5. See aliases for the complete list.',
    aliases ARRAY<STRING>
      COMMENT 'All external ids this advisory aliases (CVE-*, other GHSA-*, etc.).',
    related ARRAY<STRING>
      COMMENT 'Ids of related advisories (OSV `related`); NULL/empty when absent.',
    summary STRING
      COMMENT 'Short one-line advisory summary.',
    details STRING
      COMMENT 'Full Markdown advisory details.',
    severity_label STRING
      COMMENT 'GitHub qualitative severity: ''CRITICAL'', ''HIGH'', ''MODERATE'', ''LOW''. From database_specific.severity.',
    cvss_version STRING
      COMMENT 'CVSS metric type of severity[0]: ''CVSS_V3'' or ''CVSS_V4''.',
    cvss_vector STRING
      COMMENT 'CVSS vector string of severity[0] (OSV stores the vector, not a numeric base score).',
    cwe_ids ARRAY<STRING>
      COMMENT 'CWE identifiers (CWE-NNN) from database_specific.cwe_ids. Use EXPLODE to filter/join.',
    ecosystems ARRAY<STRING>
      COMMENT 'Distinct package ecosystems across affected_packages (e.g. Maven, npm, PyPI, Go). Convenience for filtering.',
    affected_packages ARRAY<STRUCT<
      ecosystem: STRING, package: STRING, purl: STRING, range_type: STRING,
      introduced: STRING, fixed: STRING, last_affected: STRING>>
      COMMENT 'Package-level affected edges, one struct per (package, version range). ecosystem+package identify the component; introduced/fixed/last_affected bound the vulnerable range (range_type ECOSYSTEM/SEMVER/GIT). Packages with only an explicit versions list keep a struct with null version bounds. Use LATERAL VIEW EXPLODE for the dependency graph. This is the package dependency edge CVE v5 affected_products lacks.',
    references ARRAY<STRUCT<type: STRING, url: STRING>>
      COMMENT 'Advisory reference URLs with their OSV type (ADVISORY, WEB, PACKAGE, FIX, ...).',
    github_reviewed BOOLEAN
      COMMENT 'TRUE when the advisory comes from the github-reviewed tree (curated ecosystem/version data); FALSE for the unreviewed CVE-mirror tree. Filter to TRUE for high-quality package data.',
    date_published TIMESTAMP
      COMMENT 'OSV `published` timestamp.',
    date_modified TIMESTAMP
      COMMENT 'OSV `modified` timestamp; the high-water mark used for incremental upserts.',
    date_withdrawn TIMESTAMP
      COMMENT 'OSV `withdrawn` timestamp; non-NULL means the advisory was retracted. Exclude withdrawn advisories for active analysis.',
    schema_version STRING
      COMMENT 'OSV schema version of the source record.',
    raw VARIANT
      COMMENT 'Raw VARIANT of the original OSV/GHSA record. Use for fields not surfaced into the silver layer (ecosystem_specific, database_specific.*, multi-segment ranges).'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Silver-layer Delta table of GitHub Security Advisories (GHSA), sourced from the github/advisory-database repo (OSV format). Each row is one advisory with structured metadata extracted from the OSV JSON: ids (ghsa_id, cve_id, aliases), free-text summary/details, GitHub severity label and CVSS version/vector, CWE classification (cwe_ids), the package-level affected_packages edge (ecosystem, package, purl, version range) that CVE v5 affected_products lacks, a convenience ecosystems array, references, the github_reviewed quality flag, lifecycle timestamps (published/modified/withdrawn), and the original raw VARIANT. Join cve_id to cves_v5 to enrich CVEs with package dependency edges. Filter github_reviewed = TRUE for curated package data and date_withdrawn IS NULL to exclude retracted advisories.'
""")
print(f"Table {SILVER_TABLE} ready")

# COMMAND ----------

# DBTITLE 1,Stream raw CDF into silver table
stream_cdf_to_silver(spark, RAW_TABLE, SILVER_TABLE, "ghsa_id", transform_advisories, CHECKPOINT)
