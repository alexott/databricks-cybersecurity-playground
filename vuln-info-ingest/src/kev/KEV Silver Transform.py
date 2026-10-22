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
from kev.transform import transform_kev

p = get_params(dbutils, COMMON_PARAMS)
RAW_TABLE = table_name(p, "kev_raw")
SILVER_TABLE = table_name(p, "kev_vulnerabilities")
# Checkpoint lives outside the loader's staging directory, so the per-run staging
# cleanup never destroys the stream's offsets.
CHECKPOINT = volume_path(p, "kev_checkpoints") + "/silver"

# COMMAND ----------

# DBTITLE 1,Create silver table with column comments
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    cve_id STRING NOT NULL
      COMMENT 'CVE identifier (CVE-YYYY-NNNN). Primary key and the join key to cves_v5, github_advisories and osv_vulnerabilities.',
    vendor_project STRING
      COMMENT 'Vendor or project that produces the affected product (CISA `vendorProject`).',
    product STRING
      COMMENT 'Affected product name (CISA `product`).',
    vulnerability_name STRING
      COMMENT 'CISA''s short name for the vulnerability (`vulnerabilityName`).',
    short_description STRING
      COMMENT 'One-paragraph description of the vulnerability (`shortDescription`).',
    required_action STRING
      COMMENT 'The remediation CISA requires for federal agencies (`requiredAction`).',
    date_added DATE
      COMMENT 'Date CISA added this CVE to the KEV catalog (`dateAdded`).',
    due_date DATE
      COMMENT 'Date by which the required action must be completed (`dueDate`).',
    known_ransomware_campaign_use STRING
      COMMENT 'CISA''s assessment of ransomware use: ''Known'' or ''Unknown'' (faithful source value).',
    forensic_triage STRING
      COMMENT 'Whether CISA recommends forensic triage for this CVE (`forensicTriage`, e.g. ''Yes'').',
    notes STRING
      COMMENT 'Free-text notes, usually a semicolon-separated list of reference URLs (`notes`).',
    cwes ARRAY<STRING>
      COMMENT 'CWE identifiers (CWE-NNN) CISA associates with the CVE; empty array when none. Use EXPLODE to filter/join.',
    catalog_version STRING
      COMMENT 'CISA catalog version this row was last refreshed from (`catalogVersion`, e.g. ''2026.10.04'').',
    date_released TIMESTAMP
      COMMENT 'Release timestamp of that catalog version (`dateReleased`).',
    date_removed TIMESTAMP
      COMMENT 'Catalog version''s release timestamp in which this cveID first disappeared from KEV; NULL means currently listed. Removals are rare CISA corrections (CVE added in error or later REJECTED), not de-exploitation. Filter date_removed IS NULL for the active catalog.',
    raw VARIANT
      COMMENT 'Raw VARIANT of the original per-vulnerability KEV record.'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Silver-layer Delta table of the CISA Known Exploited Vulnerabilities (KEV) catalog (https://www.cisa.gov/known-exploited-vulnerabilities-catalog). Each row is one CVE CISA has confirmed is actively exploited in the wild, with the affected vendor/product, CISA''s required remediation action and due date, the ransomware-use assessment, forensic-triage recommendation, associated CWEs, and the original raw VARIANT. The catalog is cumulative (entries are not aged out when patched); removals are rare CISA data-quality corrections and are soft-deleted via date_removed rather than physically dropped. Join cve_id to cves_v5 / github_advisories / osv_vulnerabilities to flag which CVEs are known-exploited. Filter date_removed IS NULL for the active catalog.'
""")
print(f"Table {SILVER_TABLE} ready")

# COMMAND ----------

# DBTITLE 1,Stream raw CDF into silver table
stream_cdf_to_silver(spark, RAW_TABLE, SILVER_TABLE, "cve_id", transform_kev, CHECKPOINT)
