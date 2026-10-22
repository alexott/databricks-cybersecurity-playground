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
from cve_v5.transform import transform_cves

p = get_params(dbutils, COMMON_PARAMS)
RAW_TABLE = table_name(p, "cves_v5_raw")
SILVER_TABLE = table_name(p, "cves_v5")
# Checkpoint lives outside the loaders' staging directory, so the staging cleanup
# never destroys the stream's offsets.
CHECKPOINT = volume_path(p, "cve_v5_checkpoints") + "/silver"

# COMMAND ----------

# DBTITLE 1,Create silver table
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    cve_id STRING NOT NULL
      COMMENT 'CVE identifier in the format CVE-YYYY-NNNNN.',
    cve_year INT
      COMMENT 'Year component of the CVE ID (e.g., 2026 for CVE-2026-12345).',
    state STRING
      COMMENT 'Publication state of the CVE record. Values: ''PUBLISHED'' (active), ''REJECTED'' (invalid / withdrawn). Filter to ''PUBLISHED'' for actionable analysis.',
    date_published TIMESTAMP
      COMMENT 'Timestamp when the CVE was first published. Use for recency filtering.',
    date_reserved TIMESTAMP
      COMMENT 'Timestamp when the CVE ID was reserved by a CNA (before publication details were available).',
    date_updated TIMESTAMP
      COMMENT 'Timestamp of the most recent update to the CVE record. Use to detect re-scored or re-classified vulnerabilities.',
    assigner_org_id STRING
      COMMENT 'Internal organization ID of the CVE Numbering Authority (CNA) that assigned this CVE.',
    assigner_short_name STRING
      COMMENT 'Short name of the assigning authority (e.g., ''mitre'', ''microsoft'', ''redhat''). Useful for filtering by issuer or grouping by ecosystem.',
    title STRING
      COMMENT 'Short human-readable title of the CVE.',
    description STRING
      COMMENT 'Full natural-language description of the vulnerability. Use full-text search (ILIKE / contains) when keyword filtering is needed.',
    affected_products ARRAY<STRUCT<
      vendor: STRING, product: STRING, defaultStatus: STRING,
      platforms: ARRAY<STRING>,
      versions: ARRAY<STRUCT<version: STRING, status: STRING,
        lessThan: STRING, lessThanOrEqual: STRING, versionType: STRING>>
    >>
      COMMENT 'Array of structs describing affected products. Each entry has fields like vendor, product, defaultStatus, platforms, and versions. Use LATERAL VIEW EXPLODE to filter on vendor/product.',
    cvss_version STRING
      COMMENT 'CVSS specification version used for scoring. Values: ''2.0'', ''3.0'', ''3.1'', ''4.0''.',
    cvss_base_score DOUBLE
      COMMENT 'Numeric CVSS base score (0.0-10.0). Higher is more severe.',
    cvss_severity STRING
      COMMENT 'Qualitative CVSS severity. Values: ''CRITICAL'', ''HIGH'', ''MEDIUM'', ''LOW'', ''NONE''. Prefer this over cvss_base_score for human-friendly queries.',
    cvss_vector STRING
      COMMENT 'Raw CVSS vector string encoding the attack characteristics.',
    cvss_source STRING
      COMMENT 'Source that produced the CVSS score (''CNA'' = CVE Numbering Authority, ''ADP'' = Authorized Data Publisher, typically CISA-ADP).',
    cwe_ids ARRAY<STRING>
      COMMENT 'All Common Weakness Enumeration identifiers (CWE-NNN) classifying the underlying weaknesses, collected across every problemType/description entry. Use EXPLODE to filter or join on individual CWEs. A CVE can carry several CWEs.',
    cwe_id STRING
      COMMENT 'First CWE identifier (= cwe_ids[0]), kept for backward compatibility. Prefer cwe_ids for complete weakness classification.',
    cwe_description STRING
      COMMENT 'Human-readable description of the first CWE classification.',
    ssvc_exploitation STRING
      COMMENT 'SSVC (Stakeholder-Specific Vulnerability Categorization) — exploitation maturity. Values: ''active'' (in-the-wild exploitation), ''poc'' (proof-of-concept exists), ''none'' (no public exploitation). Filter to ''active'' for highest-priority CVEs.',
    ssvc_automatable STRING
      COMMENT 'SSVC — whether exploitation is automatable. Values: ''yes'', ''no''. ''yes'' means an attacker can script exploitation at scale.',
    ssvc_technical_impact STRING
      COMMENT 'SSVC — severity of impact upon successful exploitation. Values: ''partial'', ''total''.',
    references ARRAY<STRUCT<url: STRING, name: STRING, tags: ARRAY<STRING>>>
      COMMENT 'Array of structs containing reference URLs (vendor advisories, blog posts, exploit code). Each entry has url, name, and tags.',
    raw VARIANT
      COMMENT 'Raw VARIANT JSON of the original CVE v5 record. Use for fields not extracted into the silver layer.'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Silver-layer Delta table of CVE v5 (Common Vulnerabilities and Exposures version 5) records, sourced from MITRE/NVD and other CVE Numbering Authorities (CNAs). Each row is one CVE with structured metadata extracted from the original CVE v5 JSON: identifiers (cve_id, cve_year, assigner_*), lifecycle timestamps (date_reserved/published/updated), publication state, free-text title and description, an array of affected products (with vendors, products, platforms, and version ranges), CVSS scoring with CNA-preferred / CISA-ADP-fallback (cvss_version, cvss_base_score, cvss_severity, cvss_vector, cvss_source), CWE classification (cwe_ids array, with cwe_id/cwe_description kept for the first entry), SSVC priority signals (ssvc_exploitation, ssvc_automatable, ssvc_technical_impact), an array of references, and the original raw VARIANT record. Use the extracted columns directly for analytics; consult `raw` only for fields not surfaced into the silver layer. Always filter `state = ''PUBLISHED''` to exclude rejected/invalid CVEs.'
""")
print(f"Table {SILVER_TABLE} ready")

# COMMAND ----------

# DBTITLE 1,Stream raw CDF into silver table
stream_cdf_to_silver(spark, RAW_TABLE, SILVER_TABLE, "cve_id", transform_cves, CHECKPOINT)
