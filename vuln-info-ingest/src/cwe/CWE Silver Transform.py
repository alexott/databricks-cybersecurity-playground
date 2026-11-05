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
from cwe.transform import transform_cwe

p = get_params(dbutils, COMMON_PARAMS)
RAW_TABLE = table_name(p, "cwe_raw")
SILVER_TABLE = table_name(p, "cwe_entries")
# Checkpoint lives outside the loader's staging directory, so the per-run staging
# cleanup never destroys the stream's offsets.
CHECKPOINT = volume_path(p, "cwe_checkpoints") + "/silver"

# COMMAND ----------

# DBTITLE 1,Create silver table with column comments
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    cwe_id STRING NOT NULL
      COMMENT 'CWE identifier (CWE-NNN). Primary key; joins to cves_v5.cwe_ids / cwe_id, kev_vulnerabilities.cwes and github_advisories / osv_vulnerabilities CWE ids.',
    entry_type STRING
      COMMENT 'Kind of CWE entry: ''Weakness'', ''Grouping'' (a category; formerly Category) or ''View''. CVEs should map to weaknesses; groupings and views organize them.',
    name STRING
      COMMENT 'Entry name (`Name`).',
    status STRING
      COMMENT 'Maturity of the entry: ''Stable'', ''Draft'', ''Incomplete'', ''Obsolete'' or ''Deprecated''.',
    abstraction STRING
      COMMENT 'Weakness abstraction level: ''Pillar'', ''Class'', ''Base'', ''Variant'', or ''Compound'' (chains and composites). NULL for groupings and views.',
    structure STRING
      COMMENT 'Weakness structure: ''Simple'', ''Chain'' or ''Composite''. NULL for groupings and views.',
    description STRING
      COMMENT 'Short description: the weakness Description, grouping Summary or view Objective (rendered from XHTML to plain text).',
    extended_description STRING
      COMMENT 'Longer explanation of the weakness (`Extended_Description`), plain text; NULL when absent.',
    likelihood_of_exploit STRING
      COMMENT 'MITRE''s estimate of how likely the weakness is exploited: ''High'', ''Medium'', ''Low'' or NULL.',
    mapping_usage STRING
      COMMENT 'Whether CVEs may be mapped to this entry (`Mapping_Notes/Usage`): ''Allowed'', ''Allowed-with-Review'', ''Discouraged'' or ''Prohibited''.',
    related_weaknesses ARRAY<STRUCT<nature: STRING, cwe_id: STRING, view_id: STRING, ordinal: STRING>>
      COMMENT 'Relationships to other CWEs: nature (ChildOf, PeerOf, CanPrecede, CanAlsoBe, Requires, ...), the related cwe_id, the view_id the relationship belongs to (CWE-1000 = research hierarchy) and ordinal (Primary for the main parent). Empty array when none.',
    members ARRAY<STRING>
      COMMENT 'CWE ids that are members of this grouping or view (`Has_Member`). Empty array for weaknesses.',
    related_capec_ids ARRAY<STRING>
      COMMENT 'CAPEC attack patterns (CAPEC-NNN) that exploit this weakness (`Related_Attack_Patterns`). Empty array when none.',
    applicable_platforms ARRAY<STRUCT<type: STRING, name: STRING, class: STRING, prevalence: STRING>>
      COMMENT 'Platforms where the weakness occurs: type (Language, Technology, Operating_System, Architecture), a specific name or a class (e.g. ''Not Language-Specific''), and prevalence (Often, Sometimes, Rarely, Undetermined).',
    common_consequences ARRAY<STRUCT<scopes: ARRAY<STRING>, impacts: ARRAY<STRING>, likelihood: STRING, note: STRING>>
      COMMENT 'Typical consequences when exploited: affected security scopes (Confidentiality, Integrity, Availability, Access Control, ...), technical impacts, likelihood and a note.',
    observed_examples ARRAY<STRUCT<reference: STRING, description: STRING, link: STRING>>
      COMMENT 'Real-world examples of the weakness; reference is usually a CVE id (join to cves_v5.cve_id).',
    date_submitted DATE
      COMMENT 'Date the entry was first submitted (`Content_History/Submission`).',
    date_modified DATE
      COMMENT 'Date of the latest modification in the content history; NULL if never modified.',
    catalog_version STRING
      COMMENT 'CWE catalog version this row was last refreshed from (e.g. ''5.0'').',
    catalog_date DATE
      COMMENT 'Release date of that catalog version.',
    date_removed TIMESTAMP
      COMMENT 'Load time at which this id first disappeared from the catalog; NULL means currently listed. CWE normally deprecates instead of removing, so this is a safety net. Filter date_removed IS NULL for the current catalog.',
    raw VARIANT
      COMMENT 'JSON rendering of the original catalog XML entry (repeatable tags as arrays, XHTML text as plain text). Holds the fields not flattened here, e.g. Potential_Mitigations, Detection_Methods, Demonstrative_Examples, Taxonomy_Mappings, References.'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Silver-layer Delta table of the MITRE CWE (Common Weakness Enumeration, https://cwe.mitre.org/) catalog: one row per weakness, grouping (category) and view, with names, descriptions, abstraction, mapping guidance, the CWE relationship graph, related CAPEC attack patterns, platforms, consequences, observed CVE examples, and the original entry as a VARIANT. Join cwe_id to the CWE ids of cves_v5, kev_vulnerabilities, github_advisories and osv_vulnerabilities to name and classify weaknesses. Filter status <> ''Deprecated'' and date_removed IS NULL for the current, usable entries.'
""")
print(f"Table {SILVER_TABLE} ready")

# COMMAND ----------

# DBTITLE 1,Stream raw CDF into silver table
stream_cdf_to_silver(spark, RAW_TABLE, SILVER_TABLE, "cwe_id", transform_cwe, CHECKPOINT)
