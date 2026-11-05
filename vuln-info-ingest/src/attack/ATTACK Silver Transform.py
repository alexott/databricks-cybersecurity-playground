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
from attack.transform import transform_objects, transform_relationships, view_sql

p = get_params(dbutils, COMMON_PARAMS)
RAW_TABLE = table_name(p, "attack_raw")
OBJECTS_TABLE = table_name(p, "attack_objects")
RELATIONSHIPS_TABLE = table_name(p, "attack_relationships")
# Checkpoints live outside the loader's staging directory, so the per-run staging
# cleanup never destroys the streams' offsets.
CHECKPOINTS = volume_path(p, "attack_checkpoints")

# COMMAND ----------

# DBTITLE 1,Create silver tables with column comments
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {OBJECTS_TABLE} (
    stix_id STRING NOT NULL
      COMMENT 'STIX 2.1 object id (e.g. attack-pattern--<uuid>). Primary key; attack_relationships.source_ref / target_ref point to it.',
    type STRING
      COMMENT 'STIX object type: attack-pattern (technique), x-mitre-tactic, intrusion-set (group), malware, tool, campaign, course-of-action (mitigation), x-mitre-data-source, x-mitre-data-component, x-mitre-detection-strategy, x-mitre-analytic, x-mitre-matrix, identity, marking-definition.',
    attack_id STRING
      COMMENT 'ATT&CK id from the mitre-attack external reference: T1059 / T1059.001 (technique), TA0002 (tactic), G0016 (group), S0154 (software), M1038 (mitigation), C0024 (campaign), DS/DC/DET/AN ids; NULL when the object has none. Join key to cwe_capec_attack.dst_id for techniques.',
    name STRING
      COMMENT 'Object name.',
    description STRING
      COMMENT 'Description (Markdown with citations).',
    domains ARRAY<STRING>
      COMMENT 'ATT&CK domains the object belongs to: enterprise-attack, mobile-attack, ics-attack (`x_mitre_domains`). Objects shared by domains are one row.',
    platforms ARRAY<STRING>
      COMMENT 'Platforms of a technique or software (`x_mitre_platforms`, e.g. Windows, Linux, macOS, IaaS); empty array when not applicable.',
    tactics ARRAY<STRING>
      COMMENT 'Tactic short names of a technique (kill-chain phases of the ATT&CK matrices, e.g. execution, persistence); join to shortname of x-mitre-tactic objects.',
    shortname STRING
      COMMENT 'Short name of a tactic (`x_mitre_shortname`, e.g. execution); NULL for other types.',
    is_subtechnique BOOLEAN
      COMMENT 'TRUE for sub-techniques (T1059.001), FALSE for techniques; NULL for other types.',
    aliases ARRAY<STRING>
      COMMENT 'Alternative names of groups, campaigns (`aliases`) and software (`x_mitre_aliases`); empty array when none.',
    url STRING
      COMMENT 'URL of the object on attack.mitre.org.',
    revoked BOOLEAN
      COMMENT 'TRUE if the object was revoked (replaced by another object; see revoked-by in attack_relationships).',
    deprecated BOOLEAN
      COMMENT 'TRUE if the object is deprecated (`x_mitre_deprecated`).',
    version STRING
      COMMENT 'Version of the object itself (`x_mitre_version`).',
    created TIMESTAMP
      COMMENT 'STIX created timestamp.',
    modified TIMESTAMP
      COMMENT 'STIX modified timestamp; updates are applied when it advances.',
    attack_version STRING
      COMMENT 'ATT&CK release (e.g. ''20.0'') of the bundle this object version was loaded from. An object unchanged by a later release keeps the earlier version.',
    date_removed TIMESTAMP
      COMMENT 'Load time at which the object first disappeared from the loaded bundles; NULL means currently listed. Filter date_removed IS NULL.',
    raw VARIANT
      COMMENT 'Raw VARIANT of the original STIX object (type-specific fields, external references, kill chain phases, ...).'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Silver-layer Delta table of MITRE ATT&CK (https://attack.mitre.org/) STIX 2.1 objects from attack-stix-data (Enterprise, Mobile and ICS domains), one row per object except relationships: techniques and sub-techniques, tactics, groups, software, campaigns, mitigations, data sources and components, detection strategies and analytics. Revoked and deprecated objects are kept with flags. The views attack_techniques, attack_tactics, attack_groups, attack_software, attack_mitigations and attack_campaigns give per-type columns; links between objects are in attack_relationships.'
""")
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {RELATIONSHIPS_TABLE} (
    stix_id STRING NOT NULL
      COMMENT 'STIX id of the relationship object (relationship--<uuid>). Primary key.',
    relationship_type STRING
      COMMENT 'uses (group/software/campaign -> technique or software), mitigates (mitigation -> technique), detects (detection strategy -> technique), subtechnique-of, revoked-by (revoked object -> replacement), attributed-to (campaign -> group).',
    source_ref STRING
      COMMENT 'STIX id of the source object (attack_objects.stix_id).',
    target_ref STRING
      COMMENT 'STIX id of the target object (attack_objects.stix_id).',
    source_type STRING
      COMMENT 'STIX type of the source object (prefix of source_ref).',
    target_type STRING
      COMMENT 'STIX type of the target object (prefix of target_ref).',
    description STRING
      COMMENT 'How the source uses / mitigates / detects the target (Markdown with citations).',
    revoked BOOLEAN
      COMMENT 'TRUE if the relationship was revoked.',
    deprecated BOOLEAN
      COMMENT 'TRUE if the relationship is deprecated (`x_mitre_deprecated`).',
    created TIMESTAMP
      COMMENT 'STIX created timestamp.',
    modified TIMESTAMP
      COMMENT 'STIX modified timestamp.',
    attack_version STRING
      COMMENT 'ATT&CK release of the bundle this relationship version was loaded from.',
    date_removed TIMESTAMP
      COMMENT 'Load time at which the relationship first disappeared from the loaded bundles; NULL means currently listed.',
    raw VARIANT
      COMMENT 'Raw VARIANT of the original STIX relationship object.'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Silver-layer Delta table of MITRE ATT&CK STIX relationships: which groups, software and campaigns use which techniques and software, which mitigations and detection strategies cover which techniques, sub-technique and revoked-by links. source_ref / target_ref are STIX ids of attack_objects; the view attack_relationships_resolved shows them as ATT&CK ids and names.'
""")
print(f"Tables {OBJECTS_TABLE}, {RELATIONSHIPS_TABLE} ready")

# COMMAND ----------

# DBTITLE 1,Stream raw CDF into the silver tables
stream_cdf_to_silver(spark, RAW_TABLE, OBJECTS_TABLE, "stix_id", transform_objects,
                     f"{CHECKPOINTS}/objects")
stream_cdf_to_silver(spark, RAW_TABLE, RELATIONSHIPS_TABLE, "stix_id", transform_relationships,
                     f"{CHECKPOINTS}/relationships")

# COMMAND ----------

# DBTITLE 1,Per-type views
VIEW_COMMENTS = {
    "attack_techniques": "ATT&CK techniques and sub-techniques (attack-pattern objects) with tactics, platforms, the parent of a sub-technique, and revoked / deprecated flags.",
    "attack_tactics": "ATT&CK tactics (x-mitre-tactic objects); shortname matches attack_techniques.tactics.",
    "attack_groups": "ATT&CK groups (intrusion-set objects) with aliases.",
    "attack_software": "ATT&CK software: malware and tools, with aliases and platforms.",
    "attack_mitigations": "ATT&CK mitigations (course-of-action objects).",
    "attack_campaigns": "ATT&CK campaigns with aliases and first / last seen times.",
    "attack_relationships_resolved": "ATT&CK relationships with source and target as ATT&CK ids and names, e.g. G0016 uses T1059.",
}
for name, sql in view_sql(OBJECTS_TABLE, RELATIONSHIPS_TABLE).items():
    comment = VIEW_COMMENTS[name].replace("'", "''")
    spark.sql(f"CREATE OR REPLACE VIEW {table_name(p, name)} COMMENT '{comment}' AS {sql}")
print(f"views: {', '.join(VIEW_COMMENTS)}")
