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
from atlas.transform import transform_objects, transform_relationships, view_sql

p = get_params(dbutils, COMMON_PARAMS)
RAW_TABLE = table_name(p, "atlas_raw")
OBJECTS_TABLE = table_name(p, "atlas_objects")
RELATIONSHIPS_TABLE = table_name(p, "atlas_relationships")
# Checkpoints live outside the loader's staging directory, so the per-run staging
# cleanup never destroys the streams' offsets.
CHECKPOINTS = volume_path(p, "atlas_checkpoints")

# COMMAND ----------

# DBTITLE 1,Create silver tables with column comments
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {OBJECTS_TABLE} (
    atlas_id STRING NOT NULL
      COMMENT 'ATLAS id: AML.TA0002 (tactic), AML.T0000 / AML.T0000.003 (technique / sub-technique), AML.M0011 (mitigation), AML.CS0002 (case study), AML.E0000 (entity), AML.S0000 (software), ATLAS-matrix. Primary key.',
    object_type STRING
      COMMENT 'tactic, technique, mitigation, case-study, entity, software or matrix.',
    name STRING
      COMMENT 'Object name.',
    description STRING
      COMMENT 'Description (Markdown; links to other ATLAS objects are relative paths).',
    platforms ARRAY<STRING>
      COMMENT 'Platforms a technique applies to (e.g. Enterprise); empty array when none.',
    maturity STRING
      COMMENT 'Technique maturity: how far the technique has been observed (e.g. Feasible, Demonstrated, Realized).',
    attack_id STRING
      COMMENT 'Corresponding MITRE ATT&CK id (`attack-reference`), e.g. T1596 for a technique, TA0043 for a tactic, M1044 for a mitigation; joins to attack_objects.attack_id. NULL when ATLAS-specific.',
    aliases ARRAY<STRING>
      COMMENT 'Alternative names of software; empty array when none.',
    created DATE
      COMMENT 'Date the object was created (`created-date`).',
    modified DATE
      COMMENT 'Date the object was last modified (`modified-date`).',
    url STRING
      COMMENT 'URL of the object on atlas.mitre.org; NULL for the matrix.',
    atlas_release STRING
      COMMENT 'ATLAS release (e.g. ''2026.10'') this version of the object was loaded from; an unchanged object keeps the earlier release.',
    date_removed TIMESTAMP
      COMMENT 'Load time at which the object first disappeared from the loaded release; NULL means currently listed. Filter date_removed IS NULL.',
    raw VARIANT
      COMMENT 'Raw VARIANT of the original ATLAS object (type-specific fields such as references, categories, lifecycle-phases, case-study actor and dates).'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Silver-layer Delta table of MITRE ATLAS (Adversarial Threat Landscape for AI Systems, https://atlas.mitre.org/) objects from atlas-data releases: tactics, techniques and sub-techniques against AI systems, mitigations, case studies of real attacks and exercises, entities (actors, reporters) and software. attack_id links to the corresponding ATT&CK object. The views atlas_techniques, atlas_tactics, atlas_mitigations, atlas_case_studies, atlas_software and atlas_entities give per-type columns; links are in atlas_relationships.'
""")
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {RELATIONSHIPS_TABLE} (
    relationship_id STRING NOT NULL
      COMMENT 'Synthetic key <source_id>|<relationship_type>|<target_id>|<step_id> (ATLAS relationships have no id). Primary key.',
    relationship_type STRING
      COMMENT 'achieves (technique -> tactic), specializes (sub-technique -> technique), mitigates (mitigation -> technique), employs (case study -> technique, one row per procedure step), uses (case study or entity -> software), implements (software -> technique), attributed-to and reported-by (case study -> entity), sequences (matrix -> tactic).',
    source_id STRING
      COMMENT 'ATLAS id of the source object (atlas_objects.atlas_id).',
    target_id STRING
      COMMENT 'ATLAS id of the target object (atlas_objects.atlas_id).',
    description STRING
      COMMENT 'How the source relates to the target, e.g. what the actor did in a case-study step.',
    tactic_id STRING
      COMMENT 'Tactic of a case-study step (`tactic`); NULL for other relationships.',
    step_id STRING
      COMMENT 'Id of the case-study procedure step (e.g. S02); NULL for other relationships.',
    leads_to ARRAY<STRING>
      COMMENT 'Step ids that follow this case-study step (`leads-to`); empty array otherwise.',
    atlas_release STRING
      COMMENT 'ATLAS release this version of the relationship was loaded from.',
    date_removed TIMESTAMP
      COMMENT 'Load time at which the relationship first disappeared from the loaded release; NULL means currently listed.',
    raw VARIANT
      COMMENT 'Raw VARIANT of the original relationship (also position for sequences, activity-purpose).'
  )
  USING DELTA
  TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
  COMMENT 'Silver-layer Delta table of MITRE ATLAS relationships: technique tactics and parents, mitigations, case-study procedure steps, software and entity links, and the tactic order of the matrix. The view atlas_relationships_resolved adds names and types of both ends.'
""")
print(f"Tables {OBJECTS_TABLE}, {RELATIONSHIPS_TABLE} ready")

# COMMAND ----------

# DBTITLE 1,Stream raw CDF into the silver tables
stream_cdf_to_silver(spark, RAW_TABLE, OBJECTS_TABLE, "atlas_id", transform_objects,
                     f"{CHECKPOINTS}/objects")
stream_cdf_to_silver(spark, RAW_TABLE, RELATIONSHIPS_TABLE, "atlas_id", transform_relationships,
                     f"{CHECKPOINTS}/relationships", silver_key="relationship_id")

# COMMAND ----------

# DBTITLE 1,Per-type views
VIEW_COMMENTS = {
    "atlas_techniques": "ATLAS techniques and sub-techniques with parent, tactics (from achieves relationships), platforms, maturity and the corresponding ATT&CK technique.",
    "atlas_tactics": "ATLAS tactics in matrix order (matrix_position) with the corresponding ATT&CK tactic.",
    "atlas_mitigations": "ATLAS mitigations with categories, AI lifecycle phases and the corresponding ATT&CK mitigation.",
    "atlas_case_studies": "ATLAS case studies (incidents and exercises) with type, actor, target, reporter and dates; their procedure steps are employs rows in atlas_relationships.",
    "atlas_software": "ATLAS software with type, categories and aliases.",
    "atlas_entities": "ATLAS entities (actors, teams, organizations) with their type.",
    "atlas_relationships_resolved": "ATLAS relationships with names and types of source and target.",
}
for name, sql in view_sql(OBJECTS_TABLE, RELATIONSHIPS_TABLE).items():
    comment = VIEW_COMMENTS[name].replace("'", "''")
    spark.sql(f"CREATE OR REPLACE VIEW {table_name(p, name)} COMMENT '{comment}' AS {sql}")
print(f"views: {', '.join(VIEW_COMMENTS)}")
