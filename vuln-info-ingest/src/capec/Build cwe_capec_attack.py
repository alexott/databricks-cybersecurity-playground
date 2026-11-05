# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # cwe_capec_attack — CWE -> CAPEC -> ATT&CK edge table
# MAGIC Downloads the CAPEC catalog XML (~4 MB) and the Enterprise ATT&CK STIX bundle
# MAGIC (~55 MB, version-pinned by `attack_url`), and rebuilds `cwe_capec_attack` with
# MAGIC `CREATE OR REPLACE`: CWE weakness -> CAPEC attack pattern edges and CAPEC attack
# MAGIC pattern -> ATT&CK technique edges (`capec.helpers`). CAPEC has not been released
# MAGIC since 3.9 (2023-01-24), so this is a one-time job; re-run it after a new CAPEC
# MAGIC release or when moving to another ATT&CK version.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import get_params, table_name

p = get_params(dbutils, {"catalog": str, "schema": str, "capec_url": str, "attack_url": str})
TABLE = table_name(p, "cwe_capec_attack")

# COMMAND ----------

# DBTITLE 1,Download CAPEC and ATT&CK, build the edges
import collections, json
from capec.helpers import build_rows, parse_attack_techniques, parse_capec
from common.fetch import fetch_with_retries

def download(url: str) -> bytes:
    data = fetch_with_retries(url, strip=False)
    if data is None:
        raise RuntimeError(f"not found: {url}")
    return data

capec = parse_capec(download(p["capec_url"]))
attack = parse_attack_techniques(json.loads(download(p["attack_url"])))
rows = build_rows(capec, attack)
n_dropped = sum(1 for _, t in set(capec[3]) if t not in attack[1])
print(f"CAPEC {capec[0]} ({capec[1]}), ATT&CK {attack[0]}: {len(rows)} edges "
      f"{dict(collections.Counter(r[2] for r in rows))}; "
      f"{n_dropped} technique mappings not in Enterprise ATT&CK dropped")

# COMMAND ----------

# DBTITLE 1,Replace the table
spark.sql(f"""
  CREATE OR REPLACE TABLE {TABLE} (
    src_type STRING NOT NULL
      COMMENT 'Type of the edge source: ''CWE'' or ''CAPEC''.',
    src_id STRING NOT NULL
      COMMENT 'Source id: CWE-NNN (join to cwe_entries.cwe_id, cves_v5.cwe_ids) or CAPEC-NNN.',
    rel STRING NOT NULL
      COMMENT 'Edge type: ''related_attack_pattern'' (CWE -> CAPEC attack pattern that exploits the weakness) or ''maps_to_technique'' (CAPEC -> ATT&CK technique).',
    dst_type STRING NOT NULL
      COMMENT 'Type of the edge target: ''CAPEC'' or ''technique'' (Enterprise ATT&CK technique or sub-technique).',
    dst_id STRING NOT NULL
      COMMENT 'Target id: CAPEC-NNN or an ATT&CK technique id (T1059, T1059.001).',
    dst_name STRING
      COMMENT 'Target name: the CAPEC attack pattern name, or the ATT&CK technique name from the ATT&CK bundle (sub-techniques without the parent name).',
    source_version STRING
      COMMENT 'Versions the edge was built from, e.g. ''capec_3.9;attack_enterprise_19.1''.'
  )
  USING DELTA
  COMMENT 'Edge list linking weaknesses to attacker behavior: CWE weakness -> CAPEC attack pattern (related_attack_pattern) and CAPEC attack pattern -> MITRE ATT&CK Enterprise technique (maps_to_technique), from the CAPEC catalog (https://capec.mitre.org/) with technique names from ATT&CK STIX data. Join src_id/dst_id twice to go from a CVE''s CWE ids to ATT&CK techniques. CAPEC 3.9 (2023) maps to some techniques that later ATT&CK versions revoked; mappings to techniques outside Enterprise ATT&CK are dropped.'
""")
(spark.createDataFrame(rows, "src_type string, src_id string, rel string, dst_type string, "
                             "dst_id string, dst_name string, source_version string")
 .write.insertInto(TABLE, overwrite=True))
print(f"{TABLE}: {spark.table(TABLE).count()} rows")
