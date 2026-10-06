-- Databricks notebook source
-- TODO: implement it as batch function, similar to tldextract
CREATE OR REPLACE FUNCTION community_id_hash(
  src_ip STRING,
  src_port INT,
  dst_ip STRING,
  dst_port INT,
  proto INT,
  seed INT
)
RETURNS STRING
LANGUAGE PYTHON
DETERMINISTIC
ENVIRONMENT (
      dependencies = '["communityid"]',
      environment_version = 'None'
    )
AS $$
import communityid

cid = communityid.CommunityID()
tpl = communityid.FlowTuple(proto, src_ip, dst_ip, src_port, dst_port)

return cid.calc(tpl)
$$;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import requests
-- MAGIC import json
-- MAGIC
-- MAGIC # Download the JSON file
-- MAGIC url = "https://raw.githubusercontent.com/corelight/community-id-spec/refs/heads/master/baseline/baseline_deflt.json"
-- MAGIC response = requests.get(url)
-- MAGIC data = response.json()
-- MAGIC
-- MAGIC # Prepare data for SQL
-- MAGIC rows = []
-- MAGIC for entry in data:
-- MAGIC     src_ip = entry["saddr"]
-- MAGIC     src_port = entry["sport"]
-- MAGIC     dst_ip = entry["daddr"]
-- MAGIC     dst_port = entry["dport"]
-- MAGIC     proto = entry["proto"]
-- MAGIC     seed = entry.get("seed", 0)
-- MAGIC     expected_id = entry["communityid"]
-- MAGIC     rows.append((src_ip, src_port, dst_ip, dst_port, proto, int(seed), expected_id))
-- MAGIC
-- MAGIC # Create DataFrame
-- MAGIC columns = ["src_ip", "src_port", "dst_ip", "dst_port", "proto", "seed", "expected_id"]
-- MAGIC df = spark.createDataFrame(rows, columns)
-- MAGIC df.createOrReplaceTempView("baseline_data")
-- MAGIC
-- MAGIC # Compute and compare community IDs using the UDF
-- MAGIC result = spark.sql("""
-- MAGIC SELECT
-- MAGIC   src_ip,
-- MAGIC   src_port,
-- MAGIC   dst_ip,
-- MAGIC   dst_port,
-- MAGIC   proto,
-- MAGIC   seed,
-- MAGIC   expected_id,
-- MAGIC   community_id_hash(src_ip, src_port, dst_ip, dst_port, proto, seed) AS computed_id,
-- MAGIC   CASE WHEN expected_id = community_id_hash(src_ip, src_port, dst_ip, dst_port, proto, seed) THEN 'MATCH' ELSE 'MISMATCH' END AS comparison
-- MAGIC FROM baseline_data
-- MAGIC """)
-- MAGIC display(result)
