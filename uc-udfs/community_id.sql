-- Databricks notebook source
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
PARAMETER STYLE PANDAS
HANDLER 'handler_func'
ENVIRONMENT (
  dependencies = '["communityid"]',
  environment_version = '5'
)
COMMENT 'Calculate the Community ID flow hash (https://github.com/corelight/community-id-spec). Returns NULL for invalid flows'
AS $$
import communityid
import pandas as pd
from typing import Iterator, Tuple

# CommunityID instances are cached per seed
cid_by_seed = {}

def get_cid(seed):
    cid = cid_by_seed.get(seed)
    if cid is None:
        cid = communityid.CommunityID(seed=seed)
        cid_by_seed[seed] = cid
    return cid

def to_int(val):
    return None if pd.isna(val) else int(val)

def calc_community_id(src_ip, src_port, dst_ip, dst_port, proto, seed):
    if pd.isna(src_ip) or pd.isna(dst_ip) or pd.isna(proto):
        return None
    try:
        tpl = communityid.FlowTuple(int(proto), src_ip, dst_ip, to_int(src_port), to_int(dst_port))
        return get_cid(to_int(seed) or 0).calc(tpl)
    except communityid.error.Error:
        return None

def handler_func(batch_iter: Iterator[Tuple[pd.Series, ...]]) -> Iterator[pd.Series]:
    for src_ip, src_port, dst_ip, dst_port, proto, seed in batch_iter:
        yield pd.Series([
            calc_community_id(*row)
            for row in zip(src_ip, src_port, dst_ip, dst_port, proto, seed)
        ])
$$;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import requests
-- MAGIC
-- MAGIC # Download the baseline files: default seed (0) and seed 1
-- MAGIC base_url = "https://raw.githubusercontent.com/corelight/community-id-spec/refs/heads/master/baseline"
-- MAGIC rows = []
-- MAGIC for file_name, seed in [("baseline_deflt.json", 0), ("baseline_seed1.json", 1)]:
-- MAGIC     for entry in requests.get(f"{base_url}/{file_name}").json():
-- MAGIC         rows.append((entry["saddr"], entry["sport"], entry["daddr"], entry["dport"],
-- MAGIC                      entry["proto"], seed, entry["communityid"]))
-- MAGIC
-- MAGIC # Create DataFrame
-- MAGIC schema = "src_ip string, src_port int, dst_ip string, dst_port int, proto int, seed int, expected_id string"
-- MAGIC df = spark.createDataFrame(rows, schema)
-- MAGIC df.createOrReplaceTempView("baseline_data")
-- MAGIC
-- MAGIC # Compute and compare community IDs using the UDF
-- MAGIC result = spark.sql("""
-- MAGIC SELECT *, CASE WHEN expected_id = computed_id THEN 'MATCH' ELSE 'MISMATCH' END AS comparison
-- MAGIC FROM (
-- MAGIC   SELECT *, community_id_hash(src_ip, src_port, dst_ip, dst_port, proto, seed) AS computed_id
-- MAGIC   FROM baseline_data
-- MAGIC )
-- MAGIC """)
-- MAGIC display(result)
