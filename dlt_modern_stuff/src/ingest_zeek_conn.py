# Databricks notebook source
import dlt

# COMMAND ----------

import pyspark.sql.functions as F

from typing import Optional

# COMMAND ----------

from helpers import get_qualified_table_name, NETWORK_TABLE_NAME, create_normalized_sink, sanitize_string_for_flow_name

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We're using streaming tables + append flows to make sure that we can add more (or remove not used) source locations for this data type.

# COMMAND ----------

zeek_conn_table_name = get_qualified_table_name("silver", "zeek_conn", spark)
dlt.create_streaming_table(
    name=zeek_conn_table_name,
    cluster_by = ["timestamp"],
)

# COMMAND ----------

zeek_conn_schema_hints = "`id.orig_p` int, `id.resp_p` int, ts double"
zeek_conn_renames = {
    "id.orig_h": "id_origin_host", 
    "id.orig_p": "id_origin_port", 
    "id.resp_h": "id_response_host", 
    "id.resp_p": "id_response_port",
    "orig_bytes": "origin_bytes", 
    "resp_bytes": "response_bytes",
    "orig_pkts": "origin_packets",
    "orig_ip_bytes": "origin_ip_bytes", 
    "resp_pkts": "response_packets", 
    "resp_ip_bytes": "response_ip_bytes",
    "local_orig": "local_origin", 
    "local_resp": "local_response",
    "resp_l2_addr": "response_l2_address", 
    "orig_l2_addr": "origin_l2_address",
    "ts": "timestamp",
}

def create_zeek_conn_flow(input: str, add_opts: Optional[dict] = None):
    @dlt.append_flow(name=f"zeek_conn_{sanitize_string_for_flow_name(input)}", 
                 target=zeek_conn_table_name,
                 comment=f"Ingesting from {input}")
    def flow():
        autoloader_opts = {
            "cloudFiles.format": "json",
            "cloudFiles.schemaHints": zeek_conn_schema_hints,
            #"cloudFiles.useManagedFileEvents": "true",
        } | (add_opts or {})
        df = spark.readStream.format("cloudFiles").options(**autoloader_opts).load(input)
        df = df.withColumnsRenamed(zeek_conn_renames)
        df = df.withColumns({
            "timestamp": F.col("timestamp").cast("timestamp"),
            "ingest_time": F.current_timestamp(),
        })
        return df

# COMMAND ----------

zeek_conn_input = spark.conf.get("conf.zeek_conn_input")
# We're using input location as-is, but we can pass it as a list, and generate multiple flows from it
create_zeek_conn_flow(zeek_conn_input)

# COMMAND ----------

sink_name = create_normalized_sink(NETWORK_TABLE_NAME, spark=spark)

@dlt.append_flow(name="zeek_conn_normalized", target=sink_name)
def write_normalized():
    df = dlt.read_stream(zeek_conn_table_name)
    # This could be incomplete mapping, but we can improve later
    df = df.selectExpr(
        "99 as activity_id",
        "4 as category_uid",
        "4001 as class_uid",
        "timestamp as time",
        "99 as severity_id",
        "400199 as type_uid",
        "duration*1000 as duration",
        """named_struct(
  'ip', id_response_host,
  'port', id_response_port
) as dst_endpoint""",
        """named_struct(
  'product', 'zeek',
  'version', '1.0.0',
  'uid', uid,
  'processed_time', ingest_time
) as metadata""",
        """named_struct (
  'ip', id_origin_host,
  'port', id_origin_port
) as src_endpoint""",
"""named_struct(
'bytes_in', response_bytes,
'packets_in', response_packets,
'bytes_out', origin_bytes,
'packets_out', origin_packets,
'bytes_missed', missed_bytes,
'bytes', response_bytes + origin_bytes,
'packets', response_packets + origin_packets
) as traffic""",
"""named_struct(
  'direction_id', 0,
  'protocol_name', proto,
  'flag_history', history 
) as connection_info"""
    )
    return df
