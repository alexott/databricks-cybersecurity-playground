# Databricks notebook source
import dlt

# COMMAND ----------

import pyspark.sql.functions as F

from typing import Optional

# COMMAND ----------

from helpers import HTTP_TABLE_NAME, get_qualified_table_name, create_normalized_sink, sanitize_string_for_flow_name

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We're using streaming tables + append flows to make sure that we can add more (or remove not used) source locations for this data type.

# COMMAND ----------

zeek_http_table_name = get_qualified_table_name("silver", "zeek_http", spark)
dlt.create_streaming_table(
    name = zeek_http_table_name,
)

# COMMAND ----------

zeek_http_schema_hints = "`id.orig_p` int, `id.resp_p` int, ts double, status_code int"
zeek_http_renames = {
    "id.orig_h": "id_origin_host", 
    "id.orig_p": "id_origin_port", 
    "id.resp_h": "id_response_host", 
    "id.resp_p": "id_response_port",
    "ts": "timestamp",
}

def create_zeek_http_flow(input: str, add_opts: Optional[dict] = None):
    @dlt.append_flow(name=f"zeek_http_{sanitize_string_for_flow_name(input)}", 
                 target = zeek_http_table_name,
                 comment = f"Ingesting from {input}")
    def flow():
        autoloader_opts = {
            "cloudFiles.format": "json",
            "cloudFiles.schemaHints": zeek_http_schema_hints,
        } | (add_opts or {})
        df = spark.readStream.format("cloudFiles").options(**autoloader_opts).load(input)
        df = df.withColumnsRenamed(zeek_http_renames)
        df = df.withColumns({
            "timestamp": F.col("timestamp").cast("timestamp"),
            "ingest_time": F.current_timestamp(),
        })
        return df

# COMMAND ----------

# zeek_checkpoint_loc = "/tmp/zeek_conn_checkpoint"
# #dbutils.fs.rm(zeek_checkpoint_loc, True)
# df = read_zeek_conn("/Volumes/cybersecurity/logs/logs/zeek/conn_2023-03-15-*.json.gz", {"cloudFiles.schemaLocation": zeek_checkpoint_loc})
# display(df)

# COMMAND ----------

zeek_http_input = spark.conf.get("conf.zeek_http_input")
# We're using input location as-is, but we can pass it as a list, and generate multiple flows from it
create_zeek_http_flow(zeek_http_input)

# COMMAND ----------

# DBTITLE 1,Convert into the normalized OCSF format
sink_name = create_normalized_sink(HTTP_TABLE_NAME, spark=spark)

@dlt.append_flow(name="zeek_http_normalized", target=sink_name)
def write_normalized():
    df = dlt.read_stream(zeek_http_table_name)
    # This could be incomplete mapping, but we can improve later
    df = df.selectExpr(
        "99 as activity_id",
        "4 as category_uid",
        "4002 as class_uid",
        "timestamp as time",
        "99 as severity_id",
        "400299 as type_uid",
        """named_struct(
  'hostname', host,
  'ip', id_response_host,
  'port', id_response_port
) as dst_endpoint""",
        """named_struct(
  'http_method', method,
  'user_agent', user_agent,
  'version', `version`,
  'url', uri
) as http_request""",
        """named_struct(
  'code', status_code
) as http_response""",
        """named_struct(
  'product', 'zeek',
  'version', '1.0.0',
  'uid', uid,
  'processed_time', ingest_time
) as metadata""",
        """named_struct (
  'ip', `id_origin_host`,
  'port', `id_origin_port`
) as src_endpoint""",
    )
    return df