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

apache_web_table_name = get_qualified_table_name("silver", "apache_web", spark)
dlt.create_streaming_table(
    name=apache_web_table_name,
    comment="Table for data parsed from Apache HTTP server-compatible logs"
)

# COMMAND ----------

apache_web_regex = (
    r'^(\S+) (\S+) (\S+) \[(.+?)] "(\w+) (\S+) ([^"]+)" (\d{3}) (\d+|-) "(.+)" "(.+)"?$'
)


def read_apache_web(input: str, add_opts: Optional[dict] = None):
    autoloader_opts = {
        "cloudFiles.format": "text",
    } | (add_opts or {})
    df = spark.readStream.format("cloudFiles").options(**autoloader_opts).load(input)
    df = df.withColumns(
        {
            "host": F.regexp_extract("value", apache_web_regex, 1),
            "user": F.regexp_extract("value", apache_web_regex, 3),
            "timestamp": F.unix_timestamp(
                F.regexp_extract("value", apache_web_regex, 4), "dd/MMM/yyyy:HH:mm:ss Z"
            ).cast("timestamp"),
            "method": F.regexp_extract("value", apache_web_regex, 5),
            "path": F.regexp_extract("value", apache_web_regex, 6),
            "version": F.regexp_extract("value", apache_web_regex, 7),
            "code": F.regexp_extract("value", apache_web_regex, 8).cast("int"),
            "size": F.regexp_extract("value", apache_web_regex, 9).cast("long"),
            "referrer": F.regexp_extract("value", apache_web_regex, 10),
            "agent": F.regexp_extract("value", apache_web_regex, 11),
            "ingest_time": F.current_timestamp(),
        }
    )
    return df


def create_apache_web_flow(input: str, add_opts: Optional[dict] = None):
    @dlt.append_flow(
        name=f"apache_web_{sanitize_string_for_flow_name(input)}",
        target=apache_web_table_name,
        comment=f"Ingesting from {input}",
    )
    def flow():
        return read_apache_web(input, add_opts)

# COMMAND ----------

# DBTITLE 1,Handling of Apache Web logs
apache_web_input = spark.conf.get("conf.apache_web_input")
# We're using input location as-is, but we can pass it as a list, and generate multiple flows from it
create_apache_web_flow(apache_web_input)

# COMMAND ----------

# DBTITLE 1,Handling of NGINX logs (compatible with Apache Web)
nginx_input = spark.conf.get("conf.nginx_input")
# We're using input location as-is, but we can pass it as a list, and generate multiple flows from it
create_apache_web_flow(nginx_input)

# COMMAND ----------

sink_name = create_normalized_sink(HTTP_TABLE_NAME, spark=spark)

@dlt.append_flow(
    name="apache_web_normalized", 
    target=sink_name
)
def write_normalized():
    df = dlt.read_stream(apache_web_table_name)
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
  'ip', host
) as src_endpoint""",
        """named_struct(
  'http_method', method,
  'user_agent', agent,
  'version', version,
  'url', path,
  'referrer', referrer
) as http_request""",
        """named_struct(
  'code', code,
  'length', size
) as http_response""",
        """named_struct(
  'product', 'apache_web',
  'version', '1.0.0',
  'processed_time', ingest_time
) as metadata""",
    )
    return df