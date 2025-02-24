from pyspark.sql import SparkSession, DataFrame

from typing import Optional
import re

NETWORK_TABLE_NAME = "network"
HTTP_TABLE_NAME = "http"

__catalog_key_name__ = "conf.{}_catalog_name"
__schema_key_name__ = "conf.{}_schema_name"


def get_qualified_table_name(level: str, name: str, spark: Optional[SparkSession] = None) -> str:
    if not spark:
        spark = SparkSession.getActiveSession()
    catalog = spark.conf.get(__catalog_key_name__.format(level), "")
    schema = spark.conf.get(__schema_key_name__.format(level), "")
    if catalog and not schema:
        raise Exception("Schema must be specified if catalog is specified")
    base = ""
    if catalog:
        base += f"{catalog}."
    if schema:
        base += f"{schema}."
    return f"{base}{name}"


def sanitize_string_for_flow_name(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", s)[-20:].strip("_")


def get_normalized_table_name(name: str, catalog: Optional[str] = None, schema: Optional[str] = None, 
                              spark: Optional[SparkSession] = None) -> str:
    if not spark:
        spark = SparkSession.getActiveSession()
    if not catalog:
        catalog = spark.conf.get(__catalog_key_name__.format("gold"), "")
    if not schema:
        schema = spark.conf.get(__schema_key_name__.format("gold"), "")
    if not catalog or not schema:
        raise Exception("Catalog and Schema must be specified explicitly or in Spark conf")
    return f"{catalog}.{schema}.{name}"


def create_normalized_sink(name: str, spark: Optional[SparkSession] = None) -> str:
    import dlt
    sink_name = f"{name}_ocsf"
    table_name = get_normalized_table_name(name, spark=spark)
    dlt.create_sink(sink_name, "delta", { "tableName": table_name, "mergeSchema": "true" })
    return sink_name

