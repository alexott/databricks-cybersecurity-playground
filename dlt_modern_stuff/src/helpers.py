from pyspark.sql import SparkSession

from typing import Optional
import re

NETWORK_TABLE_NAME = "network"
HTTP_TABLE_NAME = "http"

__catalog_key_name__ = "conf.{}_catalog_name"
__schema_key_name__ = "conf.{}_schema_name"


def get_qualified_table_name(level: str, name: str, spark: Optional[SparkSession] = None) -> str:
    """Generates table name with catalog and schema if specified. 

    Args:
        level (str): The level of the table (silver, gold, bronze, ...).
        name (str): The name of the table on the given level.
        spark (Optional[SparkSession], optional): Spark session. Defaults to None.

    Raises:
        Exception: ValueError if schema is not specified when catalog is specified.

    Returns:
        str: The fully qualified table name with catalog and schema.
    """
    if not spark:
        spark = SparkSession.getActiveSession()
    catalog = spark.conf.get(__catalog_key_name__.format(level), "")
    schema = spark.conf.get(__schema_key_name__.format(level), "")
    if catalog and not schema:
        raise ValueError("Schema must be specified if catalog is specified")
    base = ""
    if catalog:
        base += f"{catalog}."
    if schema:
        base += f"{schema}."
    return f"{base}{name}"


def sanitize_string_for_flow_name(s: str) -> str:
    """Sanitize a string to be used as a flow/function name.
    
    Args:
        s (str): The string to be sanitized.
        
    Returns:
        str: The sanitized string.
    """
    return re.sub(r"[^a-zA-Z0-9]+", "_", s)[-20:].strip("_")


def get_normalized_table_name(name: str, catalog: Optional[str] = None, schema: Optional[str] = None, 
                              spark: Optional[SparkSession] = None) -> str:
    """Get the name for normalized (OCSF) table with catalog and schema.
    
    Args:
        name (str): The base name of the table.
        catalog (Optional[str], optional): The catalog name. Defaults to None.
        schema (Optional[str], optional): The schema name. Defaults to None.
        spark (Optional[SparkSession], optional): Spark session. Defaults to None.

    Raises:
        Exception: Exception if catalog or schema are not specified.
        
    Returns:
        str: The normalized table name        
    """
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
    """Create a DLT sink with a normalized name.

    Args:
        name (str): The base name for the sink.
        spark (Optional[SparkSession], optional): Spark session. Defaults to None.

    Returns:
        str: The name of the created sink.
    """
    import dlt
    sink_name = f"{name}_ocsf"
    table_name = get_normalized_table_name(name, spark=spark)
    dlt.create_sink(sink_name, "delta", { "tableName": table_name, "mergeSchema": "true" })
    return sink_name

