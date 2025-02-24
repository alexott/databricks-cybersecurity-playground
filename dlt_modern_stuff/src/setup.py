# Databricks notebook source
from helpers import *
import os

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog name")
dbutils.widgets.text("schema_name", "", "Schema name")
dbutils.widgets.text("volume_path", "", "UC Volume path for data")

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_path = dbutils.widgets.get("volume_path")

# COMMAND ----------

if not catalog_name or not schema_name or not volume_path:
    raise Exception("Catalog name, Schema name and UC Volume path must be provided")

# COMMAND ----------

for name in [HTTP_TABLE_NAME, NETWORK_TABLE_NAME]:
    table_name = get_normalized_table_name(name, catalog_name, schema_name)
    print(f"Creating table {table_name}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {table_name} (
        activity_id int,
        category_uid int,
        class_uid int,
        time timestamp
    )""")
    # spark.sql(f"TRUNCATE TABLE {table_name}")

# COMMAND ----------

base_path = os.path.join(volume_path, "logs")
for i in ["apache", "nginx", "zeek_conn", "zeek_http"]:
    p = os.path.join(base_path, i)
    os.makedirs(p, exist_ok=True)

# COMMAND ----------

import urllib.request

# Apache logs
urllib.request.urlretrieve("https://raw.githubusercontent.com/elastic/examples/refs/heads/master/Common%20Data%20Formats/apache_logs/apache_logs", 
                           os.path.join(base_path, "apache", "log1.txt"))
# Nginx logs
urllib.request.urlretrieve("https://raw.githubusercontent.com/elastic/examples/refs/heads/master/Common%20Data%20Formats/nginx_logs/nginx_logs", 
                           os.path.join(base_path, "nginx", "log1.txt"))
# Zeek HTTP logs
urllib.request.urlretrieve("https://raw.githubusercontent.com/ocsf/examples/refs/heads/main/raw_sample_log_dataset/Zeek/http.log", 
                           os.path.join(base_path, "zeek_http", "log1.txt"))
urllib.request.urlretrieve("https://raw.githubusercontent.com/lipyeow-lim/security-datasets01/refs/heads/main/maccdc-2012/03/http.log.gz", 
                           os.path.join(base_path, "zeek_http", "log2.gz"))
urllib.request.urlretrieve("https://raw.githubusercontent.com/lipyeow-lim/security-datasets01/refs/heads/main/maccdc-2012/12/http.log.gz", 
                           os.path.join(base_path, "zeek_http", "log3.gz"))
# Zeek Conn logs
urllib.request.urlretrieve("https://raw.githubusercontent.com/ocsf/examples/refs/heads/main/raw_sample_log_dataset/Zeek/conn.log", 
                           os.path.join(base_path, "zeek_conn", "log1.txt"))
urllib.request.urlretrieve("https://raw.githubusercontent.com/lipyeow-lim/security-datasets01/refs/heads/main/maccdc-2012/12/conn.log.gz", 
                           os.path.join(base_path, "zeek_conn", "log2.gz"))
urllib.request.urlretrieve("https://raw.githubusercontent.com/lipyeow-lim/security-datasets01/refs/heads/main/maccdc-2012/03/conn.log.gz", 
                           os.path.join(base_path, "zeek_conn", "log3.gz"))

# COMMAND ----------

table_name = get_normalized_table_name("iocs", catalog_name, schema_name)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {table_name} (
    ioc_type string, 
    ioc string
    ) COMMENT 'These are arbitrary IPs, not related to real IoCs - just for demo purposes'
          """)
# These are arbitrary IPs, not related to real IoCs - just for demo purposes
idf = spark.createDataFrame([['IPv4', '205.251.199.192'],
                             ['IPv4', '54.148.114.85'],
                             ['IPv4', '95.217.228.176'],
                             ['IPv4', '190.104.181.125'],
                             ], schema="ioc_type string, ioc string")
idf.write.mode("overwrite").saveAsTable(table_name)