# Databricks notebook source
import dlt

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

from helpers import get_normalized_table_name, NETWORK_TABLE_NAME, HTTP_TABLE_NAME, get_qualified_table_name

# COMMAND ----------

detections_table_name = get_qualified_table_name("gold", "detections", spark)
dlt.create_streaming_table(
    name=detections_table_name,
    comment="Streaming table for detections"
)

# COMMAND ----------

# DBTITLE 1,Check network traffic for connections to known IoCs
@dlt.append_flow(
    target=detections_table_name,
)
def match_network_iocs():
    network_table = spark.readStream.table(get_normalized_table_name(NETWORK_TABLE_NAME))
    iocs_table = spark.read.table(get_normalized_table_name("iocs")).filter("ioc_type = 'IPv4'")
    matches = network_table.join(iocs_table, network_table.dst_endpoint.ip == iocs_table.ioc)
    matches = matches.selectExpr(
        "to_json(struct(*)) as details",
        "current_timestamp() as detection_time",
        "'network' as detection_source",
        "'ioc_match' as detection_type",
        "'warn' as detection_level"
    )
    return matches


# COMMAND ----------

# DBTITLE 1,Check HTTP logs for scan of admin pages from external IPs
@dlt.append_flow(
    target=detections_table_name,
)
def check_http_logs_admin_scan():
    http_table = spark.readStream.table(get_normalized_table_name(HTTP_TABLE_NAME))
    matches = http_table.filter("http_request.url like '/admin%' and not (src_endpoint.ip like '192.168.%' or src_endpoint.ip like '10.%')")
    matches = matches.selectExpr(
        "to_json(struct(*)) as details",
        "current_timestamp() as detection_time",
        "'http' as detection_source",
        "'http_admin_page_scan' as detection_type",
        "'info' as detection_level"
    )
    return matches


# COMMAND ----------

push_to_eventhub = spark.conf.get("conf.push_to_eventhubs", "false") == "true"
if push_to_eventhub:
    # name of EH namespace
    eh_ns = spark.conf.get("conf.eh_ns") 
    # name of a topic in EH namespace
    eh_topic = spark.conf.get("conf.eh_topic")     
    # Entra ID Tenant ID where service principal is created
    tenant_id = spark.conf.get("conf.azure_tenant_id")
    secret_scope = spark.conf.get("conf.secret_scope")
    sp_id_key_name = spark.conf.get("conf.sp_id_key_name")
    sp_secret_key_name = spark.conf.get("conf.sp_secret_key_name")
    client_id = dbutils.secrets.get(secret_scope, sp_id_key_name)  # Application ID of service principal
    client_secret = dbutils.secrets.get(secret_scope, sp_secret_key_name) # Client secret of service principal
    # fully qualified name of the Event Hubs server
    eh_server = eh_server = f"{eh_ns}.servicebus.windows.net"  
    # SASL config for Kafka to connect to Event Hubs
    sasl_config = f'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule' + \
      f' required clientId="{client_id}" clientSecret="{client_secret}"' + \
      f' scope="https://{eh_server}/.default" ssl.protocol="SSL";'
    # Callback class for OAuth authentication
    callback_class = "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler"
    # OAuth endpoint for Entra ID
    oauth_endpoint = f"https://login.microsoft.com/{tenant_id}/oauth2/v2.0/token"

    dlt.create_sink(
        "eventhubs",
        "kafka",
        { # Create Kafka options dictionary for connection with OAuth authentication
      "kafka.bootstrap.servers": f"{eh_server}:9093",
      "topic": eh_topic,
      "kafka.security.protocol": "SASL_SSL",
      "kafka.sasl.mechanism": "OAUTHBEARER",
      "kafka.sasl.jaas.config": sasl_config,
      "kafka.sasl.oauthbearer.token.endpoint.url": oauth_endpoint,
      "kafka.sasl.login.callback.handler.class": callback_class,
      "kafka.request.timeout.ms": "60000",
      "kafka.session.timeout.ms": "30000",
    }
    )

    @dlt.append_flow(name = "write_alerts", target = "eventhubs")
    def flowFunc():
        df = dlt.read_stream(detections_table_name)
        return df.select(F.to_json(F.struct("*")).alias("value"))