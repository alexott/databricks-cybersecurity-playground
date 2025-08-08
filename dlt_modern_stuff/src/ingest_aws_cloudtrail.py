# Databricks notebook source
import dlt

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from typing import Optional

import random
import datetime

# COMMAND ----------

from helpers import get_qualified_table_name, NETWORK_TABLE_NAME, create_normalized_sink, sanitize_string_for_flow_name

# COMMAND ----------

aws_cloudtrail_table_name = get_qualified_table_name("silver", "aws_cloudtrail", spark)
dlt.create_streaming_table(
    name=aws_cloudtrail_table_name,
    cluster_by = ["event_name", "event_time"],
    table_properties = {
        # These properties are necessary for Variant support
        "delta.minWriterVersion": "7",
        "delta.enableDeletionVectors": "true",
        "delta.minReaderVersion": "3",
        "delta.feature.variantType-preview": "supported",
        "delta.feature.deletionVectors": "supported",
    },
)

# COMMAND ----------

def normalize_aws_cloudtrail(df: DataFrame, raw_column_name: str = "_raw") -> DataFrame:
    df = df.selectExpr(f"{raw_column_name}:Records::array<variant> as _records")
    df = df.select(F.explode("_records").alias("_record"))
    df = df.selectExpr("*", "_record:resources::variant as resources")

    view_name = f"cloudtrail_{int(datetime.datetime.now().timestamp())}_{random.randint(0, 1000)}"
    df.createOrReplaceTempView(view_name)

    # TODO: rewrite to PySpark when we get support for `variant_explode_outer` in DLT
    # https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.tvf.TableValuedFunction.variant_explode_outer.html
    df = spark.sql(f"""SELECT
_record:awsRegion::string as aws_region,
_record:eventID::string as event_id,
_record:eventName::string as event_name,
_record:eventSource::string as event_source,
_record:eventType::string as event_type,
_record:eventTime::timestamp as event_time,
_record:eventVersion::string as event_version,
_record:recipientAccountId::string as recipient_account_id,
_record:requestID::string as request_id,
_record:requestParameters::variant as request_parameters,
_record:responseElements::variant as response_elements,
_record:responseElements.user.arn::string as response_user_arn,
_record:responseElements.role.arn::string as response_role_arn,
_record:responseElements.policy.arn::string as response_policy_arn,
resource.value:accountId::string as resources_account_id,
_record:sourceIPAddress::string as source_ip_address,
_record:userAgent::string as user_agent,
_record:userIdentity::variant as user_identity,
_record:userIdentity.type::string as user_identity_type,
_record:userIdentity.principalId::string as user_identity_principal,
_record:userIdentity.arn::string as user_identity_arn,
_record:userIdentity.accountId::string as user_identity_account_id,
_record:userIdentity.invokedBy::string as user_identity_invoked_by,
_record:userIdentity.accessKeyId::string as user_identity_access_ke,
_record:userIdentity.userName::string as user_identity_username,
_record:userIdentity.sessionContext.attributes.mfaAuthenticated::boolean as user_identity_session_context_attributes_mfa_authenticated,
_record:userIdentity.sessionContext.attributes.creationDate::string as user_identity_session_context_attributes_creation_date,
_record:userIdentity.sessionContext.sessionIssuer.type::string as user_identity_session_context_sesion_issuer_type,
_record:userIdentity.sessionContext.sessionIssuer.principalId::string as user_identity_session_context_sesion_issuer_principal_id,
_record:userIdentity.sessionContext.sessionIssuer.arn::string as user_identity_session_context_sesion_issuer_arn,
_record:userIdentity.sessionContext.sessionIssuer.accountId::string as user_identity_session_context_sesion_issuer_account_id,
_record:userIdentity.sessionContext.sessionIssuer.userName::string as user_identity_session_context_sesion_issuer_user_name,
_record:errorCode::string as error_code,
_record:errorMessage::string as error_message,
_record:additionalEventData::variant as additional_event_data,
_record:apiVersion::string as api_version,
_record:readOnly::boolean as read_only,
_record:serviceEventDetails::string as service_event_details,
_record:sharedEventId::string as shared_event_id,
_record:vpcEndpointId::string as vpc_endpoint_id,
_record, resource.value as resource
FROM {view_name}, LATERAL variant_explode_outer(resources) as resource""")

    return df

# COMMAND ----------

def read_aws_cloudtrail(input: str, add_opts: Optional[dict] = None) -> DataFrame:
    autoloader_opts = {
        "cloudFiles.format": "json",
        "singleVariantColumn": "_raw",
        #"cloudFiles.useManagedFileEvents": "true",
    } | (add_opts or {})
    df = spark.readStream.format("cloudFiles") \
        .options(**autoloader_opts).load(input)
    return normalize_aws_cloudtrail(df)

# COMMAND ----------

# #sdf = read_aws_cloudtrail("/Volumes/cybersecurity/logs/logs/aws-cloudtrail/")
# sdf = read_aws_cloudtrail("/Volumes/cybersecurity/logs/logs/demo/logs/aws_cloudtrail/")
# display(sdf)

# COMMAND ----------

def create_aws_cloudtrail_flow(input: str, add_opts: Optional[dict] = None):
    @dlt.append_flow(
        name=f"aws_cloudtrail_{sanitize_string_for_flow_name(input)}",
        target=aws_cloudtrail_table_name,
        comment=f"Ingesting from {input}",
    )
    def flow():
        return read_aws_cloudtrail(input, add_opts)

# COMMAND ----------

aws_cloudtrail_input = spark.conf.get("conf.aws_cloudtrail_input")
# We're using input location as-is, but we can pass it as a list, and generate multiple flows from it
create_aws_cloudtrail_flow(aws_cloudtrail_input)
