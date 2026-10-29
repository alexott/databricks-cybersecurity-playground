-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Cross-cloud user-defined functions: storage URIs and OCSF
-- MAGIC
-- MAGIC | Function | Result |
-- MAGIC |----------|--------|
-- MAGIC | `storage_uri_parse(uri)` | `STRUCT<cloud, scheme, account, container, path>` for `s3://`, `s3a://`, `s3n://`, `gs://`, `abfss://`, `abfs://`, `wasbs://`, `wasb://` URIs |
-- MAGIC | `cloud_resource_to_ocsf(id)` | `STRUCT<cloud, resource>` in the shape of the OCSF [`cloud`](https://schema.ocsf.io/objects/cloud) and [`resource_details`](https://schema.ocsf.io/objects/resource_details) objects, for an AWS ARN, an Azure resource ID or a GCP resource name |
-- MAGIC | `storage_uri_parse_trimmed`, `cloud_resource_ocsf_aws`, `cloud_resource_ocsf_azure`, `cloud_resource_ocsf_gcp_zoned`, `cloud_resource_ocsf_gcp` | helpers, don't call them directly |
-- MAGIC
-- MAGIC This file uses the functions from `aws_identifiers.sql`, `azure_identifiers.sql` and `gcp_identifiers.sql`. Create them first, in the same schema.
-- MAGIC
-- MAGIC Behavior of `storage_uri_parse`:
-- MAGIC
-- MAGIC - `cloud` is `aws`, `azure` or `gcp`. `account` is the Azure storage account (`NULL` for S3 and GCS). `container` is the bucket or container.
-- MAGIC   `path` has no leading `/`, and is `NULL` if empty.
-- MAGIC - Azure hosts are `<account>.dfs.core.<suffix>` or `<account>.blob.core.<suffix>`, so sovereign clouds (`core.chinacloudapi.cn`, `core.usgovcloudapi.net`) work.
-- MAGIC - Other values (`dbfs:/`, `/Volumes/...`, HTTPS URLs) give `NULL`.
-- MAGIC
-- MAGIC Behavior of `cloud_resource_to_ocsf`:
-- MAGIC
-- MAGIC - The format is detected from the value: `arn:` is AWS; `/subscriptions/`, `/tenants/` and `/providers/` are Azure;
-- MAGIC   `//`, `https://...googleapis.com/`, `projects/`, `folders/` and `organizations/` are GCP. Other values and invalid identifiers give `NULL`.
-- MAGIC - `cloud.provider` is `AWS`, `Azure` or `GCP`. `cloud.account` is the AWS account (`type_id` 10), the Azure subscription (13) or the GCP project (11),
-- MAGIC   and `NULL` if the identifier doesn't have one. `cloud.org` is the Azure tenant, or the GCP organization and folder.
-- MAGIC - `resource.type` is `service:resource_type` for AWS (`iam:role`), the resource type for Azure (`Microsoft.Compute/virtualMachines`),
-- MAGIC   and `service/resource_type` for GCP (`compute.googleapis.com/instances`). `resource.group.name` is the Azure resource group.
-- MAGIC - For a GCP zone (`zones/us-central1-a`, or `locations/us-central1-a` for zonal GKE clusters), the region is the zone without the last part (`us-central1`). Azure resource IDs don't have a region.
-- MAGIC - The struct has only the fields that an identifier can give. Use its fields when you build an OCSF row.
-- MAGIC
-- MAGIC Create all functions in the same schema: a function body finds the other functions in the schema of that function.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION storage_uri_parse_trimmed(uri STRING)
  RETURNS STRUCT<cloud: STRING, scheme: STRING, account: STRING, container: STRING, path: STRING>
  DETERMINISTIC
  COMMENT 'Helper for storage_uri_parse: parses the trimmed URI'
  RETURN CASE
    WHEN uri RLIKE '(?i)^(s3[an]?|gs)://[^/@]+(/|$)' THEN named_struct(
      'cloud', if(lower(regexp_extract(uri, '^([^:]+)://', 1)) = 'gs', 'gcp', 'aws'),
      'scheme', lower(regexp_extract(uri, '^([^:]+)://', 1)),
      'account', CAST(NULL AS STRING),
      'container', regexp_extract(uri, '^[^:]+://([^/]+)', 1),
      'path', nullif(regexp_extract(uri, '^[^:]+://[^/]+/?(.*)$', 1), ''))
    WHEN uri RLIKE r'(?i)^(abfss?|wasbs?)://[^/@]+@[^/.]+\.(dfs|blob)\.core\.[^/]+(/|$)' THEN named_struct(
      'cloud', 'azure',
      'scheme', lower(regexp_extract(uri, '^([^:]+)://', 1)),
      'account', regexp_extract(uri, '@([^/.]+)[.]', 1),
      'container', regexp_extract(uri, '^[^:]+://([^/@]+)@', 1),
      'path', nullif(regexp_extract(uri, '^[^:]+://[^/]+/?(.*)$', 1), ''))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION storage_uri_parse(uri STRING)
  RETURNS STRUCT<cloud: STRING, scheme: STRING, account: STRING, container: STRING, path: STRING>
  DETERMINISTIC
  COMMENT 'Parses a cloud storage URI (s3://, s3a://, s3n://, gs://, abfss://, abfs://, wasbs://, wasb://) into cloud, scheme, account, container and path. Returns NULL for other values'
  RETURN storage_uri_parse_trimmed(trim(uri));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION cloud_resource_ocsf_aws(a STRUCT<`partition`: STRING, service: STRING, region: STRING, account_id: STRING, resource: STRING, resource_type: STRING, resource_id: STRING>)
  RETURNS STRUCT<cloud: STRUCT<provider: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                               account: STRUCT<uid: STRING, type: STRING, type_id: INT>, org: STRUCT<uid: STRING, ou_uid: STRING>>,
                 resource: STRUCT<uid: STRING, name: STRING, type: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                                  `group`: STRUCT<name: STRING>>>
  DETERMINISTIC
  COMMENT 'Helper for cloud_resource_to_ocsf: maps the result of aws_arn_parse to the OCSF cloud and resource_details fields'
  RETURN CASE
    WHEN a IS NOT NULL THEN named_struct(
      'cloud', named_struct(
        'provider', 'AWS',
        'region', a.region,
        'zone', CAST(NULL AS STRING),
        'cloud_partition', a.`partition`,
        'account', CASE WHEN a.account_id RLIKE '^[0-9]{12}$' THEN named_struct('uid', a.account_id, 'type', 'AWS Account', 'type_id', 10) END,
        'org', CAST(NULL AS STRUCT<uid: STRING, ou_uid: STRING>)),
      'resource', named_struct(
        'uid', concat('arn:', a.`partition`, ':', a.service, ':', coalesce(a.region, ''), ':', coalesce(a.account_id, ''), ':', a.resource),
        'name', CASE
          WHEN a.service = 'iam' AND a.resource_type IN ('user', 'group', 'role', 'policy', 'instance-profile', 'server-certificate')
            THEN element_at(split(a.resource_id, '/'), -1)
          ELSE a.resource_id
        END,
        'type', if(a.resource_type IS NULL, a.service, concat(a.service, ':', a.resource_type)),
        'region', a.region,
        'zone', CAST(NULL AS STRING),
        'cloud_partition', a.`partition`,
        'group', CAST(NULL AS STRUCT<name: STRING>)))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION cloud_resource_ocsf_azure(id STRING, z STRUCT<tenant_id: STRING, subscription_id: STRING, resource_group: STRING, management_group: STRING,
                                                                         provider_namespace: STRING, resource_type: STRING, resource_name: STRING, parent_id: STRING>)
  RETURNS STRUCT<cloud: STRUCT<provider: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                               account: STRUCT<uid: STRING, type: STRING, type_id: INT>, org: STRUCT<uid: STRING, ou_uid: STRING>>,
                 resource: STRUCT<uid: STRING, name: STRING, type: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                                  `group`: STRUCT<name: STRING>>>
  DETERMINISTIC
  COMMENT 'Helper for cloud_resource_to_ocsf: maps the cleaned Azure resource ID and the result of azure_resource_id_parse to the OCSF cloud and resource_details fields'
  RETURN CASE
    WHEN z IS NOT NULL THEN named_struct(
      'cloud', named_struct(
        'provider', 'Azure',
        'region', CAST(NULL AS STRING),
        'zone', CAST(NULL AS STRING),
        'cloud_partition', CAST(NULL AS STRING),
        'account', CASE WHEN z.subscription_id IS NOT NULL THEN named_struct('uid', z.subscription_id, 'type', 'Azure Subscription', 'type_id', 13) END,
        'org', CASE WHEN z.tenant_id IS NOT NULL THEN named_struct('uid', z.tenant_id, 'ou_uid', CAST(NULL AS STRING)) END),
      'resource', named_struct(
        'uid', id,
        'name', z.resource_name,
        'type', z.resource_type,
        'region', CAST(NULL AS STRING),
        'zone', CAST(NULL AS STRING),
        'cloud_partition', CAST(NULL AS STRING),
        'group', CASE WHEN z.resource_group IS NOT NULL THEN named_struct('name', z.resource_group) END))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION cloud_resource_ocsf_gcp_zoned(g STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING,
                                                          resource_name: STRING, parent: STRING, segments: MAP<STRING, STRING>>,
                                                          zone STRING)
  RETURNS STRUCT<cloud: STRUCT<provider: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                               account: STRUCT<uid: STRING, type: STRING, type_id: INT>, org: STRUCT<uid: STRING, ou_uid: STRING>>,
                 resource: STRUCT<uid: STRING, name: STRING, type: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                                  `group`: STRUCT<name: STRING>>>
  DETERMINISTIC
  COMMENT 'Helper for cloud_resource_to_ocsf: maps the result of gcp_resource_name_parse and its zone to the OCSF cloud and resource_details fields'
  RETURN CASE
    WHEN g IS NOT NULL THEN named_struct(
      'cloud', named_struct(
        'provider', 'GCP',
        'region', if(zone IS NULL, g.location, nullif(regexp_extract(zone, '^(.*)-[^-]+$', 1), '')),
        'zone', zone,
        'cloud_partition', CAST(NULL AS STRING),
        'account', CASE WHEN g.project IS NOT NULL THEN named_struct('uid', g.project, 'type', 'GCP Project', 'type_id', 11) END,
        'org', CASE
          WHEN try_element_at(g.segments, 'organizations') IS NOT NULL OR try_element_at(g.segments, 'folders') IS NOT NULL
            THEN named_struct('uid', try_element_at(g.segments, 'organizations'), 'ou_uid', try_element_at(g.segments, 'folders'))
        END),
      'resource', named_struct(
        'uid', if(g.service IS NULL, g.relative_name, concat('//', g.service, '/', g.relative_name)),
        'name', g.resource_name,
        'type', if(g.service IS NULL, g.resource_type, concat(g.service, '/', g.resource_type)),
        'region', if(zone IS NULL, g.location, nullif(regexp_extract(zone, '^(.*)-[^-]+$', 1), '')),
        'zone', zone,
        'cloud_partition', CAST(NULL AS STRING),
        'group', CAST(NULL AS STRUCT<name: STRING>)))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION cloud_resource_ocsf_gcp(g STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING,
                                                          resource_name: STRING, parent: STRING, segments: MAP<STRING, STRING>>)
  RETURNS STRUCT<cloud: STRUCT<provider: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                               account: STRUCT<uid: STRING, type: STRING, type_id: INT>, org: STRUCT<uid: STRING, ou_uid: STRING>>,
                 resource: STRUCT<uid: STRING, name: STRING, type: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                                  `group`: STRUCT<name: STRING>>>
  DETERMINISTIC
  COMMENT 'Helper for cloud_resource_to_ocsf: maps the result of gcp_resource_name_parse to the OCSF cloud and resource_details fields'
  -- the zone is the zones value, or a locations value that is a zone (us-central1-a, for zonal GKE clusters)
  RETURN cloud_resource_ocsf_gcp_zoned(g, coalesce(
    try_element_at(g.segments, 'zones'),
    CASE WHEN try_element_at(g.segments, 'locations') RLIKE '[0-9]-[a-z]$' THEN try_element_at(g.segments, 'locations') END));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION cloud_resource_to_ocsf(id STRING)
  RETURNS STRUCT<cloud: STRUCT<provider: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                               account: STRUCT<uid: STRING, type: STRING, type_id: INT>, org: STRUCT<uid: STRING, ou_uid: STRING>>,
                 resource: STRUCT<uid: STRING, name: STRING, type: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                                  `group`: STRUCT<name: STRING>>>
  DETERMINISTIC
  COMMENT 'Maps an AWS ARN, an Azure resource ID or a GCP resource name to the OCSF cloud and resource_details fields (provider, account, org, region, zone, resource uid, name, type). Returns NULL for other values'
  RETURN CASE
    WHEN trim(id) LIKE 'arn:%' THEN cloud_resource_ocsf_aws(aws_arn_parse(id))
    WHEN trim(id) RLIKE '(?i)^"?/(subscriptions|tenants|providers)/' THEN cloud_resource_ocsf_azure(azure_resource_id_clean(id), azure_resource_id_parse(id))
    WHEN trim(id) RLIKE r'^(//|https://[^/]*googleapis\.com/|(projects|folders|organizations)/)' THEN cloud_resource_ocsf_gcp(gcp_resource_name_parse(id))
  END;
