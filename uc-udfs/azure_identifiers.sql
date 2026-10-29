-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## User-defined functions to parse Azure resource IDs
-- MAGIC
-- MAGIC These SQL UDFs parse [Azure Resource Manager resource IDs](https://learn.microsoft.com/en-us/azure/azure-resource-manager/templates/template-functions-resource#resourceid)
-- MAGIC in pure SQL, without a Python sandbox.
-- MAGIC
-- MAGIC | Function | Result |
-- MAGIC |----------|--------|
-- MAGIC | `azure_resource_id_parse(id)` | `STRUCT<tenant_id, subscription_id, resource_group, management_group, provider_namespace, resource_type, resource_name, parent_id>` |
-- MAGIC | `azure_resource_id_normalize(id)` | the ID in lower case, without surrounding quotes and a trailing `/`: a join key |
-- MAGIC | `azure_resource_id_clean(id)` | the ID without surrounding spaces and quotes and a trailing `/` (the original case is kept) |
-- MAGIC | `azure_resource_id_parse_cleaned`, `azure_resource_id_parse_parts` | helpers, don't call them directly |
-- MAGIC
-- MAGIC Behavior:
-- MAGIC
-- MAGIC - An invalid value or `NULL` gives `NULL`. The functions don't raise errors.
-- MAGIC - These ID forms are supported: resource group, subscription, management group and tenant level resources, child resources (`.../virtualNetworks/vnet1/subnets/subnet1`),
-- MAGIC   extension resources (`<resource ID>/providers/Microsoft.Authorization/roleAssignments/<guid>`), scopes (`/subscriptions/<id>`, `/subscriptions/<id>/resourceGroups/<name>`, `/tenants/<id>`),
-- MAGIC   and IDs that end with a provider namespace (`/tenants/<id>/providers/Microsoft.aadiam` in Entra ID diagnostic logs).
-- MAGIC - The ID is split at the last `/providers/` that is followed by a provider namespace with a `.` (`Microsoft.Web`), so a resource can be named `providers`.
-- MAGIC   An ID with a namespace without `.` gives `NULL`.
-- MAGIC - `resource_type` is the provider namespace and all types (`Microsoft.Network/virtualNetworks/subnets`), and `resource_name` is the last name (`subnet1`).
-- MAGIC   For an ID that ends with a provider namespace, `resource_type` is the namespace and `resource_name` is `NULL`.
-- MAGIC - `parent_id` is the resource that is extended (extension resources), the parent resource (child resources), or the resource group, subscription or tenant scope.
-- MAGIC   It is `NULL` for subscriptions, management groups and tenant level resources: the ID doesn't encode the management group.
-- MAGIC - Azure resource IDs are case-insensitive, and logs use different case for the same ID (`resourceGroups`, `resourcegroups`, `RESOURCEGROUPS`).
-- MAGIC   The keywords are matched without case, and the values keep the original case. Use `azure_resource_id_normalize` to join IDs from different sources.
-- MAGIC - One pair of surrounding double quotes is removed: Entra ID audit logs have JSON-encoded strings in `modifiedProperties[].newValue`.
-- MAGIC - In Entra ID sign-in logs, `properties.resourceId` is the application ID of the resource (a GUID), not a resource ID, so it gives `NULL`.
-- MAGIC   IDs in URL-encoded `requestUri` filters are not extracted: use `url_decode` and `regexp_extract` first.
-- MAGIC
-- MAGIC Create all functions in the same schema: a function body finds the other functions in the schema of that function.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION azure_resource_id_clean(id STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Removes surrounding spaces, one pair of surrounding double quotes and trailing slashes from an Azure resource ID. The case is not changed'
  RETURN nullif(regexp_replace(regexp_replace(trim(id), '^"(.*)"$', '$1'), '/+$', ''), '');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION azure_resource_id_normalize(id STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Returns the Azure resource ID in lower case, without surrounding spaces and quotes and trailing slashes. Use it to join IDs from different sources'
  RETURN lower(azure_resource_id_clean(id));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION azure_resource_id_parse_parts(s STRING, prefix STRING, t ARRAY<STRING>)
  RETURNS STRUCT<tenant_id: STRING, subscription_id: STRING, resource_group: STRING, management_group: STRING,
                 provider_namespace: STRING, resource_type: STRING, resource_name: STRING, parent_id: STRING>
  DETERMINISTIC
  COMMENT 'Helper for azure_resource_id_parse: builds the result from the ID, the part before the last /providers/ and the segments after it'
  RETURN CASE
    WHEN (prefix = '' OR prefix RLIKE '(?i)^/(subscriptions|tenants|providers)/')
      AND size(t) % 2 = 1 AND NOT array_contains(t, '') THEN named_struct(
      'tenant_id', nullif(regexp_extract(s, '(?i)^/tenants/([^/]+)', 1), ''),
      'subscription_id', nullif(regexp_extract(s, '(?i)^/subscriptions/([^/]+)', 1), ''),
      'resource_group', nullif(regexp_extract(s, '(?i)^/subscriptions/[^/]+/resourceGroups/([^/]+)', 1), ''),
      'management_group', nullif(regexp_extract(s, r'(?i)^/providers/Microsoft\.Management/managementGroups/([^/]+)', 1), ''),
      'provider_namespace', get(t, 0),
      'resource_type', array_join(concat(array(get(t, 0)), filter(t, (x, i) -> i % 2 = 1)), '/'),
      'resource_name', if(size(t) = 1, NULL, element_at(t, -1)),
      'parent_id', if(size(t) >= 5, regexp_extract(s, '^(.*)/[^/]+/[^/]+$', 1), nullif(prefix, '')))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION azure_resource_id_parse_cleaned(s STRING)
  RETURNS STRUCT<tenant_id: STRING, subscription_id: STRING, resource_group: STRING, management_group: STRING,
                 provider_namespace: STRING, resource_type: STRING, resource_name: STRING, parent_id: STRING>
  DETERMINISTIC
  COMMENT 'Helper for azure_resource_id_parse: parses the result of azure_resource_id_clean'
  RETURN CASE
    WHEN s RLIKE '(?i)/providers/' THEN azure_resource_id_parse_parts(
      s,
      -- the last /providers/ that is followed by a namespace (Microsoft.Web): a resource can be named providers
      regexp_extract(s, r'(?i)^(.*)/providers/([^/]*\.[^/]*(/.*)?)$', 1),
      split(regexp_extract(s, r'(?i)^(.*)/providers/([^/]*\.[^/]*(/.*)?)$', 2), '/'))
    WHEN s RLIKE '(?i)^/subscriptions/[^/]+$' THEN named_struct(
      'tenant_id', CAST(NULL AS STRING),
      'subscription_id', regexp_extract(s, '(?i)^/subscriptions/([^/]+)', 1),
      'resource_group', CAST(NULL AS STRING),
      'management_group', CAST(NULL AS STRING),
      'provider_namespace', CAST(NULL AS STRING),
      'resource_type', 'Microsoft.Resources/subscriptions',
      'resource_name', regexp_extract(s, '(?i)^/subscriptions/([^/]+)', 1),
      'parent_id', CAST(NULL AS STRING))
    WHEN s RLIKE '(?i)^/subscriptions/[^/]+/resourceGroups/[^/]+$' THEN named_struct(
      'tenant_id', CAST(NULL AS STRING),
      'subscription_id', regexp_extract(s, '(?i)^/subscriptions/([^/]+)', 1),
      'resource_group', regexp_extract(s, '(?i)/resourceGroups/([^/]+)$', 1),
      'management_group', CAST(NULL AS STRING),
      'provider_namespace', CAST(NULL AS STRING),
      'resource_type', 'Microsoft.Resources/resourceGroups',
      'resource_name', regexp_extract(s, '(?i)/resourceGroups/([^/]+)$', 1),
      'parent_id', regexp_extract(s, '(?i)^(/subscriptions/[^/]+)', 1))
    WHEN s RLIKE '(?i)^/tenants/[^/]+$' THEN named_struct(
      'tenant_id', regexp_extract(s, '(?i)^/tenants/([^/]+)', 1),
      'subscription_id', CAST(NULL AS STRING),
      'resource_group', CAST(NULL AS STRING),
      'management_group', CAST(NULL AS STRING),
      'provider_namespace', CAST(NULL AS STRING),
      'resource_type', CAST(NULL AS STRING),
      'resource_name', CAST(NULL AS STRING),
      'parent_id', CAST(NULL AS STRING))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION azure_resource_id_parse(id STRING)
  RETURNS STRUCT<tenant_id: STRING, subscription_id: STRING, resource_group: STRING, management_group: STRING,
                 provider_namespace: STRING, resource_type: STRING, resource_name: STRING, parent_id: STRING>
  DETERMINISTIC
  COMMENT 'Parses an Azure resource ID into tenant_id, subscription_id, resource_group, management_group, provider_namespace, resource_type, resource_name and parent_id. Returns NULL for an invalid ID'
  RETURN azure_resource_id_parse_cleaned(azure_resource_id_clean(id));
