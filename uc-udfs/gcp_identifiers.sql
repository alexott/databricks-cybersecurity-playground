-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## User-defined functions to parse GCP identifiers
-- MAGIC
-- MAGIC These SQL UDFs parse [GCP resource names](https://google.aip.dev/122), [IAM principal identifiers](https://cloud.google.com/iam/docs/principal-identifiers)
-- MAGIC and [service account emails](https://cloud.google.com/iam/docs/service-account-types) in pure SQL, without a Python sandbox.
-- MAGIC
-- MAGIC | Function | Result |
-- MAGIC |----------|--------|
-- MAGIC | `gcp_resource_name_parse(name)` | `STRUCT<service, relative_name, project, location, resource_type, resource_name, parent, segments MAP<STRING,STRING>>` |
-- MAGIC | `gcp_principal_parse(member)` | `STRUCT<principal_type, identity, domain, pool, deleted BOOLEAN, uid>` |
-- MAGIC | `gcp_service_account_parse(email)` | `STRUCT<account_type, project_id, project_number, service>` |
-- MAGIC | `gcp_resource_name_parse_trimmed`, `gcp_resource_name_parse_relative`, `gcp_resource_name_parse_segments`, `gcp_resource_name_build_checked`, `gcp_resource_name_build`, `gcp_principal_parse_core`, `gcp_principal_build`, `gcp_service_account_parse_email` | helpers, don't call them directly |
-- MAGIC
-- MAGIC Behavior of `gcp_resource_name_parse`:
-- MAGIC
-- MAGIC - It takes full resource names (`//compute.googleapis.com/projects/p/zones/z/instances/i`, Cloud Asset Inventory),
-- MAGIC   resource URIs and selfLinks (`https://www.googleapis.com/compute/v1/projects/p/zones/z/instances/i`), and relative names (`projects/p/zones/z/instances/i`, audit log `protoPayload.resourceName`).
-- MAGIC   For a relative name, `service` is `NULL`: use `coalesce` with `protoPayload.serviceName`.
-- MAGIC - `segments` maps each collection to its ID (`{"projects":"p","zones":"z","instances":"i"}`). `resource_type` is the last collection, `resource_name` is the last ID,
-- MAGIC   and `parent` is the relative name without the last collection and ID. `location` is the `locations`, `zones` or `regions` value, or `global`.
-- MAGIC - `service` and `relative_name` are a join key between Cloud Asset Inventory, selfLinks and audit logs. Cloud Storage selfLinks (`/storage/v1/b/B/o/O`) use the collections `b` and `o`, so they don't match.
-- MAGIC - `global` without an ID (`projects/p/global/networks/n`) gives `location` `global`, and it is not in `segments` and `parent`. `relative_name` keeps it.
-- MAGIC - The projects `-` and `_` (`projects/-/serviceAccounts/...`, `projects/_/buckets/...`) give `project` `NULL`. `segments` keeps them.
-- MAGIC - The Cloud Storage bucket name `//storage.googleapis.com/BUCKET` becomes `projects/_/buckets/BUCKET`, the same as in audit logs. An object name can have `/` (`.../objects/dir/file.txt`).
-- MAGIC - A value with a collection and no ID, a duplicate collection, or an empty segment gives `NULL`.
-- MAGIC
-- MAGIC Behavior of `gcp_principal_parse`:
-- MAGIC
-- MAGIC - `principal_type` is one of `user`, `service_account`, `group`, `domain`, `all_users`, `all_authenticated_users`, `all_principals`, `cloud_identity_customer`,
-- MAGIC   `service_account_set`, `service_agent_set`, `workforce_identity`, `workforce_identity_set`, `workload_identity`, `workload_identity_set`, `gke_workload`, `agent_identity`,
-- MAGIC   `principal` and `principal_set` (other `principal://` and `principalSet://` identifiers). An unknown value gives `NULL`.
-- MAGIC - The allow policy form (`user:alex@example.com`) and the deny policy form (`principal://goog/subject/alex@example.com`) give the same result.
-- MAGIC - `identity` is the email, domain, subject or attribute. `domain` is the part after `@` for users, service accounts and groups. `pool` is the workforce or workload identity pool ID.
-- MAGIC - Deleted principals (`deleted:user:alex@example.com?uid=123`) give `deleted` `true` and `uid`.
-- MAGIC
-- MAGIC Behavior of `gcp_service_account_parse`:
-- MAGIC
-- MAGIC - `account_type` is `user_managed` (`NAME@PROJECT_ID.iam.gserviceaccount.com`), `default_app_engine` (`PROJECT_ID@appspot.gserviceaccount.com`),
-- MAGIC   `default_compute` (`PROJECT_NUMBER-compute@developer.gserviceaccount.com`), or `service_agent` (`service-PROJECT_NUMBER@gcp-sa-SERVICE.iam.gserviceaccount.com`, `service-PROJECT_NUMBER@compute-system.iam.gserviceaccount.com`,
-- MAGIC   any `@gcp-sa-SERVICE.iam.gserviceaccount.com`, and other `*.gserviceaccount.com` domains).
-- MAGIC   Google says that these formats can change.
-- MAGIC - A `serviceAccount:` prefix is removed. A value that isn't a service account email gives `NULL`.
-- MAGIC
-- MAGIC Create all functions in the same schema: a function body finds the other functions in the schema of that function.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_resource_name_build(service STRING, relative_name STRING, segs ARRAY<STRING>, has_global BOOLEAN, m MAP<STRING, STRING>)
  RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING,
                 parent: STRING, segments: MAP<STRING, STRING>>
  DETERMINISTIC
  COMMENT 'Helper for gcp_resource_name_parse: builds the result from the collection/ID segments and their map'
  RETURN named_struct(
    'service', service,
    'relative_name', relative_name,
    'project', CASE WHEN try_element_at(m, 'projects') NOT IN ('-', '_') THEN try_element_at(m, 'projects') END,
    'location', coalesce(try_element_at(m, 'locations'), try_element_at(m, 'zones'), try_element_at(m, 'regions'), if(has_global, 'global', NULL)),
    'resource_type', element_at(segs, -2),
    'resource_name', element_at(segs, -1),
    'parent', if(size(segs) > 2, array_join(slice(segs, 1, size(segs) - 2), '/'), NULL),
    'segments', m);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_resource_name_build_checked(service STRING, relative_name STRING, segs ARRAY<STRING>, has_global BOOLEAN, valid BOOLEAN)
  RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING,
                 parent: STRING, segments: MAP<STRING, STRING>>
  DETERMINISTIC
  COMMENT 'Helper for gcp_resource_name_parse: builds the result if the segments are valid'
  -- the arguments of a function call are computed also when the CASE branch is not used,
  -- so the map gets empty arrays for invalid segments: map_from_arrays fails on different lengths and duplicate keys
  RETURN CASE
    WHEN valid THEN gcp_resource_name_build(service, relative_name, segs, has_global,
      map_from_arrays(filter(segs, (x, i) -> valid AND i % 2 = 0), filter(segs, (x, i) -> valid AND i % 2 = 1)))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_resource_name_parse_segments(service STRING, relative_name STRING, segs ARRAY<STRING>, has_global BOOLEAN)
  RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING,
                 parent: STRING, segments: MAP<STRING, STRING>>
  DETERMINISTIC
  COMMENT 'Helper for gcp_resource_name_parse: checks the collection/ID segments'
  RETURN gcp_resource_name_build_checked(service, relative_name, segs, has_global,
    size(segs) % 2 = 0 AND NOT array_contains(segs, '')
      AND size(array_distinct(filter(segs, (x, i) -> i % 2 = 0))) = size(segs) DIV 2
      AND get(segs, 0) RLIKE '^[a-zA-Z][a-zA-Z0-9]*$');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_resource_name_parse_relative(service STRING, relative_name STRING)
  RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING,
                 parent: STRING, segments: MAP<STRING, STRING>>
  DETERMINISTIC
  COMMENT 'Helper for gcp_resource_name_parse: splits the relative name into collection/ID segments'
  RETURN gcp_resource_name_parse_segments(
    service,
    relative_name,
    CASE
      -- a Cloud Storage object name can have /
      WHEN regexp_replace(relative_name, '^(projects/[^/]+)/global(/|$)', '$1$2') RLIKE '(^|/)buckets/[^/]+/objects/.'
        THEN concat(
          split(regexp_extract(relative_name, '^(.*?/objects)/(.*)$', 1), '/'),
          array(regexp_extract(relative_name, '^(.*?/objects)/(.*)$', 2)))
      ELSE split(regexp_replace(relative_name, '^(projects/[^/]+)/global(/|$)', '$1$2'), '/')
    END,
    relative_name RLIKE '^projects/[^/]+/global(/|$)');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_resource_name_parse_trimmed(name STRING)
  RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING,
                 parent: STRING, segments: MAP<STRING, STRING>>
  DETERMINISTIC
  COMMENT 'Helper for gcp_resource_name_parse: gets the service and the relative name from the trimmed name'
  RETURN gcp_resource_name_parse_relative(
    CASE
      WHEN name RLIKE '^//[^/]+/.' THEN regexp_extract(name, '^//([^/]+)/', 1)
      WHEN name RLIKE r'^https://www\.googleapis\.com/' THEN
        concat(nullif(regexp_extract(name, '^https://[^/]+/(?:([^/]+)/)?(?:v[0-9]+[a-z0-9]*|alpha|beta)/(.*)$', 1), ''), '.googleapis.com')
      WHEN name RLIKE '^https://[^/]+/' THEN regexp_extract(name, '^https://([^/]+)/', 1)
    END,
    CASE
      -- a Cloud Storage bucket full name has no collection
      WHEN name RLIKE r'^//storage\.googleapis\.com/[^/]+$' THEN concat('projects/_/buckets/', regexp_extract(name, '^//[^/]+/(.*)$', 1))
      WHEN name RLIKE '^//[^/]+/.' THEN regexp_extract(name, '^//[^/]+/(.*)$', 1)
      WHEN name RLIKE '^https://' THEN nullif(regexp_extract(name, '^https://[^/]+/(?:[^/]+/)?(?:v[0-9]+[a-z0-9]*|alpha|beta)/(.*)$', 1), '')
      WHEN name RLIKE '^[a-zA-Z]' AND NOT name RLIKE '://' THEN name
    END);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_resource_name_parse(name STRING)
  RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING,
                 parent: STRING, segments: MAP<STRING, STRING>>
  DETERMINISTIC
  COMMENT 'Parses a GCP full resource name (//service/...), resource URI (https://...) or relative name into service, relative_name, project, location, resource_type, resource_name, parent and a map of collections to IDs. Returns NULL for an invalid name'
  RETURN gcp_resource_name_parse_trimmed(trim(name));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_principal_build(r ARRAY<STRING>, deleted BOOLEAN, uid STRING)
  RETURNS STRUCT<principal_type: STRING, identity: STRING, domain: STRING, pool: STRING, deleted: BOOLEAN, uid: STRING>
  DETERMINISTIC
  COMMENT 'Helper for gcp_principal_parse: builds the result from the type, identity and pool'
  RETURN CASE
    WHEN get(r, 0) IS NOT NULL THEN named_struct(
      'principal_type', get(r, 0),
      'identity', get(r, 1),
      'domain', CASE
        WHEN get(r, 0) = 'domain' THEN get(r, 1)
        WHEN get(r, 0) IN ('user', 'service_account', 'group') THEN nullif(regexp_extract(get(r, 1), '@([^@]+)$', 1), '')
      END,
      'pool', get(r, 2),
      'deleted', deleted,
      'uid', uid)
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_principal_parse_core(c STRING, deleted BOOLEAN, uid STRING)
  RETURNS STRUCT<principal_type: STRING, identity: STRING, domain: STRING, pool: STRING, deleted: BOOLEAN, uid: STRING>
  DETERMINISTIC
  COMMENT 'Helper for gcp_principal_parse: parses the principal identifier without deleted: and ?uid='
  RETURN gcp_principal_build(
    -- array(principal_type, identity, pool)
    CASE
      WHEN c RLIKE '^user:.' THEN array('user', substr(c, 6), NULL)
      WHEN c RLIKE r'^serviceAccount:[^\[]+\.svc\.id\.goog\[[^\]]+\]$' THEN array('gke_workload',
        regexp_extract(c, r'\[([^\]]+)\]$', 1), regexp_extract(c, r'^serviceAccount:([^\[]+)\[', 1))
      WHEN c RLIKE '^serviceAccount:.' THEN array('service_account', substr(c, 16), NULL)
      WHEN c RLIKE '^group:.' THEN array('group', substr(c, 7), NULL)
      WHEN c RLIKE '^domain:.' THEN array('domain', substr(c, 8), NULL)
      WHEN c = 'allUsers' THEN array('all_users', NULL, NULL)
      WHEN c = 'allAuthenticatedUsers' THEN array('all_authenticated_users', NULL, NULL)
      WHEN c RLIKE '^principal://goog/subject/.' THEN array('user', regexp_extract(c, '^principal://goog/subject/(.*)$', 1), NULL)
      WHEN c RLIKE r'^principal://iam\.googleapis\.com/projects/[^/]+/serviceAccounts/.' THEN array('service_account',
        regexp_extract(c, '/serviceAccounts/(.*)$', 1), NULL)
      WHEN c RLIKE '^principalSet://goog/group/.' THEN array('group', regexp_extract(c, '^principalSet://goog/group/(.*)$', 1), NULL)
      WHEN c = 'principalSet://goog/public:all' THEN array('all_principals', NULL, NULL)
      WHEN c RLIKE '^principalSet://goog/cloudIdentityCustomerId/.' THEN array('cloud_identity_customer',
        regexp_extract(c, '^principalSet://goog/cloudIdentityCustomerId/(.*)$', 1), NULL)
      WHEN c RLIKE r'^principalSet://cloudresourcemanager\.googleapis\.com/(projects|folders|organizations)/[^/]+/type/ServiceAccount$' THEN array('service_account_set',
        regexp_extract(c, r'^principalSet://cloudresourcemanager\.googleapis\.com/([^/]+/[^/]+)/type/', 1), NULL)
      WHEN c RLIKE r'^principalSet://cloudresourcemanager\.googleapis\.com/(projects|folders|organizations)/[^/]+/type/ServiceAgent$' THEN array('service_agent_set',
        regexp_extract(c, r'^principalSet://cloudresourcemanager\.googleapis\.com/([^/]+/[^/]+)/type/', 1), NULL)
      WHEN c RLIKE r'^principal(Set)?://iam\.googleapis\.com/projects/[^/]+/locations/[^/]+/workloadIdentityPools/[^/]+\.svc\.id\.goog/.' THEN array('gke_workload',
        regexp_extract(c, '/workloadIdentityPools/[^/]+/(?:subject/)?(.*)$', 1), regexp_extract(c, '/workloadIdentityPools/([^/]+)/', 1))
      WHEN c RLIKE r'^principal://iam\.googleapis\.com/locations/[^/]+/workforcePools/[^/]+/subject/.' THEN array('workforce_identity',
        regexp_extract(c, '/workforcePools/[^/]+/subject/(.*)$', 1), regexp_extract(c, '/workforcePools/([^/]+)/', 1))
      WHEN c RLIKE r'^principalSet://iam\.googleapis\.com/locations/[^/]+/workforcePools/[^/]+/.' THEN array('workforce_identity_set',
        regexp_extract(c, '/workforcePools/[^/]+/(.*)$', 1), regexp_extract(c, '/workforcePools/([^/]+)/', 1))
      WHEN c RLIKE r'^principal://iam\.googleapis\.com/projects/[^/]+/locations/[^/]+/workloadIdentityPools/[^/]+/subject/.' THEN array('workload_identity',
        regexp_extract(c, '/workloadIdentityPools/[^/]+/subject/(.*)$', 1), regexp_extract(c, '/workloadIdentityPools/([^/]+)/', 1))
      WHEN c RLIKE r'^principalSet://iam\.googleapis\.com/projects/[^/]+/locations/[^/]+/workloadIdentityPools/[^/]+/.' THEN array('workload_identity_set',
        regexp_extract(c, '/workloadIdentityPools/[^/]+/(.*)$', 1), regexp_extract(c, '/workloadIdentityPools/([^/]+)/', 1))
      WHEN c RLIKE '^principal://[^/]+/resources/.' THEN array('agent_identity', regexp_extract(c, '^principal://[^/]+/resources/(.*)$', 1), NULL)
      WHEN c RLIKE '^principal://.' THEN array('principal', substr(c, 13), NULL)
      WHEN c RLIKE '^principalSet://.' THEN array('principal_set', substr(c, 16), NULL)
    END,
    deleted,
    uid);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_principal_parse(member STRING)
  RETURNS STRUCT<principal_type: STRING, identity: STRING, domain: STRING, pool: STRING, deleted: BOOLEAN, uid: STRING>
  DETERMINISTIC
  COMMENT 'Parses a GCP IAM principal identifier (user:, serviceAccount:, group:, domain:, principal://, principalSet://, deleted:...) into principal_type, identity, domain, pool, deleted and uid. Returns NULL for an unknown value'
  RETURN gcp_principal_parse_core(
    regexp_replace(regexp_replace(trim(member), '^deleted:', ''), r'\?uid=[^?]*$', ''),
    trim(member) LIKE 'deleted:%',
    nullif(regexp_extract(trim(member), r'\?uid=([^?]*)$', 1), ''));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_service_account_parse_email(e STRING)
  RETURNS STRUCT<account_type: STRING, project_id: STRING, project_number: STRING, service: STRING>
  DETERMINISTIC
  COMMENT 'Helper for gcp_service_account_parse: parses the email without the serviceAccount: prefix'
  RETURN CASE
    -- service agents: service-PROJECT_NUMBER@gcp-sa-SERVICE and also older domains such as compute-system
    WHEN e RLIKE r'^service-[0-9]+@[^.@]+\.iam\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'service_agent',
      'project_id', CAST(NULL AS STRING),
      'project_number', regexp_extract(e, '^service-([0-9]+)@', 1),
      'service', regexp_extract(e, '@(?:gcp-sa-)?([^.@]+)[.]', 1))
    WHEN e RLIKE r'^[^@]+@gcp-sa-[^.@]+\.iam\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'service_agent',
      'project_id', CAST(NULL AS STRING),
      'project_number', CAST(NULL AS STRING),
      'service', regexp_extract(e, '@gcp-sa-([^.@]+)[.]', 1))
    WHEN e RLIKE r'^[^@]+@[^.@]+\.iam\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'user_managed',
      'project_id', regexp_extract(e, '@([^.@]+)[.]', 1),
      'project_number', CAST(NULL AS STRING),
      'service', CAST(NULL AS STRING))
    WHEN e RLIKE r'^[^@]+@appspot\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'default_app_engine',
      'project_id', regexp_extract(e, '^([^@]+)@', 1),
      'project_number', CAST(NULL AS STRING),
      'service', CAST(NULL AS STRING))
    WHEN e RLIKE r'^[0-9]+-compute@developer\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'default_compute',
      'project_id', CAST(NULL AS STRING),
      'project_number', regexp_extract(e, '^([0-9]+)-compute@', 1),
      'service', CAST(NULL AS STRING))
    WHEN e RLIKE r'^[^@]+@[^.@]+\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'service_agent',
      'project_id', CAST(NULL AS STRING),
      'project_number', nullif(regexp_extract(e, '^([0-9]+)@', 1), ''),
      'service', regexp_extract(e, '@([^.@]+)[.]', 1))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_service_account_parse(email STRING)
  RETURNS STRUCT<account_type: STRING, project_id: STRING, project_number: STRING, service: STRING>
  DETERMINISTIC
  COMMENT 'Parses a GCP service account email into account_type (user_managed, default_app_engine, default_compute, service_agent), project_id, project_number and service. A serviceAccount: prefix is removed. Returns NULL for other values'
  RETURN gcp_service_account_parse_email(regexp_replace(trim(email), '^serviceAccount:', ''));
