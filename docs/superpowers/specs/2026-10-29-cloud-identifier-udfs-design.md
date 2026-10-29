# Cloud identifier UDFs — design

Date: 2026-10-29

## Goal

Add Unity Catalog SQL functions to `uc-udfs/` that parse cloud identifiers: AWS ARNs and IAM unique IDs,
Azure resource IDs, GCP resource names, IAM principals and service accounts, and cloud storage URIs.
Use cases, in priority order:

1. Enrichment and joins across sources (CloudTrail ↔ IAM inventory, Activity Log ↔ Resource Graph,
   GCP audit logs ↔ Cloud Asset Inventory).
2. Detection and triage (long-term vs. temporary access keys, assumed-role resolution, default
   service accounts, account ID from a leaked access key).
3. Normalization into OCSF `cloud` and `resource_details` objects.

Out of scope: OCI OCIDs, HTTPS storage URLs, `r2://`, incomplete ARNs (`arn:aws:sqs`), Azure tenant
and AWS organization (not encoded in the identifiers).

## Decisions

| Topic | Decision |
|---|---|
| Output shape | Each cloud parser returns its own native field names; one adapter, `cloud_resource_to_ocsf`, maps them to OCSF |
| Implementation | Pure SQL (`regexp_extract`, `split`, `named_struct`, `CASE`), no Python sandbox |
| Invalid input | Returns `NULL` and never raises an error (the same as `community_id.sql`, unlike `ip_classification.sql`) |
| Case | AWS and GCP keep the original case. Azure keywords are matched without case; values keep the original case |
| Reuse | Wrapper functions call the parsers and don't copy the regexes. All functions must be in the same schema |
| Tests | One SQL test notebook per file: a `VALUES` table of inputs and expected outputs, `raise_error` on a mismatch |
| Test data | Examples from the vendor documentation listed in each section, plus log shapes |

## Layout

```
uc-udfs/
  aws_identifiers.sql
  azure_identifiers.sql
  gcp_identifiers.sql
  cloud_resources.sql          (depends on the three files above)
  tests/
    aws_identifiers_test.sql
    azure_identifiers_test.sql
    gcp_identifiers_test.sql
    cloud_resources_test.sql
  README.md                    (one entry per new file)
```

Each `.sql` file uses the Databricks notebook source format with a `-- MAGIC %md` header: a table of
functions and a "Behavior" list (the same as `ip_classification.sql`). Every function is
`DETERMINISTIC` and has a `COMMENT`.

## 1. AWS (`aws_identifiers.sql`)

Sources: [ARN reference](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html),
[IAM identifiers](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html).

### `aws_arn_parse(arn STRING)`

Returns `STRUCT<partition, service, region, account_id, resource, resource_type, resource_id>` (all `STRING`).

- The input must match `^arn:(aws[a-z-]*):([^:]+):([^:]*):([^:]*):(.+)$`. If not, the result is `NULL`.
  The partition pattern covers `aws`, `aws-cn`, `aws-us-gov`, `aws-iso*`, `aws-eusc`.
- An empty region or account gives `NULL`. The literal account `aws` (`arn:aws:iam::aws:...`) and
  wildcards (`*`) are kept.
- `resource` is everything after the fifth colon. It is split at the first `/` or `:`, whichever
  comes first. The part before is `resource_type`, the part after is `resource_id`. If there is no
  separator, `resource_type` is `NULL` and `resource_id` is `resource`.
- S3 special case: service `s3` with no account (bucket/object ARNs) gives `resource_type` `bucket`
  (no `/`) or `object` (with `/`), and `resource_id` is the full `resource`.

### `aws_role_name(arn STRING)`

Returns the role name: the last path segment of an IAM `role/...` ARN, or the segment before the
session name of an STS `assumed-role/...` ARN. Other input gives `NULL`.

### `aws_assumed_role_to_role_arn(arn STRING)`

- `arn:<p>:sts::<acct>:assumed-role/<role>/<session>` gives `arn:<p>:iam::<acct>:role/<role>`.
- An IAM role ARN is returned unchanged. Other input gives `NULL`.
- Caveat (document it): assumed-role ARNs don't have the role path. The result is not the inventory
  ARN for roles with a path: IAM Identity Center roles (`aws-reserved/sso.amazonaws.com/`),
  service roles (`service-role/`), service-linked roles (`aws-service-role/<svc>/`). For these,
  join on `(account_id, aws_role_name(arn))`.

### `aws_unique_id_type(id STRING)`

- The input must match `^[A-Z0-9]{16,128}(:.*)?$`. The optional `:...` suffix is ignored (the form
  `AROA...:session` of `aws:userId` and CloudTrail `principalId`).
- The prefix gives the label:

| Prefix | Label | Prefix | Label |
|---|---|---|---|
| `ABIA` | `sts_bearer_token` | `ANPA` | `managed_policy` |
| `ACCA` | `context_specific_credential` | `ANVA` | `managed_policy_version` |
| `AGPA` | `group` | `APKA` | `public_key` |
| `AIDA` | `user` | `AROA` | `role` |
| `AIPA` | `instance_profile` | `ASCA` | `certificate` |
| `AKIA` | `access_key` | `ASIA` | `temporary_access_key` |

- An unknown prefix or an invalid value gives `NULL`.

### `aws_account_id_from_access_key(key_id STRING)`

- Only for `AKIA`/`ASIA` keys. Other input gives `NULL`.
- Base32-decode the 10 characters after the prefix (`aggregate` over the characters, each character
  is 5 bits). Take the first 48 bits as `BIGINT` `x`. The account is `(x & 0x7fffffffff80) >> 7`,
  left-padded with zeros to 12 digits.
- The same result as `aws sts get-access-key-info`, without an API call.
- The AWS docs pages don't have keys that decode. The test vectors come from the published
  write-up of this technique and must be verified during implementation.

### Test cases

`aws_arn_parse`:

| ARN | resource_type | resource_id | Other |
|---|---|---|---|
| `arn:aws:iam::123456789012:user/John` | `user` | `John` | region `NULL` |
| `arn:aws:iam::123456789012:user/division_abc/subdivision_xyz/Jane` | `user` | `division_abc/subdivision_xyz/Jane` | |
| `arn:aws:iam::123456789012:group/division_abc/subdivision_xyz/product_A/Developers` | `group` | `division_abc/subdivision_xyz/product_A/Developers` | |
| `arn:aws:iam::123456789012:role/aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer` | `role` | `aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer` | `aws_role_name` = `AWSServiceRoleForAccessAnalyzer` |
| `arn:aws:iam::123456789012:role/service-role/QuickSightAction` | `role` | `service-role/QuickSightAction` | `aws_role_name` = `QuickSightAction` |
| `arn:aws:iam::123456789012:root` | `NULL` | `root` | |
| `arn:aws:sts::123456789012:self` | `NULL` | `self` | |
| `arn:aws:sts::123456789012:federated-user/Paulo` | `federated-user` | `Paulo` | `aws_role_name` = `NULL` |
| `arn:aws:sts::123456789012:assumed-role/Accounting-Role/Mary` | `assumed-role` | `Accounting-Role/Mary` | role ARN `arn:aws:iam::123456789012:role/Accounting-Role` |
| `arn:aws:iam::123456789012:u2f/user/John/default` | `u2f` | `user/John/default` | |
| `arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/a1b2c3d4567890abcdefEXAMPLE11111` | `oidc-provider` | `oidc.eks.us-west-2.amazonaws.com/id/a1b2c3d4567890abcdefEXAMPLE11111` | |
| `arn:aws:iam::aws:contextProvider/IdentityCenter` | `contextProvider` | `IdentityCenter` | account `aws` |
| `arn:aws:sns:us-east-1:123456789012:example-sns-topic-name` | `NULL` | `example-sns-topic-name` | |
| `arn:aws:ec2:us-east-1:123456789012:vpc/vpc-0e9801d129EXAMPLE` | `vpc` | `vpc-0e9801d129EXAMPLE` | |
| `arn:aws:ec2:*:123456789012:vpc/*` | `vpc` | `*` | region `*` |
| `arn:aws:qbusiness:*:123456789012:*` | `NULL` | `*` | |
| `arn:aws:iam::*:user/division_abc/subdivision_xyz/Richard` | `user` | `division_abc/subdivision_xyz/Richard` | account `*` |
| `arn:aws:lambda:us-east-2:123456789012:function:my-function:prod` | `function` | `my-function:prod` | |
| `arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/x:*` | `log-group` | `/aws/lambda/x:*` | |
| `arn:aws:s3:::example_bucket` | `bucket` | `example_bucket` | |
| `arn:aws:s3:::example_bucket/marketing/newproductlaunch/widget/*` | `object` | `example_bucket/marketing/newproductlaunch/widget/*` | |
| `arn:aws:s3:::amzn-s3-demo-bucket/????-test` | `object` | `amzn-s3-demo-bucket/????-test` | |
| `arn:aws-cn:iam::123456789012:root`, `arn:aws-us-gov:iam::123456789012:root` | `NULL` | `root` | partition |
| `arn:aws:sqs`, `AWS Internal`, `123456789012`, `''`, `NULL` | | | result `NULL` |

`aws_unique_id_type`:

The literal example IDs are not in this spec: the repository secret scanner blocks
AWS key-shaped strings (see "Secret scanning" below). The cases are:

| Input | Result |
|---|---|
| the two IAM user unique IDs from the "Unique identifiers" section of the IAM identifiers page | `user` |
| the role unique IDs from the same section, including the `:role-session-name` form and the ID with digits `0`, `1`, `8`, `9` | `role` |
| the AWS documentation example access key ID (`AKIA` prefix, `EXAMPLE` suffix) | `access_key` |
| `111122223333`, a 21-character ID with the unknown prefix `XXXX`, a lowercase user ID, `NULL` | `NULL` |

Also test one ID for each of the 12 prefixes, real CloudTrail `userIdentity` shapes (IAM user,
assumed role, Identity Center role, federated user, root, AWS service), and non-ARN values.

## 2. Azure (`azure_identifiers.sql`)

Source: [ARM template resource functions](https://learn.microsoft.com/en-us/azure/azure-resource-manager/templates/template-functions-resource)
(`resourceId`, `extensionResourceId`, `subscriptionResourceId`, `managementGroupResourceId`, `tenantResourceId`).

### `azure_resource_id_parse(id STRING)`

Returns `STRUCT<subscription_id, resource_group, management_group, provider_namespace, resource_type, resource_name, parent_id>`
(all `STRING`).

1. Trim, remove a trailing `/`. If the value doesn't start with `/`, the result is `NULL`.
2. `subscription_id`, `resource_group`, `management_group` come from the leading scope:
   `/subscriptions/{s}`, `/subscriptions/{s}/resourceGroups/{rg}`,
   `/providers/Microsoft.Management/managementGroups/{mg}`.
3. The part after the last `/providers/` is `{Namespace}/{type1}/{name1}[/{type2}/{name2}...]`:
   - `provider_namespace` = `Namespace`;
   - `resource_type` = the namespace and all types, joined with `/` (`Microsoft.Network/virtualNetworks/subnets`);
   - `resource_name` = the last segment;
   - an odd number of type/name segments (a collection, not a resource) gives `NULL`.
4. `parent_id`:
   - extension resource: the part before the last `/providers/` (the resource or scope that is extended);
   - child resource: the ID without the last type/name pair;
   - top-level resource: its resource group or subscription scope;
   - subscription, management group, tenant-level resource: `NULL` (the ID doesn't encode the management group).
5. Scope-only IDs:
   - `/subscriptions/{s}` gives type `Microsoft.Resources/subscriptions`, name `{s}`, parent `NULL`;
   - `/subscriptions/{s}/resourceGroups/{rg}` gives type `Microsoft.Resources/resourceGroups`,
     name `{rg}`, parent `/subscriptions/{s}`.
6. The keywords `subscriptions`, `resourceGroups`, `providers` are matched without case. Values keep
   the original case. Subscription IDs are not validated as GUIDs.

### `azure_resource_id_normalize(id STRING)`

`lower(trim(id))` without a trailing `/`. A join key for Activity Log (often upper case),
Resource Graph and Defender.

### Test cases

`{s}` = `aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e`, `{g}` = a GUID.

| ID | resource_type | resource_name | parent_id / other |
|---|---|---|---|
| `/subscriptions/{s}/resourceGroups/otherResourceGroup/providers/Microsoft.Storage/storageAccounts/examplestorage` | `Microsoft.Storage/storageAccounts` | `examplestorage` | `/subscriptions/{s}/resourceGroups/otherResourceGroup` |
| `/subscriptions/{s}/resourceGroups/examplegroup/providers/Microsoft.SQL/servers/serverName/databases/databaseName` | `Microsoft.SQL/servers/databases` | `databaseName` | `.../providers/Microsoft.SQL/servers/serverName` |
| `/subscriptions/{s}/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/vnet1/subnets/subnet1` | `Microsoft.Network/virtualNetworks/subnets` | `subnet1` | `.../virtualNetworks/vnet1` |
| `/subscriptions/{s}/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1/providers/Microsoft.Authorization/roleAssignments/{g}` | `Microsoft.Authorization/roleAssignments` | `{g}` | `.../virtualMachines/vm1`; `resource_group` `rg1` |
| `/subscriptions/{s}/resourceGroups/rg1/providers/Microsoft.Authorization/roleAssignments/{g}` | `Microsoft.Authorization/roleAssignments` | `{g}` | `/subscriptions/{s}/resourceGroups/rg1` |
| `/subscriptions/{s}/providers/Microsoft.Authorization/roleDefinitions/{g}` | `Microsoft.Authorization/roleDefinitions` | `{g}` | `/subscriptions/{s}`; `resource_group` `NULL` |
| `/providers/Microsoft.Management/managementGroups/mg1` | `Microsoft.Management/managementGroups` | `mg1` | `NULL`; `management_group` `mg1` |
| `/providers/Microsoft.Management/managementGroups/mg1/providers/Microsoft.Authorization/policyDefinitions/p1` | `Microsoft.Authorization/policyDefinitions` | `p1` | `/providers/Microsoft.Management/managementGroups/mg1` |
| `/providers/Microsoft.Authorization/policyDefinitions/{g}` | `Microsoft.Authorization/policyDefinitions` | `{g}` | `NULL` |
| `/subscriptions/{s}` | `Microsoft.Resources/subscriptions` | `{s}` | `NULL` |
| `/subscriptions/{s}/resourceGroups/rg1` | `Microsoft.Resources/resourceGroups` | `rg1` | `/subscriptions/{s}` |
| `/SUBSCRIPTIONS/{S}/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.COMPUTE/VIRTUALMACHINES/VM1` | `MICROSOFT.COMPUTE/VIRTUALMACHINES` | `VM1` | `_normalize` is the same as for the mixed-case form |
| `/subscriptions/{s}/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1/` | `Microsoft.Compute/virtualMachines` | `vm1` | trailing `/` |
| `.../providers/Microsoft.Compute/virtualMachines`, `vm1`, `''`, `/`, `NULL` | | | result `NULL` |

## 3. GCP (`gcp_identifiers.sql`)

Sources: [AIP-122 resource names](https://google.aip.dev/122),
[Cloud Asset Inventory resource name formats](https://docs.cloud.google.com/asset-inventory/docs/resource-name-format),
[IAM principal identifiers](https://docs.cloud.google.com/iam/docs/principal-identifiers),
[service account types](https://docs.cloud.google.com/iam/docs/service-account-types).

### `gcp_resource_name_parse(name STRING)`

Returns `STRUCT<service STRING, relative_name STRING, project STRING, location STRING, resource_type STRING, resource_name STRING, parent STRING, segments MAP<STRING,STRING>>`.

Input forms, all converted to a relative name:

- full resource name `//<service>/<relative>`: `service` is the host;
- resource URI `https://<host>/[<svc>/]<version>/<relative>`: the path up to and including the version
  segment (`v\d+` with an optional `alpha`/`beta` suffix and number) is removed. `service` is the
  host, but for `www.googleapis.com` it is `<first path segment>.googleapis.com`;
- relative name (audit log `protoPayload.resourceName`): `service` is `NULL`
  (use `coalesce` with `protoPayload.serviceName`).

The relative name is split into collection/ID pairs (`segments`).
`resource_type` = the last collection, `resource_name` = the last ID, `parent` = the relative name
without the last pair, `project` = `segments['projects']` (a project ID or number),
`location` = `coalesce(segments['locations'], segments['zones'], segments['regions'])`, or `'global'`.

Exceptions:

1. `global` is a singleton segment (`projects/P/global/networks/N`). It is removed from the pairs and sets
   `location = 'global'`. `parent` is `projects/P`. `relative_name` keeps `global`.
2. Wildcard projects `-` (`projects/-/serviceAccounts/...`) and `_` (`projects/_/buckets/...`) give
   `project = NULL`. `segments` keeps the value.
3. The GCS bucket full name `//storage.googleapis.com/BUCKET` has no collection. It is converted to
   `projects/_/buckets/BUCKET` (the same as the audit log form).
4. GCS object names can contain `/`: the text after `objects/` is the object name.
5. Other values with an odd number of segments give `NULL`.

`service` + `relative_name` is the join key between Asset Inventory, selfLinks and audit logs.
Document: GCS selfLinks (`/storage/v1/b/B/o/O`) use the collections `b`/`o` and don't match that key.

### `gcp_principal_parse(member STRING)`

Returns `STRUCT<principal_type STRING, identity STRING, domain STRING, pool STRING, deleted BOOLEAN, uid STRING>`.

| Input | principal_type | identity |
|---|---|---|
| `user:E`, `principal://goog/subject/E` | `user` | `E` |
| `serviceAccount:E`, `principal://iam.googleapis.com/projects/-/serviceAccounts/E` | `service_account` | `E` |
| `group:E`, `principalSet://goog/group/E` | `group` | `E` |
| `domain:D` | `domain` | `D` |
| `allUsers` | `all_users` | `NULL` |
| `allAuthenticatedUsers` | `all_authenticated_users` | `NULL` |
| `principalSet://goog/public:all` | `all_principals` | `NULL` |
| `principalSet://goog/cloudIdentityCustomerId/C` | `cloud_identity_customer` | `C` |
| `principalSet://cloudresourcemanager.googleapis.com/{projects\|folders\|organizations}/N/type/ServiceAccount` | `service_account_set` | `{projects\|folders\|organizations}/N` |
| same with `/type/ServiceAgent` | `service_agent_set` | `{projects\|folders\|organizations}/N` |
| `principal://iam.googleapis.com/locations/global/workforcePools/P/subject/S` | `workforce_identity` | `S` |
| `principalSet://iam.googleapis.com/locations/global/workforcePools/P/...` | `workforce_identity_set` | rest after `P/` |
| `principal://iam.googleapis.com/projects/N/locations/global/workloadIdentityPools/P/subject/S` | `workload_identity` | `S` |
| `principalSet://iam.googleapis.com/projects/N/locations/global/workloadIdentityPools/P/...` | `workload_identity_set` | rest after `P/` |
| workload pool `P` ending in `.svc.id.goog` (GKE), `serviceAccount:P.svc.id.goog[ns/ksa]` | `gke_workload` | `ns/ksa` (subject form: rest after `subject/`) |
| `principal://TRUST_DOMAIN/resources/...` | `agent_identity` | rest after `resources/` |
| other `principal://...` / `principalSet://...` | `principal` / `principal_set` | rest after `://` |
| other | `NULL` | |

- `domain` = the part after `@` for email identities, `D` for `domain:D`.
- `pool` = the workforce or workload pool ID.
- `deleted:` prefix and `?uid=N` suffix give `deleted = true`, `uid = N`. The rest is parsed as usual.
  Otherwise `deleted = false`.

### `gcp_service_account_parse(email STRING)`

Returns `STRUCT<account_type STRING, project_id STRING, project_number STRING, service STRING>`.
An optional `serviceAccount:` prefix is removed.

| Pattern | account_type | Extracted |
|---|---|---|
| `NAME@PROJECT_ID.iam.gserviceaccount.com` (`PROJECT_ID` doesn't start with `gcp-sa-`) | `user_managed` | `project_id` |
| `PROJECT_ID@appspot.gserviceaccount.com` | `default_app_engine` | `project_id` |
| `PROJECT_NUMBER-compute@developer.gserviceaccount.com` | `default_compute` | `project_number` |
| `service-PROJECT_NUMBER@gcp-sa-X.iam.gserviceaccount.com` | `service_agent` | `project_number`, `service` = `X` |
| other `...@gcp-sa-X.iam.gserviceaccount.com` | `service_agent` | `service` = `X` |
| `LOCAL@SVC.gserviceaccount.com` (other domains: `cloudservices`, `cloudbuild`, `system`, ...) | `service_agent` | `service` = `SVC`; `project_number` if `LOCAL` is all digits |
| other | `NULL` | |

Document: Google says these formats can change.

### Test cases

- Asset Inventory formats with sample values: Compute instance (`zones`), disk, `global/networks`,
  `global/firewalls`, `regions/.../subnetworks`, GKE cluster and node pool (`locations`), KMS key ring,
  key and key version, BigQuery dataset and table, IAM service account (`projects/-/...` and by
  project ID), IAM role, project by ID and by number, folder, organization, `//storage.googleapis.com/B`,
  Cloud SQL instance, Pub/Sub topic, Cloud Run service, Secret Manager secret by project number.
- GCS audit log names `projects/_/buckets/B` and `projects/_/buckets/B/objects/dir/file.txt`.
- AIP-122 examples in the three forms: `publishers/123/books/les-miserables`,
  `//library.googleapis.com/publishers/123/books/les-miserables`,
  `https://library.googleapis.com/v1/publishers/123/books/les-miserables`, and the Compute selfLink
  `https://www.googleapis.com/compute/v1/projects/P/zones/Z/instances/I`.
- Invalid: `projects/P/instances`, `foo`, `''`, `NULL`.
- All principal examples from the IAM page, with the deleted forms
  (`deleted:user:alex@example.com?uid=123456789012345678901`).
- Each row of the service account table, with `bqcx-442188550395-jujw@gcp-sa-bigquery-condel.iam.gserviceaccount.com`
  (gives `service_agent`, `service` `bigquery-condel`, `project_number` `NULL`) and
  `service-agent-manager@system.gserviceaccount.com`.

## 4. Cross-cloud (`cloud_resources.sql`)

### `storage_uri_parse(uri STRING)`

Returns `STRUCT<cloud, scheme, account, container, path>` (all `STRING`).

| Input | cloud | account | container | path |
|---|---|---|---|---|
| `s3://b/k/x.parquet`, `s3a://...`, `s3n://...` | `aws` | `NULL` | `b` | `k/x.parquet` |
| `gs://b/o` | `gcp` | `NULL` | `b` | `o` |
| `abfss://c@acct.dfs.core.windows.net/p`, `abfs://...` | `azure` | `acct` | `c` | `p` |
| `wasbs://c@acct.blob.core.windows.net/p`, `wasb://...` | `azure` | `acct` | `c` | `p` |

- The Azure host is `<acct>.(dfs|blob).core.<suffix>`, so sovereign clouds work.
- `path` has no leading `/`. An empty path gives `NULL`.
- `scheme` is the lowercase scheme. Other values (`dbfs:/`, `/Volumes/...`, HTTPS URLs) give `NULL`.

### `cloud_resource_to_ocsf(id STRING)`

Source: OCSF objects [`cloud`](https://schema.ocsf.io/objects/cloud),
[`account`](https://schema.ocsf.io/objects/account),
[`resource_details`](https://schema.ocsf.io/objects/resource_details).
`cloud.project_uid` has been deprecated since 1.4.0, so the GCP project goes into `cloud.account`.

Returns:

```
STRUCT<
  cloud:    STRUCT<provider STRING, region STRING, zone STRING, cloud_partition STRING,
                   account: STRUCT<uid STRING, type STRING, type_id INT>,
                   org:     STRUCT<uid STRING, ou_uid STRING>>,
  resource: STRUCT<uid STRING, name STRING, type STRING, region STRING, zone STRING,
                   cloud_partition STRING, group: STRUCT<name STRING>>>
```

Detection: `arn:` → AWS. `/subscriptions/...` or `/providers/...` → Azure. `//...`,
`https://...googleapis.com/...`, relative `projects/`, `folders/`, `organizations/` → GCP.
Other values give `NULL`, and so does a value that the parser returns as `NULL`.

| OCSF field | AWS | Azure | GCP |
|---|---|---|---|
| `cloud.provider` | `AWS` | `Azure` | `GCP` |
| `cloud.account` (uid / type / type_id) | `account_id` / `AWS Account` / `10` | `subscription_id` / `Azure Subscription` / `13` | `project` / `GCP Project` / `11` |
| `cloud.region`, `resource.region` | `region` | `NULL` | `regions` or `locations`; for `zones`, the zone without the last `-x` part (`us-central1-a` → `us-central1`) |
| `cloud.zone`, `resource.zone` | `NULL` | `NULL` | `zones` |
| `cloud_partition` (both) | `partition` | `NULL` | `NULL` |
| `cloud.org` (uid / ou_uid) | `NULL` | `NULL` | `segments['organizations']` / `segments['folders']` |
| `resource.uid` | the ARN | the ID | `//service/relative_name`, or `relative_name` if `service` is `NULL` |
| `resource.name` | `resource_id`; last path segment for IAM `user`, `group`, `role`, `policy`, `instance-profile`, `server-certificate` | `resource_name` | `resource_name` |
| `resource.type` | `service:resource_type` (`ec2:instance`, `iam:role`, `s3:object`); `service` if `resource_type` is `NULL` | `resource_type` | `service/resource_type` (`compute.googleapis.com/instances`); `resource_type` if `service` is `NULL` |
| `resource.group.name` | `NULL` | `resource_group` | `NULL` |

- The `provider` strings are a choice (OCSF only gives examples: "AWS, MS Azure, GCP"). Document them.
- Not mapped: Azure tenant, AWS organization (not in the identifiers), Azure management group
  (no clear OCSF field).

### Test cases

- `storage_uri_parse`: each row of the table, sovereign Azure host (`core.chinacloudapi.cn`),
  bucket with no path (`s3://b`, `s3://b/`), invalid values.
- `cloud_resource_to_ocsf`: one AWS IAM role, one S3 object, one EC2 instance; one Azure VM and one
  role assignment; one GCP Compute instance in a zone, one `global` network, one folder; invalid values.

## Secret scanning

The Databricks pre-commit hook flags AWS key-shaped strings (`AKIA...`, `ASIA...`, `AIDA...`,
`AROA...`) as `aws-access-token`, also when they are documentation examples. The hook must not be
bypassed without approval in #no-secrets-in-code. This affects `tests/aws_identifiers_test.sql`
(unique ID and access key test vectors). The implementation plan must pick one of:

- get approval for the documentation example values, and commit with `SKIP_SECRET_SCAN=1`;
- build the test values at run time in SQL (for example `concat('AKI', 'A', ...)`), so that the
  file doesn't contain the literal strings.

## README

Add one entry for each new file to `uc-udfs/README.md`, in the same style as the existing entries.
