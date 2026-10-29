-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Tests for `gcp_identifiers.sql`
-- MAGIC
-- MAGIC Each cell fails with `raise_error` and lists the failed cases. Create the functions from `../gcp_identifiers.sql` in the same schema first.
-- MAGIC
-- MAGIC The names are the formats from AIP-122, the Cloud Asset Inventory resource name formats, the IAM principal identifiers page
-- MAGIC and the service account types page, with placeholder project IDs and numbers.

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: gcp_resource_name_parse',
          raise_error(concat('gcp_resource_name_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(gcp_resource_name_parse(input)) AS actual
    FROM VALUES
      ('//compute.googleapis.com/projects/my-project/zones/us-central1-a/instances/vm1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/zones/us-central1-a/instances/vm1","project":"my-project","location":"us-central1-a","resource_type":"instances","resource_name":"vm1","parent":"projects/my-project/zones/us-central1-a","segments":{"projects":"my-project","zones":"us-central1-a","instances":"vm1"}}'),
      ('//compute.googleapis.com/projects/my-project/zones/us-central1-a/disks/disk1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/zones/us-central1-a/disks/disk1","project":"my-project","location":"us-central1-a","resource_type":"disks","resource_name":"disk1","parent":"projects/my-project/zones/us-central1-a","segments":{"projects":"my-project","zones":"us-central1-a","disks":"disk1"}}'),
      ('//compute.googleapis.com/projects/my-project/global/networks/default',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/global/networks/default","project":"my-project","location":"global","resource_type":"networks","resource_name":"default","parent":"projects/my-project","segments":{"projects":"my-project","networks":"default"}}'),
      ('//compute.googleapis.com/projects/my-project/global/firewalls/allow-ssh',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/global/firewalls/allow-ssh","project":"my-project","location":"global","resource_type":"firewalls","resource_name":"allow-ssh","parent":"projects/my-project","segments":{"projects":"my-project","firewalls":"allow-ssh"}}'),
      ('//compute.googleapis.com/projects/my-project/regions/us-central1/subnetworks/subnet1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/regions/us-central1/subnetworks/subnet1","project":"my-project","location":"us-central1","resource_type":"subnetworks","resource_name":"subnet1","parent":"projects/my-project/regions/us-central1","segments":{"projects":"my-project","regions":"us-central1","subnetworks":"subnet1"}}'),
      ('//container.googleapis.com/projects/my-project/locations/us-central1/clusters/c1',
       '{"service":"container.googleapis.com","relative_name":"projects/my-project/locations/us-central1/clusters/c1","project":"my-project","location":"us-central1","resource_type":"clusters","resource_name":"c1","parent":"projects/my-project/locations/us-central1","segments":{"projects":"my-project","locations":"us-central1","clusters":"c1"}}'),
      ('//container.googleapis.com/projects/my-project/locations/us-central1/clusters/c1/nodePools/pool1',
       '{"service":"container.googleapis.com","relative_name":"projects/my-project/locations/us-central1/clusters/c1/nodePools/pool1","project":"my-project","location":"us-central1","resource_type":"nodePools","resource_name":"pool1","parent":"projects/my-project/locations/us-central1/clusters/c1","segments":{"projects":"my-project","locations":"us-central1","clusters":"c1","nodePools":"pool1"}}'),
      ('//cloudkms.googleapis.com/projects/my-project/locations/global/keyRings/kr1',
       '{"service":"cloudkms.googleapis.com","relative_name":"projects/my-project/locations/global/keyRings/kr1","project":"my-project","location":"global","resource_type":"keyRings","resource_name":"kr1","parent":"projects/my-project/locations/global","segments":{"projects":"my-project","locations":"global","keyRings":"kr1"}}'),
      ('//cloudkms.googleapis.com/projects/my-project/locations/global/keyRings/kr1/cryptoKeys/k1/cryptoKeyVersions/1',
       '{"service":"cloudkms.googleapis.com","relative_name":"projects/my-project/locations/global/keyRings/kr1/cryptoKeys/k1/cryptoKeyVersions/1","project":"my-project","location":"global","resource_type":"cryptoKeyVersions","resource_name":"1","parent":"projects/my-project/locations/global/keyRings/kr1/cryptoKeys/k1","segments":{"projects":"my-project","locations":"global","keyRings":"kr1","cryptoKeys":"k1","cryptoKeyVersions":"1"}}'),
      ('//bigquery.googleapis.com/projects/my-project/datasets/ds1',
       '{"service":"bigquery.googleapis.com","relative_name":"projects/my-project/datasets/ds1","project":"my-project","resource_type":"datasets","resource_name":"ds1","parent":"projects/my-project","segments":{"projects":"my-project","datasets":"ds1"}}'),
      ('//bigquery.googleapis.com/projects/my-project/datasets/ds1/tables/t1',
       '{"service":"bigquery.googleapis.com","relative_name":"projects/my-project/datasets/ds1/tables/t1","project":"my-project","resource_type":"tables","resource_name":"t1","parent":"projects/my-project/datasets/ds1","segments":{"projects":"my-project","datasets":"ds1","tables":"t1"}}'),
      ('//iam.googleapis.com/projects/-/serviceAccounts/sa1@my-project.iam.gserviceaccount.com',
       '{"service":"iam.googleapis.com","relative_name":"projects/-/serviceAccounts/sa1@my-project.iam.gserviceaccount.com","resource_type":"serviceAccounts","resource_name":"sa1@my-project.iam.gserviceaccount.com","parent":"projects/-","segments":{"projects":"-","serviceAccounts":"sa1@my-project.iam.gserviceaccount.com"}}'),
      ('//iam.googleapis.com/projects/my-project/serviceAccounts/sa1@my-project.iam.gserviceaccount.com',
       '{"service":"iam.googleapis.com","relative_name":"projects/my-project/serviceAccounts/sa1@my-project.iam.gserviceaccount.com","project":"my-project","resource_type":"serviceAccounts","resource_name":"sa1@my-project.iam.gserviceaccount.com","parent":"projects/my-project","segments":{"projects":"my-project","serviceAccounts":"sa1@my-project.iam.gserviceaccount.com"}}'),
      ('//iam.googleapis.com/projects/my-project/roles/customRole',
       '{"service":"iam.googleapis.com","relative_name":"projects/my-project/roles/customRole","project":"my-project","resource_type":"roles","resource_name":"customRole","parent":"projects/my-project","segments":{"projects":"my-project","roles":"customRole"}}'),
      ('//cloudresourcemanager.googleapis.com/projects/my-project',
       '{"service":"cloudresourcemanager.googleapis.com","relative_name":"projects/my-project","project":"my-project","resource_type":"projects","resource_name":"my-project","segments":{"projects":"my-project"}}'),
      ('//cloudresourcemanager.googleapis.com/projects/123456789012',
       '{"service":"cloudresourcemanager.googleapis.com","relative_name":"projects/123456789012","project":"123456789012","resource_type":"projects","resource_name":"123456789012","segments":{"projects":"123456789012"}}'),
      ('//cloudresourcemanager.googleapis.com/folders/123456789012',
       '{"service":"cloudresourcemanager.googleapis.com","relative_name":"folders/123456789012","resource_type":"folders","resource_name":"123456789012","segments":{"folders":"123456789012"}}'),
      ('//cloudresourcemanager.googleapis.com/organizations/123456789012',
       '{"service":"cloudresourcemanager.googleapis.com","relative_name":"organizations/123456789012","resource_type":"organizations","resource_name":"123456789012","segments":{"organizations":"123456789012"}}'),
      ('//storage.googleapis.com/my-bucket',
       '{"service":"storage.googleapis.com","relative_name":"projects/_/buckets/my-bucket","resource_type":"buckets","resource_name":"my-bucket","parent":"projects/_","segments":{"projects":"_","buckets":"my-bucket"}}'),
      ('//storage.googleapis.com/projects/_/buckets/my-bucket/objects/a/b.txt',
       '{"service":"storage.googleapis.com","relative_name":"projects/_/buckets/my-bucket/objects/a/b.txt","resource_type":"objects","resource_name":"a/b.txt","parent":"projects/_/buckets/my-bucket","segments":{"projects":"_","buckets":"my-bucket","objects":"a/b.txt"}}'),
      ('//cloudsql.googleapis.com/projects/my-project/instances/db1',
       '{"service":"cloudsql.googleapis.com","relative_name":"projects/my-project/instances/db1","project":"my-project","resource_type":"instances","resource_name":"db1","parent":"projects/my-project","segments":{"projects":"my-project","instances":"db1"}}'),
      ('//pubsub.googleapis.com/projects/my-project/topics/t1',
       '{"service":"pubsub.googleapis.com","relative_name":"projects/my-project/topics/t1","project":"my-project","resource_type":"topics","resource_name":"t1","parent":"projects/my-project","segments":{"projects":"my-project","topics":"t1"}}'),
      ('//run.googleapis.com/projects/my-project/locations/us-central1/services/svc1',
       '{"service":"run.googleapis.com","relative_name":"projects/my-project/locations/us-central1/services/svc1","project":"my-project","location":"us-central1","resource_type":"services","resource_name":"svc1","parent":"projects/my-project/locations/us-central1","segments":{"projects":"my-project","locations":"us-central1","services":"svc1"}}'),
      ('//secretmanager.googleapis.com/projects/123456789012/secrets/s1',
       '{"service":"secretmanager.googleapis.com","relative_name":"projects/123456789012/secrets/s1","project":"123456789012","resource_type":"secrets","resource_name":"s1","parent":"projects/123456789012","segments":{"projects":"123456789012","secrets":"s1"}}'),
      -- audit log protoPayload.resourceName
      ('projects/_/buckets/my-bucket',
       '{"relative_name":"projects/_/buckets/my-bucket","resource_type":"buckets","resource_name":"my-bucket","parent":"projects/_","segments":{"projects":"_","buckets":"my-bucket"}}'),
      ('projects/_/buckets/my-bucket/objects/dir/file.txt',
       '{"relative_name":"projects/_/buckets/my-bucket/objects/dir/file.txt","resource_type":"objects","resource_name":"dir/file.txt","parent":"projects/_/buckets/my-bucket","segments":{"projects":"_","buckets":"my-bucket","objects":"dir/file.txt"}}'),
      -- AIP-122 examples in the three forms
      ('publishers/123/books/les-miserables',
       '{"relative_name":"publishers/123/books/les-miserables","resource_type":"books","resource_name":"les-miserables","parent":"publishers/123","segments":{"publishers":"123","books":"les-miserables"}}'),
      ('//library.googleapis.com/publishers/123/books/les-miserables',
       '{"service":"library.googleapis.com","relative_name":"publishers/123/books/les-miserables","resource_type":"books","resource_name":"les-miserables","parent":"publishers/123","segments":{"publishers":"123","books":"les-miserables"}}'),
      ('https://library.googleapis.com/v1/publishers/123/books/les-miserables',
       '{"service":"library.googleapis.com","relative_name":"publishers/123/books/les-miserables","resource_type":"books","resource_name":"les-miserables","parent":"publishers/123","segments":{"publishers":"123","books":"les-miserables"}}'),
      -- selfLinks
      ('https://www.googleapis.com/compute/v1/projects/my-project/zones/us-central1-a/instances/vm1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/zones/us-central1-a/instances/vm1","project":"my-project","location":"us-central1-a","resource_type":"instances","resource_name":"vm1","parent":"projects/my-project/zones/us-central1-a","segments":{"projects":"my-project","zones":"us-central1-a","instances":"vm1"}}'),
      ('https://compute.googleapis.com/compute/v1/projects/my-project/global/networks/default',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/global/networks/default","project":"my-project","location":"global","resource_type":"networks","resource_name":"default","parent":"projects/my-project","segments":{"projects":"my-project","networks":"default"}}'),
      ('https://www.googleapis.com/compute/beta/projects/my-project/zones/us-central1-a/instances/vm1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/zones/us-central1-a/instances/vm1","project":"my-project","location":"us-central1-a","resource_type":"instances","resource_name":"vm1","parent":"projects/my-project/zones/us-central1-a","segments":{"projects":"my-project","zones":"us-central1-a","instances":"vm1"}}'),
      ('https://www.googleapis.com/compute/projects/my-project/zones/us-central1-a/instances/vm1', NULL),
      ('https://www.googleapis.com/compute/v1beta1/projects/my-project/zones/us-central1-a/instances/vm1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/zones/us-central1-a/instances/vm1","project":"my-project","location":"us-central1-a","resource_type":"instances","resource_name":"vm1","parent":"projects/my-project/zones/us-central1-a","segments":{"projects":"my-project","zones":"us-central1-a","instances":"vm1"}}'),
      ('https://www.googleapis.com/storage/v1/b/my-bucket/o/file.txt',
       '{"service":"storage.googleapis.com","relative_name":"b/my-bucket/o/file.txt","resource_type":"o","resource_name":"file.txt","parent":"b/my-bucket","segments":{"b":"my-bucket","o":"file.txt"}}'),
      ('  //pubsub.googleapis.com/projects/my-project/topics/t1 ',
       '{"service":"pubsub.googleapis.com","relative_name":"projects/my-project/topics/t1","project":"my-project","resource_type":"topics","resource_name":"t1","parent":"projects/my-project","segments":{"projects":"my-project","topics":"t1"}}'),
      ('projects/my-project/', NULL),
      ('projects/my-project/instances', NULL),
      ('projects/my-project/zones/z1/zones/z2', NULL),
      ('projects//instances/i1', NULL),
      ('/projects/my-project', NULL),
      ('https://www.googleapis.com/', NULL),
      ('s3://bucket/key', NULL),
      ('foo', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: gcp_principal_parse',
          raise_error(concat('gcp_principal_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(gcp_principal_parse(input)) AS actual
    FROM VALUES
      ('user:alex@example.com', '{"principal_type":"user","identity":"alex@example.com","domain":"example.com","deleted":false}'),
      ('principal://goog/subject/alex@example.com', '{"principal_type":"user","identity":"alex@example.com","domain":"example.com","deleted":false}'),
      ('deleted:user:alex@example.com?uid=123456789012345678901',
       '{"principal_type":"user","identity":"alex@example.com","domain":"example.com","deleted":true,"uid":"123456789012345678901"}'),
      ('deleted:principal://goog/subject/alex@example.com?uid=123456789012345678901',
       '{"principal_type":"user","identity":"alex@example.com","domain":"example.com","deleted":true,"uid":"123456789012345678901"}'),
      ('serviceAccount:my-service-account@my-project.iam.gserviceaccount.com',
       '{"principal_type":"service_account","identity":"my-service-account@my-project.iam.gserviceaccount.com","domain":"my-project.iam.gserviceaccount.com","deleted":false}'),
      ('principal://iam.googleapis.com/projects/-/serviceAccounts/my-service-account@my-project.iam.gserviceaccount.com',
       '{"principal_type":"service_account","identity":"my-service-account@my-project.iam.gserviceaccount.com","domain":"my-project.iam.gserviceaccount.com","deleted":false}'),
      ('deleted:serviceAccount:my-service-account@my-project.iam.gserviceaccount.com?uid=123456789012345678901',
       '{"principal_type":"service_account","identity":"my-service-account@my-project.iam.gserviceaccount.com","domain":"my-project.iam.gserviceaccount.com","deleted":true,"uid":"123456789012345678901"}'),
      ('deleted:principal://iam.googleapis.com/projects/-/serviceAccounts/my-service-account@my-project.iam.gserviceaccount.com?uid=123456789012345678901',
       '{"principal_type":"service_account","identity":"my-service-account@my-project.iam.gserviceaccount.com","domain":"my-project.iam.gserviceaccount.com","deleted":true,"uid":"123456789012345678901"}'),
      ('group:my-group@example.com', '{"principal_type":"group","identity":"my-group@example.com","domain":"example.com","deleted":false}'),
      ('principalSet://goog/group/my-group@example.com', '{"principal_type":"group","identity":"my-group@example.com","domain":"example.com","deleted":false}'),
      ('deleted:group:my-group@example.com?uid=123456789012345678901',
       '{"principal_type":"group","identity":"my-group@example.com","domain":"example.com","deleted":true,"uid":"123456789012345678901"}'),
      ('deleted:principalSet://goog/group/my-group@example.com?uid=123456789012345678901',
       '{"principal_type":"group","identity":"my-group@example.com","domain":"example.com","deleted":true,"uid":"123456789012345678901"}'),
      ('domain:example.com', '{"principal_type":"domain","identity":"example.com","domain":"example.com","deleted":false}'),
      ('allUsers', '{"principal_type":"all_users","deleted":false}'),
      ('allAuthenticatedUsers', '{"principal_type":"all_authenticated_users","deleted":false}'),
      ('principalSet://goog/public:all', '{"principal_type":"all_principals","deleted":false}'),
      ('principalSet://goog/cloudIdentityCustomerId/C01Abc35', '{"principal_type":"cloud_identity_customer","identity":"C01Abc35","deleted":false}'),
      ('principalSet://cloudresourcemanager.googleapis.com/projects/123456789012/type/ServiceAccount',
       '{"principal_type":"service_account_set","identity":"projects/123456789012","deleted":false}'),
      ('principalSet://cloudresourcemanager.googleapis.com/folders/123456789012/type/ServiceAccount',
       '{"principal_type":"service_account_set","identity":"folders/123456789012","deleted":false}'),
      ('principalSet://cloudresourcemanager.googleapis.com/organizations/123456789012/type/ServiceAccount',
       '{"principal_type":"service_account_set","identity":"organizations/123456789012","deleted":false}'),
      ('principalSet://cloudresourcemanager.googleapis.com/projects/123456789012/type/ServiceAgent',
       '{"principal_type":"service_agent_set","identity":"projects/123456789012","deleted":false}'),
      ('principal://iam.googleapis.com/locations/global/workforcePools/my-pool/subject/alex@example.com',
       '{"principal_type":"workforce_identity","identity":"alex@example.com","pool":"my-pool","deleted":false}'),
      ('deleted:principal://iam.googleapis.com/locations/global/workforcePools/my-pool/subject/alex@example.com',
       '{"principal_type":"workforce_identity","identity":"alex@example.com","pool":"my-pool","deleted":true}'),
      ('principalSet://iam.googleapis.com/locations/global/workforcePools/my-pool/group/my-group',
       '{"principal_type":"workforce_identity_set","identity":"group/my-group","pool":"my-pool","deleted":false}'),
      ('principalSet://iam.googleapis.com/locations/global/workforcePools/my-pool/attribute.department/sales',
       '{"principal_type":"workforce_identity_set","identity":"attribute.department/sales","pool":"my-pool","deleted":false}'),
      ('principalSet://iam.googleapis.com/locations/global/workforcePools/my-pool/*',
       '{"principal_type":"workforce_identity_set","identity":"*","pool":"my-pool","deleted":false}'),
      ('principal://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/my-pool/subject/system:serviceaccount:ns1:sa1',
       '{"principal_type":"workload_identity","identity":"system:serviceaccount:ns1:sa1","pool":"my-pool","deleted":false}'),
      ('principalSet://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/my-pool/group/admins',
       '{"principal_type":"workload_identity_set","identity":"group/admins","pool":"my-pool","deleted":false}'),
      ('principalSet://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/my-pool/*',
       '{"principal_type":"workload_identity_set","identity":"*","pool":"my-pool","deleted":false}'),
      ('principal://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/my-project.svc.id.goog/subject/ns/default/sa/app',
       '{"principal_type":"gke_workload","identity":"ns/default/sa/app","pool":"my-project.svc.id.goog","deleted":false}'),
      ('principalSet://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/my-project.svc.id.goog/namespace/default',
       '{"principal_type":"gke_workload","identity":"namespace/default","pool":"my-project.svc.id.goog","deleted":false}'),
      ('serviceAccount:my-project.svc.id.goog[default/app]',
       '{"principal_type":"gke_workload","identity":"default/app","pool":"my-project.svc.id.goog","deleted":false}'),
      ('principal://agents.global.org-123456789012.system.id.goog/resources/aiplatform/projects/123456789012/locations/us-central1/reasoningEngines/1',
       '{"principal_type":"agent_identity","identity":"aiplatform/projects/123456789012/locations/us-central1/reasoningEngines/1","deleted":false}'),
      ('principalSet://agents.global.org-123456789012.system.id.goog/*',
       '{"principal_type":"principal_set","identity":"agents.global.org-123456789012.system.id.goog/*","deleted":false}'),
      ('principal://example.com/foo', '{"principal_type":"principal","identity":"example.com/foo","deleted":false}'),
      (' user:alex@example.com ', '{"principal_type":"user","identity":"alex@example.com","domain":"example.com","deleted":false}'),
      ('alex@example.com', NULL),
      ('user:', NULL),
      ('projectOwner:my-project', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: gcp_service_account_parse',
          raise_error(concat('gcp_service_account_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(gcp_service_account_parse(input)) AS actual
    FROM VALUES
      ('my-service-account@my-project.iam.gserviceaccount.com', '{"account_type":"user_managed","project_id":"my-project"}'),
      ('serviceAccount:my-service-account@my-project.iam.gserviceaccount.com', '{"account_type":"user_managed","project_id":"my-project"}'),
      ('my-project@appspot.gserviceaccount.com', '{"account_type":"default_app_engine","project_id":"my-project"}'),
      ('123456789012-compute@developer.gserviceaccount.com', '{"account_type":"default_compute","project_number":"123456789012"}'),
      ('123456789012@cloudservices.gserviceaccount.com', '{"account_type":"service_agent","project_number":"123456789012","service":"cloudservices"}'),
      ('123456789012@cloudbuild.gserviceaccount.com', '{"account_type":"service_agent","project_number":"123456789012","service":"cloudbuild"}'),
      ('service-123456789012@gcp-sa-pubsub.iam.gserviceaccount.com', '{"account_type":"service_agent","project_number":"123456789012","service":"pubsub"}'),
      ('service-123456789012@compute-system.iam.gserviceaccount.com', '{"account_type":"service_agent","project_number":"123456789012","service":"compute-system"}'),
      ('bqcx-442188550395-jujw@gcp-sa-bigquery-condel.iam.gserviceaccount.com', '{"account_type":"service_agent","service":"bigquery-condel"}'),
      ('service-agent-manager@system.gserviceaccount.com', '{"account_type":"service_agent","service":"system"}'),
      ('alex@example.com', NULL),
      ('my-project.iam.gserviceaccount.com', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);
