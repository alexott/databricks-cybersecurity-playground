-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Tests for `cloud_resources.sql`
-- MAGIC
-- MAGIC Each cell fails with `raise_error` and lists the failed cases. Create the functions from `../aws_identifiers.sql`, `../azure_identifiers.sql`,
-- MAGIC `../gcp_identifiers.sql` and `../cloud_resources.sql` in the same schema first.

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: storage_uri_parse',
          raise_error(concat('storage_uri_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(storage_uri_parse(input)) AS actual
    FROM VALUES
      ('s3://my-bucket/k/x.parquet', '{"cloud":"aws","scheme":"s3","container":"my-bucket","path":"k/x.parquet"}'),
      ('s3a://my-bucket/k', '{"cloud":"aws","scheme":"s3a","container":"my-bucket","path":"k"}'),
      ('s3n://my-bucket/k', '{"cloud":"aws","scheme":"s3n","container":"my-bucket","path":"k"}'),
      ('s3://my-bucket', '{"cloud":"aws","scheme":"s3","container":"my-bucket"}'),
      ('s3://my-bucket/', '{"cloud":"aws","scheme":"s3","container":"my-bucket"}'),
      ('S3://my-bucket/k', '{"cloud":"aws","scheme":"s3","container":"my-bucket","path":"k"}'),
      ('gs://my-bucket/o/p.json', '{"cloud":"gcp","scheme":"gs","container":"my-bucket","path":"o/p.json"}'),
      ('abfss://container1@account1.dfs.core.windows.net/p/q',
       '{"cloud":"azure","scheme":"abfss","account":"account1","container":"container1","path":"p/q"}'),
      ('abfs://container1@account1.dfs.core.windows.net/p',
       '{"cloud":"azure","scheme":"abfs","account":"account1","container":"container1","path":"p"}'),
      ('wasbs://container1@account1.blob.core.windows.net/p',
       '{"cloud":"azure","scheme":"wasbs","account":"account1","container":"container1","path":"p"}'),
      ('wasb://container1@account1.blob.core.windows.net',
       '{"cloud":"azure","scheme":"wasb","account":"account1","container":"container1"}'),
      ('abfss://container1@account1.dfs.core.chinacloudapi.cn/p',
       '{"cloud":"azure","scheme":"abfss","account":"account1","container":"container1","path":"p"}'),
      (' s3://my-bucket/k ', '{"cloud":"aws","scheme":"s3","container":"my-bucket","path":"k"}'),
      ('abfss://account1.dfs.core.windows.net/p', NULL),
      ('dbfs:/mnt/x', NULL),
      ('/Volumes/main/default/v1/f.csv', NULL),
      ('https://account1.blob.core.windows.net/container1/p', NULL),
      ('s3://', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: cloud_resource_to_ocsf',
          raise_error(concat('cloud_resource_to_ocsf: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(cloud_resource_to_ocsf(input)) AS actual
    FROM VALUES
      ('arn:aws:iam::123456789012:role/application_abc/component_xyz/RDSAccess',
       '{"cloud":{"provider":"AWS","cloud_partition":"aws","account":{"uid":"123456789012","type":"AWS Account","type_id":10}},"resource":{"uid":"arn:aws:iam::123456789012:role/application_abc/component_xyz/RDSAccess","name":"RDSAccess","type":"iam:role","cloud_partition":"aws"}}'),
      ('arn:aws:s3:::example_bucket/marketing/report.csv',
       '{"cloud":{"provider":"AWS","cloud_partition":"aws"},"resource":{"uid":"arn:aws:s3:::example_bucket/marketing/report.csv","name":"example_bucket/marketing/report.csv","type":"s3:object","cloud_partition":"aws"}}'),
      ('arn:aws:ec2:us-east-1:123456789012:instance/i-0123456789abcdef0',
       '{"cloud":{"provider":"AWS","region":"us-east-1","cloud_partition":"aws","account":{"uid":"123456789012","type":"AWS Account","type_id":10}},"resource":{"uid":"arn:aws:ec2:us-east-1:123456789012:instance/i-0123456789abcdef0","name":"i-0123456789abcdef0","type":"ec2:instance","region":"us-east-1","cloud_partition":"aws"}}'),
      ('arn:aws:sns:us-east-1:123456789012:example-sns-topic-name',
       '{"cloud":{"provider":"AWS","region":"us-east-1","cloud_partition":"aws","account":{"uid":"123456789012","type":"AWS Account","type_id":10}},"resource":{"uid":"arn:aws:sns:us-east-1:123456789012:example-sns-topic-name","name":"example-sns-topic-name","type":"sns","region":"us-east-1","cloud_partition":"aws"}}'),
      ('arn:aws:iam::aws:policy/AdministratorAccess',
       '{"cloud":{"provider":"AWS","cloud_partition":"aws"},"resource":{"uid":"arn:aws:iam::aws:policy/AdministratorAccess","name":"AdministratorAccess","type":"iam:policy","cloud_partition":"aws"}}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1',
       '{"cloud":{"provider":"Azure","account":{"uid":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","type":"Azure Subscription","type_id":13}},"resource":{"uid":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1","name":"vm1","type":"Microsoft.Compute/virtualMachines","group":{"name":"rg1"}}}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Authorization/roleAssignments/bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f',
       '{"cloud":{"provider":"Azure","account":{"uid":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","type":"Azure Subscription","type_id":13}},"resource":{"uid":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Authorization/roleAssignments/bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f","name":"bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f","type":"Microsoft.Authorization/roleAssignments","group":{"name":"rg1"}}}'),
      ('/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/Microsoft.aadiam',
       '{"cloud":{"provider":"Azure","org":{"uid":"aaaabbbb-0000-cccc-1111-dddd2222eeee"}},"resource":{"uid":"/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/Microsoft.aadiam","type":"Microsoft.aadiam"}}'),
      ('"/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/NSG1/"',
       '{"cloud":{"provider":"Azure","account":{"uid":"AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E","type":"Azure Subscription","type_id":13}},"resource":{"uid":"/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/NSG1","name":"NSG1","type":"MICROSOFT.NETWORK/NETWORKSECURITYGROUPS","group":{"name":"RG1"}}}'),
      ('//compute.googleapis.com/projects/my-project/zones/us-central1-a/instances/vm1',
       '{"cloud":{"provider":"GCP","region":"us-central1","zone":"us-central1-a","account":{"uid":"my-project","type":"GCP Project","type_id":11}},"resource":{"uid":"//compute.googleapis.com/projects/my-project/zones/us-central1-a/instances/vm1","name":"vm1","type":"compute.googleapis.com/instances","region":"us-central1","zone":"us-central1-a"}}'),
      ('//container.googleapis.com/projects/my-project/locations/us-central1-a/clusters/c1',
       '{"cloud":{"provider":"GCP","region":"us-central1","zone":"us-central1-a","account":{"uid":"my-project","type":"GCP Project","type_id":11}},"resource":{"uid":"//container.googleapis.com/projects/my-project/locations/us-central1-a/clusters/c1","name":"c1","type":"container.googleapis.com/clusters","region":"us-central1","zone":"us-central1-a"}}'),
      ('//container.googleapis.com/projects/my-project/locations/us-central1/clusters/c1',
       '{"cloud":{"provider":"GCP","region":"us-central1","account":{"uid":"my-project","type":"GCP Project","type_id":11}},"resource":{"uid":"//container.googleapis.com/projects/my-project/locations/us-central1/clusters/c1","name":"c1","type":"container.googleapis.com/clusters","region":"us-central1"}}'),
      ('https://www.googleapis.com/compute/v1/projects/my-project/global/networks/default',
       '{"cloud":{"provider":"GCP","region":"global","account":{"uid":"my-project","type":"GCP Project","type_id":11}},"resource":{"uid":"//compute.googleapis.com/projects/my-project/global/networks/default","name":"default","type":"compute.googleapis.com/networks","region":"global"}}'),
      ('//cloudresourcemanager.googleapis.com/folders/123456789012',
       '{"cloud":{"provider":"GCP","org":{"ou_uid":"123456789012"}},"resource":{"uid":"//cloudresourcemanager.googleapis.com/folders/123456789012","name":"123456789012","type":"cloudresourcemanager.googleapis.com/folders"}}'),
      ('projects/my-project/topics/t1',
       '{"cloud":{"provider":"GCP","account":{"uid":"my-project","type":"GCP Project","type_id":11}},"resource":{"uid":"projects/my-project/topics/t1","name":"t1","type":"topics"}}'),
      ('arn:aws:sqs', NULL),
      ('/foo/bar', NULL),
      ('s3://my-bucket/k', NULL),
      ('vm1', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);
