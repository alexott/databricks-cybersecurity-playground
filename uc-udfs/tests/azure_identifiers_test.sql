-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Tests for `azure_identifiers.sql`
-- MAGIC
-- MAGIC Each cell fails with `raise_error` and lists the failed cases. Create the functions from `../azure_identifiers.sql` in the same schema first.
-- MAGIC
-- MAGIC The IDs are the formats from the ARM template functions documentation and the ID shapes found in Azure logs
-- MAGIC (Entra ID audit, sign-in and diagnostic logs, NSG and Key Vault logs, Log Analytics exports), with placeholder subscription and tenant IDs and names.

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: azure_resource_id_parse',
          raise_error(concat('azure_resource_id_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(azure_resource_id_parse(input)) AS actual
    FROM VALUES
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/otherResourceGroup/providers/Microsoft.Storage/storageAccounts/examplestorage',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"otherResourceGroup","provider_namespace":"Microsoft.Storage","resource_type":"Microsoft.Storage/storageAccounts","resource_name":"examplestorage","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/otherResourceGroup"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/examplegroup/providers/Microsoft.SQL/servers/serverName/databases/databaseName',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"examplegroup","provider_namespace":"Microsoft.SQL","resource_type":"Microsoft.SQL/servers/databases","resource_name":"databaseName","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/examplegroup/providers/Microsoft.SQL/servers/serverName"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/vnet1/subnets/subnet1',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Network","resource_type":"Microsoft.Network/virtualNetworks/subnets","resource_name":"subnet1","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/vnet1"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1/providers/Microsoft.Authorization/roleAssignments/bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/roleAssignments","resource_name":"bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Authorization/roleAssignments/bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/roleAssignments","resource_name":"bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/providers/Microsoft.Authorization/roleDefinitions/b24988ac-6180-42a0-ab88-20f7382dd24c',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/roleDefinitions","resource_name":"b24988ac-6180-42a0-ab88-20f7382dd24c","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"}'),
      ('/providers/Microsoft.Management/managementGroups/mg1',
       '{"management_group":"mg1","provider_namespace":"Microsoft.Management","resource_type":"Microsoft.Management/managementGroups","resource_name":"mg1"}'),
      ('/providers/Microsoft.Management/managementGroups/mg1/providers/Microsoft.Authorization/policyDefinitions/p1',
       '{"management_group":"mg1","provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/policyDefinitions","resource_name":"p1","parent_id":"/providers/Microsoft.Management/managementGroups/mg1"}'),
      ('/providers/Microsoft.Management/managementGroups/cccc2c2c-dd3d-ee4e-ff5f-aaaaaa6a6a6a',
       '{"management_group":"cccc2c2c-dd3d-ee4e-ff5f-aaaaaa6a6a6a","provider_namespace":"Microsoft.Management","resource_type":"Microsoft.Management/managementGroups","resource_name":"cccc2c2c-dd3d-ee4e-ff5f-aaaaaa6a6a6a"}'),
      ('/providers/Microsoft.Authorization/policyDefinitions/bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f',
       '{"provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/policyDefinitions","resource_name":"bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_type":"Microsoft.Resources/subscriptions","resource_name":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","resource_type":"Microsoft.Resources/resourceGroups","resource_name":"rg1","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"}'),
      ('/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/Microsoft.aadiam',
       '{"tenant_id":"aaaabbbb-0000-cccc-1111-dddd2222eeee","provider_namespace":"Microsoft.aadiam","resource_type":"Microsoft.aadiam","parent_id":"/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee"}'),
      ('/TENANTS/AAAABBBB-0000-CCCC-1111-DDDD2222EEEE/PROVIDERS/MICROSOFT.AADIAM',
       '{"tenant_id":"AAAABBBB-0000-CCCC-1111-DDDD2222EEEE","provider_namespace":"MICROSOFT.AADIAM","resource_type":"MICROSOFT.AADIAM","parent_id":"/TENANTS/AAAABBBB-0000-CCCC-1111-DDDD2222EEEE"}'),
      ('/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/microsoft.aadiam',
       '{"tenant_id":"aaaabbbb-0000-cccc-1111-dddd2222eeee","provider_namespace":"microsoft.aadiam","resource_type":"microsoft.aadiam","parent_id":"/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee"}'),
      -- a resource named providers: the split is at the last /providers/ that is followed by a namespace
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Web/sites/providers/config/web',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Web","resource_type":"Microsoft.Web/sites/config","resource_name":"web","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Web/sites/providers"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Web/sites/providers/providers/Microsoft.Authorization/roleAssignments/r1',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/roleAssignments","resource_name":"r1","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Web/sites/providers"}'),
      ('/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee',
       '{"tenant_id":"aaaabbbb-0000-cccc-1111-dddd2222eeee"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/providers/Microsoft.Compute',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","provider_namespace":"Microsoft.Compute","resource_type":"Microsoft.Compute","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourcegroups/rg1/providers/microsoft.operationalinsights/workspaces/ws1',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"microsoft.operationalinsights","resource_type":"microsoft.operationalinsights/workspaces","resource_name":"ws1","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourcegroups/rg1"}'),
      ('/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/NSG1',
       '{"subscription_id":"AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E","resource_group":"RG1","provider_namespace":"MICROSOFT.NETWORK","resource_type":"MICROSOFT.NETWORK/NETWORKSECURITYGROUPS","resource_name":"NSG1","parent_id":"/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1"}'),
      ('/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.KEYVAULT/VAULTS/KV1',
       '{"subscription_id":"AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E","resource_group":"RG1","provider_namespace":"MICROSOFT.KEYVAULT","resource_type":"MICROSOFT.KEYVAULT/VAULTS","resource_name":"KV1","parent_id":"/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1/',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Compute","resource_type":"Microsoft.Compute/virtualMachines","resource_name":"vm1","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1"}'),
      ('"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/providers/Microsoft.Authorization/roleDefinitions/b24988ac-6180-42a0-ab88-20f7382dd24c"',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/roleDefinitions","resource_name":"b24988ac-6180-42a0-ab88-20f7382dd24c","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"}'),
      ('/Subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/ResourceGroups/rg1/Providers/Microsoft.Web/sites/app1',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Web","resource_type":"Microsoft.Web/sites","resource_name":"app1","parent_id":"/Subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/ResourceGroups/rg1"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines', NULL),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers', NULL),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute//vm1', NULL),
      ('/foo/bar/providers/Microsoft.Compute/virtualMachines/vm1', NULL),
      ('00000003-0000-0000-c000-000000000000', NULL),
      ('vm1', NULL),
      ('/', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: azure_resource_id_normalize, azure_resource_id_clean',
          raise_error(concat('azure_resource_id_normalize, azure_resource_id_clean: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(normalized, 'NULL'), ' / ', coalesce(cleaned, 'NULL'),
                ', expected ', coalesce(expected_normalized, 'NULL'), ' / ', coalesce(expected_cleaned, 'NULL')) AS failure
  FROM (
    SELECT input, expected_normalized, expected_cleaned,
           azure_resource_id_normalize(input) AS normalized, azure_resource_id_clean(input) AS cleaned
    FROM VALUES
      ('/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.COMPUTE/VIRTUALMACHINES/VM1',
       '/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourcegroups/rg1/providers/microsoft.compute/virtualmachines/vm1',
       '/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.COMPUTE/VIRTUALMACHINES/VM1'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1/',
       '/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourcegroups/rg1/providers/microsoft.compute/virtualmachines/vm1',
       '/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1'),
      ('/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/Microsoft.aadiam',
       '/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/microsoft.aadiam',
       '/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/Microsoft.aadiam'),
      -- the three casings of the Entra ID diagnostic logs give the same key
      ('/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/microsoft.aadiam',
       '/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/microsoft.aadiam',
       '/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/microsoft.aadiam'),
      ('/TENANTS/AAAABBBB-0000-CCCC-1111-DDDD2222EEEE/PROVIDERS/MICROSOFT.AADIAM',
       '/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/microsoft.aadiam',
       '/TENANTS/AAAABBBB-0000-CCCC-1111-DDDD2222EEEE/PROVIDERS/MICROSOFT.AADIAM'),
      ('  "/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"  ',
       '/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e',
       '/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e'),
      ('/', NULL, NULL),
      ('', NULL, NULL),
      (NULL, NULL, NULL)
      AS t(input, expected_normalized, expected_cleaned)
  )
  WHERE NOT (normalized <=> expected_normalized AND cleaned <=> expected_cleaned)
);
