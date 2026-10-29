-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Tests for `aws_identifiers.sql`
-- MAGIC
-- MAGIC Each cell fails with `raise_error` and lists the failed cases. Create the functions from `../aws_identifiers.sql` in the same schema first.
-- MAGIC
-- MAGIC The values that look like AWS keys (`AKIA...`, `AIDA...`, ...) are built with `concat`, because the repository secret scanner blocks them.
-- MAGIC They are the examples from the AWS documentation and from the published write-ups about access key IDs, not real keys.

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: aws_arn_parse',
          raise_error(concat('aws_arn_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(aws_arn_parse(input)) AS actual
    FROM VALUES
      ('arn:aws:iam::123456789012:user/John',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"user/John","resource_type":"user","resource_id":"John"}'),
      ('arn:aws:iam::123456789012:user/division_abc/subdivision_xyz/Jane',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"user/division_abc/subdivision_xyz/Jane","resource_type":"user","resource_id":"division_abc/subdivision_xyz/Jane"}'),
      ('arn:aws:iam::123456789012:group/division_abc/subdivision_xyz/product_A/Developers',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"group/division_abc/subdivision_xyz/product_A/Developers","resource_type":"group","resource_id":"division_abc/subdivision_xyz/product_A/Developers"}'),
      ('arn:aws:iam::123456789012:role/aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"role/aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer","resource_type":"role","resource_id":"aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer"}'),
      ('arn:aws:iam::123456789012:root',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"root","resource_id":"root"}'),
      ('arn:aws:sts::123456789012:self',
       '{"partition":"aws","service":"sts","account_id":"123456789012","resource":"self","resource_id":"self"}'),
      ('arn:aws:sts::123456789012:federated-user/Paulo',
       '{"partition":"aws","service":"sts","account_id":"123456789012","resource":"federated-user/Paulo","resource_type":"federated-user","resource_id":"Paulo"}'),
      ('arn:aws:sts::123456789012:assumed-role/Accounting-Role/Mary',
       '{"partition":"aws","service":"sts","account_id":"123456789012","resource":"assumed-role/Accounting-Role/Mary","resource_type":"assumed-role","resource_id":"Accounting-Role/Mary"}'),
      ('arn:aws:iam::123456789012:u2f/user/John/default',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"u2f/user/John/default","resource_type":"u2f","resource_id":"user/John/default"}'),
      ('arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/a1b2c3d4567890abcdefEXAMPLE11111',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/a1b2c3d4567890abcdefEXAMPLE11111","resource_type":"oidc-provider","resource_id":"oidc.eks.us-west-2.amazonaws.com/id/a1b2c3d4567890abcdefEXAMPLE11111"}'),
      ('arn:aws:iam::aws:contextProvider/IdentityCenter',
       '{"partition":"aws","service":"iam","account_id":"aws","resource":"contextProvider/IdentityCenter","resource_type":"contextProvider","resource_id":"IdentityCenter"}'),
      ('arn:aws:sns:us-east-1:123456789012:example-sns-topic-name',
       '{"partition":"aws","service":"sns","region":"us-east-1","account_id":"123456789012","resource":"example-sns-topic-name","resource_id":"example-sns-topic-name"}'),
      ('arn:aws:ec2:us-east-1:123456789012:vpc/vpc-0e9801d129EXAMPLE',
       '{"partition":"aws","service":"ec2","region":"us-east-1","account_id":"123456789012","resource":"vpc/vpc-0e9801d129EXAMPLE","resource_type":"vpc","resource_id":"vpc-0e9801d129EXAMPLE"}'),
      ('arn:aws:ec2:*:123456789012:vpc/*',
       '{"partition":"aws","service":"ec2","region":"*","account_id":"123456789012","resource":"vpc/*","resource_type":"vpc","resource_id":"*"}'),
      ('arn:aws:qbusiness:*:123456789012:*',
       '{"partition":"aws","service":"qbusiness","region":"*","account_id":"123456789012","resource":"*","resource_id":"*"}'),
      ('arn:aws:iam::*:user/division_abc/subdivision_xyz/Richard',
       '{"partition":"aws","service":"iam","account_id":"*","resource":"user/division_abc/subdivision_xyz/Richard","resource_type":"user","resource_id":"division_abc/subdivision_xyz/Richard"}'),
      ('arn:aws:lambda:us-east-2:123456789012:function:my-function:prod',
       '{"partition":"aws","service":"lambda","region":"us-east-2","account_id":"123456789012","resource":"function:my-function:prod","resource_type":"function","resource_id":"my-function:prod"}'),
      ('arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/x:*',
       '{"partition":"aws","service":"logs","region":"us-east-1","account_id":"123456789012","resource":"log-group:/aws/lambda/x:*","resource_type":"log-group","resource_id":"/aws/lambda/x:*"}'),
      ('arn:aws:s3:::example_bucket',
       '{"partition":"aws","service":"s3","resource":"example_bucket","resource_type":"bucket","resource_id":"example_bucket"}'),
      ('arn:aws:s3:::example_bucket/marketing/newproductlaunch/widget/*',
       '{"partition":"aws","service":"s3","resource":"example_bucket/marketing/newproductlaunch/widget/*","resource_type":"object","resource_id":"example_bucket/marketing/newproductlaunch/widget/*"}'),
      ('arn:aws:s3:::amzn-s3-demo-bucket/????-test',
       '{"partition":"aws","service":"s3","resource":"amzn-s3-demo-bucket/????-test","resource_type":"object","resource_id":"amzn-s3-demo-bucket/????-test"}'),
      ('arn:aws:s3:us-west-2:123456789012:accesspoint/my-access-point',
       '{"partition":"aws","service":"s3","region":"us-west-2","account_id":"123456789012","resource":"accesspoint/my-access-point","resource_type":"accesspoint","resource_id":"my-access-point"}'),
      ('arn:aws-cn:iam::123456789012:root',
       '{"partition":"aws-cn","service":"iam","account_id":"123456789012","resource":"root","resource_id":"root"}'),
      ('arn:aws-us-gov:iam::123456789012:root',
       '{"partition":"aws-us-gov","service":"iam","account_id":"123456789012","resource":"root","resource_id":"root"}'),
      ('  arn:aws:iam::123456789012:root  ',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"root","resource_id":"root"}'),
      ('arn:aws:iam::123456789012:user/',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"user/","resource_type":"user"}'),
      ('arn:aws:sqs', NULL),
      ('arn:aws:iam::123456789012:', NULL),
      ('arn:azure:iam::123456789012:root', NULL),
      ('AWS Internal', NULL),
      ('123456789012', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: aws_role_name, aws_assumed_role_to_role_arn',
          raise_error(concat('aws_role_name, aws_assumed_role_to_role_arn: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(name, 'NULL'), ' / ', coalesce(role_arn, 'NULL'),
                ', expected ', coalesce(expected_name, 'NULL'), ' / ', coalesce(expected_role_arn, 'NULL')) AS failure
  FROM (
    SELECT input, expected_name, expected_role_arn, aws_role_name(input) AS name, aws_assumed_role_to_role_arn(input) AS role_arn
    FROM VALUES
      ('arn:aws:sts::123456789012:assumed-role/Accounting-Role/Mary', 'Accounting-Role', 'arn:aws:iam::123456789012:role/Accounting-Role'),
      ('arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_Admin_0123456789abcdef/alice@example.com',
       'AWSReservedSSO_Admin_0123456789abcdef', 'arn:aws:iam::123456789012:role/AWSReservedSSO_Admin_0123456789abcdef'),
      ('arn:aws-us-gov:sts::123456789012:assumed-role/Ops/i-0abc', 'Ops', 'arn:aws-us-gov:iam::123456789012:role/Ops'),
      ('arn:aws:iam::123456789012:role/S3Access', 'S3Access', 'arn:aws:iam::123456789012:role/S3Access'),
      ('arn:aws:iam::123456789012:role/application_abc/component_xyz/RDSAccess', 'RDSAccess',
       'arn:aws:iam::123456789012:role/application_abc/component_xyz/RDSAccess'),
      ('arn:aws:iam::123456789012:role/aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer',
       'AWSServiceRoleForAccessAnalyzer', 'arn:aws:iam::123456789012:role/aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer'),
      ('arn:aws:iam::123456789012:role/service-role/QuickSightAction', 'QuickSightAction',
       'arn:aws:iam::123456789012:role/service-role/QuickSightAction'),
      ('arn:aws:sts::123456789012:federated-user/Paulo', NULL, NULL),
      ('arn:aws:iam::123456789012:role/', NULL, NULL),
      ('arn:aws:sts::123456789012:assumed-role/', NULL, NULL),
      ('arn:aws:iam::123456789012:user/John', NULL, NULL),
      ('arn:aws:iam::123456789012:root', NULL, NULL),
      ('AWS Internal', NULL, NULL),
      (NULL, NULL, NULL)
      AS t(input, expected_name, expected_role_arn)
  )
  WHERE NOT (name <=> expected_name AND role_arn <=> expected_role_arn)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: aws_unique_id_type',
          raise_error(concat('aws_unique_id_type: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, aws_unique_id_type(input) AS actual
    FROM VALUES
      -- examples from the "Unique identifiers" section of the IAM identifiers page
      (concat('AI', 'DA', 'JQABLZS4A3QDU576Q'), 'user'),
      (concat('AI', 'DA', 'CKCEVSQ6C2EXAMPLE'), 'user'),
      (concat('AR', 'OA', 'DBQP57FF2AEXAMPLE'), 'role'),
      (concat('AR', 'OA', 'DBQP57FF2AEXAMPLE', ':role-session-name'), 'role'),
      (concat('AR', 'OA', '1234567890EXAMPLE'), 'role'),
      -- the AWS documentation example access key ID
      (concat('AK', 'IA', 'IOSFODNN7EXAMPLE'), 'access_key'),
      -- one ID for each prefix
      (concat('AB', 'IA', 'EXAMPLE123456789'), 'sts_bearer_token'),
      (concat('AC', 'CA', 'EXAMPLE123456789'), 'context_specific_credential'),
      (concat('AG', 'PA', 'EXAMPLE123456789'), 'group'),
      (concat('AI', 'PA', 'EXAMPLE123456789'), 'instance_profile'),
      (concat('AN', 'PA', 'EXAMPLE123456789'), 'managed_policy'),
      (concat('AN', 'VA', 'EXAMPLE123456789'), 'managed_policy_version'),
      (concat('AP', 'KA', 'EXAMPLE123456789'), 'public_key'),
      (concat('AS', 'CA', 'EXAMPLE123456789'), 'certificate'),
      (concat('AS', 'IA', 'EXAMPLE123456789'), 'temporary_access_key'),
      (concat(' AI', 'DA', 'JQABLZS4A3QDU576Q '), 'user'),
      ('111122223333', NULL),
      ('XXXXJQABLZS4A3QDU576Q', NULL),
      (lower(concat('AI', 'DA', 'JQABLZS4A3QDU576Q')), NULL),
      (concat('AI', 'DA', 'SHORT'), NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: aws_account_id_from_access_key',
          raise_error(concat('aws_account_id_from_access_key: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, aws_account_id_from_access_key(input) AS actual
    FROM VALUES
      -- vectors from https://awsteele.com/blog/2020/09/26/aws-access-key-format.html
      (concat('AS', 'IA', 'Y34FZKBOKMUTVV7A'), '609629065308'),
      (concat('AS', 'IA', 'Y34FZKBNKMUTVV7A'), '609629065306'),
      (concat('AS', 'IA', 'Y34FZKAOKMUTVV7A'), '609629065244'),
      (concat('AS', 'IA', 'QAAAAAAAAAAAAAAA'), '000000000000'),
      (concat('AS', 'IA', '6RVFFB77AAAAAAAA'), '999999999998'),
      (concat('AK', 'IA', 'Y34FZKBOKMUTVV7A'), '609629065308'),
      (concat(' AS', 'IA', 'Y34FZKBOKMUTVV7A '), '609629065308'),
      -- a key created before 2019-03-29 (the AWS documentation example) doesn't encode the account ID
      (concat('AK', 'IA', 'IOSFODNN7EXAMPLE'), NULL),
      (concat('AR', 'OA', 'Y34FZKBOKMUTVV7A'), NULL),
      (concat('AS', 'IA', 'Y34FZKBOKMUTVV7'), NULL),
      (concat('AS', 'IA', 'Y34FZKBOKMUTVV71'), NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);
