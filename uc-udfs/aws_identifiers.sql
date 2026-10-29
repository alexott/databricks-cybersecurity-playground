-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## User-defined functions to parse AWS identifiers
-- MAGIC
-- MAGIC These SQL UDFs parse [ARNs](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html)
-- MAGIC and [IAM unique IDs](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html) in pure SQL, without a Python sandbox.
-- MAGIC
-- MAGIC | Function | Result |
-- MAGIC |----------|--------|
-- MAGIC | `aws_arn_parse(arn)` | `STRUCT<partition, service, region, account_id, resource, resource_type, resource_id>` |
-- MAGIC | `aws_role_name(arn)` | the role name from an IAM role ARN or an STS assumed-role ARN |
-- MAGIC | `aws_assumed_role_to_role_arn(arn)` | the IAM role ARN for an STS assumed-role ARN (an IAM role ARN is returned unchanged) |
-- MAGIC | `aws_unique_id_type(id)` | the type of an IAM unique ID or access key ID from its prefix: `access_key`, `temporary_access_key`, `user`, `role`, ... |
-- MAGIC | `aws_account_id_from_access_key(key_id)` | the 12-digit account ID encoded in an `AKIA`/`ASIA` access key ID |
-- MAGIC | `aws_arn_parse_parts`, `aws_role_name_parsed`, `aws_assumed_role_to_role_arn_parsed` | helpers, don't call them directly |
-- MAGIC
-- MAGIC Behavior:
-- MAGIC
-- MAGIC - An invalid value or `NULL` gives `NULL`. The functions don't raise errors, so you can use them on log fields that sometimes don't have an ARN (`AWS Internal`, an account ID).
-- MAGIC - `aws_arn_parse` splits `resource` at the first `/` or `:`: `instance/i-0abc` gives `instance` and `i-0abc`, `function:fn:prod` gives `function` and `fn:prod`.
-- MAGIC   If there is no separator, `resource_type` is `NULL` (`arn:aws:sns:us-east-1:123456789012:topic`). The IAM path stays in `resource_id` (`role/division/Admin` gives `division/Admin`).
-- MAGIC - S3 bucket and object ARNs (`arn:aws:s3:::bucket/key`) don't have a type: `resource_type` is `bucket` or `object`, and `resource_id` is the full resource.
-- MAGIC - An empty region or account gives `NULL`. Wildcards (`*`) and the account `aws` (`arn:aws:iam::aws:policy/...`) are kept.
-- MAGIC - Incomplete ARNs (`arn:aws:sqs`, allowed in IAM policies) give `NULL`.
-- MAGIC - **Role paths:** an assumed-role ARN (`arn:aws:sts::123456789012:assumed-role/Admin/alice`) doesn't have the role path. So `aws_assumed_role_to_role_arn` doesn't give the
-- MAGIC   inventory ARN for roles with a path: IAM Identity Center roles (`aws-reserved/sso.amazonaws.com/`), service roles (`service-role/`), and service-linked roles (`aws-service-role/`).
-- MAGIC   For these roles, join on the account ID and `aws_role_name(arn)`.
-- MAGIC - `aws_unique_id_type` ignores a `:...` suffix (`AROA...:session-name`, the form of `aws:userId` and of CloudTrail `userIdentity.principalId`).
-- MAGIC - `aws_account_id_from_access_key` gives the same result as `aws sts get-access-key-info`, without an API call. It uses the
-- MAGIC   [decoding found by Aidan Steele and Tal Be'ery](https://awsteele.com/blog/2020/09/26/aws-access-key-format.html).
-- MAGIC   Keys created before 2019-03-29 (5th character before `Q`) don't encode the account ID, so they give `NULL`.
-- MAGIC
-- MAGIC Create all functions in the same schema: a function body finds the other functions in the schema of that function.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_arn_parse_parts(p ARRAY<STRING>)
  RETURNS STRUCT<`partition`: STRING, service: STRING, region: STRING, account_id: STRING, resource: STRING, resource_type: STRING, resource_id: STRING>
  DETERMINISTIC
  COMMENT 'Helper for aws_arn_parse: builds the result from the ARN split on the first 5 colons'
  RETURN CASE
    WHEN size(p) = 6 AND get(p, 0) = 'arn' AND get(p, 1) RLIKE '^aws[a-z-]*$' AND get(p, 2) != '' AND get(p, 5) != '' THEN named_struct(
      'partition', get(p, 1),
      'service', get(p, 2),
      'region', nullif(get(p, 3), ''),
      'account_id', nullif(get(p, 4), ''),
      'resource', get(p, 5),
      'resource_type', CASE
        WHEN get(p, 2) = 's3' AND get(p, 4) = '' THEN if(get(p, 5) LIKE '%/%', 'object', 'bucket')
        ELSE nullif(regexp_extract(get(p, 5), '^([^/:]*)[/:]', 1), '')
      END,
      'resource_id', CASE
        WHEN get(p, 2) = 's3' AND get(p, 4) = '' THEN get(p, 5)
        WHEN get(p, 5) RLIKE '[/:]' THEN nullif(regexp_extract(get(p, 5), '^[^/:]*[/:](.*)$', 1), '')
        ELSE get(p, 5)
      END)
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_arn_parse(arn STRING)
  RETURNS STRUCT<`partition`: STRING, service: STRING, region: STRING, account_id: STRING, resource: STRING, resource_type: STRING, resource_id: STRING>
  DETERMINISTIC
  COMMENT 'Parses an AWS ARN into partition, service, region, account_id, resource, resource_type and resource_id. Returns NULL for an invalid ARN'
  RETURN aws_arn_parse_parts(split(trim(arn), ':', 6));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_role_name_parsed(a STRUCT<`partition`: STRING, service: STRING, region: STRING, account_id: STRING, resource: STRING, resource_type: STRING, resource_id: STRING>)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Helper for aws_role_name: gets the role name from the result of aws_arn_parse'
  RETURN CASE
    WHEN a.service = 'iam' AND a.resource_type = 'role' THEN nullif(element_at(split(a.resource_id, '/'), -1), '')
    WHEN a.service = 'sts' AND a.resource_type = 'assumed-role' THEN nullif(get(split(a.resource_id, '/'), 0), '')
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_role_name(arn STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Returns the role name from an IAM role ARN (last path segment) or an STS assumed-role ARN. Returns NULL for other values'
  RETURN aws_role_name_parsed(aws_arn_parse(arn));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_assumed_role_to_role_arn_parsed(a STRUCT<`partition`: STRING, service: STRING, region: STRING, account_id: STRING, resource: STRING, resource_type: STRING, resource_id: STRING>)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Helper for aws_assumed_role_to_role_arn: builds the IAM role ARN from the result of aws_arn_parse'
  RETURN CASE
    WHEN a.service = 'iam' AND a.resource_type = 'role' AND a.resource_id IS NOT NULL THEN concat('arn:', a.`partition`, ':iam::', a.account_id, ':', a.resource)
    WHEN a.service = 'sts' AND a.resource_type = 'assumed-role' AND nullif(get(split(a.resource_id, '/'), 0), '') IS NOT NULL
      THEN concat('arn:', a.`partition`, ':iam::', a.account_id, ':role/', get(split(a.resource_id, '/'), 0))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_assumed_role_to_role_arn(arn STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Converts an STS assumed-role ARN to the IAM role ARN (without the role path, which the assumed-role ARN does not have). An IAM role ARN is returned unchanged. Returns NULL for other values'
  RETURN aws_assumed_role_to_role_arn_parsed(aws_arn_parse(arn));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_unique_id_type(id STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Returns the type of an IAM unique ID or access key ID from its prefix (access_key, temporary_access_key, user, role, group, ...). A :suffix is ignored. Returns NULL for an unknown prefix or an invalid value'
  RETURN CASE
    WHEN trim(id) RLIKE '^[A-Z0-9]{16,128}(:.*)?$' THEN CASE substr(trim(id), 1, 4)
      WHEN 'ABIA' THEN 'sts_bearer_token'
      WHEN 'ACCA' THEN 'context_specific_credential'
      WHEN 'AGPA' THEN 'group'
      WHEN 'AIDA' THEN 'user'
      WHEN 'AIPA' THEN 'instance_profile'
      WHEN 'AKIA' THEN 'access_key'
      WHEN 'ANPA' THEN 'managed_policy'
      WHEN 'ANVA' THEN 'managed_policy_version'
      WHEN 'APKA' THEN 'public_key'
      WHEN 'AROA' THEN 'role'
      WHEN 'ASCA' THEN 'certificate'
      WHEN 'ASIA' THEN 'temporary_access_key'
    END
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_account_id_from_access_key(key_id STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Returns the 12-digit AWS account ID encoded in an AKIA/ASIA access key ID (the same as aws sts get-access-key-info). Returns NULL for keys created before 2019-03-29 and for invalid values'
  RETURN CASE
    -- the 5th character must be Q-Z or 2-7: older keys do not encode the account ID
    WHEN trim(key_id) RLIKE '^(AKIA|ASIA)[Q-Z2-7][A-Z2-7]{15}$' THEN lpad(
      CAST(shiftright(
        CAST(conv(substr(array_join(transform(sequence(5, 14),
          -- base32: each character is 5 bits
          i -> lpad(conv(CAST(instr('ABCDEFGHIJKLMNOPQRSTUVWXYZ234567', substr(trim(key_id), i, 1)) - 1 AS STRING), 10, 2), 5, '0')), ''),
          1, 48), 2, 10) AS BIGINT)
        -- 0x7fffffffff80: drop the top bit and the low 7 bits of the first 6 bytes
        & 140737488355200, 7) AS STRING),
      12, '0')
  END;
