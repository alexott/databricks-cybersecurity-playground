-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## UDFs to support OCSF mappings

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ocsf_network_activity_name(activity_id INTEGER)
  RETURNS STRING
  COMMENT 'Maps network activity_id into the name'
  RETURN CASE activity_id
    WHEN 0 THEN 'Unknown'
    WHEN 1 THEN 'Open'
    WHEN 2 THEN 'Close'
    WHEN 3 THEN 'Reset'
    WHEN 4 THEN 'Fail'
    WHEN 5 THEN 'Refuse'
    WHEN 6 THEN 'Traffic'
    WHEN 7 THEN 'Listen'
    ELSE 'Other'
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ocsf_dns_activity_name(activity_id INTEGER)
  RETURNS STRING
  COMMENT 'Maps DNS activity_id into the name'
  RETURN CASE activity_id
    WHEN 0 THEN 'Unknown'
    WHEN 1 THEN 'Query'
    WHEN 2 THEN 'Response'
    WHEN 6 THEN 'Traffic'
    ELSE 'Other'
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ocsf_http_activity_name(activity_id INTEGER)
  RETURNS STRING
  COMMENT 'Maps HTTP activity_id into the name'
  RETURN CASE activity_id
    WHEN 0 THEN 'Unknown'
    WHEN 1 THEN 'Connect'
    WHEN 2 THEN 'Delete'
    WHEN 3 THEN 'Get'
    WHEN 4 THEN 'Head'
    WHEN 5 THEN 'Options'
    WHEN 6 THEN 'Post'
    WHEN 7 THEN 'Put'
    WHEN 8 THEN 'Trace'
    WHEN 9 THEN 'Patch'
    ELSE 'Other'
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ocsf_file_activity_name(activity_id INTEGER)
  RETURNS STRING
  COMMENT 'Maps file system activity_id into the name'
  RETURN CASE activity_id
    WHEN 0 THEN 'Unknown'
    WHEN 1 THEN 'Create'
    WHEN 2 THEN 'Read'
    WHEN 3 THEN 'Update'
    WHEN 4 THEN 'Delete'
    WHEN 5 THEN 'Rename'
    WHEN 6 THEN 'Set Attributes'
    WHEN 7 THEN 'Set Security'
    WHEN 8 THEN 'Get Attributes'
    WHEN 9 THEN 'Get Security'
    WHEN 9 THEN 'Encrypt'
    WHEN 9 THEN 'Decrypt'
    WHEN 9 THEN 'Mount'
    WHEN 9 THEN 'Unmount'
    WHEN 9 THEN 'Open'
    ELSE 'Other'
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ocsf_http_activity_id(verb STRING)
  RETURNS INT
  COMMENT 'Maps HTTP verb into OCSF numeric code'
  RETURN CASE lower(verb)
    WHEN 'connect' THEN 1
    WHEN 'delete' THEN 2
    WHEN 'get' THEN 3
    WHEN 'head' THEN 4
    WHEN 'options' THEN 5
    WHEN 'post' THEN 6
    WHEN 'put' THEN 7
    WHEN 'trace' THEN 8
    WHEN 'patch' THEN 9
    WHEN '' THEN 0  -- TODO: handle null string
    ELSE 99
    END;