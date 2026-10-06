-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## User-defined functions to classify IP addresses
-- MAGIC
-- MAGIC Databricks SQL has [native IP functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin#ip-address-functions)
-- MAGIC (`ip_cidr_contains`, `ip_as_binary`, `ip_version`, ...), but no functions that tell if an address is private, loopback, reserved, etc.
-- MAGIC These SQL UDFs fill this gap. They use `ip_cidr_contains` internally, so they work for IPv4 and IPv6 without a Python sandbox.
-- MAGIC
-- MAGIC | Function | IPv4 ranges | IPv6 ranges |
-- MAGIC |----------|-------------|-------------|
-- MAGIC | `ip_is_private` | 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16 (RFC 1918), 100.64.0.0/10 (CGNAT, RFC 6598) | fc00::/7 (RFC 4193) |
-- MAGIC | `ip_is_loopback` | 127.0.0.0/8 | ::1/128 |
-- MAGIC | `ip_is_link_local` | 169.254.0.0/16 | fe80::/10 |
-- MAGIC | `ip_is_multicast` | 224.0.0.0/4 | ff00::/8 |
-- MAGIC | `ip_is_reserved` | 0.0.0.0/8, 192.0.0.0/24, 192.0.2.0/24, 198.18.0.0/15, 198.51.100.0/24, 203.0.113.0/24, 240.0.0.0/4 | ::/128, 64:ff9b::/96, 100::/64, 2001:db8::/32 |
-- MAGIC | `ip_is_global` | not in any range above | not in any range above |
-- MAGIC | `ip_classify` | one label: `loopback` > `link_local` > `multicast` > `private` > `reserved` > `global` | same |
-- MAGIC
-- MAGIC Behavior:
-- MAGIC
-- MAGIC - There are two sets of functions. `ip_is_*` and `ip_classify` take an IP address as a `STRING` (`'10.0.0.1'`).
-- MAGIC   `ip_is_*_binary` and `ip_classify_binary` take the binary form from `ip_as_binary`. Use them when the binary form is already in a column.
-- MAGIC   Unity Catalog doesn't allow two functions with the same name and different parameter types, so the names are different.
-- MAGIC - A CIDR block or an invalid value raises an error. `NULL` gives `NULL`.
-- MAGIC - An IPv4-mapped IPv6 address (`::ffff:10.0.0.1`) is classified by its embedded IPv4 address. `ip_cidr_contains` does this natively.
-- MAGIC - Because of the previous point, `::ffff:0:0/96` is not in the `ip_is_reserved` list: `ip_cidr_contains` matches every IPv4 address against it.
-- MAGIC - The unspecified IPv6 address `::` is reserved (the same as `0.0.0.0`, which is in `0.0.0.0/8`), so it is not global.
-- MAGIC
-- MAGIC The `STRING` functions call `ip_as_binary` and give the result to the `_binary` function. When a UDF gives a value to another UDF,
-- MAGIC the value is calculated only one time. So the address is parsed one time, and not one time for each range.
-- MAGIC On 106M rows, `ip_is_global` is 2x faster than with the string given to each `ip_cidr_contains` call.
-- MAGIC
-- MAGIC Create all functions in the same schema: a function body finds the other functions in the schema of that function.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_private_binary(ip BINARY)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (binary form from ip_as_binary) is private: RFC 1918 and CGNAT 100.64.0.0/10 (RFC 6598) for IPv4, unique-local fc00::/7 (RFC 4193) for IPv6'
  RETURN CASE
    WHEN ip IS NULL THEN NULL
    WHEN length(ip) NOT IN (4, 16) THEN raise_error(concat('Expected an IP address, not a CIDR block: ', ip_as_string(ip)))
    ELSE ip_cidr_contains('10.0.0.0/8', ip)
      OR ip_cidr_contains('172.16.0.0/12', ip)
      OR ip_cidr_contains('192.168.0.0/16', ip)
      OR ip_cidr_contains('100.64.0.0/10', ip)
      OR ip_cidr_contains('fc00::/7', ip)
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_private(ip STRING)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (STRING) is private: RFC 1918 and CGNAT 100.64.0.0/10 (RFC 6598) for IPv4, unique-local fc00::/7 (RFC 4193) for IPv6'
  RETURN ip_is_private_binary(ip_as_binary(ip));
-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_loopback_binary(ip BINARY)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (binary form from ip_as_binary) is loopback address: 127.0.0.0/8 or ::1'
  RETURN CASE
    WHEN ip IS NULL THEN NULL
    WHEN length(ip) NOT IN (4, 16) THEN raise_error(concat('Expected an IP address, not a CIDR block: ', ip_as_string(ip)))
    ELSE ip_cidr_contains('127.0.0.0/8', ip)
      OR ip_cidr_contains('::1/128', ip)
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_loopback(ip STRING)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (STRING) is loopback address: 127.0.0.0/8 or ::1'
  RETURN ip_is_loopback_binary(ip_as_binary(ip));
-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_link_local_binary(ip BINARY)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (binary form from ip_as_binary) is link-local address: 169.254.0.0/16 or fe80::/10'
  RETURN CASE
    WHEN ip IS NULL THEN NULL
    WHEN length(ip) NOT IN (4, 16) THEN raise_error(concat('Expected an IP address, not a CIDR block: ', ip_as_string(ip)))
    ELSE ip_cidr_contains('169.254.0.0/16', ip)
      OR ip_cidr_contains('fe80::/10', ip)
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_link_local(ip STRING)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (STRING) is link-local address: 169.254.0.0/16 or fe80::/10'
  RETURN ip_is_link_local_binary(ip_as_binary(ip));
-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_multicast_binary(ip BINARY)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (binary form from ip_as_binary) is multicast address: 224.0.0.0/4 or ff00::/8'
  RETURN CASE
    WHEN ip IS NULL THEN NULL
    WHEN length(ip) NOT IN (4, 16) THEN raise_error(concat('Expected an IP address, not a CIDR block: ', ip_as_string(ip)))
    ELSE ip_cidr_contains('224.0.0.0/4', ip)
      OR ip_cidr_contains('ff00::/8', ip)
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_multicast(ip STRING)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (STRING) is multicast address: 224.0.0.0/4 or ff00::/8'
  RETURN ip_is_multicast_binary(ip_as_binary(ip));
-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_reserved_binary(ip BINARY)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (binary form from ip_as_binary) is in an IANA special-purpose range that is not private, loopback, link-local, or multicast (this-network, documentation, benchmarking, future use, NAT64, discard, unspecified)'
  RETURN CASE
    WHEN ip IS NULL THEN NULL
    WHEN length(ip) NOT IN (4, 16) THEN raise_error(concat('Expected an IP address, not a CIDR block: ', ip_as_string(ip)))
    ELSE ip_cidr_contains('0.0.0.0/8', ip)
      OR ip_cidr_contains('192.0.0.0/24', ip)
      OR ip_cidr_contains('192.0.2.0/24', ip)
      OR ip_cidr_contains('198.18.0.0/15', ip)
      OR ip_cidr_contains('198.51.100.0/24', ip)
      OR ip_cidr_contains('203.0.113.0/24', ip)
      OR ip_cidr_contains('240.0.0.0/4', ip)
      OR ip_cidr_contains('::/128', ip)
      OR ip_cidr_contains('64:ff9b::/96', ip)
      OR ip_cidr_contains('100::/64', ip)
      OR ip_cidr_contains('2001:db8::/32', ip)
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_reserved(ip STRING)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (STRING) is in an IANA special-purpose range that is not private, loopback, link-local, or multicast (this-network, documentation, benchmarking, future use, NAT64, discard, unspecified)'
  RETURN ip_is_reserved_binary(ip_as_binary(ip));
-- COMMAND ----------

-- `ip_classify_binary` and `ip_is_global_binary` repeat the ranges instead of calling the functions above:
-- one CASE or one NOT (...) over all ranges is faster than five function calls.

CREATE OR REPLACE FUNCTION ip_classify_binary(ip BINARY)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Returns one label for the IP address (binary form from ip_as_binary): loopback, link_local, multicast, private, reserved, or global (first match in this order)'
  RETURN CASE
    WHEN ip IS NULL THEN NULL
    WHEN length(ip) NOT IN (4, 16) THEN raise_error(concat('Expected an IP address, not a CIDR block: ', ip_as_string(ip)))
    WHEN ip_cidr_contains('127.0.0.0/8', ip)
      OR ip_cidr_contains('::1/128', ip) THEN 'loopback'
    WHEN ip_cidr_contains('169.254.0.0/16', ip)
      OR ip_cidr_contains('fe80::/10', ip) THEN 'link_local'
    WHEN ip_cidr_contains('224.0.0.0/4', ip)
      OR ip_cidr_contains('ff00::/8', ip) THEN 'multicast'
    WHEN ip_cidr_contains('10.0.0.0/8', ip)
      OR ip_cidr_contains('172.16.0.0/12', ip)
      OR ip_cidr_contains('192.168.0.0/16', ip)
      OR ip_cidr_contains('100.64.0.0/10', ip)
      OR ip_cidr_contains('fc00::/7', ip) THEN 'private'
    WHEN ip_cidr_contains('0.0.0.0/8', ip)
      OR ip_cidr_contains('192.0.0.0/24', ip)
      OR ip_cidr_contains('192.0.2.0/24', ip)
      OR ip_cidr_contains('198.18.0.0/15', ip)
      OR ip_cidr_contains('198.51.100.0/24', ip)
      OR ip_cidr_contains('203.0.113.0/24', ip)
      OR ip_cidr_contains('240.0.0.0/4', ip)
      OR ip_cidr_contains('::/128', ip)
      OR ip_cidr_contains('64:ff9b::/96', ip)
      OR ip_cidr_contains('100::/64', ip)
      OR ip_cidr_contains('2001:db8::/32', ip) THEN 'reserved'
    ELSE 'global'
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_classify(ip STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Returns one label for the IP address (STRING): loopback, link_local, multicast, private, reserved, or global (first match in this order)'
  RETURN ip_classify_binary(ip_as_binary(ip));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_global_binary(ip BINARY)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (binary form from ip_as_binary) is globally routable: not private, loopback, link-local, multicast, or reserved'
  RETURN CASE
    WHEN ip IS NULL THEN NULL
    WHEN length(ip) NOT IN (4, 16) THEN raise_error(concat('Expected an IP address, not a CIDR block: ', ip_as_string(ip)))
    ELSE NOT (
         ip_cidr_contains('127.0.0.0/8', ip)
      OR ip_cidr_contains('::1/128', ip)
      OR ip_cidr_contains('169.254.0.0/16', ip)
      OR ip_cidr_contains('fe80::/10', ip)
      OR ip_cidr_contains('224.0.0.0/4', ip)
      OR ip_cidr_contains('ff00::/8', ip)
      OR ip_cidr_contains('10.0.0.0/8', ip)
      OR ip_cidr_contains('172.16.0.0/12', ip)
      OR ip_cidr_contains('192.168.0.0/16', ip)
      OR ip_cidr_contains('fc00::/7', ip)
      OR ip_cidr_contains('0.0.0.0/8', ip)
      OR ip_cidr_contains('100.64.0.0/10', ip)
      OR ip_cidr_contains('192.0.0.0/24', ip)
      OR ip_cidr_contains('192.0.2.0/24', ip)
      OR ip_cidr_contains('198.18.0.0/15', ip)
      OR ip_cidr_contains('198.51.100.0/24', ip)
      OR ip_cidr_contains('203.0.113.0/24', ip)
      OR ip_cidr_contains('240.0.0.0/4', ip)
      OR ip_cidr_contains('::/128', ip)
      OR ip_cidr_contains('64:ff9b::/96', ip)
      OR ip_cidr_contains('100::/64', ip)
      OR ip_cidr_contains('2001:db8::/32', ip)
    )
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ip_is_global(ip STRING)
  RETURNS BOOLEAN
  DETERMINISTIC
  COMMENT 'Returns true if the IP address (STRING) is globally routable: not private, loopback, link-local, multicast, or reserved'
  RETURN ip_is_global_binary(ip_as_binary(ip));
-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Tests
-- MAGIC
-- MAGIC Each row has an address and its expected label. The query tests the `STRING` and the `_binary` functions, and fails if a function gives a different answer.
-- MAGIC The second query checks `NULL` input.

-- COMMAND ----------

WITH cases(ip, expected) AS (
  VALUES
    ('10.1.2.3', 'private'), ('172.16.0.1', 'private'), ('172.31.255.255', 'private'),
    ('192.168.1.1', 'private'), ('fd12:3456::1', 'private'), ('fc00::1', 'private'),
    ('::ffff:10.0.0.1', 'private'), ('100.64.0.1', 'private'), ('100.127.255.255', 'private'),
    ('127.0.0.1', 'loopback'), ('127.255.255.254', 'loopback'), ('::1', 'loopback'),
    ('169.254.169.254', 'link_local'), ('fe80::1', 'link_local'), ('febf::1', 'link_local'),
    ('224.0.0.1', 'multicast'), ('239.255.255.250', 'multicast'), ('ff02::1', 'multicast'),
    ('0.0.0.0', 'reserved'), ('192.0.0.8', 'reserved'),
    ('192.0.2.1', 'reserved'), ('198.18.0.1', 'reserved'), ('198.51.100.7', 'reserved'),
    ('203.0.113.9', 'reserved'), ('240.0.0.1', 'reserved'), ('255.255.255.255', 'reserved'),
    ('::', 'reserved'), ('64:ff9b::808:808', 'reserved'), ('100::1', 'reserved'),
    ('2001:db8::1', 'reserved'), ('::ffff:192.0.2.1', 'reserved'),
    ('8.8.8.8', 'global'), ('172.32.0.1', 'global'), ('100.63.255.255', 'global'), ('100.128.0.1', 'global'),
    ('2606:4700:4700::1111', 'global'), ('::ffff:8.8.8.8', 'global'), ('fec0::1', 'global')
)
SELECT
  count(*) AS cases,
  count_if(ip_classify(ip) <=> expected
    AND ip_is_private(ip) <=> (expected = 'private')
    AND ip_is_loopback(ip) <=> (expected = 'loopback')
    AND ip_is_link_local(ip) <=> (expected = 'link_local')
    AND ip_is_multicast(ip) <=> (expected = 'multicast')
    AND ip_is_reserved(ip) <=> (expected = 'reserved')
    AND ip_is_global(ip) <=> (expected = 'global')) AS passed_string,
  count_if(ip_classify_binary(ip_as_binary(ip)) <=> expected
    AND ip_is_private_binary(ip_as_binary(ip)) <=> (expected = 'private')
    AND ip_is_loopback_binary(ip_as_binary(ip)) <=> (expected = 'loopback')
    AND ip_is_link_local_binary(ip_as_binary(ip)) <=> (expected = 'link_local')
    AND ip_is_multicast_binary(ip_as_binary(ip)) <=> (expected = 'multicast')
    AND ip_is_reserved_binary(ip_as_binary(ip)) <=> (expected = 'reserved')
    AND ip_is_global_binary(ip_as_binary(ip)) <=> (expected = 'global')) AS passed_binary,
  assert_true(passed_string = cases AND passed_binary = cases, 'ip classification tests failed') AS check
FROM cases;

-- COMMAND ----------

SELECT
  ip_classify(NULL) IS NULL AND ip_is_global(NULL) IS NULL AND ip_is_private(NULL) IS NULL
    AND ip_classify_binary(NULL) IS NULL AND ip_is_private_binary(NULL) IS NULL AS null_ok;

-- A CIDR block gives an error, for example:
-- SELECT ip_is_private('10.0.0.0/8')               -- Expected an IP address, not a CIDR block: 10.0.0.0/8
-- SELECT ip_is_private_binary(ip_as_binary('10.0.0.0/8')) -- the same error
