-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Community ID flow hash as SQL UDFs
-- MAGIC
-- MAGIC These functions calculate the [Community ID](https://github.com/corelight/community-id-spec) (version 1) flow hash in pure SQL.
-- MAGIC They don't use Python or external packages, so there is no data transfer between the JVM/Photon and a Python sandbox.
-- MAGIC
-- MAGIC | Function | Input |
-- MAGIC |----------|-------|
-- MAGIC | `community_id_hash` | IP addresses as `STRING` (`'10.0.0.1'`, `'2001:db8::1'`) |
-- MAGIC | `community_id_hash_binary` | IP addresses in the binary form from `ip_as_binary` (4 bytes for IPv4, 16 bytes for IPv6) |
-- MAGIC | `community_id_icmp_peer` | helper: the ICMP/ICMPv6 message type of the other side of a request/response pair |
-- MAGIC
-- MAGIC Algorithm (the same as the [`communityid`](https://pypi.org/project/communityid/) Python package):
-- MAGIC
-- MAGIC 1. For ICMP (1) and ICMPv6 (58), the source port is the message type, and the destination port is the code.
-- MAGIC    For request/response types (echo, timestamp, router/neighbor solicitation, ...), the destination port is replaced by the type of the other side.
-- MAGIC    Other ICMP types are one-way: the endpoints are not reordered.
-- MAGIC 2. The endpoints are ordered: the smaller address (in network byte order) goes first. If the addresses are equal, the smaller port goes first.
-- MAGIC 3. The hash input is: seed (2 bytes), source address (4 or 16 bytes), destination address, protocol (1 byte), padding (1 zero byte),
-- MAGIC    source port (2 bytes) and destination port (2 bytes). The ports are not included when both ports are `NULL`.
-- MAGIC 4. The result is `'1:' || base64(sha1(input))`.
-- MAGIC
-- MAGIC Behavior:
-- MAGIC
-- MAGIC - The functions return `NULL` for an invalid flow: a `NULL` or invalid address, a CIDR block, `NULL` or out-of-range protocol (0..255),
-- MAGIC   ports out of range (0..65535), only one port, or no ports for TCP (6), UDP (17), SCTP (132), ICMP and ICMPv6.
-- MAGIC - A `NULL` seed is the same as seed 0. A seed out of range (0..65535) gives `NULL`.
-- MAGIC - An IPv4-mapped IPv6 address (`::ffff:10.0.0.1`) is hashed as a 16-byte IPv6 address, the same as in the Python package.
-- MAGIC
-- MAGIC `community_id_hash` calls `try_ip_as_binary` and gives the result to `community_id_hash_binary`. When a UDF gives a value to another UDF,
-- MAGIC the value is calculated only one time, so each address is parsed one time.
-- MAGIC
-- MAGIC Create all functions in the same schema: a function body finds the other functions in the schema of that function.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION community_id_icmp_peer(proto INT, msg_type INT)
  RETURNS INT
  DETERMINISTIC
  COMMENT 'For ICMP (1) and ICMPv6 (58) request/response message types, returns the message type of the other side (for example 8 -> 0 for echo). Returns NULL for one-way message types and other protocols'
  RETURN CASE proto
    WHEN 1 THEN CASE msg_type
      WHEN 8 THEN 0 WHEN 0 THEN 8       -- echo / echo reply
      WHEN 13 THEN 14 WHEN 14 THEN 13   -- timestamp / timestamp reply
      WHEN 15 THEN 16 WHEN 16 THEN 15   -- information / information reply
      WHEN 10 THEN 9 WHEN 9 THEN 10     -- router solicitation / router advertisement
      WHEN 17 THEN 18 WHEN 18 THEN 17   -- address mask / address mask reply
    END
    WHEN 58 THEN CASE msg_type
      WHEN 128 THEN 129 WHEN 129 THEN 128   -- echo request / echo reply
      WHEN 130 THEN 131 WHEN 131 THEN 130   -- MLD listener query / report
      WHEN 133 THEN 134 WHEN 134 THEN 133   -- router solicitation / advertisement
      WHEN 135 THEN 136 WHEN 136 THEN 135   -- neighbor solicitation / advertisement
      WHEN 139 THEN 140 WHEN 140 THEN 139   -- who-are-you request / reply
      WHEN 144 THEN 145 WHEN 145 THEN 144   -- home agent address discovery request / reply
    END
  END;

-- COMMAND ----------

-- Internal: hashes an already ordered flow. Ports are either both NULL or both set.
CREATE OR REPLACE FUNCTION community_id_render(seed INT, saddr BINARY, daddr BINARY, proto INT, sport INT, dport INT)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Internal helper for community_id_hash_binary: renders the Community ID v1 value for an ordered flow tuple'
  RETURN concat('1:', base64(unhex(sha1(concat(
    unhex(lpad(hex(seed), 4, '0')),
    saddr,
    daddr,
    unhex(concat(lpad(hex(proto), 2, '0'), '00')),
    if(sport IS NULL, X'', unhex(concat(lpad(hex(sport), 4, '0'), lpad(hex(dport), 4, '0'))))
  )))));

-- COMMAND ----------

-- Internal: puts the smaller endpoint first. one_way = true disables the ordering (one-way ICMP messages).
CREATE OR REPLACE FUNCTION community_id_order(seed INT, saddr BINARY, daddr BINARY, proto INT, sport INT, dport INT, one_way BOOLEAN)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Internal helper for community_id_hash_binary: puts the smaller endpoint first and renders the Community ID'
  RETURN CASE
    WHEN one_way OR saddr < daddr OR (saddr = daddr AND coalesce(sport < dport, false))
      THEN community_id_render(seed, saddr, daddr, proto, sport, dport)
    ELSE community_id_render(seed, daddr, saddr, proto, dport, sport)
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION community_id_hash_binary(
  src_ip BINARY,
  src_port INT,
  dst_ip BINARY,
  dst_port INT,
  proto INT,
  seed INT
)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Calculate the Community ID flow hash (https://github.com/corelight/community-id-spec) from IP addresses in the binary form from ip_as_binary. Returns NULL for invalid flows'
  RETURN CASE
    WHEN src_ip IS NULL OR dst_ip IS NULL OR proto IS NULL THEN NULL
    WHEN length(src_ip) NOT IN (4, 16) OR length(dst_ip) NOT IN (4, 16) THEN NULL
    WHEN proto NOT BETWEEN 0 AND 255 OR coalesce(seed, 0) NOT BETWEEN 0 AND 65535 THEN NULL
    WHEN (src_port IS NULL) != (dst_port IS NULL) THEN NULL
    WHEN src_port NOT BETWEEN 0 AND 65535 OR dst_port NOT BETWEEN 0 AND 65535 THEN NULL
    WHEN src_port IS NULL AND proto IN (1, 6, 17, 58, 132) THEN NULL
    WHEN proto IN (1, 58) THEN community_id_order(coalesce(seed, 0), src_ip, dst_ip, proto, src_port,
      coalesce(community_id_icmp_peer(proto, src_port), dst_port), community_id_icmp_peer(proto, src_port) IS NULL)
    ELSE community_id_order(coalesce(seed, 0), src_ip, dst_ip, proto, src_port, dst_port, false)
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION community_id_hash(
  src_ip STRING,
  src_port INT,
  dst_ip STRING,
  dst_port INT,
  proto INT,
  seed INT
)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Calculate the Community ID flow hash (https://github.com/corelight/community-id-spec). Returns NULL for invalid flows'
  RETURN community_id_hash_binary(try_ip_as_binary(src_ip), src_port, try_ip_as_binary(dst_ip), dst_port, proto, seed);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import requests
-- MAGIC
-- MAGIC # Download the baseline files: default seed (0) and seed 1
-- MAGIC base_url = "https://raw.githubusercontent.com/corelight/community-id-spec/refs/heads/master/baseline"
-- MAGIC rows = []
-- MAGIC for file_name, seed in [("baseline_deflt.json", 0), ("baseline_seed1.json", 1)]:
-- MAGIC     for entry in requests.get(f"{base_url}/{file_name}").json():
-- MAGIC         rows.append((entry["saddr"], entry["sport"], entry["daddr"], entry["dport"],
-- MAGIC                      entry["proto"], seed, entry["communityid"]))
-- MAGIC
-- MAGIC # Create DataFrame
-- MAGIC schema = "src_ip string, src_port int, dst_ip string, dst_port int, proto int, seed int, expected_id string"
-- MAGIC df = spark.createDataFrame(rows, schema)
-- MAGIC df.createOrReplaceTempView("baseline_data")
-- MAGIC
-- MAGIC # Compute and compare community IDs using the UDF
-- MAGIC result = spark.sql("""
-- MAGIC SELECT *, CASE WHEN expected_id = computed_id THEN 'MATCH' ELSE 'MISMATCH' END AS comparison
-- MAGIC FROM (
-- MAGIC   SELECT *, community_id_hash(src_ip, src_port, dst_ip, dst_port, proto, seed) AS computed_id
-- MAGIC   FROM baseline_data
-- MAGIC )
-- MAGIC """)
-- MAGIC display(result)

-- COMMAND ----------

-- Invalid flows give NULL. A protocol without ports (GRE, 47) can have no ports.
SELECT
  community_id_hash(NULL, 1, '10.0.0.1', 2, 6, 0) IS NULL
    AND community_id_hash('10.0.0.0/8', 1, '10.0.0.1', 2, 6, 0) IS NULL
    AND community_id_hash('10.0.0.300', 1, '10.0.0.1', 2, 6, 0) IS NULL
    AND community_id_hash('10.0.0.2', NULL, '10.0.0.1', NULL, 6, 0) IS NULL
    AND community_id_hash('10.0.0.2', 1, '10.0.0.1', NULL, 47, 0) IS NULL
    AND community_id_hash('10.0.0.2', 70000, '10.0.0.1', 2, 6, 0) IS NULL
    AND community_id_hash('10.0.0.2', 1, '10.0.0.1', 2, 256, 0) IS NULL
    AND community_id_hash('10.0.0.2', 1, '10.0.0.1', 2, 6, 65536) IS NULL
    AND community_id_hash('10.0.0.2', NULL, '10.0.0.1', NULL, 47, NULL) IS NOT NULL AS invalid_ok;
