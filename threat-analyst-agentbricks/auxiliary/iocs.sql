CREATE TABLE fe_lakewatch_catalog.lakewatch_field.oss_iocs (
  dataset STRING COMMENT 'The source of the threat intelligence data.',
  timestamp TIMESTAMP COMMENT 'The time when the data was collected.',
  msg_hash STRING,
  ioc_content STRING,
  ioc_description STRING COMMENT 'A description of the IOC.',
  id BIGINT,
  ioc STRING COMMENT 'IoC itself, content depends on `ioc_type` value.',
  ioc_title STRING,
  ioc_type STRING COMMENT 'The type of the IoC. Possible values are: `URL` for full URLs, `hostname` for exact hostnames, `IPv4` for exact IP addresses, `domain` for domains and hostnames inside it, files with different hash types: `FileHash-TElfHash`, `FileHash-SHA256`, `FileHash-MD5`, `FileHash-SHA1`, `FileHash-SSDEEP`, `FileHash-SHA384`, `FileHash-TLSH`, `FileHash-ImpHash`

',
  ioc_id STRING,
  first_seen TIMESTAMP COMMENT 'The first time the IOC was seen.',
  abuseurl STRUCT<blacklists: STRUCT<spamhaus_dbl: STRING, surbl: STRING>, date_added: STRING, host: STRING, id: STRING, larted: STRING, reporter: STRING, tags: ARRAY<STRING>, threat: STRING, url: STRING, url_status: STRING, urlhaus_reference: STRING> COMMENT 'Information related to abuse URL associated with the IOC.',
  file_name STRING,
  file_size BIGINT,
  file_type STRING,
  file_type_mime STRING,
  last_seen TIMESTAMP COMMENT 'The last time the IOC was seen.',
  malwarebazaar STRUCT<anonymous: BIGINT, code_sign: ARRAY<STRING>, dhash_icon: STRING, file_name: STRING, file_size: BIGINT, file_type: STRING, file_type_mime: STRING, first_seen: STRING, imphash: STRING, intelligence: STRUCT<clamav: STRING, downloads: STRING, mail: STRING, uploads: STRING>, last_seen: STRING, md5_hash: STRING, origin_country: STRING, reporter: STRING, sha1_hash: STRING, sha256_hash: STRING, sha3_384_hash: STRING, signature: STRING, ssdeep: STRING, tags: ARRAY<STRING>, telfhash: STRING, tlsh: STRING> COMMENT 'Information related to malware bazaar associated with the IOC.',
  abusemalware STRUCT<file_size: STRING, file_type: STRING, firstseen: STRING, imphash: STRING, md5_hash: STRING, sha256_hash: STRING, signature: STRING, ssdeep: STRING, tlsh: STRING, urlhaus_download: STRING, virustotal: STRING> COMMENT 'Information related to abuse malware associated with the IOC.',
  geoip STRUCT<city: STRING, country: STRING, country_code: STRING, latitude: DOUBLE, longitude: DOUBLE, accuracy_radius: INT> COMMENT 'Geographic information associated with the IOC.',
  asn STRUCT<as_number: INT, as_org: STRING, as_network: STRING>,
  psl STRUCT<domain_public_suffix: STRING, registered_domain: STRING>)
USING delta
PARTITIONED BY (ioc_type)
COMMENT 'The iocs table contains threat intelligence data related to Indicators of Compromise (IoCs). It includes information such as the dataset source, timestamp of when the data was collected, and a unique identifier for each IoC. The table also provides details on the type of IoC, its description, and associated metadata such as file name, size, and type. Additionally, the table includes information on the first and last time an IoC was seen, as well as any relevant geographic and network information.

Sources:  OpenThread Exchange, AbuseURL, Malware Bazaar, Abuse Malware. See [blog post](https://alexott.blogspot.com/2022/10/ingesting-indicators-of-compromise-with.html) for details ([source code](https://github.com/alexott/databricks-cybersecurity-playground/tree/main/iocs-ingest)).'
