"""Spark side of the OSV load: raw-table id columns and the OSV -> silver flatten.
`raw` is VARIANT, so every field — scalars AND complex types — is read with
try_variant_get(raw, '$.path', '<TYPE>'); complex type hints go straight in, with
no ::STRING + from_json round-trip. Same approach as ghsa/transform.py, made
generic for all OSV sources (GIT ranges with `repo`, distro ecosystems like
'Ubuntu:22.04:LTS', non-CVSS severity types, `upstream`).

No module-level pyspark import — the functions take a pyspark DataFrame at call
time, so the module imports fine without a local Spark install."""

AFFECTED_TYPE = """ARRAY<STRUCT<
    package: STRUCT<ecosystem: STRING, name: STRING, purl: STRING>,
    ranges: ARRAY<STRUCT<type: STRING, repo: STRING,
        events: ARRAY<STRUCT<introduced: STRING, fixed: STRING, last_affected: STRING>>>>,
    versions: ARRAY<STRING>
>>"""
SEVERITY_TYPE = "ARRAY<STRUCT<type: STRING, score: STRING>>"
REFERENCES_TYPE = "ARRAY<STRUCT<type: STRING, url: STRING>>"

_ID = "try_variant_get(raw, '$.id', 'string')"
_ALIASES = "try_variant_get(raw, '$.aliases', 'ARRAY<STRING>')"
_UPSTREAM = "try_variant_get(raw, '$.upstream', 'ARRAY<STRING>')"
_AFFECTED = f"try_variant_get(raw, '$.affected', '{AFFECTED_TYPE}')"
_SEVERITY = f"try_variant_get(raw, '$.severity', '{SEVERITY_TYPE}')"

# The id itself (GIT-ecosystem records are CVE-*), else the first CVE alias, else
# the first CVE in upstream (distro records such as UBUNTU-CVE-*).
_CVE_ID = f"""CASE WHEN upper({_ID}) LIKE 'CVE-%' THEN {_ID} ELSE coalesce(
  get(filter({_ALIASES}, x -> upper(x) LIKE 'CVE-%'), 0),
  get(filter({_UPSTREAM}, x -> upper(x) LIKE 'CVE-%'), 0)) END"""

# One struct per (package, range); packages with no ranges keep a null-version
# struct. GIT ranges usually have no package, only `repo`.
_AFFECTED_PACKAGES = f"""
COALESCE(FLATTEN(TRANSFORM(
  COALESCE({_AFFECTED}, array()),
  a -> CASE
    WHEN a.ranges IS NULL OR size(a.ranges) = 0
      THEN array(named_struct(
        'ecosystem', a.package.ecosystem, 'package', a.package.name, 'purl', a.package.purl,
        'repo', CAST(NULL AS STRING), 'range_type', CAST(NULL AS STRING),
        'introduced', CAST(NULL AS STRING), 'fixed', CAST(NULL AS STRING),
        'last_affected', CAST(NULL AS STRING)))
    ELSE TRANSFORM(a.ranges, r -> named_struct(
        'ecosystem', a.package.ecosystem, 'package', a.package.name, 'purl', a.package.purl,
        'repo', r.repo, 'range_type', r.type,
        'introduced', get(filter(r.events, e -> e.introduced IS NOT NULL), 0).introduced,
        'fixed', get(filter(r.events, e -> e.fixed IS NOT NULL), 0).fixed,
        'last_affected', get(filter(r.events, e -> e.last_affected IS NOT NULL), 0).last_affected))
  END)), array())
"""
# Base ecosystem names: 'Ubuntu:22.04:LTS' -> 'Ubuntu', 'Debian:12' -> 'Debian'.
_ECOSYSTEMS = f"""ARRAY_DISTINCT(FILTER(
  TRANSFORM(COALESCE({_AFFECTED}, array()), a -> split_part(a.package.ecosystem, ':', 1)),
  x -> x IS NOT NULL AND x != ''))"""
# Newest CVSS entry (CVSS_V4 > CVSS_V3 > CVSS_V2); other types (e.g. 'Ubuntu') are skipped.
_CVSS = f"""get(array_sort(filter({_SEVERITY}, s -> s.type LIKE 'CVSS_%'),
  (l, r) -> CASE WHEN l.type > r.type THEN -1 WHEN l.type < r.type THEN 1 ELSE 0 END), 0)"""


def extract_raw_data(df):
    """One JSON record per row in `value` (string) -> osv_raw columns.

    `raw` is NULL (and so is `id`) when the text does not parse; callers count and
    drop those rows."""
    return df.selectExpr("try_parse_json(value) as raw").selectExpr(
        "raw",
        f"{_ID} as id",
        f"{_CVE_ID} as cve_id",
        "try_variant_get(raw, '$.published', 'timestamp') as date_published",
        "try_variant_get(raw, '$.modified', 'timestamp') as date_modified",
        "try_variant_get(raw, '$.withdrawn', 'timestamp') as date_withdrawn",
    )


def transform_vulnerabilities(df, keep: list[str] | None = None):
    """osv_raw rows (needs `raw` VARIANT) -> typed silver rows.

    `keep` lists extra columns to pass through unchanged (e.g. CDF `_change_type`)."""
    return df.selectExpr(
        f"{_ID} as id",
        f"regexp_extract({_ID}, '^([A-Za-z]+)-', 1) as id_prefix",
        f"{_CVE_ID} as cve_id",
        f"{_ALIASES} as aliases",
        f"{_UPSTREAM} as upstream",
        "try_variant_get(raw, '$.related', 'ARRAY<STRING>') as related",
        "try_variant_get(raw, '$.summary', 'string') as summary",
        "try_variant_get(raw, '$.details', 'string') as details",
        f"{_SEVERITY} as severity",
        f"{_CVSS}.type as cvss_version",
        f"{_CVSS}.score as cvss_vector",
        "try_variant_get(raw, '$.database_specific.cwe_ids', 'ARRAY<STRING>') as cwe_ids",
        f"{_ECOSYSTEMS} as ecosystems",
        f"{_AFFECTED_PACKAGES} as affected_packages",
        f"try_variant_get(raw, '$.references', '{REFERENCES_TYPE}') as references",
        "try_variant_get(raw, '$.published', 'timestamp') as date_published",
        "try_variant_get(raw, '$.modified', 'timestamp') as date_modified",
        "try_variant_get(raw, '$.withdrawn', 'timestamp') as date_withdrawn",
        "try_variant_get(raw, '$.schema_version', 'string') as schema_version",
        "raw",
        *(keep or []),
    )
