"""Flatten raw GHSA (OSV) VARIANT records into the typed silver schema.
`raw` is VARIANT, so every field — scalars AND complex types — is read with
try_variant_get(raw, '$.path', '<TYPE>'); complex type hints go straight in, with
no ::STRING + from_json round-trip. Higher-order functions build affected_packages
(Approach A: one struct per package-range).

No module-level pyspark import — transform_advisories takes a pyspark DataFrame at
call time, so the module imports fine without a local Spark install."""

AFFECTED_TYPE = """ARRAY<STRUCT<
    package: STRUCT<ecosystem: STRING, name: STRING, purl: STRING>,
    ranges: ARRAY<STRUCT<type: STRING,
        events: ARRAY<STRUCT<introduced: STRING, fixed: STRING, last_affected: STRING>>>>,
    versions: ARRAY<STRING>
>>"""
REFERENCES_TYPE = "ARRAY<STRUCT<type: STRING, url: STRING>>"

_AFFECTED = f"try_variant_get(raw, '$.affected', '{AFFECTED_TYPE}')"
_ALIASES = "try_variant_get(raw, '$.aliases', 'ARRAY<STRING>')"

# One struct per (package, range); packages with no ranges keep a null-version struct.
_AFFECTED_PACKAGES = f"""
COALESCE(FLATTEN(TRANSFORM(
  COALESCE({_AFFECTED}, array()),
  a -> CASE
    WHEN a.ranges IS NULL OR size(a.ranges) = 0
      THEN array(named_struct(
        'ecosystem', a.package.ecosystem, 'package', a.package.name, 'purl', a.package.purl,
        'range_type', CAST(NULL AS STRING), 'introduced', CAST(NULL AS STRING),
        'fixed', CAST(NULL AS STRING), 'last_affected', CAST(NULL AS STRING)))
    ELSE TRANSFORM(a.ranges, r -> named_struct(
        'ecosystem', a.package.ecosystem, 'package', a.package.name, 'purl', a.package.purl,
        'range_type', r.type,
        'introduced', get(filter(r.events, e -> e.introduced IS NOT NULL), 0).introduced,
        'fixed', get(filter(r.events, e -> e.fixed IS NOT NULL), 0).fixed,
        'last_affected', get(filter(r.events, e -> e.last_affected IS NOT NULL), 0).last_affected))
  END)), array())
"""
_ECOSYSTEMS = f"ARRAY_DISTINCT(FILTER(TRANSFORM(COALESCE({_AFFECTED}, array()), a -> a.package.ecosystem), x -> x IS NOT NULL))"


def transform_advisories(df):
    """raw VARIANT + github_reviewed -> typed silver row.

    `df` is a pyspark DataFrame with `raw` (VARIANT) and `github_reviewed` columns."""
    return df.selectExpr(
        "try_variant_get(raw, '$.id', 'string') as ghsa_id",
        f"get(filter({_ALIASES}, x -> upper(x) like 'CVE-%'), 0) as cve_id",
        f"{_ALIASES} as aliases",
        "try_variant_get(raw, '$.related', 'ARRAY<STRING>') as related",
        "try_variant_get(raw, '$.summary', 'string') as summary",
        "try_variant_get(raw, '$.details', 'string') as details",
        "try_variant_get(raw, '$.database_specific.severity', 'string') as severity_label",
        "try_variant_get(raw, '$.severity[0].type', 'string') as cvss_version",
        "try_variant_get(raw, '$.severity[0].score', 'string') as cvss_vector",
        "try_variant_get(raw, '$.database_specific.cwe_ids', 'ARRAY<STRING>') as cwe_ids",
        f"{_ECOSYSTEMS} as ecosystems",
        f"{_AFFECTED_PACKAGES} as affected_packages",
        f"try_variant_get(raw, '$.references', '{REFERENCES_TYPE}') as references",
        "github_reviewed",
        "try_variant_get(raw, '$.published', 'timestamp') as date_published",
        "try_variant_get(raw, '$.modified', 'timestamp') as date_modified",
        "try_variant_get(raw, '$.withdrawn', 'timestamp') as date_withdrawn",
        "try_variant_get(raw, '$.schema_version', 'string') as schema_version",
        "raw",
    )
