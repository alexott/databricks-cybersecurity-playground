"""Flatten raw CVE v5 VARIANT records into the typed silver schema. Every field is
read with try_variant_get (complex types are passed as the type hint directly).
CVSS: CNA metrics first, CISA-ADP as fallback; SSVC from the CISA-ADP container.

No module-level pyspark import, so the module imports without a Spark install."""

CVSS_VERSIONS = [("4.0", "cvssV4_0"), ("3.1", "cvssV3_1"), ("3.0", "cvssV3_0"), ("2.0", "cvssV2_0")]

AFFECTED_TYPE = """ARRAY<STRUCT<
    vendor: STRING, product: STRING, defaultStatus: STRING,
    platforms: ARRAY<STRING>,
    versions: ARRAY<STRUCT<version: STRING, status: STRING,
        lessThan: STRING, lessThanOrEqual: STRING, versionType: STRING>>
>>"""
REFERENCES_TYPE = "ARRAY<STRUCT<url: STRING, name: STRING, tags: ARRAY<STRING>>>"
DESCRIPTIONS_TYPE = "ARRAY<STRUCT<lang: STRING, value: STRING>>"
PROBLEM_TYPES_TYPE = "ARRAY<STRUCT<descriptions: ARRAY<STRUCT<cweId: STRING, description: STRING>>>>"
# CISA-ADP is located by providerMetadata.shortName among the first adp entries.
_ADP_SLOTS = 3


def _v(path: str, typ: str = "string"):
    from pyspark.sql import functions as F
    return F.try_variant_get(F.col("raw"), f"$.{path}", typ)


def _cisa_adp(path: str, typ: str = "string"):
    """Field from the CISA-ADP container, wherever it sits in containers.adp[]."""
    from pyspark.sql import functions as F
    return F.coalesce(*[
        F.when(_v(f"containers.adp[{i}].providerMetadata.shortName") == "CISA-ADP",
               _v(f"containers.adp[{i}].{path}", typ))
        for i in range(_ADP_SLOTS)])


def _cvss(field, alias: str):
    """CVSS columns from `<base>.metrics[0]`, newest CVSS version first. The version
    is the first one whose object carries a baseScore."""
    from pyspark.sql import functions as F
    base = "metrics[0]"
    return [
        F.coalesce(*[F.when(field(f"{base}.{key}.baseScore", "double").isNotNull(), F.lit(ver))
                     for ver, key in CVSS_VERSIONS]).alias(f"{alias}_cvss_version"),
        F.coalesce(*[field(f"{base}.{key}.baseScore", "double") for _, key in CVSS_VERSIONS]
                   ).alias(f"{alias}_cvss_base_score"),
        F.coalesce(*[field(f"{base}.{key}.baseSeverity") for _, key in CVSS_VERSIONS]
                   ).alias(f"{alias}_cvss_severity"),
        F.coalesce(*[field(f"{base}.{key}.vectorString") for _, key in CVSS_VERSIONS]
                   ).alias(f"{alias}_cvss_vector"),
    ]


def transform_cves(df):
    """Raw CVE v5 rows (raw VARIANT + extract_raw_data columns) -> silver rows."""
    from pyspark.sql import functions as F

    cna = lambda path, typ="string": _v(f"containers.cna.{path}", typ)  # noqa: E731
    descriptions = f"try_variant_get(raw, '$.containers.cna.descriptions', '{DESCRIPTIONS_TYPE}')"
    problem_types = f"try_variant_get(raw, '$.containers.cna.problemTypes', '{PROBLEM_TYPES_TYPE}')"
    return (
        df.select(
            F.col("cve_id"),
            F.col("cve_year"),
            _v("cveMetadata.state").alias("state"),
            F.col("date_published"),
            _v("cveMetadata.dateReserved", "timestamp").alias("date_reserved"),
            F.col("date_updated"),
            _v("cveMetadata.assignerOrgId").alias("assigner_org_id"),
            _v("cveMetadata.assignerShortName").alias("assigner_short_name"),
            cna("title").alias("title"),
            # English description preferred, else the first one.
            F.expr(f"coalesce(get(filter({descriptions}, x -> x.lang = 'en'), 0).value, "
                   f"get({descriptions}, 0).value)").alias("description"),
            cna("affected", AFFECTED_TYPE).alias("affected_products"),
            *_cvss(cna, "cna"),
            *_cvss(_cisa_adp, "adp"),
            # All CWE ids across every problemTypes[].descriptions[] entry.
            F.expr(f"array_distinct(filter(transform(flatten(transform("
                   f"coalesce({problem_types}, array()), pt -> coalesce(pt.descriptions, array()))), "
                   f"d -> d.cweId), x -> x IS NOT NULL))").alias("cwe_ids"),
            cna("problemTypes[0].descriptions[0].description").alias("cwe_description"),
            _cisa_adp("metrics[0].other.content.options[0].Exploitation").alias("ssvc_exploitation"),
            _cisa_adp("metrics[0].other.content.options[1].Automatable").alias("ssvc_automatable"),
            _cisa_adp("metrics[0].other.content.options[2]['Technical Impact']").alias("ssvc_technical_impact"),
            cna("references", REFERENCES_TYPE).alias("references"),
            F.col("raw"),
        )
        # get() is 0-indexed and returns NULL for an empty array (unlike element_at under ANSI).
        .withColumn("cwe_id", F.expr("get(cwe_ids, 0)"))
        .withColumn("cvss_version", F.coalesce("cna_cvss_version", "adp_cvss_version"))
        .withColumn("cvss_base_score", F.coalesce("cna_cvss_base_score", "adp_cvss_base_score"))
        .withColumn("cvss_severity", F.coalesce("cna_cvss_severity", "adp_cvss_severity"))
        .withColumn("cvss_vector", F.coalesce("cna_cvss_vector", "adp_cvss_vector"))
        .withColumn("cvss_source", F.when(F.col("cna_cvss_base_score").isNotNull(), F.lit("CNA"))
                    .when(F.col("adp_cvss_base_score").isNotNull(), F.lit("ADP")))
        .select("cve_id", "cve_year", "state", "date_published", "date_reserved", "date_updated",
                "assigner_org_id", "assigner_short_name", "title", "description", "affected_products",
                "cvss_version", "cvss_base_score", "cvss_severity", "cvss_vector", "cvss_source",
                "cwe_ids", "cwe_id", "cwe_description", "ssvc_exploitation", "ssvc_automatable",
                "ssvc_technical_impact", "references", "raw")
    )
