"""GHSA loader helpers. Pure-Python functions are unit-tested; extract_raw_data
is Spark and tested with the pytest-spark session.

pyspark is imported lazily so the pure helpers import without a Spark install."""
from __future__ import annotations


def first_cve_alias(aliases: list[str] | None) -> str | None:
    """First alias that looks like a CVE id, else None."""
    for a in aliases or []:
        if a and a.upper().startswith("CVE-"):
            return a
    return None


def github_reviewed_from_path(path: str) -> bool:
    """True iff the advisory file sits under the github-reviewed/ tree."""
    return "/github-reviewed/" in path


def extract_raw_data(df):
    """Add raw-table id columns from the VARIANT (merge key + convenience).

    `df` is a pyspark DataFrame with a `raw` VARIANT column."""
    aliases = "try_variant_get(raw, '$.aliases', 'ARRAY<STRING>')"
    return df.selectExpr(
        "raw",
        "try_variant_get(raw, '$.id', 'string') as ghsa_id",
        f"get(filter({aliases}, x -> upper(x) like 'CVE-%'), 0) as cve_id",
        "try_variant_get(raw, '$.published', 'timestamp') as date_published",
        "try_variant_get(raw, '$.modified', 'timestamp') as date_modified",
    )
