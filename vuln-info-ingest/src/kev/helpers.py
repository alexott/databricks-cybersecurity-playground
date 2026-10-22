"""KEV loader helpers. `extract_raw_data` is Spark and tested with the
pytest-spark session; pyspark is imported lazily (via the DataFrame passed in)
so this module imports without a local Spark install."""
from __future__ import annotations


def extract_raw_data(df):
    """Add raw-table id/convenience columns from the per-vulnerability VARIANT.

    `df` is a pyspark DataFrame with a `raw` VARIANT column plus the catalog-level
    `catalog_version` and `date_released` columns stamped at load time. `cve_id`
    is the MERGE key; `date_added` is the KEV listing date."""
    return df.selectExpr(
        "raw",
        "try_variant_get(raw, '$.cveID', 'string') as cve_id",
        "try_variant_get(raw, '$.dateAdded', 'date') as date_added",
        "catalog_version",
        "date_released",
    )
