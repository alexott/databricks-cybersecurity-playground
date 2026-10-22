"""Flatten raw KEV (CISA Known Exploited Vulnerabilities) VARIANT records into the
typed silver schema. `raw` is the per-vulnerability VARIANT, so every field is read
with try_variant_get(raw, '$.path', '<TYPE>') (complex types are passed as
the type hint directly). The catalog-level columns (catalog_version,
date_released) and the soft-delete marker (date_removed) are carried through from
the raw table — date_removed is maintained by the loader MERGE, not derived here.

No module-level pyspark import — transform_kev takes a pyspark DataFrame at call
time, so the module imports fine without a local Spark install."""


def transform_kev(df):
    """raw VARIANT (+ catalog_version, date_released, date_removed) -> typed silver row.

    `df` is a pyspark DataFrame with `raw` (VARIANT), `catalog_version` (STRING),
    `date_released` (TIMESTAMP) and `date_removed` (TIMESTAMP) columns."""
    return df.selectExpr(
        "try_variant_get(raw, '$.cveID', 'string') as cve_id",
        "try_variant_get(raw, '$.vendorProject', 'string') as vendor_project",
        "try_variant_get(raw, '$.product', 'string') as product",
        "try_variant_get(raw, '$.vulnerabilityName', 'string') as vulnerability_name",
        "try_variant_get(raw, '$.shortDescription', 'string') as short_description",
        "try_variant_get(raw, '$.requiredAction', 'string') as required_action",
        "try_variant_get(raw, '$.dateAdded', 'date') as date_added",
        "try_variant_get(raw, '$.dueDate', 'date') as due_date",
        "try_variant_get(raw, '$.knownRansomwareCampaignUse', 'string') as known_ransomware_campaign_use",
        "try_variant_get(raw, '$.forensicTriage', 'string') as forensic_triage",
        "try_variant_get(raw, '$.notes', 'string') as notes",
        "COALESCE(try_variant_get(raw, '$.cwes', 'ARRAY<STRING>'), array()) as cwes",
        "catalog_version",
        "date_released",
        "date_removed",
        "raw",
    )
