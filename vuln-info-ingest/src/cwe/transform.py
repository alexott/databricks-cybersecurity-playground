"""Flatten raw CWE (Common Weakness Enumeration) VARIANT entries into the typed silver
schema. `raw` is the per-entry JSON rendering of the catalog XML (cwe.helpers), so
tags the schema allows to repeat are arrays and container elements are objects
(`$.Related_Weaknesses.Related_Weakness[*]`). Every field is read with
try_variant_get(raw, '$.path', '<TYPE>'); complex types are passed as the type hint.
Weakness-only fields are NULL / empty for Groupings and Views. The catalog-level
columns and the soft-delete marker (date_removed) are carried through from the raw
table — date_removed is maintained by the loader MERGE, not derived here.

No module-level pyspark import — transform_cwe takes a pyspark DataFrame at call
time, so the module imports fine without a local Spark install."""

_PLATFORM_TYPES = ("Language", "Technology", "Operating_System", "Architecture")


def _platforms(t: str) -> str:
    return (f"transform(COALESCE(try_variant_get(raw, '$.Applicable_Platforms.{t}', "
            f"'ARRAY<STRUCT<Name STRING, Class STRING, Prevalence STRING>>'), array()), "
            f"x -> named_struct('type', '{t}', 'name', x.Name, 'class', x.Class, "
            f"'prevalence', x.Prevalence))")


def transform_cwe(df):
    """raw VARIANT (+ entry_type, catalog_version, catalog_date, date_removed) -> typed silver row.

    `df` is a pyspark DataFrame with `raw` (VARIANT), `entry_type` (STRING),
    `catalog_version` (STRING), `catalog_date` (DATE) and `date_removed` (TIMESTAMP)."""
    return df.selectExpr(
        "concat('CWE-', try_variant_get(raw, '$.ID', 'string')) as cwe_id",
        "entry_type",
        "try_variant_get(raw, '$.Name', 'string') as name",
        "try_variant_get(raw, '$.Status', 'string') as status",
        "try_variant_get(raw, '$.Abstraction', 'string') as abstraction",
        "try_variant_get(raw, '$.Structure', 'string') as structure",
        # Weakness Description, Grouping Summary, View Objective
        "COALESCE(try_variant_get(raw, '$.Description', 'string'), "
        "try_variant_get(raw, '$.Summary', 'string'), "
        "try_variant_get(raw, '$.Objective', 'string')) as description",
        "try_variant_get(raw, '$.Extended_Description', 'string') as extended_description",
        "try_variant_get(raw, '$.Likelihood_Of_Exploit', 'string') as likelihood_of_exploit",
        "try_variant_get(raw, '$.Mapping_Notes.Usage', 'string') as mapping_usage",
        "transform(COALESCE(try_variant_get(raw, '$.Related_Weaknesses.Related_Weakness', "
        "'ARRAY<STRUCT<Nature STRING, CWE_ID STRING, View_ID STRING, Ordinal STRING>>'), array()), "
        "x -> named_struct('nature', x.Nature, 'cwe_id', concat('CWE-', x.CWE_ID), "
        "'view_id', concat('CWE-', x.View_ID), 'ordinal', x.Ordinal)) as related_weaknesses",
        # Grouping: Relationships/Has_Member, View: Members/Has_Member
        "transform(COALESCE(try_variant_get(raw, '$.Relationships.Has_Member', 'ARRAY<STRUCT<CWE_ID STRING>>'), "
        "try_variant_get(raw, '$.Members.Has_Member', 'ARRAY<STRUCT<CWE_ID STRING>>'), array()), "
        "x -> concat('CWE-', x.CWE_ID)) as members",
        "transform(COALESCE(try_variant_get(raw, '$.Related_Attack_Patterns.Related_Attack_Pattern', "
        "'ARRAY<STRUCT<CAPEC_ID STRING>>'), array()), x -> concat('CAPEC-', x.CAPEC_ID)) as related_capec_ids",
        "concat(" + ", ".join(_platforms(t) for t in _PLATFORM_TYPES) + ") as applicable_platforms",
        "transform(COALESCE(try_variant_get(raw, '$.Common_Consequences.Consequence', "
        "'ARRAY<STRUCT<Scope ARRAY<STRING>, Impact ARRAY<STRING>, Likelihood STRING, Note ARRAY<STRING>>>'), array()), "
        "x -> named_struct('scopes', COALESCE(x.Scope, array()), 'impacts', COALESCE(x.Impact, array()), "
        "'likelihood', x.Likelihood, 'note', array_join(x.Note, '\\n'))) as common_consequences",
        # `Reference` is repeatable elsewhere in the schema, so it is an array here too.
        "transform(COALESCE(try_variant_get(raw, '$.Observed_Examples.Observed_Example', "
        "'ARRAY<STRUCT<Reference ARRAY<STRING>, Description STRING, Link STRING>>'), array()), "
        "x -> named_struct('reference', x.Reference[0], 'description', x.Description, 'link', x.Link)) "
        "as observed_examples",
        "to_date(try_variant_get(raw, '$.Content_History.Submission[0].Submission_Date', 'string')) "
        "as date_submitted",
        "array_max(transform(try_variant_get(raw, '$.Content_History.Modification', "
        "'ARRAY<STRUCT<Modification_Date STRING>>'), x -> to_date(x.Modification_Date))) as date_modified",
        "catalog_version",
        "catalog_date",
        "date_removed",
        "raw",
    )
