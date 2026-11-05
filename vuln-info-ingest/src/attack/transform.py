"""Flatten raw MITRE ATT&CK STIX 2.1 VARIANT objects into the two silver tables:
attack_objects (every object except relationships: techniques, tactics, groups,
software, mitigations, campaigns, data sources, ...) and attack_relationships. Fields
are read with try_variant_get(raw, '$.path', '<TYPE>'); fields of other object types
are NULL. Each transform keeps only its own object types, so both silver tables can be
synced from the one raw change feed. The ATT&CK version and the soft-delete marker
(date_removed) are carried through from the raw table.

`view_sql` defines the per-type views over the silver tables.

No module-level pyspark import — the transforms take a pyspark DataFrame at call
time, so the module imports fine without a local Spark install."""

_REFS = "try_variant_get(raw, '$.external_references', 'ARRAY<STRUCT<source_name STRING, external_id STRING, url STRING>>')"


def transform_objects(df):
    """raw rows (+ attack_version, date_removed) -> attack_objects rows (no relationships)."""
    return df.filter("type <> 'relationship'").selectExpr(
        "stix_id",
        "type",
        "attack_id",
        "try_variant_get(raw, '$.name', 'string') as name",
        "try_variant_get(raw, '$.description', 'string') as description",
        "COALESCE(try_variant_get(raw, '$.x_mitre_domains', 'ARRAY<STRING>'), array()) as domains",
        "COALESCE(try_variant_get(raw, '$.x_mitre_platforms', 'ARRAY<STRING>'), array()) as platforms",
        # Kill-chain phases of the ATT&CK matrices (mitre-attack, mitre-mobile-attack, mitre-ics-attack)
        "transform(filter(COALESCE(try_variant_get(raw, '$.kill_chain_phases', "
        "'ARRAY<STRUCT<kill_chain_name STRING, phase_name STRING>>'), array()), "
        "k -> k.kill_chain_name LIKE 'mitre-%'), k -> k.phase_name) as tactics",
        "try_variant_get(raw, '$.x_mitre_shortname', 'string') as shortname",
        "CASE WHEN type = 'attack-pattern' THEN "
        "COALESCE(try_variant_get(raw, '$.x_mitre_is_subtechnique', 'boolean'), false) END as is_subtechnique",
        # groups and campaigns: aliases; software: x_mitre_aliases
        "COALESCE(try_variant_get(raw, '$.aliases', 'ARRAY<STRING>'), "
        "try_variant_get(raw, '$.x_mitre_aliases', 'ARRAY<STRING>'), array()) as aliases",
        f"get(filter({_REFS}, r -> r.source_name = 'mitre-attack'), 0).url as url",
        "COALESCE(try_variant_get(raw, '$.revoked', 'boolean'), false) as revoked",
        "COALESCE(try_variant_get(raw, '$.x_mitre_deprecated', 'boolean'), false) as deprecated",
        "try_variant_get(raw, '$.x_mitre_version', 'string') as version",
        "try_variant_get(raw, '$.created', 'timestamp') as created",
        "modified",
        "attack_version",
        "date_removed",
        "raw",
    )


def transform_relationships(df):
    """raw rows (+ attack_version, date_removed) -> attack_relationships rows."""
    return df.filter("type = 'relationship'").selectExpr(
        "stix_id",
        "try_variant_get(raw, '$.relationship_type', 'string') as relationship_type",
        "try_variant_get(raw, '$.source_ref', 'string') as source_ref",
        "try_variant_get(raw, '$.target_ref', 'string') as target_ref",
        "substring_index(try_variant_get(raw, '$.source_ref', 'string'), '--', 1) as source_type",
        "substring_index(try_variant_get(raw, '$.target_ref', 'string'), '--', 1) as target_type",
        "try_variant_get(raw, '$.description', 'string') as description",
        "COALESCE(try_variant_get(raw, '$.revoked', 'boolean'), false) as revoked",
        "COALESCE(try_variant_get(raw, '$.x_mitre_deprecated', 'boolean'), false) as deprecated",
        "try_variant_get(raw, '$.created', 'timestamp') as created",
        "modified",
        "attack_version",
        "date_removed",
        "raw",
    )


def view_sql(objects: str, relationships: str) -> dict[str, str]:
    """SELECT statements of the per-type views over the (fully qualified) silver tables.
    Rows removed from the source (date_removed set) are left out; revoked and
    deprecated objects are kept with their flags."""
    common = "domains, url, revoked, deprecated, version, modified, attack_version, stix_id"

    def by_type(id_col: str, cols: str, types: str) -> str:
        return (f"SELECT attack_id AS {id_col}, {cols}, {common} FROM {objects} "
                f"WHERE type IN ({types}) AND date_removed IS NULL")

    return {
        "attack_techniques": by_type(
            "technique_id",
            "name, is_subtechnique, CASE WHEN is_subtechnique THEN substring_index(attack_id, '.', 1) END "
            "AS parent_technique_id, tactics, platforms, description", "'attack-pattern'"),
        "attack_tactics": by_type("tactic_id", "name, shortname, description", "'x-mitre-tactic'"),
        "attack_groups": by_type("group_id", "name, aliases, description", "'intrusion-set'"),
        "attack_software": by_type("software_id", "name, type AS software_type, aliases, platforms, description",
                                   "'malware', 'tool'"),
        "attack_mitigations": by_type("mitigation_id", "name, description", "'course-of-action'"),
        "attack_campaigns": by_type(
            "campaign_id", "name, aliases, try_variant_get(raw, '$.first_seen', 'timestamp') AS first_seen, "
            "try_variant_get(raw, '$.last_seen', 'timestamp') AS last_seen, description", "'campaign'"),
        "attack_relationships_resolved": (
            f"SELECT s.attack_id AS source_id, s.name AS source_name, r.source_type, r.relationship_type, "
            f"t.attack_id AS target_id, t.name AS target_name, r.target_type, r.description, "
            f"r.revoked, r.deprecated, r.modified, r.stix_id, r.source_ref, r.target_ref "
            f"FROM {relationships} r "
            f"LEFT JOIN {objects} s ON s.stix_id = r.source_ref "
            f"LEFT JOIN {objects} t ON t.stix_id = r.target_ref "
            f"WHERE r.date_removed IS NULL"),
    }
