"""Flatten raw MITRE ATLAS VARIANT records into the two silver tables: atlas_objects
(tactics, techniques, mitigations, case studies, entities, software and the matrix)
and atlas_relationships. ATLAS field names contain hyphens, so paths are written
$['created-date']. Each transform keeps only its own record types, so both silver
tables are synced from the one raw change feed. The release and the soft-delete
marker (date_removed) are carried through from the raw table.

`view_sql` defines the per-type views; technique tactics and parents come from the
achieves / specializes relationships, tactic order from the matrix's sequences.

No module-level pyspark import — the transforms take a pyspark DataFrame at call
time, so the module imports fine without a local Spark install."""

# object type -> path of its pages on atlas.mitre.org
_URL_PATHS = {"tactic": "tactics", "technique": "techniques", "mitigation": "mitigations",
              "case-study": "studies", "entity": "entities", "software": "software"}


def transform_objects(df):
    """raw rows (+ atlas_release, date_removed) -> atlas_objects rows (no relationships)."""
    url_case = " ".join(f"WHEN '{t}' THEN concat('https://atlas.mitre.org/{p}/', atlas_id)"
                        for t, p in _URL_PATHS.items())
    return df.filter("object_type <> 'relationship'").selectExpr(
        "atlas_id",
        "object_type",
        "try_variant_get(raw, '$.name', 'string') as name",
        "try_variant_get(raw, '$.description', 'string') as description",
        "COALESCE(try_variant_get(raw, '$.platforms', 'ARRAY<STRING>'), array()) as platforms",
        "try_variant_get(raw, '$.maturity', 'string') as maturity",
        "try_variant_get(raw, '$[\\'attack-reference\\'].id', 'string') as attack_id",
        "COALESCE(try_variant_get(raw, '$.aliases', 'ARRAY<STRING>'), array()) as aliases",
        "to_date(try_variant_get(raw, '$[\\'created-date\\']', 'string')) as created",
        "modified",
        f"CASE object_type {url_case} END as url",
        "atlas_release",
        "date_removed",
        "raw",
    )


def transform_relationships(df):
    """raw rows (+ atlas_release, date_removed) -> atlas_relationships rows."""
    return df.filter("object_type = 'relationship'").selectExpr(
        "atlas_id as relationship_id",
        "try_variant_get(raw, '$[\\'relationship-type\\']', 'string') as relationship_type",
        "try_variant_get(raw, '$.source', 'string') as source_id",
        "try_variant_get(raw, '$.target', 'string') as target_id",
        "try_variant_get(raw, '$.description', 'string') as description",
        "try_variant_get(raw, '$.tactic', 'string') as tactic_id",
        "try_variant_get(raw, '$[\\'step-id\\']', 'string') as step_id",
        "COALESCE(try_variant_get(raw, '$[\\'leads-to\\']', 'ARRAY<STRING>'), array()) as leads_to",
        "atlas_release",
        "date_removed",
        "raw",
    )


def view_sql(objects: str, relationships: str) -> dict[str, str]:
    """SELECT statements of the per-type views over the (fully qualified) silver tables.
    Rows removed from the source (date_removed set) are left out."""
    common = "description, url, modified, atlas_release"

    def by_type(id_col: str, cols: str, object_type: str) -> str:
        return (f"SELECT o.atlas_id AS {id_col}, {cols}, {common} FROM {objects} o "
                f"WHERE o.object_type = '{object_type}' AND o.date_removed IS NULL")

    def rels(rel_type: str) -> str:
        return f"{relationships} WHERE relationship_type = '{rel_type}' AND date_removed IS NULL"

    raw = "try_variant_get(o.raw, '$[\\'{}\\']', '{}')"
    return {
        "atlas_techniques": (
            f"SELECT o.atlas_id AS technique_id, o.name, p.parent_id IS NOT NULL AS is_subtechnique, "
            f"p.parent_id AS parent_technique_id, COALESCE(t.tactics, array()) AS tactics, o.platforms, "
            f"o.maturity, o.attack_id AS attack_technique_id, o.description, o.url, o.modified, o.atlas_release "
            f"FROM {objects} o "
            f"LEFT JOIN (SELECT source_id, min(target_id) AS parent_id FROM {rels('specializes')} "
            f"GROUP BY source_id) p ON p.source_id = o.atlas_id "
            f"LEFT JOIN (SELECT source_id, sort_array(collect_set(target_id)) AS tactics FROM {rels('achieves')} "
            f"GROUP BY source_id) t ON t.source_id = o.atlas_id "
            f"WHERE o.object_type = 'technique' AND o.date_removed IS NULL"),
        "atlas_tactics": (
            f"SELECT o.atlas_id AS tactic_id, o.name, o.attack_id AS attack_tactic_id, "
            f"s.position AS matrix_position, o.description, o.url, o.modified, o.atlas_release "
            f"FROM {objects} o "
            f"LEFT JOIN (SELECT target_id, min(try_variant_get(raw, '$.position', 'int')) AS position "
            f"FROM {rels('sequences')} GROUP BY target_id) s ON s.target_id = o.atlas_id "
            f"WHERE o.object_type = 'tactic' AND o.date_removed IS NULL"),
        "atlas_mitigations": by_type(
            "mitigation_id",
            f"o.name, o.attack_id AS attack_mitigation_id, {raw.format('categories', 'ARRAY<STRING>')} AS categories, "
            f"{raw.format('lifecycle-phases', 'ARRAY<STRING>')} AS lifecycle_phases", "mitigation"),
        "atlas_case_studies": by_type(
            "case_study_id",
            f"o.name, {raw.format('type', 'string')} AS case_study_type, {raw.format('actor', 'string')} AS actor, "
            f"{raw.format('target', 'string')} AS target, {raw.format('reporter', 'string')} AS reporter, "
            f"to_date({raw.format('start-date', 'string')}) AS start_date, "
            f"to_date({raw.format('end-date', 'string')}) AS end_date", "case-study"),
        "atlas_software": by_type(
            "software_id",
            f"o.name, {raw.format('software-type', 'string')} AS software_type, "
            f"{raw.format('software-categories', 'ARRAY<STRING>')} AS categories, o.aliases", "software"),
        "atlas_entities": by_type(
            "entity_id", f"o.name, {raw.format('entity-type', 'string')} AS entity_type", "entity"),
        "atlas_relationships_resolved": (
            f"SELECT r.source_id, s.name AS source_name, s.object_type AS source_type, r.relationship_type, "
            f"r.target_id, t.name AS target_name, t.object_type AS target_type, r.description, "
            f"r.tactic_id, r.step_id, r.leads_to, r.relationship_id "
            f"FROM {relationships} r "
            f"LEFT JOIN {objects} s ON s.atlas_id = r.source_id "
            f"LEFT JOIN {objects} t ON t.atlas_id = r.target_id "
            f"WHERE r.date_removed IS NULL"),
    }
