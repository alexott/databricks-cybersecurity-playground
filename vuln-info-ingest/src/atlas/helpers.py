"""MITRE ATLAS loader helpers: release lookup in atlas-data's dist/manifest.yaml and
flattening of an ATLAS YAML release (format 6) into raw-table records (pure Python).
`extract_raw_data` is Spark and takes the DataFrame passed in, so this module imports
without a local Spark install.

A format-6 release has one mapping per object section (tactics, techniques, ...)
keyed by ATLAS id, plus `relationships`: source id -> relationship type -> list of
{source, target, relationship-type, ...}. Relationships have no id, so their
`atlas_id` is `<source>|<type>|<target>|<step-id>` (step-id only for case-study steps;
unique in every release). Relationships have no modified date either, so changes are
detected with a content hash of every record."""
from __future__ import annotations

import hashlib
import json

# Section of the release file -> object type, for objects without `object-type`.
SECTIONS = {"tactics": "tactic", "techniques": "technique", "mitigations": "mitigation",
            "case-studies": "case-study", "entities": "entity", "software": "software"}


def resolve_release(manifest: list[dict], release: str) -> tuple[str, str, str]:
    """(release, release date, path under dist/) of the format-6 file of `release`
    ('latest' = newest release date, or e.g. '2026.10')."""
    entries = sorted(manifest, key=lambda e: e.get("release-date", ""), reverse=True)
    if release != "latest":
        entries = [e for e in entries if e.get("release") == release]
        if not entries:
            raise ValueError(f"no ATLAS release {release} in the manifest")
    e = entries[0]
    paths = [v["path"] for v in e.get("versions", []) if str(v.get("format-version", "")).startswith("6.")]
    if not paths:
        raise ValueError(f"ATLAS release {e.get('release')} has no format 6 file")
    return e["release"], e.get("release-date"), paths[0]


def content_hash(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _record(atlas_id: str, object_type: str, modified, obj: dict) -> dict:
    # Unquoted YAML dates load as datetime.date; str() gives 'YYYY-MM-DD' either way.
    obj = json.loads(json.dumps(obj, default=str))
    return {"atlas_id": atlas_id, "object_type": object_type,
            "modified": None if modified is None else str(modified),
            "content_hash": content_hash(obj), "raw": obj}


def parse_atlas(doc: dict) -> tuple[str, list[dict]]:
    """Parsed ATLAS release (format 6) -> (release, records). A record has atlas_id,
    object_type, modified ('YYYY-MM-DD' or None), content_hash and the raw object.
    Objects: the matrix and every object section; the collection (release metadata)
    is not a record. Relationships are records of object_type 'relationship'."""
    if not str(doc.get("format-version", "")).startswith("6."):
        raise ValueError(f"unsupported ATLAS format-version {doc.get('format-version')!r}, expected 6.x")
    records = []
    matrix = doc.get("matrix")
    if matrix:
        records.append(_record(matrix["id"], "matrix", matrix.get("modified-date"), matrix))
    for section, object_type in SECTIONS.items():
        for atlas_id, obj in (doc.get(section) or {}).items():
            records.append(_record(atlas_id, obj.get("object-type", object_type), obj.get("modified-date"), obj))
    for rels in (doc.get("relationships") or {}).values():
        for rel_type, items in rels.items():
            for r in items:
                rel_id = f"{r['source']}|{r.get('relationship-type', rel_type)}|{r['target']}|{r.get('step-id', '')}"
                records.append(_record(rel_id, "relationship", None, r))
    return doc["collection"]["version"], records


def extract_raw_data(df):
    """Staged string columns -> raw-table columns: `raw` parsed to VARIANT, `modified`
    as DATE. `df` has atlas_id, object_type, modified, content_hash, raw (JSON text)
    and the atlas_release stamped at load time; `atlas_id` is the MERGE key."""
    return df.selectExpr(
        "try_parse_json(raw) as raw",
        "atlas_id",
        "object_type",
        "to_date(modified) as modified",
        "content_hash",
        "atlas_release",
    )
