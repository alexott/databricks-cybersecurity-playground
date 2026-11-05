"""MITRE ATT&CK loader helpers: version lookup in the attack-stix-data index.json and
merging the domain STIX bundles (pure Python). `extract_raw_data` is Spark and takes
the DataFrame passed in, so this module imports without a local Spark install."""
from __future__ import annotations


def resolve_versions(index: dict, domains: list[str], version: str) -> dict[str, tuple[str, str]]:
    """{domain: (version, bundle URL)} from attack-stix-data's index.json. `version`
    is 'latest' (the first, newest listed version of each domain) or e.g. '19.1'.
    Domains are the directory names: enterprise-attack, mobile-attack, ics-attack."""
    by_domain = {}
    for c in index.get("collections", []):
        versions = c.get("versions", [])
        if versions:
            by_domain[versions[0]["url"].rstrip("/").split("/")[-2]] = versions
    out = {}
    for d in domains:
        if d not in by_domain:
            raise ValueError(f"unknown ATT&CK domain {d!r}; known: {sorted(by_domain)}")
        vs = by_domain[d] if version == "latest" else [v for v in by_domain[d] if v["version"] == version]
        if not vs:
            raise ValueError(f"no ATT&CK {version} for {d}")
        out[d] = (vs[0]["version"], vs[0]["url"])
    return out


def versions_label(versions: dict[str, tuple[str, str]]) -> str:
    """Stable label of the loaded versions for the fetch log, e.g.
    'enterprise-attack=20.0,mobile-attack=20.0'."""
    return ",".join(f"{d}={v}" for d, (v, _) in sorted(versions.items()))


def bundle_objects(bundles: dict[str, tuple[str, dict]]) -> list[tuple[str, dict]]:
    """{domain: (version, STIX bundle)} -> [(version, object)], one per STIX id, with
    the version of the bundle the object was taken from. Objects shared by domains
    (e.g. groups and software) are identical; on a difference the newest `modified`
    wins. The per-domain x-mitre-collection objects are dropped: they only list the
    bundle's object refs (several MB)."""
    out: dict[str, tuple[str, dict]] = {}
    for version, bundle in bundles.values():
        for o in bundle.get("objects", []):
            if o.get("type") == "x-mitre-collection":
                continue
            prev = out.get(o["id"])
            if prev is None or o.get("modified", "") > prev[1].get("modified", ""):
                out[o["id"]] = (version, o)
    return list(out.values())


def extract_raw_data(df):
    """Add raw-table id columns from the per-object VARIANT.

    `df` is a pyspark DataFrame with a `raw` VARIANT column and the `attack_version`
    stamped at load time. `stix_id` is the MERGE key; `attack_id` is the ATT&CK id
    (T1059, G0016, S0154, ...) from the `mitre-attack` external reference, NULL for
    relationships and other objects without one; `modified` gates updates."""
    return df.selectExpr(
        "raw",
        "try_variant_get(raw, '$.id', 'string') as stix_id",
        "try_variant_get(raw, '$.type', 'string') as type",
        "get(filter(try_variant_get(raw, '$.external_references', "
        "'ARRAY<STRUCT<source_name STRING, external_id STRING>>'), "
        "r -> r.source_name = 'mitre-attack'), 0).external_id as attack_id",
        "try_variant_get(raw, '$.modified', 'timestamp') as modified",
        "attack_version",
    )
