import json
import pathlib

import pytest
from pyspark.sql import SparkSession

from attack import helpers, transform

_DATA = pathlib.Path(__file__).resolve().parent.parent / "data" / "attack"
_BASE = "https://raw.githubusercontent.com/mitre-attack/attack-stix-data/master"


def _load(name):
    return json.loads((_DATA / name).read_text())


def _bundles():
    return {"enterprise-attack": ("20.0", _load("enterprise-attack.json")),
            "mobile-attack": ("19.2", _load("mobile-attack.json"))}


def test_resolve_versions_latest_and_pinned():
    index = _load("index.json")
    assert helpers.resolve_versions(index, ["enterprise-attack", "ics-attack"], "latest") == {
        "enterprise-attack": ("20.0", f"{_BASE}/enterprise-attack/enterprise-attack-20.0.json"),
        "ics-attack": ("20.0", f"{_BASE}/ics-attack/ics-attack-20.0.json")}
    assert helpers.resolve_versions(index, ["mobile-attack"], "19.2") == {
        "mobile-attack": ("19.2", f"{_BASE}/mobile-attack/mobile-attack-19.2.json")}


def test_resolve_versions_errors():
    index = _load("index.json")
    with pytest.raises(ValueError, match="unknown ATT&CK domain"):
        helpers.resolve_versions(index, ["pre-attack"], "latest")
    with pytest.raises(ValueError, match="no ATT&CK 1.0"):
        helpers.resolve_versions(index, ["enterprise-attack"], "1.0")


def test_versions_label():
    assert helpers.versions_label({"mobile-attack": ("20.0", "u"), "enterprise-attack": ("19.1", "u")}) == \
        "enterprise-attack=19.1,mobile-attack=20.0"


def test_bundle_objects_dedupes_shared_and_skips_collections():
    pairs = helpers.bundle_objects(_bundles())
    objs = [o for _, o in pairs]
    assert len({o["id"] for o in objs}) == len(objs)
    assert "x-mitre-collection" not in {o["type"] for o in objs}
    kimsuky = [(v, o) for v, o in pairs if o.get("name") == "Kimsuky"]
    assert len(kimsuky) == 1 and kimsuky[0][1]["x_mitre_domains"] == ["enterprise-attack", "mobile-attack"]
    assert kimsuky[0][0] == "20.0"   # taken from the first bundle it is in
    assert [v for v, o in pairs if o.get("name") == "Scheduled Task/Job"] == ["19.2"]
    assert len(objs) == 14 + 1   # enterprise sample (without the collection) + Mobile-only technique


def test_bundle_objects_keeps_newest_modified():
    old = {"type": "malware", "id": "malware--1", "modified": "2025-01-01T00:00:00.000Z", "name": "old"}
    new = dict(old, modified="2026-01-01T00:00:00.000Z", name="new")
    pairs = helpers.bundle_objects({"a": ("2", {"objects": [old]}), "b": ("1", {"objects": [new]})})
    assert [(v, o["name"]) for v, o in pairs] == [("1", "new")]


def _raw_df(spark):
    """The staged shape: the object as a VARIANT plus the version stamped at load time."""
    rows = [(json.dumps(o), v) for v, o in helpers.bundle_objects(_bundles())]
    return (spark.createDataFrame(rows, "j string, attack_version string")
            .selectExpr("try_parse_json(j) as raw", "attack_version"))


def _silver(spark):
    from pyspark.sql import functions as F
    raw = helpers.extract_raw_data(_raw_df(spark)).withColumn("date_removed", F.lit(None).cast("timestamp"))
    return transform.transform_objects(raw), transform.transform_relationships(raw)


def test_extract_raw_data(spark_session: SparkSession):
    df = helpers.extract_raw_data(_raw_df(spark_session))
    assert df.columns == ["raw", "stix_id", "type", "attack_id", "modified", "attack_version"]
    rows = {r.attack_id: r for r in df.collect() if r.attack_id}
    assert rows["T1059.001"].type == "attack-pattern"
    assert rows["T1059.001"].stix_id.startswith("attack-pattern--")
    assert rows["T1059.001"].modified is not None
    assert {r.type for r in df.collect() if r.attack_id is None} == {"relationship"}


def test_transform_objects(spark_session: SparkSession):
    objects, _ = _silver(spark_session)
    rows = {r.attack_id: r for r in objects.collect()}
    assert "relationship" not in {r.type for r in rows.values()}
    t = rows["T1059.001"]
    assert (t.name, t.is_subtechnique, t.tactics, t.revoked, t.deprecated) == \
        ("PowerShell", True, ["execution"], False, False)
    assert "Windows" in t.platforms and t.domains == ["enterprise-attack"]
    assert t.url == "https://attack.mitre.org/techniques/T1059/001"
    assert rows["T1059"].is_subtechnique is False
    assert rows["T1562.002"].revoked is True
    assert rows["TA0002"].shortname == "execution"
    assert rows["G0016"].aliases[:2] == ["APT29", "IRON RITUAL"]
    assert "Cobalt Strike" in rows["S0154"].aliases
    assert rows["T1603"].tactics == ["execution", "persistence"]   # mobile kill chain
    assert rows["G0094"].domains == ["enterprise-attack", "mobile-attack"]
    assert (t.attack_version, t.date_removed) == ("20.0", None)


def test_transform_relationships(spark_session: SparkSession):
    _, rels = _silver(spark_session)
    rows = {r.relationship_type: r for r in rels.collect()}
    assert set(rows) == {"subtechnique-of", "revoked-by", "uses", "mitigates"}
    r = rows["uses"]
    assert (r.source_type, r.target_type) == ("intrusion-set", "malware")
    assert r.source_ref.startswith("intrusion-set--") and r.target_ref.startswith("malware--")


def test_views(spark_session: SparkSession):
    objects, rels = _silver(spark_session)
    objects.createOrReplaceTempView("attack_objects_t")
    rels.createOrReplaceTempView("attack_relationships_t")
    views = transform.view_sql("attack_objects_t", "attack_relationships_t")
    assert set(views) == {"attack_techniques", "attack_tactics", "attack_groups", "attack_software",
                          "attack_mitigations", "attack_campaigns", "attack_relationships_resolved"}
    techniques = {r.technique_id: r for r in spark_session.sql(views["attack_techniques"]).collect()}
    assert techniques["T1059.001"].parent_technique_id == "T1059"
    assert techniques["T1059"].parent_technique_id is None
    assert {r.software_type for r in spark_session.sql(views["attack_software"]).collect()} == {"malware"}
    resolved = {(r.source_id, r.relationship_type, r.target_id): r
                for r in spark_session.sql(views["attack_relationships_resolved"]).collect()}
    assert ("G0016", "uses", "S0154") in resolved
    assert ("T1059.001", "subtechnique-of", "T1059") in resolved
    assert ("M1038", "mitigates", "T1059") in resolved
    assert resolved[("T1562.002", "revoked-by", "T1685.001")].target_name == "Disable or Modify Windows Event Log"
    for name in ("attack_tactics", "attack_groups", "attack_mitigations", "attack_campaigns"):
        assert spark_session.sql(views[name]).count() >= 1
