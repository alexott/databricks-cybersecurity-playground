import json
import pathlib

import pytest
import yaml
from pyspark.sql import SparkSession

from atlas import helpers, transform

_DATA = pathlib.Path(__file__).resolve().parent.parent / "data" / "atlas"


def _doc():
    return yaml.safe_load((_DATA / "ATLAS-2026.10.yaml").read_text())


def _manifest():
    return yaml.safe_load((_DATA / "manifest.yaml").read_text())


def test_resolve_release_latest_and_pinned():
    assert helpers.resolve_release(_manifest(), "latest") == ("2026.10", "2026-10-28", "v6/ATLAS-2026.10.yaml")
    assert helpers.resolve_release(_manifest(), "2026.09") == ("2026.09", "2026-09-15", "v6/ATLAS-2026.09.yaml")


def test_resolve_release_errors():
    with pytest.raises(ValueError, match="no ATLAS release 1999.01"):
        helpers.resolve_release(_manifest(), "1999.01")
    legacy_only = [{"release": "2021.05", "release-date": "2021-05-13",
                    "versions": [{"format-version": "2.0.0", "path": "legacy/ATLAS-2.0.0.yaml"}]}]
    with pytest.raises(ValueError, match="no format 6"):
        helpers.resolve_release(legacy_only, "latest")


def test_parse_atlas_records():
    release, records = helpers.parse_atlas(_doc())
    assert release == "2026.10"
    by_id = {r["atlas_id"]: r for r in records}
    assert len(by_id) == len(records)
    assert by_id["AML.T0000.003"]["object_type"] == "technique"
    assert by_id["AML.T0000.003"]["modified"] == "2026-09-15"
    assert by_id["AML.CS0002"]["object_type"] == "case-study"
    assert by_id["ATLAS-matrix"]["object_type"] == "matrix"
    assert "ATLAS-collection" not in by_id
    rel = by_id["AML.CS0003|employs|AML.T0000|S00"]
    assert (rel["object_type"], rel["modified"]) == ("relationship", None)
    assert rel["raw"]["tactic"] == "AML.TA0002"
    assert "AML.T0000.003|specializes|AML.T0000|" in by_id
    json.dumps(records)   # serializable


def test_content_hash_ignores_key_order():
    assert helpers.content_hash({"a": 1, "b": [1, 2]}) == helpers.content_hash({"b": [1, 2], "a": 1})
    assert helpers.content_hash({"a": 1}) != helpers.content_hash({"a": 2})


def test_parse_atlas_rejects_other_formats():
    with pytest.raises(ValueError, match="format-version"):
        helpers.parse_atlas({"format-version": "5.6.0", "matrices": []})


def _raw(spark):
    from pyspark.sql import functions as F
    release, records = helpers.parse_atlas(_doc())
    staged = spark.createDataFrame(
        [(r["atlas_id"], r["object_type"], r["modified"], r["content_hash"], json.dumps(r["raw"]))
         for r in records],
        "atlas_id string, object_type string, modified string, content_hash string, raw string")
    return (helpers.extract_raw_data(staged.withColumn("atlas_release", F.lit(release)))
            .withColumn("date_removed", F.lit(None).cast("timestamp")))


def test_extract_raw_data(spark_session: SparkSession):
    df = helpers.extract_raw_data(spark_session.createDataFrame(
        [("AML.T0000", "technique", "2026-09-15", "h", '{"id": "AML.T0000"}', "2026.10")],
        "atlas_id string, object_type string, modified string, content_hash string, raw string, "
        "atlas_release string"))
    assert df.columns == ["raw", "atlas_id", "object_type", "modified", "content_hash", "atlas_release"]
    r = df.first()
    assert str(r.modified) == "2026-09-15" and r.raw is not None


def _silver(spark):
    raw = _raw(spark)
    return transform.transform_objects(raw), transform.transform_relationships(raw)


def test_transform_objects(spark_session: SparkSession):
    objects, _ = _silver(spark_session)
    rows = {r.atlas_id: r for r in objects.collect()}
    assert "relationship" not in {r.object_type for r in rows.values()}
    t = rows["AML.T0000"]
    assert (t.name, t.maturity, t.attack_id, t.platforms) == \
        ("Search Open Technical Databases", "Realized", "T1596", ["Enterprise"])
    assert (str(t.created), str(t.modified)) == ("2021-05-13", "2026-09-15")
    assert t.url == "https://atlas.mitre.org/techniques/AML.T0000"
    assert rows["AML.CS0002"].url == "https://atlas.mitre.org/studies/AML.CS0002"
    assert rows["AML.M0011"].attack_id == "M1044"
    assert rows["ATLAS-matrix"].url is None
    assert (t.atlas_release, t.date_removed) == ("2026.10", None)


def test_transform_relationships(spark_session: SparkSession):
    _, rels = _silver(spark_session)
    rows = {r.relationship_id: r for r in rels.collect()}
    r = rows["AML.CS0003|employs|AML.T0000|S00"]
    assert (r.relationship_type, r.source_id, r.target_id, r.tactic_id, r.step_id) == \
        ("employs", "AML.CS0003", "AML.T0000", "AML.TA0002", "S00")
    assert isinstance(r.leads_to, list)
    s = rows["AML.T0000.003|specializes|AML.T0000|"]
    assert (s.step_id, s.tactic_id, s.leads_to) == (None, None, [])


def test_views(spark_session: SparkSession):
    objects, rels = _silver(spark_session)
    objects.createOrReplaceTempView("atlas_objects_t")
    rels.createOrReplaceTempView("atlas_relationships_t")
    views = transform.view_sql("atlas_objects_t", "atlas_relationships_t")
    assert set(views) == {"atlas_techniques", "atlas_tactics", "atlas_mitigations", "atlas_case_studies",
                          "atlas_software", "atlas_entities", "atlas_relationships_resolved"}
    t = {r.technique_id: r for r in spark_session.sql(views["atlas_techniques"]).collect()}
    assert (t["AML.T0000.003"].is_subtechnique, t["AML.T0000.003"].parent_technique_id) == (True, "AML.T0000")
    assert (t["AML.T0000"].is_subtechnique, t["AML.T0000"].parent_technique_id) == (False, None)
    assert t["AML.T0000"].tactics == ["AML.TA0002"]
    assert t["AML.T0000.003"].attack_technique_id == "T1596.005"
    tactics = {r.tactic_id: r for r in spark_session.sql(views["atlas_tactics"]).collect()}
    assert tactics["AML.TA0002"].attack_tactic_id == "TA0043"
    assert tactics["AML.TA0002"].matrix_position is not None
    m = {r.mitigation_id: r for r in spark_session.sql(views["atlas_mitigations"]).collect()}
    assert (m["AML.M0011"].categories, m["AML.M0011"].lifecycle_phases) == (["Technical - Cyber"], ["Deployment"])
    cs = {r.case_study_id: r for r in spark_session.sql(views["atlas_case_studies"]).collect()}
    assert cs["AML.CS0002"].case_study_type is not None and cs["AML.CS0002"].start_date is not None
    sw = spark_session.sql(views["atlas_software"]).first()
    assert sw.software_type is not None
    assert spark_session.sql(views["atlas_entities"]).first().entity_type is not None
    resolved = {(r.source_id, r.relationship_type, r.target_id): r
                for r in spark_session.sql(views["atlas_relationships_resolved"]).collect()}
    r = resolved[("AML.CS0003", "employs", "AML.T0000")]
    assert (r.target_name, r.source_type, r.target_type) == ("Search Open Technical Databases", "case-study", "technique")
