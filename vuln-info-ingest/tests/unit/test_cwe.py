import json
import pathlib
import xml.etree.ElementTree as ET

import pytest
from pyspark.sql import SparkSession

from cwe import helpers, transform

_CATALOG = pathlib.Path(__file__).resolve().parent.parent / "data" / "cwe" / "catalog.xml"
_NS = 'xmlns="http://cwe.mitre.org/cwe-8" xmlns:xhtml="http://www.w3.org/1999/xhtml"'


def _el(xml: str):
    return ET.fromstring(xml.replace("<X", f"<X {_NS}", 1))


def _catalog():
    return helpers.parse_catalog(_CATALOG.read_bytes())


def test_parse_catalog_version_and_entries():
    version, date, entries = _catalog()
    assert (version, date) == ("5.0", "2026-10-28")
    assert [(t, e["ID"]) for t, e in entries] == \
        [("Weakness", "79"), ("Weakness", "1187"), ("Grouping", "1001"), ("View", "1000")]


def test_parse_catalog_rejects_other_root():
    with pytest.raises(ValueError, match="not a CWE catalog"):
        helpers.parse_catalog(b"<Attack_Pattern_Catalog/>")


def test_element_to_json_repeated_tags_are_always_lists():
    one = helpers.element_to_json(_el(
        '<X><Related_Weaknesses><Related_Weakness Nature="ChildOf" CWE_ID="74"/></Related_Weaknesses></X>'))
    assert one == {"Related_Weaknesses": {"Related_Weakness": [{"Nature": "ChildOf", "CWE_ID": "74"}]}}
    # a tag not known as repeatable still keeps every value when it repeats
    two = helpers.element_to_json(_el("<X><Likelihood>High</Likelihood><Likelihood>Low</Likelihood></X>"))
    assert two == {"Likelihood": ["High", "Low"]}


def test_element_to_json_text_and_attributes():
    e = _el('<X><Note Type="Maintenance">  some\n   text </Note><Usage>Allowed</Usage></X>')
    assert helpers.element_to_json(e) == \
        {"Note": [{"Type": "Maintenance", "Text": "some text"}], "Usage": "Allowed"}


def test_render_text_xhtml_blocks_and_indent():
    e = _el('<X><xhtml:p>First   para</xhtml:p><xhtml:ul><xhtml:li>one</xhtml:li>'
            '<xhtml:li>two <xhtml:b>bold</xhtml:b></xhtml:li></xhtml:ul>'
            '<xhtml:div>if (x)<xhtml:br/><xhtml:div style="margin-left:1em;">run();</xhtml:div></xhtml:div></X>')
    assert helpers.render_text(e) == "First para\n- one\n- two bold\nif (x)\n  run();"


def test_raw_record_is_json_with_rendered_text():
    _, _, entries = _catalog()
    w79 = entries[0][1]
    json.dumps(w79)   # serializable
    example = w79["Demonstrative_Examples"]["Demonstrative_Example"][0]
    assert example["Example_Code"][0]["Language"] == "PHP"
    assert "$username = $_GET['username'];\necho" in example["Example_Code"][0]["Text"]


def _raw_df(spark):
    """The staged shape: the entry as a VARIANT plus the columns stamped at load time."""
    version, date, entries = _catalog()
    rows = [(json.dumps(e), t, version, date) for t, e in entries]
    return (spark.createDataFrame(rows, "json_text string, entry_type string, "
                                        "catalog_version string, catalog_date string")
            .selectExpr("try_parse_json(json_text) as raw", "entry_type", "catalog_version",
                        "cast(catalog_date as date) as catalog_date"))


def _silver_rows(spark, date_removed=None):
    """raw table rows (with the loader-maintained date_removed) -> silver rows by cwe_id."""
    raw = helpers.extract_raw_data(_raw_df(spark)).selectExpr(
        "*", f"cast({'NULL' if date_removed is None else repr(date_removed)} as timestamp) as date_removed")
    return {r.cwe_id: r for r in transform.transform_cwe(raw).collect()}


def test_extract_raw_data_ids(spark_session: SparkSession):
    df = helpers.extract_raw_data(_raw_df(spark_session))
    assert df.columns == ["raw", "cwe_id", "entry_type", "catalog_version", "catalog_date"]
    assert [r.cwe_id for r in df.collect()] == ["CWE-79", "CWE-1187", "CWE-1001", "CWE-1000"]


def test_transform_weakness(spark_session: SparkSession):
    r = _silver_rows(spark_session)["CWE-79"]
    assert (r.entry_type, r.status, r.abstraction, r.structure) == ("Weakness", "Stable", "Base", "Simple")
    assert r.description.startswith("The product does not neutralize")
    assert r.mapping_usage == "Allowed"
    assert r.likelihood_of_exploit == "High"
    assert r.related_weaknesses[0].asDict() == \
        {"nature": "ChildOf", "cwe_id": "CWE-74", "view_id": "CWE-1000", "ordinal": "Primary"}
    assert r.related_capec_ids == ["CAPEC-209", "CAPEC-588", "CAPEC-591"]
    assert [p.asDict() for p in r.applicable_platforms][:2] == [
        {"type": "Language", "name": None, "class": "Not Language-Specific", "prevalence": "Undetermined"},
        {"type": "Language", "name": "Go", "class": None, "prevalence": "Undetermined"}]
    c = r.common_consequences[0]
    assert (c.scopes, c.impacts) == (["Access Control", "Confidentiality"],
                                    ["Bypass Protection Mechanism", "Read Application Data"])
    assert [o.reference for o in r.observed_examples] == ["CVE-2024-49038", "CVE-2024-54142"]
    assert str(r.date_submitted) == "2006-07-19"
    assert str(r.date_modified) == "2008-08-15"
    assert r.members == []
    assert (r.catalog_version, str(r.catalog_date), r.date_removed) == ("5.0", "2026-10-28", None)
    assert r.raw is not None


def test_transform_deprecated_grouping_view(spark_session: SparkSession):
    rows = _silver_rows(spark_session)
    assert rows["CWE-1187"].status == "Deprecated"
    assert rows["CWE-1187"].mapping_usage == "Prohibited"
    g = rows["CWE-1001"]
    assert (g.entry_type, g.abstraction) == ("Grouping", None)
    assert g.description.startswith("This category identifies")
    assert g.members == ["CWE-111", "CWE-227"]
    assert g.related_weaknesses == [] and g.related_capec_ids == []
    v = rows["CWE-1000"]
    assert v.entry_type == "View" and v.members == ["CWE-284", "CWE-435"]
    assert v.description.startswith("This view")


def test_transform_carries_date_removed(spark_session: SparkSession):
    assert _silver_rows(spark_session, "2026-11-01 00:00:00")["CWE-79"].date_removed is not None
