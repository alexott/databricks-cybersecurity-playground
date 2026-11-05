import json
import pathlib

import pytest

from capec import helpers

_DATA = pathlib.Path(__file__).resolve().parent.parent / "data" / "capec"


def _capec():
    return helpers.parse_capec((_DATA / "capec.xml").read_bytes())


def _attack():
    return helpers.parse_attack_techniques(json.loads((_DATA / "enterprise-attack.json").read_text()))


def test_parse_capec():
    version, date, cwe_edges, technique_edges = _capec()
    assert (version, date) == ("3.9", "2023-01-24")
    assert cwe_edges[:2] == [("CWE-276", "CAPEC-1", "Accessing Functionality Not Properly Constrained by ACLs"),
                             ("CWE-285", "CAPEC-1", "Accessing Functionality Not Properly Constrained by ACLs")]
    assert len(cwe_edges) == 17
    # only ATT&CK mappings, ids with the T prefix, duplicates kept (dropped in build_rows)
    assert technique_edges == [("CAPEC-1", "T1574.010"), ("CAPEC-571", "T1562.002"), ("CAPEC-571", "T1562.002"),
                               ("CAPEC-571", "T1562.006"), ("CAPEC-571", "T1562.008"),
                               ("CAPEC-648", "T1113"), ("CAPEC-648", "T1513")]


def test_parse_capec_rejects_other_root():
    with pytest.raises(ValueError, match="not a CAPEC catalog"):
        helpers.parse_capec(b"<Weakness_Catalog/>")


def test_parse_attack_techniques():
    version, names = _attack()
    assert version == "20.0"
    assert names["T1574.010"] == "Services File Permissions Weakness"
    # revoked techniques keep their name: CAPEC 3.9 still maps to them
    assert names["T1562.002"] == "Disable Windows Event Logging"
    assert "T1513" not in names   # a Mobile ATT&CK technique


def test_build_rows():
    rows = helpers.build_rows(_capec(), _attack())
    source = "capec_3.9;attack_enterprise_20.0"
    assert ("CWE", "CWE-267", "related_attack_pattern", "CAPEC", "CAPEC-648",
            "Collect Data from Screen Capture", source) in rows
    techniques = [r for r in rows if r[2] == "maps_to_technique"]
    # deduplicated; T1513 (not in Enterprise ATT&CK) is dropped
    assert [(r[1], r[4], r[5]) for r in techniques] == [
        ("CAPEC-1", "T1574.010", "Services File Permissions Weakness"),
        ("CAPEC-571", "T1562.002", "Disable Windows Event Logging"),
        ("CAPEC-571", "T1562.006", "Indicator Blocking"),
        ("CAPEC-571", "T1562.008", "Disable or Modify Cloud Logs"),
        ("CAPEC-648", "T1113", "Screen Capture")]
    assert {r[0] for r in techniques} == {"CAPEC"} and {r[3] for r in techniques} == {"technique"}
    assert len(rows) == 17 + 5
    assert len(set(rows)) == len(rows)
