import datetime
import io
import json
import pathlib
import tarfile

import pytest
from pyspark.sql import SparkSession

from cve_v5 import helpers, transform

UTC = datetime.timezone.utc
_DATA = pathlib.Path(__file__).resolve().parent.parent / "data" / "cve_v5"


# --- pure helpers ------------------------------------------------------------

def test_parse_ts():
    assert helpers.parse_ts("2024-05-30T15:40:23.392Z") == \
        datetime.datetime(2024, 5, 30, 15, 40, 23, 392000, tzinfo=UTC)


def test_raw_github_url_passes_raw_links_through():
    # The format deltaLog.json actually uses.
    link = "https://raw.githubusercontent.com/CVEProject/cvelistV5/main/cves/2026/84xxx/CVE-2026-84284.json"
    assert helpers.raw_github_url(link) == link


def test_raw_github_url():
    assert helpers.raw_github_url(
        "https://github.com/CVEProject/cvelistV5/blob/main/cves/2024/1xxx/CVE-2024-1234.json") == \
        "https://raw.githubusercontent.com/CVEProject/cvelistV5/main/cves/2024/1xxx/CVE-2024-1234.json"
    with pytest.raises(ValueError):
        helpers.raw_github_url("https://example.com/x.json")


def _entry(ts, new=(), updated=()):
    def items(ids):
        return [{"cveId": i, "githubLink": f"https://raw.githubusercontent.com/o/r/main/cves/{i}-{ts}.json"} for i in ids]
    return {"fetchTime": ts, "numberOfChanges": len(new) + len(updated), "new": items(new), "updated": items(updated)}


_LOG = [  # newest first, like deltaLog.json
    _entry("2026-10-22T12:00:00.000Z", updated=["CVE-1"]),
    _entry("2026-10-22T11:00:00.000Z", new=["CVE-2"], updated=["CVE-1"]),
    _entry("2026-10-22T10:00:00.000Z", new=["CVE-3"]),
]


def test_plan_incremental_since_watermark_latest_link_wins():
    wm = helpers.parse_ts("2026-10-22T10:00:00.000Z")
    changes, newest, full = helpers.plan_incremental(_LOG, wm, threshold=10)
    assert not full
    assert newest == helpers.parse_ts("2026-10-22T12:00:00.000Z")
    assert changes == {"CVE-1": "https://raw.githubusercontent.com/o/r/main/cves/CVE-1-2026-10-22T12:00:00.000Z.json",
                       "CVE-2": "https://raw.githubusercontent.com/o/r/main/cves/CVE-2-2026-10-22T11:00:00.000Z.json"}


def test_plan_incremental_overlap_refetches_recent_entries():
    # raw.githubusercontent.com is CDN-cached: re-read entries shortly before the
    # watermark so a record fetched stale last run is fetched again.
    wm = helpers.parse_ts("2026-10-22T11:30:00.000Z")
    changes, newest, full = helpers.plan_incremental(_LOG, wm, threshold=10,
                                                     overlap=datetime.timedelta(minutes=45))
    assert not full and set(changes) == {"CVE-1", "CVE-2"}
    assert newest == helpers.parse_ts("2026-10-22T12:00:00.000Z")
    changes, _, _ = helpers.plan_incremental(_LOG, wm, threshold=10)
    assert set(changes) == {"CVE-1"}


def test_plan_incremental_no_watermark_needs_full():
    assert helpers.plan_incremental(_LOG, None, threshold=10) == ({}, None, True)


def test_plan_incremental_watermark_older_than_log_window_needs_full():
    wm = helpers.parse_ts("2026-10-01T00:00:00.000Z")
    _, _, full = helpers.plan_incremental(_LOG, wm, threshold=10)
    assert full


def test_plan_incremental_over_threshold_needs_full():
    wm = helpers.parse_ts("2026-10-22T10:00:00.000Z")
    _, _, full = helpers.plan_incremental(_LOG, wm, threshold=1)
    assert full


def test_plan_incremental_nothing_new():
    wm = helpers.parse_ts("2026-10-22T12:00:00.000Z")
    assert helpers.plan_incremental(_LOG, wm, threshold=10) == ({}, None, False)
    assert helpers.plan_incremental([], wm, threshold=10) == ({}, None, False)


def test_newest_fetch_time():
    assert helpers.newest_fetch_time(_LOG) == helpers.parse_ts("2026-10-22T12:00:00.000Z")
    assert helpers.newest_fetch_time([]) is None


def test_iter_tar_records_only_cve_files(tmp_path):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data in [("cvelistV5-main/cves/2024/1xxx/CVE-2024-1234.json", b'{\n "a": 1\n}'),
                           ("cvelistV5-main/cves/deltaLog.json", b"[]"),
                           ("cvelistV5-main/cves/delta.json", b"{}"),
                           ("cvelistV5-main/README.md", b"x")]:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    buf.seek(0)
    assert list(helpers.iter_tar_records(buf)) == [b'{"a":1}']


# --- Spark -------------------------------------------------------------------

def _raw_df(spark, *files):
    rows = [((_DATA / f).read_text(),) for f in files]
    return helpers.extract_raw_data(
        spark.createDataFrame(rows, "value string").selectExpr("try_parse_json(value) as raw"))


def test_extract_raw_data(spark_session: SparkSession):
    r = _raw_df(spark_session, "log4shell.json").first()
    assert (r.data_type, r.data_version, r.cve_id, r.cve_year) == ("CVE_RECORD", "5.1", "CVE-2021-44228", 2021)
    assert r.date_published == datetime.datetime(2021, 12, 10)
    assert r.date_updated == datetime.datetime(2025, 2, 4, 14, 25, 34, 416000)


def test_extract_raw_data_unparsable(spark_session: SparkSession):
    df = helpers.extract_raw_data(
        spark_session.createDataFrame([("not json",)], "value string").selectExpr("try_parse_json(value) as raw"))
    assert df.first().cve_id is None


def test_transform_cna_cvss_cwes_ssvc_affected(spark_session: SparkSession):
    r = transform.transform_cves(_raw_df(spark_session, "log4shell.json")).first()
    assert r.state == "PUBLISHED"
    assert r.date_reserved == datetime.datetime(2021, 11, 26)
    assert r.assigner_short_name == "apache"
    assert r.description.startswith("Apache Log4j2 2.0-beta9")
    assert (r.cvss_version, r.cvss_base_score, r.cvss_severity, r.cvss_source) == ("3.1", 10.0, "CRITICAL", "CNA")
    assert r.cvss_vector.startswith("CVSS:3.1/")
    assert r.cwe_ids == ["CWE-502", "CWE-400"]
    assert r.cwe_id == "CWE-502"
    assert r.cwe_description == "CWE-502 Deserialization of Untrusted Data"
    assert (r.ssvc_exploitation, r.ssvc_automatable, r.ssvc_technical_impact) == ("active", "yes", "total")
    a = r.affected_products[0]
    assert (a.vendor, a.product, a.defaultStatus, a.platforms) == \
        ("Apache Software Foundation", "Apache Log4j2", "unaffected", ["Java"])
    assert [(v.version, v.lessThan, v.lessThanOrEqual) for v in a.versions] == \
        [("2.0-beta9", "2.3.1", None), ("2.4", None, "2.12.1")]
    assert r.references[0].tags == ["vendor-advisory"]


def test_transform_adp_fallback_and_non_en_description(spark_session: SparkSession):
    r = transform.transform_cves(_raw_df(spark_session, "cvss31_adp_ssvc.json")).first()
    assert (r.cvss_version, r.cvss_base_score, r.cvss_severity, r.cvss_source) == ("3.1", 7.5, "HIGH", "ADP")
    assert r.description == "Only en-US description"
    assert r.cwe_ids == [] and r.cwe_id is None
    assert r.ssvc_exploitation is None
    assert r.references[0].url == "https://example.com/a" and r.references[0].tags is None


def test_transform_rejected(spark_session: SparkSession):
    r = transform.transform_cves(_raw_df(spark_session, "rejected.json")).first()
    assert r.state == "REJECTED"
    assert r.affected_products is None and r.cvss_version is None and r.cvss_source is None
    assert r.cwe_ids == []


def test_transform_columns(spark_session: SparkSession):
    cols = transform.transform_cves(_raw_df(spark_session, "rejected.json")).columns
    assert cols == ["cve_id", "cve_year", "state", "date_published", "date_reserved", "date_updated",
                    "assigner_org_id", "assigner_short_name", "title", "description", "affected_products",
                    "cvss_version", "cvss_base_score", "cvss_severity", "cvss_vector", "cvss_source",
                    "cwe_ids", "cwe_id", "cwe_description", "ssvc_exploitation", "ssvc_automatable",
                    "ssvc_technical_impact", "references", "raw"]
