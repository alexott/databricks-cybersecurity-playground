import datetime
import json
import pathlib
import zipfile

from pyspark.sql import SparkSession

from osv import helpers, transform

_DATA = pathlib.Path(__file__).resolve().parent.parent / "data" / "osv"
UTC = datetime.timezone.utc


def test_parse_osv_ts():
    assert helpers.parse_osv_ts("2026-10-05T06:30:02.388571428Z") == \
        datetime.datetime(2026, 10, 5, 6, 30, 2, 388571, tzinfo=UTC)
    assert helpers.parse_osv_ts("2019-05-07T03:38:45Z") == datetime.datetime(2019, 5, 7, 3, 38, 45, tzinfo=UTC)
    assert helpers.parse_osv_ts("2026-10-02T11:38:35.372Z") == \
        datetime.datetime(2026, 10, 2, 11, 38, 35, 372000, tzinfo=UTC)


_CSV = [
    "2026-10-05T06:15:06.355219882Z,Root/ROOT-OS-DEBIAN-13-CVE-2026-8927\n",
    "2026-10-05T06:10:00Z,PyPI/PYSEC-2026-1\n",
    "2026-10-05T06:05:00.5Z,GIT/CVE-2026-1\n",
    "2026-10-05T06:04:00Z,PyPI/PYSEC-2026-1\n",   # older duplicate of an id already seen
    "2026-10-05T06:00:00Z,[EMPTY]/BELL-CVE-2026-1\n",
    "2026-10-05T05:00:00Z,npm/GHSA-old\n",
]


def test_changed_since_stops_at_watermark_and_dedups():
    since = datetime.datetime(2026, 10, 5, 6, 0, 0, tzinfo=UTC)
    changes, newest, exceeded = helpers.changed_since(_CSV, since, limit=100)
    assert not exceeded
    assert newest == datetime.datetime(2026, 10, 5, 6, 15, 6, 355219, tzinfo=UTC)
    # boundary is inclusive; GHSA-old is older than the watermark
    assert set(changes) == {"ROOT-OS-DEBIAN-13-CVE-2026-8927", "PYSEC-2026-1", "CVE-2026-1", "BELL-CVE-2026-1"}
    assert changes["PYSEC-2026-1"] == "PyPI/PYSEC-2026-1"
    assert changes["BELL-CVE-2026-1"] == "[EMPTY]/BELL-CVE-2026-1"


def test_changed_since_exceeded_and_empty():
    since = datetime.datetime(2026, 1, 1, tzinfo=UTC)
    changes, _, exceeded = helpers.changed_since(_CSV, since, limit=2)
    assert exceeded and len(changes) > 2
    changes, newest, exceeded = helpers.changed_since([], since, limit=2)
    assert changes == {} and newest is None and not exceeded


def test_record_url_and_pinned_url():
    base = "https://storage.googleapis.com/osv-vulnerabilities"
    assert helpers.record_url(base, "[EMPTY]/BELL-CVE-2026-1") == f"{base}/%5BEMPTY%5D/BELL-CVE-2026-1.json"
    assert helpers.record_url(base, "Red Hat/RHSA-2024:0001") == f"{base}/Red%20Hat/RHSA-2024%3A0001.json"
    assert helpers.pinned_url(f"{base}/all.zip", "123") == f"{base}/all.zip?generation=123"


def test_iter_zip_records_single_line(tmp_path):
    zpath = tmp_path / "all.zip"
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("A-1.json", '{"id": "A-1", "details": "a\\nb"}')
        z.writestr("B-2.json", json.dumps({"id": "B-2"}, indent=2))   # pretty-printed
        z.writestr("README.txt", "not a record")
    with zipfile.ZipFile(zpath) as z:
        recs = list(helpers.iter_zip_records(z))
    assert len(recs) == 2
    assert all(b"\n" not in r for r in recs)
    assert [json.loads(r)["id"] for r in recs] == ["A-1", "B-2"]
    assert json.loads(recs[0])["details"] == "a\nb"


# --- Spark transform tests (pytest-spark session) ---------------------------


def _text_df(spark, *filenames):
    lines = [json.dumps(json.loads((_DATA / f).read_text())) for f in filenames]
    return spark.createDataFrame([(x,) for x in lines], "value string")


def _row(spark, filename):
    raw = transform.extract_raw_data(_text_df(spark, filename))
    return transform.transform_vulnerabilities(raw).first()


def test_extract_raw_data_ids(spark_session: SparkSession):
    df = transform.extract_raw_data(_text_df(
        spark_session, "git_cve.json", "ubuntu_upstream.json", "pypi_aliases.json", "withdrawn_empty.json"))
    rows = {r["id"]: r for r in df.collect()}
    assert rows["CVE-2026-104470"]["cve_id"] == "CVE-2026-104470"           # the id itself is a CVE
    assert rows["UBUNTU-CVE-2026-94603"]["cve_id"] == "CVE-2026-94603"      # from upstream
    assert rows["PYSEC-2026-4112"]["cve_id"] == "CVE-2026-102831"           # from aliases
    assert rows["BELL-CVE-2026-98163"]["date_withdrawn"] is not None
    # format in Spark (session tz = UTC): collect() would convert to the local tz
    mod = df.filter("id = 'PYSEC-2026-4112'").selectExpr(
        "date_format(date_modified, 'yyyy-MM-dd HH:mm:ss.SSSSSS')").first()[0]
    assert mod == "2026-10-05 06:30:02.388571"


def test_extract_raw_data_unparsable(spark_session: SparkSession):
    df = spark_session.createDataFrame([("not json",)], "value string")
    row = transform.extract_raw_data(df).first()
    assert row["raw"] is None and row["id"] is None


def test_git_repo_range(spark_session: SparkSession):
    row = _row(spark_session, "git_cve.json")
    assert row["id_prefix"] == "CVE"
    assert row["cwe_ids"] == ["CWE-918"]
    assert row["cvss_version"] == "CVSS_V4" and row["cvss_vector"].startswith("CVSS:4.0/")
    pkgs = row["affected_packages"]
    assert len(pkgs) == 1
    assert pkgs[0]["package"] is None
    assert pkgs[0]["repo"] == "https://github.com/yeswiki/yeswiki"
    assert pkgs[0]["range_type"] == "GIT" and pkgs[0]["introduced"] == "0"
    assert row["ecosystems"] == []


def test_distro_ecosystem_and_non_cvss_severity(spark_session: SparkSession):
    row = _row(spark_session, "ubuntu_upstream.json")
    assert row["id_prefix"] == "UBUNTU"
    assert row["upstream"] == ["CVE-2026-94603"]
    assert row["ecosystems"] == ["Ubuntu"]
    assert row["affected_packages"][0]["ecosystem"] == "Ubuntu:26.04:LTS"
    assert row["affected_packages"][0]["package"] == "podman"
    assert row["severity"][0]["type"] == "Ubuntu" and row["severity"][0]["score"] == "medium"
    assert row["cvss_version"] is None and row["cvss_vector"] is None


def test_pypi_package_range(spark_session: SparkSession):
    row = _row(spark_session, "pypi_aliases.json")
    assert row["id_prefix"] == "PYSEC"
    assert row["ecosystems"] == ["PyPI"]
    p = row["affected_packages"][0]
    assert (p["package"], p["introduced"], p["fixed"]) == ("notebook", "7.5.0", "7.6.3")
    assert row["cvss_version"] == "CVSS_V3"


def test_withdrawn_no_affected(spark_session: SparkSession):
    row = _row(spark_session, "withdrawn_empty.json")
    assert row["date_withdrawn"] is not None
    assert row["affected_packages"] == [] and row["ecosystems"] == []
    assert row["schema_version"] == "1.9.0"


def test_newest_cvss_wins(spark_session: SparkSession):
    rec = {"id": "X-1", "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/v3"},
                                     {"type": "CVSS_V4", "score": "CVSS:4.0/v4"},
                                     {"type": "CVSS_V2", "score": "v2"}]}
    raw = transform.extract_raw_data(spark_session.createDataFrame([(json.dumps(rec),)], "value string"))
    row = transform.transform_vulnerabilities(raw).first()
    assert (row["cvss_version"], row["cvss_vector"]) == ("CVSS_V4", "CVSS:4.0/v4")


def test_transform_keeps_extra_columns(spark_session: SparkSession):
    raw = transform.extract_raw_data(_text_df(spark_session, "pypi_aliases.json"))
    df = transform.transform_vulnerabilities(raw.selectExpr("*", "'delete' as _change_type"), keep=["_change_type"])
    assert df.first()["_change_type"] == "delete"
