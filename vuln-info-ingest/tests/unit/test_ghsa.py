import pathlib

from pyspark.sql import SparkSession

from ghsa import helpers, transform


def test_first_cve_alias():
    assert helpers.first_cve_alias(["GHSA-xxxx-yyyy-zzzz", "CVE-2021-44228"]) == "CVE-2021-44228"
    assert helpers.first_cve_alias(["cve-2020-0001"]) == "cve-2020-0001"
    assert helpers.first_cve_alias(["GHSA-only"]) is None
    assert helpers.first_cve_alias([]) is None
    assert helpers.first_cve_alias(None) is None


def test_github_reviewed_from_path():
    assert helpers.github_reviewed_from_path(
        "advisory-database-main/advisories/github-reviewed/2021/12/GHSA-x/GHSA-x.json") is True
    assert helpers.github_reviewed_from_path(
        "advisory-database-main/advisories/unreviewed/2024/01/GHSA-y/GHSA-y.json") is False


# --- Spark transform tests (pytest-spark session) ---------------------------
_DATA = pathlib.Path(__file__).resolve().parent.parent / "data" / "ghsa"


def _raw_df(spark, filename, github_reviewed=True):
    text = (_DATA / filename).read_text()
    return (spark.createDataFrame([(text, github_reviewed)], "json_text string, github_reviewed boolean")
            .selectExpr("try_parse_json(json_text) as raw", "github_reviewed"))


def test_log4shell_affected_packages(spark_session: SparkSession):
    df = transform.transform_advisories(_raw_df(spark_session, "log4shell.json"))
    row = df.first()
    assert row["ghsa_id"] == "GHSA-jfh8-c2jp-5v3q"
    assert row["cve_id"] == "CVE-2021-44228"
    assert row["severity_label"] == "CRITICAL"
    assert row["github_reviewed"] is True
    pkgs = row["affected_packages"]
    assert len(pkgs) == 10
    assert all(p["ecosystem"] == "Maven" for p in pkgs)
    core = [p for p in pkgs if p["package"] == "org.apache.logging.log4j:log4j-core"]
    assert core and core[0]["introduced"] == "2.13.0" and core[0]["fixed"] == "2.15.0"


def test_versions_only_keeps_package(spark_session: SparkSession):
    df = transform.transform_advisories(_raw_df(spark_session, "versions_only.json"))
    pkgs = df.first()["affected_packages"]
    assert len(pkgs) == 1
    assert pkgs[0]["ecosystem"] == "npm" and pkgs[0]["package"] == "demo"
    assert pkgs[0]["introduced"] is None and pkgs[0]["fixed"] is None


def test_no_affected_empty_array(spark_session: SparkSession):
    df = transform.transform_advisories(_raw_df(spark_session, "unreviewed_no_ecosystem.json", github_reviewed=False))
    row = df.first()
    assert row["affected_packages"] == []
    assert row["ecosystems"] == []
    assert row["cve_id"] == "CVE-2024-0001"
    assert row["github_reviewed"] is False


def test_withdrawn_advisory(spark_session: SparkSession):
    df = transform.transform_advisories(_raw_df(spark_session, "withdrawn.json"))
    row = df.first()
    assert row["date_withdrawn"] is not None
    pkgs = row["affected_packages"]
    assert len(pkgs) == 1 and pkgs[0]["ecosystem"] == "PyPI"
    assert pkgs[0]["introduced"] == "0" and pkgs[0]["fixed"] == "2.0.0"


def test_extract_raw_data_ids(spark_session: SparkSession):
    df = helpers.extract_raw_data(_raw_df(spark_session, "log4shell.json").drop("github_reviewed"))
    row = df.first()
    assert row["ghsa_id"] == "GHSA-jfh8-c2jp-5v3q"
    assert row["cve_id"] == "CVE-2021-44228"
    assert row["date_modified"] is not None
