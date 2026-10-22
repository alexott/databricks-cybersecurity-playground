import pathlib

from pyspark.sql import SparkSession

from kev import helpers, transform

_DATA = pathlib.Path(__file__).resolve().parent.parent / "data" / "kev"


def _raw_df(spark, filename, catalog_version="2026.10.04",
            date_released="2026-10-04T18:52:56Z", date_removed=None):
    """Build the raw-shaped DataFrame the loader stages: the per-vulnerability
    object as a VARIANT plus the catalog-level columns stamped at load time."""
    text = (_DATA / filename).read_text()
    return (spark.createDataFrame(
                [(text, catalog_version, date_released, date_removed)],
                "json_text string, catalog_version string, "
                "date_released string, date_removed string")
            .selectExpr(
                "try_parse_json(json_text) as raw",
                "catalog_version",
                "cast(date_released as timestamp) as date_released",
                "cast(date_removed as timestamp) as date_removed"))


def test_transform_ransomware_known(spark_session: SparkSession):
    df = transform.transform_kev(_raw_df(spark_session, "ransomware_known.json"))
    row = df.first()
    assert row["cve_id"] == "CVE-2026-59310"
    assert row["vendor_project"] == "Broadcom"
    assert row["product"] == "VMware vCenter"
    assert row["known_ransomware_campaign_use"] == "Known"
    assert row["forensic_triage"] == "Yes"
    assert row["cwes"] == ["CWE-22"]
    assert str(row["date_added"]) == "2026-08-18"
    assert str(row["due_date"]) == "2026-08-21"
    assert row["catalog_version"] == "2026.10.04"
    assert row["date_released"] is not None
    assert row["date_removed"] is None


def test_transform_empty_cwes(spark_session: SparkSession):
    df = transform.transform_kev(_raw_df(spark_session, "no_cwes.json"))
    row = df.first()
    assert row["cve_id"] == "CVE-2026-53362"
    assert row["known_ransomware_campaign_use"] == "Unknown"
    assert row["cwes"] == []


def test_transform_carries_date_removed(spark_session: SparkSession):
    df = transform.transform_kev(
        _raw_df(spark_session, "plain.json", date_removed="2026-10-05T00:00:00Z"))
    row = df.first()
    assert row["cve_id"] == "CVE-2026-88779"
    assert row["date_removed"] is not None


def test_transform_keeps_raw(spark_session: SparkSession):
    df = transform.transform_kev(_raw_df(spark_session, "plain.json"))
    assert "raw" in df.columns
    assert df.first()["raw"] is not None


def test_extract_raw_data_ids(spark_session: SparkSession):
    src = _raw_df(spark_session, "ransomware_known.json").drop("date_removed")
    df = helpers.extract_raw_data(src)
    row = df.first()
    assert row["cve_id"] == "CVE-2026-59310"
    assert str(row["date_added"]) == "2026-08-18"
    assert row["catalog_version"] == "2026.10.04"
    assert row["date_released"] is not None
