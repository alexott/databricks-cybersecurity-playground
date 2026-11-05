import datetime
import gzip
import pathlib

import pytest
from pyspark.sql import SparkSession

from epss import helpers, transform

_DATA = pathlib.Path(__file__).resolve().parent.parent / "data" / "epss"
D = datetime.date


def test_score_url_and_filename():
    assert helpers.score_filename(D(2026, 11, 4)) == "epss_scores-2026-11-04.csv.gz"
    assert helpers.score_url("https://epss.example.com", D(2021, 4, 14)) == \
        "https://epss.example.com/epss_scores-2021-04-14.csv.gz"


def test_dates_to_load_skips_loaded_and_clamps_to_first_date():
    today = D(2026, 11, 4)
    loaded = {D(2026, 11, 2), D(2026, 11, 3)}
    assert helpers.dates_to_load(today, 3, loaded) == [D(2026, 11, 1), D(2026, 11, 4)]
    assert helpers.dates_to_load(D(2021, 4, 15), 3000, set()) == [D(2021, 4, 14), D(2021, 4, 15)]
    assert helpers.dates_to_load(today, 0, set()) == [today]


def test_batches():
    assert helpers.batches([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert helpers.batches([], 2) == []


@pytest.mark.parametrize("name, model_version, columns", [
    ("epss_scores-2021-04-14.csv.gz", None, ["cve", "epss"]),
    ("epss_scores-2022-02-03.csv.gz", None, ["cve", "epss", "percentile"]),
    ("epss_scores-2023-03-07.csv.gz", "v2023.03.01", ["cve", "epss", "percentile"]),
    ("epss_scores-2026-11-04.csv.gz", "v2026.06.15", ["cve", "epss", "percentile"]),
])
def test_parse_header(name, model_version, columns):
    assert helpers.parse_header((_DATA / name).read_bytes()) == (model_version, columns)


def test_parse_header_rejects_unknown_columns():
    with pytest.raises(ValueError, match="unexpected EPSS columns"):
        helpers.parse_header(gzip.compress(b"cve,score\nCVE-1,0.1\n"))


def test_to_history_all_formats(spark_session: SparkSession):
    model_versions = {D(2021, 4, 14): None, D(2022, 2, 3): None,
                      D(2023, 3, 7): "v2023.03.01", D(2026, 11, 4): "v2026.06.15"}
    df = transform.to_history(transform.read_scores(spark_session, str(_DATA)), model_versions)
    assert df.columns == ["cve_id", "score_date", "epss", "percentile", "model_version"]
    rows = {(r.cve_id, str(r.score_date)): r for r in df.collect()}
    assert len(rows) == 9
    r = rows[("CVE-2020-5902", "2021-04-14")]
    assert (r.epss, r.percentile, r.model_version) == (0.65117, None, None)
    r = rows[("CVE-2021-4034", "2022-02-03")]
    assert (r.epss, r.percentile, r.model_version) == (0.04225, 0.7411899992581052, None)
    # multi-member gzip (comment line in its own member), exponent notation
    r = rows[("CVE-2023-0001", "2023-03-07")]
    assert (r.epss, r.percentile, r.model_version) == (0.00043, 0.06968, "v2023.03.01")
    r = rows[("CVE-2021-44228", "2026-11-04")]
    assert (r.epss, r.percentile, r.model_version) == (0.99999, 1.0, "v2026.06.15")


def test_to_history_without_model_versions(spark_session: SparkSession):
    df = transform.to_history(
        transform.read_scores(spark_session, str(_DATA / "epss_scores-2021-04-14.csv.gz")),
        {D(2021, 4, 14): None})
    assert [r.model_version for r in df.collect()] == [None, None]
