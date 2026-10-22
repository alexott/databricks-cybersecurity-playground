import datetime
import gzip
import io
import urllib.error

import pytest
from pyspark.sql import SparkSession

from common import fetch_log, http, merge, params, staging


class FakeWidgets:
    def __init__(self, values):
        self.values = values
        self.created = []

    def text(self, name, default):
        self.created.append(name)

    def get(self, name):
        return self.values.get(name, "")


class FakeDbutils:
    def __init__(self, values):
        self.widgets = FakeWidgets(values)


def test_get_params_converts_types():
    dbu = FakeDbutils({"catalog": "c", "schema": "s", "volume": "v", "threads": " 8 ", "ratio": "0.9"})
    p = params.get_params(dbu, {**params.COMMON_PARAMS, "threads": int, "ratio": float})
    assert p == {"catalog": "c", "schema": "s", "volume": "v", "threads": 8, "ratio": 0.9}
    assert dbu.widgets.created == ["catalog", "schema", "volume", "threads", "ratio"]


def test_get_params_missing_names_parameter():
    dbu = FakeDbutils({"catalog": "c", "schema": "  "})
    with pytest.raises(ValueError, match="schema"):
        params.get_params(dbu, params.COMMON_PARAMS)


def test_get_params_bad_number_names_parameter():
    dbu = FakeDbutils({"threads": "many"})
    with pytest.raises(ValueError, match="threads"):
        params.get_params(dbu, {"threads": int})


def test_table_name_quotes_identifiers():
    p = {"catalog": "cyber-sec", "schema": "vu`ln", "volume": "ref"}
    assert params.table_name(p, "osv_raw") == "`cyber-sec`.`vu``ln`.`osv_raw`"
    assert params.volume_name(p) == "`cyber-sec`.`vu``ln`.`ref`"


def test_volume_path():
    p = {"catalog": "c", "schema": "s", "volume": "reference"}
    assert params.volume_path(p, "osv_checkpoints") == "/Volumes/c/s/reference/osv_checkpoints"


def test_to_single_line():
    assert staging.to_single_line(b'{"a":1}') == b'{"a":1}'
    assert staging.to_single_line(b'{\n  "a": 1\n}') == b'{"a":1}'


def test_write_text_parts_and_clear(tmp_path):
    n, parts = staging.write_text_parts([b'{"i":%d}' % i for i in range(5)], str(tmp_path), 2)
    assert (n, parts) == (5, 3)
    with gzip.open(tmp_path / "part-00002.jsonl.gz") as f:
        assert f.read() == b'{"i":4}\n'
    (tmp_path / "keep.txt").write_text("x")
    staging.clear_parts(str(tmp_path))
    assert sorted(p.name for p in tmp_path.iterdir()) == ["keep.txt"]


def test_write_text_parts_empty(tmp_path):
    assert staging.write_text_parts([], str(tmp_path / "out"), 2) == (0, 0)


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _fake_urlopen(outcomes):
    calls = []

    def urlopen(url, timeout=None):
        calls.append(url)
        o = outcomes.pop(0)
        if isinstance(o, Exception):
            raise o
        return _Resp(o)
    return urlopen, calls


def _http_error(code):
    return urllib.error.HTTPError("u", code, "x", {}, None)


def test_fetch_with_retries_retries_transient(monkeypatch):
    urlopen, calls = _fake_urlopen([_http_error(503), urllib.error.URLError("dns"), b" data \n"])
    monkeypatch.setattr(http.urllib.request, "urlopen", urlopen)
    assert http.fetch_with_retries("u", sleep=lambda s: None) == b"data"
    assert len(calls) == 3


def test_fetch_with_retries_404_is_none(monkeypatch):
    urlopen, _ = _fake_urlopen([_http_error(404)])
    monkeypatch.setattr(http.urllib.request, "urlopen", urlopen)
    assert http.fetch_with_retries("u", sleep=lambda s: None) is None


def test_fetch_with_retries_raises_other_and_exhausted(monkeypatch):
    urlopen, _ = _fake_urlopen([_http_error(403)])
    monkeypatch.setattr(http.urllib.request, "urlopen", urlopen)
    with pytest.raises(urllib.error.HTTPError):
        http.fetch_with_retries("u", sleep=lambda s: None)
    urlopen, _ = _fake_urlopen([_http_error(500)] * 4)
    monkeypatch.setattr(http.urllib.request, "urlopen", urlopen)
    with pytest.raises(urllib.error.HTTPError):
        http.fetch_with_retries("u", sleep=lambda s: None)


def test_dedupe_latest(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [("a", 1, "old"), ("a", 3, "new"), ("a", None, "null"), ("b", None, "only")],
        "id string, m int, v string")
    rows = sorted(merge.dedupe_latest(df, "id", "m").collect())
    assert [(r.id, r.v) for r in rows] == [("a", "new"), ("b", "only")]
    assert merge.dedupe_latest(df, "id", "m").columns == ["id", "m", "v"]


_CDF = [("a", "insert", 1, "a1"), ("a", "update_preimage", 2, "a1"), ("a", "update_postimage", 2, "a2"),
        ("b", "insert", 1, "b1"), ("b", "delete", 3, "b1")]
_CDF_SCHEMA = "id string, _change_type string, _commit_version long, v string"


def test_latest_changes_latest_commit_wins(spark_session: SparkSession):
    df = spark_session.createDataFrame(_CDF, _CDF_SCHEMA)
    out = merge.latest_changes(df, "id", apply_deletes=False)
    assert sorted((r.id, r._change_type, r.v) for r in out.collect()) == \
        [("a", "update_postimage", "a2"), ("b", "insert", "b1")]
    assert "_commit_version" not in out.columns


def test_latest_changes_with_deletes(spark_session: SparkSession):
    df = spark_session.createDataFrame(_CDF, _CDF_SCHEMA)
    out = merge.latest_changes(df, "id", apply_deletes=True)
    assert sorted((r.id, r._change_type) for r in out.collect()) == \
        [("a", "update_postimage"), ("b", "delete")]


def test_read_watermark(spark_session: SparkSession):
    assert fetch_log.read_watermark(spark_session, "no_such_fetch_log") is None
    spark_session.createDataFrame(
        # aware inputs: createDataFrame reads naive datetimes in the machine's local time zone
        [(datetime.datetime(2026, 10, 1, 6, 0, 0, 123456, tzinfo=datetime.timezone.utc),),
         (datetime.datetime(2026, 10, 2, tzinfo=datetime.timezone.utc),)],
        "watermark timestamp").createOrReplaceTempView("fetch_log_t")
    assert fetch_log.read_watermark(spark_session, "fetch_log_t") == \
        datetime.datetime(2026, 10, 2, tzinfo=datetime.timezone.utc)
    spark_session.createDataFrame([(None,)], "watermark timestamp").createOrReplaceTempView("fetch_log_empty")
    assert fetch_log.read_watermark(spark_session, "fetch_log_empty") is None
