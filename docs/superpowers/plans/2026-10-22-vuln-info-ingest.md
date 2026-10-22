# vuln-info-ingest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One DAB-deployed project `vuln-info-ingest/` that ingests CVE v5, GHSA, OSV and CISA KEV data into Unity Catalog with periodic (default 6h) and initial-load serverless jobs.

**Architecture:** Per-source Python packages under `src/` (pure `helpers.py` / Spark `transform.py` + Databricks notebooks), a shared `src/common/` package (parameters, staging, HTTP, dedupe, fetch log, generic CDF→silver merge), and one job YAML per source. All settings arrive as job parameters read through widgets; table and Volume names are derived from `catalog`/`schema`/`volume`.

**Tech Stack:** Databricks Asset Bundles (CLI ≥ 0.250), serverless notebook tasks (`environment_version = "6"`), PySpark 4 VARIANT (`try_variant_get`), Delta MERGE + Change Data Feed, pytest + pytest-spark (poetry).

**Spec:** `docs/superpowers/specs/2026-10-22-vuln-info-ingest-design.md`

## Global Constraints

- **Never write the name or path of the private repo the original loaders come from** into any file, comment, test or commit message of this repo. Refer to "the original loaders". The controller gives you its local path; in this plan it is `$ORIG` (the directory containing `CVEv5Load/`, `GHSALoad/`, `OSVLoad/`, `KEVLoad/`; its sibling `../tests/` holds the original tests). The controller also gives you `$FORBIDDEN`, an extended regex of internal names (the private repo, internal catalogs/workspaces and internal skill names) that must never appear in this repo; the originals contain some of them.
- No `config.py` files. Every setting is a job parameter; notebooks read them with `common.params.get_params`; no in-code defaults for parameters.
- Default schedule `0 0 */6 * * ?` (Quartz), timezone `UTC`.
- Table names (unchanged from the originals): `cves_v5_raw`, `cves_v5`, `cves_v5_fetch_log`, `ghsa_raw`, `github_advisories`, `ghsa_fetch_log`, `osv_raw`, `osv_vulnerabilities`, `osv_fetch_log`, `kev_raw`, `kev_vulnerabilities`, `kev_fetch_log`.
- Staging path `/Volumes/{catalog}/{schema}/{volume}/<source>`; checkpoints `/Volumes/{catalog}/{schema}/{volume}/<source>_checkpoints/silver`. `<source>` ∈ `cve_v5`, `ghsa`, `osv`, `kev`.
- Every notebook starts with:
  ```
  # Databricks notebook source
  # /// script
  # [tool.databricks.environment]
  # environment_version = "6"
  # ///
  ```
- `helpers.py`, `transform.py` and `common/*.py` have no module-level pyspark/delta import and never touch `dbutils`.
- Local VARIANT access must use `try_variant_get` (OSS Spark has no `raw:path::TYPE` syntax).
- Python ≥ 3.11 (uses `datetime.UTC`); serverless env 6 is Python 3.12.
- Commits end with the line `Co-authored-by: Isaac <no-reply@databricks.com>`.
- Do not deploy or run bundle jobs; `bundle validate` only, with a profile the user chooses.

## Review Focus

1. Catalog/schema names that need quoting (e.g. `cyber-sec`) — expected: tables are addressed as `` `cat`.`sch`.`tbl` `` and work. Test: `test_table_name_quotes_identifiers` (Task 1).
2. Missing, empty or non-numeric job parameter — expected: the task fails immediately with a message naming the parameter. Tests: `test_get_params_*` (Task 1).
3. Silver job lagging so one micro-batch holds several raw commits for the same key — expected: latest change wins, no "multiple source rows matched" MERGE error. Test: `test_latest_changes_*` (Task 2).
4. CVE incremental with no watermark, or a watermark older than the deltaLog window — expected: `needs_full_load=true`, not a silent gap. Tests: `test_plan_incremental_*` (Task 6).
5. One record download failing transiently or 404 during incremental — expected: retries on 429/5xx, 404 skipped, other errors raised. Tests: `test_fetch_with_retries_*` (Task 2).

---

## File Structure

```
vuln-info-ingest/
  databricks.yml                         bundle, targets               (Task 8)
  resources/variables.yml                bundle variables              (Task 8)
  resources/vuln_*.job.yml               6 jobs                        (Task 8)
  pyproject.toml  pytest.ini             test tooling                  (Task 1)
  README.md                              docs                          (Task 9)
  src/common/__init__.py                                               (Task 1)
  src/common/params.py                   get_params, table_name, volume_path, COMMON_PARAMS (Task 1)
  src/common/staging.py                  to_single_line, write_text_parts, clear_parts (Task 2)
  src/common/http.py                     fetch_with_retries            (Task 2)
  src/common/merge.py                    dedupe_latest, latest_changes (Task 2)
  src/common/fetch_log.py                append_fetch_log, read_watermark (Task 2)
  src/common/silver.py                   stream_cdf_to_silver          (Task 2)
  src/common/Create volume.py            notebook                      (Task 8)
  src/kev/   __init__.py helpers.py transform.py "Load KEV.py" "KEV Silver Transform.py" kev_vulnerabilities_comments.sql (Task 3)
  src/ghsa/  __init__.py helpers.py transform.py "Load advisories.py" "GHSA Silver Transform.py" github_advisories_comments.sql (Task 4)
  src/osv/   __init__.py helpers.py transform.py "Full load.py" "Incremental load.py" "OSV Silver Transform.py" (Task 5)
  src/cve_v5/__init__.py helpers.py transform.py (Task 6); "Full load.py" "Incremental load.py" "CVE v5 Silver Transform.py" cves_v5_comments.sql (Task 7)
  tests/conftest.py                                                    (Task 1)
  tests/unit/test_common.py test_kev.py test_ghsa.py test_osv.py test_cve_v5.py
  tests/data/{kev,ghsa,osv,cve_v5}/*.json
```

All paths below are relative to `vuln-info-ingest/` unless they start with `$ORIG` or `docs/`.

---

### Task 1: Project scaffold and `common.params`

**Files:**
- Create: `pyproject.toml`, `pytest.ini`, `tests/conftest.py`, `tests/unit/__init__.py` (empty), `src/common/__init__.py` (empty), `src/common/params.py`
- Test: `tests/unit/test_common.py`

**Interfaces:**
- Produces:
  - `COMMON_PARAMS: dict[str, type] = {"catalog": str, "schema": str, "volume": str}`
  - `get_params(dbutils, spec: dict[str, type]) -> dict[str, object]`
  - `table_name(p: dict, name: str) -> str` → `` `cat`.`sch`.`name` ``
  - `volume_path(p: dict, sub: str) -> str` → `/Volumes/{catalog}/{schema}/{volume}/{sub}`
  - `volume_name(p: dict) -> str` → `` `cat`.`sch`.`vol` ``

- [ ] **Step 1: Create tooling files**

`pyproject.toml`:
```toml
[tool.poetry]
name = "vuln-info-ingest"
version = "0.1.0"
description = "Ingestion of vulnerability data (CVE v5, GHSA, OSV, CISA KEV) into Databricks."
authors = ["Alex Ott <alexott@gmail.com>"]
package-mode = false

[tool.poetry.dependencies]
python = ">=3.11,<3.14"

[tool.poetry.group.test.dependencies]
pytest = "^9.0.3"
pytest-spark = "^0.8.0"
pyspark = ">=4.0.1"
```

`pytest.ini`:
```ini
[pytest]
addopts = -p no:warnings
testpaths = tests
spark_options =
    spark.sql.catalogImplementation: in-memory
    spark.sql.session.timeZone: UTC
    spark.sql.shuffle.partitions: 1
```

`tests/conftest.py`:
```python
import pathlib
import sys

# Source packages (common, cve_v5, ghsa, osv, kev) live under src/, as on Databricks.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))
```

Then: `cd vuln-info-ingest && poetry env use "$(uv python find 3.12)" && poetry install --with test`

- [ ] **Step 2: Write the failing tests** — `tests/unit/test_common.py`:

```python
import pytest

from common import params


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
```

- [ ] **Step 3: Run to verify failure**

Run: `poetry run pytest tests/unit/test_common.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'common.params'` (or ImportError).

- [ ] **Step 4: Implement** — `src/common/params.py`:

```python
"""Job parameters: read every setting from notebook widgets (job parameters are
pushed into them) and derive table / Volume names. No defaults in code: the job
YAML is the single place where defaults live."""
from __future__ import annotations

COMMON_PARAMS: dict[str, type] = {"catalog": str, "schema": str, "volume": str}


def get_params(dbutils, spec: dict[str, type]) -> dict[str, object]:
    """Read the parameters in `spec` (name -> str/int/float), converting types.
    Raises ValueError naming the parameter when it is missing, empty or malformed."""
    out: dict[str, object] = {}
    for name, typ in spec.items():
        # Creating the widget lets the notebook also run interactively.
        dbutils.widgets.text(name, "")
        value = dbutils.widgets.get(name).strip()
        if not value:
            raise ValueError(f"missing required job parameter: {name}")
        try:
            out[name] = typ(value)
        except ValueError as e:
            raise ValueError(f"job parameter {name}={value!r} is not a valid {typ.__name__}") from e
    return out


def _quote(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"


def table_name(p: dict, name: str) -> str:
    """Fully qualified, quoted name of a table in the configured catalog/schema."""
    return ".".join(_quote(x) for x in (p["catalog"], p["schema"], name))


def volume_name(p: dict) -> str:
    """Fully qualified, quoted name of the scratch Volume."""
    return table_name(p, p["volume"])


def volume_path(p: dict, sub: str) -> str:
    """Path of a sub-directory on the scratch Volume."""
    return f"/Volumes/{p['catalog']}/{p['schema']}/{p['volume']}/{sub}"
```

- [ ] **Step 5: Run to verify pass**

Run: `poetry run pytest tests/unit/test_common.py -v` — Expected: 5 passed.

- [ ] **Step 6: Commit**

Add `vuln-info-ingest/.gitignore` with `.venv/`, `__pycache__/`, `.pytest_cache/`, `.databricks/`, then:
```bash
git add vuln-info-ingest
git commit -m "vuln-info-ingest: project scaffold and job parameter helpers" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 2: Shared helpers — staging, HTTP, dedupe, fetch log, silver merge

**Files:**
- Create: `src/common/staging.py`, `src/common/http.py`, `src/common/merge.py`, `src/common/fetch_log.py`, `src/common/silver.py`
- Test: `tests/unit/test_common.py` (append)

**Interfaces:**
- Produces:
  - `staging.to_single_line(data: bytes) -> bytes`
  - `staging.write_text_parts(records: Iterable[bytes], out_dir: str, batch: int) -> tuple[int, int]` (writes `part-00000.jsonl.gz`…; returns (records, parts))
  - `staging.clear_parts(out_dir: str) -> None` (removes `part-*.jsonl.gz` only)
  - `http.fetch_with_retries(url: str, attempts: int = 4, timeout: int = 60, sleep=time.sleep) -> bytes | None` (None on 404)
  - `merge.dedupe_latest(df, key: str, order_col: str)` → one row per key, highest `order_col` (nulls last)
  - `merge.latest_changes(df, key: str, apply_deletes: bool)` → CDF rows filtered to `insert`/`update_postimage` (+`delete` if `apply_deletes`), one row per key with highest `_commit_version`, `_commit_version`/`_commit_timestamp` dropped, `_change_type` kept
  - `fetch_log.append_fetch_log(spark, table: str, row: tuple, schema: str) -> None`
  - `fetch_log.read_watermark(spark, table: str) -> datetime.datetime | None` (UTC-aware, max of `watermark` column)
  - `silver.stream_cdf_to_silver(spark, raw_table, silver_table, key, transform_fn, checkpoint, apply_deletes=False) -> None`

- [ ] **Step 1: Write the failing tests** — add the imports below to the import block at the top of `tests/unit/test_common.py` and append the tests at the end:

```python
import datetime
import gzip
import io
import urllib.error

from pyspark.sql import SparkSession

from common import fetch_log, http, merge, staging


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
        [(datetime.datetime(2026, 10, 1, 6, 0, 0, 123456),), (datetime.datetime(2026, 10, 2),)],
        "watermark timestamp").createOrReplaceTempView("fetch_log_t")
    assert fetch_log.read_watermark(spark_session, "fetch_log_t") == \
        datetime.datetime(2026, 10, 2, tzinfo=datetime.timezone.utc)
    spark_session.createDataFrame([(None,)], "watermark timestamp").createOrReplaceTempView("fetch_log_empty")
    assert fetch_log.read_watermark(spark_session, "fetch_log_empty") is None
```

Note: the session time zone is UTC (pytest.ini), so the naive datetimes above are UTC.

- [ ] **Step 2: Run to verify failure**

Run: `poetry run pytest tests/unit/test_common.py -v` — Expected: FAIL with `ImportError: cannot import name 'fetch_log' from 'common'`.

- [ ] **Step 3: Implement**

`src/common/staging.py`:
```python
"""Staging of JSON records as gzip JSON-line parts on a Volume (pure Python)."""
from __future__ import annotations

import glob
import gzip
import json
import os
from typing import Iterable


def to_single_line(data: bytes) -> bytes:
    """Compact JSON is passed through; JSON with newlines is re-serialized onto one line."""
    if b"\n" in data or b"\r" in data:
        return json.dumps(json.loads(data), separators=(",", ":")).encode("utf-8")
    return data


def write_text_parts(records: Iterable[bytes], out_dir: str, batch: int) -> tuple[int, int]:
    """Write JSON lines into gzip parts (part-00000.jsonl.gz, ...) of `batch` records.
    Returns (records written, parts written)."""
    os.makedirs(out_dir, exist_ok=True)
    n, parts, out = 0, 0, None
    for rec in records:
        if n % batch == 0:
            if out:
                out.close()
            # compresslevel=1: full snapshots are many GB of JSON; speed matters more than size
            out = gzip.open(f"{out_dir}/part-{parts:05d}.jsonl.gz", "wb", compresslevel=1)
            parts += 1
        out.write(rec)
        out.write(b"\n")
        n += 1
    if out:
        out.close()
    return n, parts


def clear_parts(out_dir: str) -> None:
    """Remove only our own staging parts, never other files in the directory."""
    for f in glob.glob(f"{out_dir}/part-*.jsonl.gz"):
        os.remove(f)
```

`src/common/http.py`:
```python
"""HTTP download with retries for single-record fetches (pure Python)."""
from __future__ import annotations

import time
import urllib.error
import urllib.request

_RETRY_CODES = (429, 500, 502, 503, 504)


def fetch_with_retries(url: str, attempts: int = 4, timeout: int = 60, sleep=time.sleep) -> bytes | None:
    """Body of `url` (stripped), or None on 404 (record deleted after it was listed).
    Retries 429/5xx and connection errors with exponential backoff; other errors raise."""
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as r:
                return r.read().strip()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code not in _RETRY_CODES or attempt == attempts - 1:
                raise
        except urllib.error.URLError:
            if attempt == attempts - 1:
                raise
        sleep(2 ** attempt)
    return None
```

`src/common/merge.py`:
```python
"""Row-selection helpers used before Delta MERGE (a MERGE fails when several source
rows match one target row). pyspark is imported lazily."""
from __future__ import annotations


def dedupe_latest(df, key: str, order_col: str):
    """One row per `key`: the one with the highest `order_col` (nulls last)."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    w = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def latest_changes(df, key: str, apply_deletes: bool):
    """Change Data Feed rows -> the last change per `key` (highest _commit_version).
    Keeps inserts and update post-images, plus deletes when `apply_deletes`.
    `_change_type` is kept; the other CDF metadata columns are dropped."""
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    types = ["insert", "update_postimage"] + (["delete"] if apply_deletes else [])
    w = Window.partitionBy(key).orderBy(F.col("_commit_version").desc())
    return (df.filter(F.col("_change_type").isin(types))
            .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
            .drop("_rn", "_commit_version", "_commit_timestamp"))
```

`src/common/fetch_log.py`:
```python
"""Per-run fetch-log tables: one appended row per load run; the max `watermark`
column is the resume point for incremental loads."""
from __future__ import annotations

import datetime

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def append_fetch_log(spark, table: str, row: tuple, schema: str) -> None:
    spark.createDataFrame([row], schema).write.mode("append").saveAsTable(table)


def read_watermark(spark, table: str) -> datetime.datetime | None:
    """Max `watermark` of the fetch log as a UTC datetime, None if no table/rows.
    Read as epoch micros: collect() of a TIMESTAMP gives a naive datetime in the
    driver's local time zone, which could shift the watermark."""
    if not spark.catalog.tableExists(table):
        return None
    us = spark.table(table).selectExpr("unix_micros(max(watermark))").first()[0]
    return None if us is None else _EPOCH + datetime.timedelta(microseconds=us)
```

`src/common/silver.py`:
```python
"""Generic raw -> silver sync: stream the raw table's Change Data Feed and MERGE
each micro-batch, transformed, into the silver table on `key`."""
from __future__ import annotations

from typing import Callable

from common.merge import latest_changes


def stream_cdf_to_silver(spark, raw_table: str, silver_table: str, key: str,
                         transform_fn: Callable, checkpoint: str, apply_deletes: bool = False) -> None:
    """If silver is empty, MERGE the full raw snapshot first (CDF history may not
    cover the whole table) and start the stream at the latest raw version; then
    process all pending changes (availableNow) and return.

    `transform_fn(df)` gets raw rows plus `_change_type`. Without `apply_deletes` it
    must return exactly the silver columns. With `apply_deletes` it must also keep
    `_change_type`; a last change of type 'delete' removes the silver row."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    def merge(df):
        src = transform_fn(latest_changes(df, key, apply_deletes)).alias("s")
        m = DeltaTable.forName(spark, silver_table).alias("t").merge(src, f"t.{key} = s.{key}")
        if apply_deletes:
            values = {c: f"s.{c}" for c in spark.table(silver_table).columns}
            m = (m.whenMatchedDelete(condition="s._change_type = 'delete'")
                 .whenMatchedUpdate(set=values)
                 .whenNotMatchedInsert(condition="s._change_type != 'delete'", values=values))
        else:
            m = m.whenMatchedUpdateAll().whenNotMatchedInsertAll()
        m.execute()

    def merge_batch(batch_df, batch_id):
        if not batch_df.isEmpty():
            merge(batch_df)

    stream = spark.readStream.option("readChangeFeed", "true")
    if spark.table(silver_table).isEmpty():
        print("Initial load: processing full raw snapshot...")
        merge(spark.table(raw_table)
              .withColumn("_change_type", F.lit("insert"))
              .withColumn("_commit_version", F.lit(0).cast("long")))
        print(f"Initial load complete: {spark.table(silver_table).count()} rows")
        version = spark.sql(f"DESCRIBE HISTORY {raw_table} LIMIT 1").select("version").first()[0]
        print(f"Starting streaming from version {version}...")
        stream = stream.option("startingVersion", version)

    (stream.table(raw_table)
     .writeStream
     .foreachBatch(merge_batch)
     .option("checkpointLocation", checkpoint)
     .trigger(availableNow=True)
     .start()
     .awaitTermination())
    print("Streaming merge complete")
```

- [ ] **Step 4: Run to verify pass**

Run: `poetry run pytest tests/unit/test_common.py -v` — Expected: all pass. Also `poetry run python -c "import sys; sys.path.insert(0,'src'); import common.silver"` succeeds (lazy delta import).

- [ ] **Step 5: Commit**

```bash
git add vuln-info-ingest
git commit -m "vuln-info-ingest: shared staging, HTTP, dedupe, fetch-log and silver helpers" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 3: KEV source

**Files:**
- Create: `src/kev/__init__.py` (empty), `src/kev/helpers.py`, `src/kev/transform.py`, `src/kev/kev_vulnerabilities_comments.sql`, `src/kev/Load KEV.py`, `src/kev/KEV Silver Transform.py`
- Test: `tests/unit/test_kev.py`, `tests/data/kev/*.json`

**Interfaces:**
- Consumes: `common.params.{COMMON_PARAMS,get_params,table_name,volume_path}`, `common.merge.dedupe_latest`, `common.fetch_log.append_fetch_log`, `common.silver.stream_cdf_to_silver`
- Produces: `kev.helpers.extract_raw_data(df)`, `kev.transform.transform_kev(df)` (unchanged signatures)

- [ ] **Step 1: Copy code, data and tests**

```bash
cp $ORIG/KEVLoad/helpers.py $ORIG/KEVLoad/transform.py $ORIG/KEVLoad/kev_vulnerabilities_comments.sql src/kev/
cp -r $ORIG/../tests/data/kev tests/data/kev
cp $ORIG/../tests/unit/test_kev.py tests/unit/test_kev.py
```
In the `src/kev/transform.py` docstring replace the parenthetical `(see the ... skill, 'Silver: Complex Type Extraction')` with `(complex types are passed as the type hint directly)`. Check `grep -rniE "$FORBIDDEN" src/kev tests/unit/test_kev.py tests/data/kev` returns nothing.

- [ ] **Step 2: Repoint the test imports**

In `tests/unit/test_kev.py` replace everything from the first line through `transform = _load("transform")` (the `importlib`/`_DIR`/`_load` block and the module loads) with:
```python
import pathlib

from pyspark.sql import SparkSession

from kev import helpers, transform
```
Keep the `_DATA = ...` line (it already points at `tests/data/kev`) and everything after it unchanged.

- [ ] **Step 3: Run tests**

Run: `poetry run pytest tests/unit/test_kev.py -v` — Expected: all pass (same count as the original file).

- [ ] **Step 4: Write `src/kev/Load KEV.py`**

```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # KEV loader — fetch the CISA Known Exploited Vulnerabilities catalog, upsert kev_raw
# MAGIC Downloads the single ~1.8 MB catalog JSON to ephemeral /tmp, stages one JSON
# MAGIC line per vulnerability on a scratch Volume, then does a full-snapshot MERGE
# MAGIC into `kev_raw` on `cve_id`. KEV carries no per-record modified timestamp, so
# MAGIC the catalog version is the freshness token: matched rows refresh only when the
# MAGIC catalog version advanced (or a previously removed cveID reappears). CISA
# MAGIC removals are rare data-quality corrections (a CVE added in error or later
# MAGIC REJECTED), so we soft-delete — cveIDs absent from the snapshot keep their row
# MAGIC with `date_removed` set rather than being physically deleted.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "catalog_url": str})
CATALOG_URL = p["catalog_url"]
VOLUME_PATH = volume_path(p, "kev")
RAW_TABLE = table_name(p, "kev_raw")
FETCH_LOG_TABLE = table_name(p, "kev_fetch_log")

# COMMAND ----------

# DBTITLE 1,Fetch the catalog JSON to /tmp
import glob, json, urllib.request, datetime
from kev.helpers import extract_raw_data
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log
from delta.tables import DeltaTable
from pyspark.sql import functions as F

local = "/tmp/kev.json"
urllib.request.urlretrieve(CATALOG_URL, local)
with open(local, "r", encoding="utf-8") as f:
    catalog = json.load(f)
catalog_version = catalog["catalogVersion"]
date_released = catalog["dateReleased"]
vulns = catalog["vulnerabilities"]
print(f"catalog {catalog_version} released {date_released}: {len(vulns)} vulnerabilities")
```
Then copy the original cells **"Stage one JSON line per vulnerability on the Volume"**, **"Create kev_raw if needed (CDF on) and upsert on cve_id with soft-delete"** verbatim from `$ORIG/KEVLoad/Load KEV.py` (each preceded by `# COMMAND ----------` and a blank line), and replace the two remaining original cells with:
```python
# COMMAND ----------

# DBTITLE 1,Read staged JSONL, parse raw VARIANT, stamp catalog columns
staged = spark.read.json(VOLUME_PATH)   # column: raw(string)
src = (staged.selectExpr("try_parse_json(raw) as raw")
       .withColumn("catalog_version", F.lit(catalog_version))
       .withColumn("date_released", F.lit(date_released).cast("timestamp")))
src = extract_raw_data(src).filter("cve_id IS NOT NULL")
# One row per cve_id so the MERGE never sees multiple source rows for one target
# (Delta errors on that). KEV ids are unique per catalog, so this is a guard.
src = (dedupe_latest(src, "cve_id", "date_added")
       .withColumn("date_removed", F.lit(None).cast("timestamp")))

# COMMAND ----------

# DBTITLE 1,Fetch-log row
n = spark.table(RAW_TABLE).count()
n_active = spark.table(RAW_TABLE).filter("date_removed IS NULL").count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), catalog_version,
     datetime.datetime.fromisoformat(date_released.replace("Z", "+00:00")),
     len(vulns), n, n_active),
    "fetch_time timestamp, catalog_version string, date_released timestamp, "
    "n_vulnerabilities int, n_rows_after int, n_active_after int")
print(f"kev_raw rows: {n} ({n_active} active, {n - n_active} removed)")
```
Cell order: Parameters, Fetch, Stage, Read staged, Create+MERGE, Fetch-log.

- [ ] **Step 5: Write `src/kev/KEV Silver Transform.py`**

```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path
from common.silver import stream_cdf_to_silver
from kev.transform import transform_kev

p = get_params(dbutils, COMMON_PARAMS)
RAW_TABLE = table_name(p, "kev_raw")
SILVER_TABLE = table_name(p, "kev_vulnerabilities")
# Checkpoint lives outside the loader's staging directory, so the per-run staging
# cleanup never destroys the stream's offsets.
CHECKPOINT = volume_path(p, "kev_checkpoints") + "/silver"
```
Then the original cell **"Create silver table (inline comments mirrored in kev_vulnerabilities_comments.sql)"** copied verbatim from `$ORIG/KEVLoad/KEV Silver Transform.py`, then:
```python
# COMMAND ----------

# DBTITLE 1,Stream raw CDF into silver table
stream_cdf_to_silver(spark, RAW_TABLE, SILVER_TABLE, "cve_id", transform_kev, CHECKPOINT)
```

- [ ] **Step 6: Check notebooks compile**

Run: `poetry run python -m py_compile "src/kev/Load KEV.py" "src/kev/KEV Silver Transform.py"` — Expected: no output.

- [ ] **Step 7: Commit**

```bash
git add vuln-info-ingest
git commit -m "vuln-info-ingest: KEV loader and silver transform" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 4: GHSA source

**Files:**
- Create: `src/ghsa/__init__.py` (empty), `src/ghsa/helpers.py`, `src/ghsa/transform.py`, `src/ghsa/github_advisories_comments.sql`, `src/ghsa/Load advisories.py`, `src/ghsa/GHSA Silver Transform.py`
- Test: `tests/unit/test_ghsa.py`, `tests/data/ghsa/*.json`

**Interfaces:**
- Consumes: same `common` functions as Task 3.
- Produces: `ghsa.helpers.{first_cve_alias, github_reviewed_from_path, extract_raw_data}`, `ghsa.transform.transform_advisories(df)` (unchanged)

- [ ] **Step 1: Copy code, data and tests**

```bash
cp $ORIG/GHSALoad/helpers.py $ORIG/GHSALoad/transform.py $ORIG/GHSALoad/github_advisories_comments.sql src/ghsa/
cp -r $ORIG/../tests/data/ghsa tests/data/ghsa
cp $ORIG/../tests/unit/test_ghsa.py tests/unit/test_ghsa.py
```
In the `src/ghsa/transform.py` docstring replace the parenthetical `(see local ... skill, 'Silver: Complex Type Extraction')` with `(complex types are passed as the type hint directly)`. In `src/ghsa/helpers.py` docstring drop the trailing ` (Task 2)`. `grep -rniE "$FORBIDDEN" src/ghsa tests/unit/test_ghsa.py tests/data/ghsa` must return nothing.

- [ ] **Step 2: Repoint test imports** — in `tests/unit/test_ghsa.py` replace the `importlib`/`_DIR`/`_load` block and both `helpers = _load(...)` / `transform = _load(...)` lines with a single header:
```python
import pathlib

from pyspark.sql import SparkSession

from ghsa import helpers, transform
```
Remove the now-duplicate later `from pyspark.sql import SparkSession  # noqa: E402` line. Keep `_DATA` and all tests.

- [ ] **Step 3: Run tests** — `poetry run pytest tests/unit/test_ghsa.py -v` — Expected: all pass.

- [ ] **Step 4: Write `src/ghsa/Load advisories.py`**

```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # GHSA loader — fetch GitHub Advisory Database, upsert ghsa_raw
# MAGIC Full tarball fetch to ephemeral /tmp (no persistent clone, no git pull),
# MAGIC consolidated-JSONL staging on a scratch Volume, idempotent MERGE into
# MAGIC `ghsa_raw` on `ghsa_id` gated by `date_modified`.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "tarball_url": str, "part_records": int})
TARBALL_URL = p["tarball_url"]
BATCH = p["part_records"]
VOLUME_PATH = volume_path(p, "ghsa")
RAW_TABLE = table_name(p, "ghsa_raw")
FETCH_LOG_TABLE = table_name(p, "ghsa_fetch_log")

# COMMAND ----------

# DBTITLE 1,Fetch tarball to /tmp and extract
import glob, json, shutil, tarfile, urllib.request, datetime
from ghsa.helpers import github_reviewed_from_path
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log
from delta.tables import DeltaTable
from pyspark.sql import functions as F

WORK = "/tmp/ghsa_src"
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK, exist_ok=True)
tgz = "/tmp/ghsa.tar.gz"
urllib.request.urlretrieve(TARBALL_URL, tgz)
with tarfile.open(tgz, "r:gz") as t:
    t.extractall(WORK)
print("extracted tarball")
```
Then copy the original cell **"Consolidate advisory JSON into batched JSONL on the Volume"** verbatim **except delete its `BATCH = 20000` line** (now a parameter). Then:
```python
# COMMAND ----------

# DBTITLE 1,Read staged JSONL, parse raw VARIANT, extract id columns
staged = spark.read.json(VOLUME_PATH)   # columns: github_reviewed, raw(string)
src = (staged.selectExpr("github_reviewed", "try_parse_json(raw) as raw")
       .withColumn("ghsa_id", F.expr("try_variant_get(raw, '$.id', 'string')"))
       .withColumn("cve_id", F.expr(
           "get(filter(try_variant_get(raw,'$.aliases','ARRAY<STRING>'), x -> upper(x) like 'CVE-%'), 0)"))
       .withColumn("date_published", F.expr("try_variant_get(raw, '$.published', 'timestamp')"))
       .withColumn("date_modified", F.expr("try_variant_get(raw, '$.modified', 'timestamp')"))
       .filter("ghsa_id IS NOT NULL"))
# One row per ghsa_id (keep the newest) so the MERGE never sees multiple source
# rows for one target. GHSA ids are unique per repo file, so this is a guard.
src = dedupe_latest(src, "ghsa_id", "date_modified")
```
Then the original cell **"Create ghsa_raw if needed (CDF on) and upsert on ghsa_id gated by modified"** verbatim, then:
```python
# COMMAND ----------

# DBTITLE 1,Fetch-log row
n = spark.table(RAW_TABLE).count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), "refs/heads/main", len(paths), n),
    "fetch_time timestamp, source_ref string, n_advisories int, n_rows_after int")
print(f"ghsa_raw rows: {n}")
```

- [ ] **Step 5: Write `src/ghsa/GHSA Silver Transform.py`** — same as Task 3 Step 5 with these values:
```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path
from common.silver import stream_cdf_to_silver
from ghsa.transform import transform_advisories

p = get_params(dbutils, COMMON_PARAMS)
RAW_TABLE = table_name(p, "ghsa_raw")
SILVER_TABLE = table_name(p, "github_advisories")
# Checkpoint lives outside the loader's staging directory, so the per-run staging
# cleanup never destroys the stream's offsets.
CHECKPOINT = volume_path(p, "ghsa_checkpoints") + "/silver"
```
Then the original **"Create silver table (inline comments mirrored in github_advisories_comments.sql)"** cell verbatim, then:
```python
# COMMAND ----------

# DBTITLE 1,Stream raw CDF into silver table
stream_cdf_to_silver(spark, RAW_TABLE, SILVER_TABLE, "ghsa_id", transform_advisories, CHECKPOINT)
```

- [ ] **Step 6: Compile check** — `poetry run python -m py_compile "src/ghsa/Load advisories.py" "src/ghsa/GHSA Silver Transform.py"`

- [ ] **Step 7: Commit**

```bash
git add vuln-info-ingest
git commit -m "vuln-info-ingest: GHSA loader and silver transform" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 5: OSV source

**Files:**
- Create: `src/osv/__init__.py` (empty), `src/osv/helpers.py`, `src/osv/transform.py`, `src/osv/Full load.py`, `src/osv/Incremental load.py`, `src/osv/OSV Silver Transform.py`
- Test: `tests/unit/test_osv.py`, `tests/data/osv/*.json`

**Interfaces:**
- Consumes: `common.staging.{to_single_line, write_text_parts, clear_parts}`, `common.http.fetch_with_retries`, `common.fetch_log.{append_fetch_log, read_watermark}`, `common.merge.dedupe_latest`, `common.silver.stream_cdf_to_silver`
- Produces: `osv.helpers.{parse_osv_ts, changed_since, record_url, pinned_url, iter_zip_records}`, `osv.transform.{extract_raw_data, transform_vulnerabilities(df, keep=...)}`; task value `needs_full_load` (`"true"`/`"false"`) set by `Incremental load`.

- [ ] **Step 1: Copy code, data and tests**

```bash
cp $ORIG/OSVLoad/helpers.py $ORIG/OSVLoad/transform.py src/osv/
cp -r $ORIG/../tests/data/osv tests/data/osv
cp $ORIG/../tests/unit/test_osv.py tests/unit/test_osv.py
```

- [ ] **Step 2: Move staging helpers out of `osv/helpers.py`**

In `src/osv/helpers.py`: delete the functions `to_single_line`, `write_text_parts`, `clear_parts`; delete the now-unused imports `glob`, `gzip`, `json`, `os`; add `from common.staging import to_single_line` after the stdlib imports (used by `iter_zip_records`). Change `Iterable, Iterator` import to keep only what is still used. Update the module docstring's test reference to `tests/unit/test_osv.py` (already correct) and remove any mention of the private repo. `grep -rniE "$FORBIDDEN" src/osv tests/unit/test_osv.py tests/data/osv` must return nothing (fix docstrings in `transform.py` the same way as Task 3 if it matches).

- [ ] **Step 3: Repoint tests**

In `tests/unit/test_osv.py`: replace the `importlib`/`_DIR`/`_load` block and the `helpers = _load("helpers")` / `transform = _load("transform")` lines with `from osv import helpers, transform` (placed after the stdlib imports; keep `_DATA` and `UTC`). Remove the later duplicate `from pyspark.sql import SparkSession  # noqa: E402` and put `from pyspark.sql import SparkSession` in the header. Delete `test_write_text_parts` and `test_clear_parts` (covered in `test_common.py`). If `test_iter_zip_records_single_line` or another remaining test calls `helpers.to_single_line`, keep it — `osv.helpers` still exposes it via the import.

- [ ] **Step 4: Run tests** — `poetry run pytest tests/unit/test_osv.py -v` — Expected: all remaining tests pass.

- [ ] **Step 5: Write `src/osv/Full load.py`**

```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # OSV full load — pinned all.zip snapshot, upsert osv_raw
# MAGIC Downloads one pinned version (GCS `generation`) of
# MAGIC `gs://osv-vulnerabilities/all.zip` (~2.6 GB, ~2M records, one file per id,
# MAGIC all ecosystems) to the scratch Volume, writes every record as one JSON line into
# MAGIC gzip parts, then `MERGE`s into `osv_raw` on `id` gated by `modified`. Ids that
# MAGIC are no longer in the snapshot are deleted. Used for the initial load, and as the
# MAGIC fallback when `Incremental load` sees too many changes.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "bucket_url": str, "part_records": int,
                         "min_snapshot_ratio": float})
ALL_ZIP_URL = f"{p['bucket_url']}/all.zip"
VOLUME_PATH = volume_path(p, "osv")
FULL_STAGING = f"{VOLUME_PATH}/full"
RAW_TABLE = table_name(p, "osv_raw")
FETCH_LOG_TABLE = table_name(p, "osv_fetch_log")
PART_RECORDS = p["part_records"]
MIN_SNAPSHOT_RATIO = p["min_snapshot_ratio"]

# COMMAND ----------

# DBTITLE 1,Resolve and download the pinned all.zip
import shutil, zipfile, urllib.request, datetime
from email.utils import parsedate_to_datetime
from osv.helpers import pinned_url, iter_zip_records
from osv.transform import extract_raw_data
from common.staging import write_text_parts, clear_parts
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log
from delta.tables import DeltaTable

# all.zip is rebuilt every ~15 minutes: pin one generation so the whole download
# comes from the same object.
with urllib.request.urlopen(urllib.request.Request(ALL_ZIP_URL, method="HEAD")) as r:
    generation = r.headers["x-goog-generation"]
    size = int(r.headers["Content-Length"])
    # Everything modified before the snapshot was built is in it: this is the
    # watermark for the next incremental run.
    snapshot_time = parsedate_to_datetime(r.headers["Last-Modified"])
print(f"all.zip generation={generation} size={size} built={snapshot_time}")

os.makedirs(VOLUME_PATH, exist_ok=True)
zip_path = f"{VOLUME_PATH}/all.zip"
with urllib.request.urlopen(pinned_url(ALL_ZIP_URL, generation)) as r, open(zip_path, "wb") as out:
    shutil.copyfileobj(r, out, length=16 * 1024 * 1024)
assert os.path.getsize(zip_path) == size, "incomplete all.zip download"
print("downloaded")
```
Then copy the original cell **"Write records as gzip JSON-line parts on the Volume"** verbatim. Then:
```python
# COMMAND ----------

# DBTITLE 1,Read parts, parse raw VARIANT, extract id columns
parsed = extract_raw_data(spark.read.text(f"{FULL_STAGING}/part-*.jsonl.gz"))
# One pass for both counts (no .cache() on serverless).
n_unparsed, n_src = parsed.selectExpr("count_if(id IS NULL)", "count(DISTINCT id)").first()
if n_unparsed:
    print(f"WARNING: {n_unparsed} records did not parse and are skipped")
# One row per id (keep the newest) so the MERGE never sees several source rows for
# one target. all.zip has one file per id, so this is a defensive guard.
src = dedupe_latest(parsed.filter("id IS NOT NULL"), "id", "date_modified")
```
Then the original cell **"Create osv_raw if needed (CDF on) and merge the snapshot"** verbatim, then:
```python
# COMMAND ----------

# DBTITLE 1,Fetch-log row
n_after = spark.table(RAW_TABLE).count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), "full", generation, snapshot_time, n_records, n_unparsed, n_after),
    "fetch_time timestamp, mode string, source_generation string, watermark timestamp, "
    "n_records long, n_unparsed long, n_rows_after long")
print(f"osv_raw rows: {n_before} -> {n_after}")
```

- [ ] **Step 6: Write `src/osv/Incremental load.py`**

```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # OSV incremental load — modified_id.csv since the watermark, upsert osv_raw
# MAGIC Streams `gs://osv-vulnerabilities/modified_id.csv` (newest first) and stops at
# MAGIC the stored watermark minus a small overlap. Changed records are fetched one by
# MAGIC one (`<ecosystem>/<id>.json`) and `MERGE`d into `osv_raw` gated by `modified`.
# MAGIC With more than `full_reload_threshold` changes it sets the task value
# MAGIC `needs_full_load=true` so the job runs `Full load` next. Needs one `Full load`
# MAGIC run first (initial-load job). Idempotent and safe to schedule.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "bucket_url": str, "part_records": int,
                         "full_reload_threshold": int, "watermark_overlap_hours": float,
                         "fetch_threads": int})
BUCKET_URL = p["bucket_url"]
MODIFIED_CSV_URL = f"{BUCKET_URL}/modified_id.csv"
INCREMENTAL_STAGING = volume_path(p, "osv") + "/incremental"
RAW_TABLE = table_name(p, "osv_raw")
FETCH_LOG_TABLE = table_name(p, "osv_fetch_log")
PART_RECORDS = p["part_records"]
FULL_RELOAD_THRESHOLD = p["full_reload_threshold"]
FETCH_THREADS = p["fetch_threads"]

import datetime
# Re-read changes this far before the stored watermark: the exporter writes
# continuously, so a record can show up in modified_id.csv a bit late. The MERGE is
# gated by `modified`, so re-reading is idempotent.
WATERMARK_OVERLAP = datetime.timedelta(hours=p["watermark_overlap_hours"])
dbutils.jobs.taskValues.set(key="needs_full_load", value="false")

# COMMAND ----------

# DBTITLE 1,Find records changed since the watermark
import io, urllib.request
from concurrent.futures import ThreadPoolExecutor
from osv.helpers import changed_since, record_url, to_single_line
from osv.transform import extract_raw_data
from common.staging import write_text_parts, clear_parts
from common.http import fetch_with_retries
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log, read_watermark
from delta.tables import DeltaTable

watermark = read_watermark(spark, FETCH_LOG_TABLE)
if watermark is None:
    raise RuntimeError(f"no watermark in {FETCH_LOG_TABLE}: run the OSV initial-load job first")
since = watermark - WATERMARK_OVERLAP
print(f"watermark={watermark}, reading changes since {since}")

# The CSV is ~130 MB but sorted newest first: read it as a stream and stop early.
with urllib.request.urlopen(MODIFIED_CSV_URL) as r:
    changes, newest, exceeded = changed_since(
        io.TextIOWrapper(r, encoding="utf-8"), since, FULL_RELOAD_THRESHOLD)
print(f"{len(changes)}{'+' if exceeded else ''} changed records, newest modified={newest}")

# COMMAND ----------

# DBTITLE 1,Too many changes -> full load; no changes -> done
if exceeded:
    print(f"more than {FULL_RELOAD_THRESHOLD} changes: the job runs Full load next")
    dbutils.jobs.taskValues.set(key="needs_full_load", value="true")
    dbutils.notebook.exit("full load")
if not changes:
    dbutils.notebook.exit("no changes")

# COMMAND ----------

# DBTITLE 1,Fetch changed records and write gzip JSON-line parts
def fetch(path: str) -> bytes | None:
    """One record as a JSON line; None if it is gone (deleted after the CSV was written)."""
    data = fetch_with_retries(record_url(BUCKET_URL, path))
    return None if data is None else to_single_line(data)


clear_parts(INCREMENTAL_STAGING)
with ThreadPoolExecutor(FETCH_THREADS) as pool:
    # Stream results into the parts instead of keeping up to 100K records in memory.
    records = (x for x in pool.map(fetch, changes.values()) if x is not None)
    n_records, n_parts = write_text_parts(records, INCREMENTAL_STAGING, PART_RECORDS)
print(f"wrote {n_records} records into {n_parts} parts, {len(changes) - n_records} not found")

# COMMAND ----------

# DBTITLE 1,Parse and merge into osv_raw (no deletes: withdrawn records stay, flagged)
n_unparsed = 0
if n_records:
    parsed = extract_raw_data(spark.read.text(f"{INCREMENTAL_STAGING}/part-*.jsonl.gz"))
    n_unparsed = parsed.filter("id IS NULL").count()
    if n_unparsed:
        print(f"WARNING: {n_unparsed} records did not parse and are skipped")
    src = dedupe_latest(parsed.filter("id IS NOT NULL"), "id", "date_modified")
    (DeltaTable.forName(spark, RAW_TABLE).alias("t")
     .merge(src.alias("s"), "t.id = s.id")
     .whenMatchedUpdateAll(condition="t.date_modified IS NULL OR s.date_modified > t.date_modified")
     .whenNotMatchedInsertAll()
     .execute())

# COMMAND ----------

# DBTITLE 1,Fetch-log row
# The CSV can lag behind the all.zip snapshot, so never move the watermark back.
new_watermark = max(watermark, newest) if newest else watermark
n_after = spark.table(RAW_TABLE).count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), "incremental", None, new_watermark, n_records, n_unparsed, n_after),
    "fetch_time timestamp, mode string, source_generation string, watermark timestamp, "
    "n_records long, n_unparsed long, n_rows_after long")
print(f"osv_raw rows: {n_after}, watermark -> {new_watermark}")
```

- [ ] **Step 7: Write `src/osv/OSV Silver Transform.py`**

```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path
from common.silver import stream_cdf_to_silver
from osv.transform import transform_vulnerabilities

p = get_params(dbutils, COMMON_PARAMS)
RAW_TABLE = table_name(p, "osv_raw")
SILVER_TABLE = table_name(p, "osv_vulnerabilities")
# Checkpoint lives outside the loaders' staging directory, so the staging cleanup
# never destroys the stream's offsets.
CHECKPOINT = volume_path(p, "osv_checkpoints") + "/silver"
```
Then the original **"Create silver table with column comments"** cell verbatim, then:
```python
# COMMAND ----------

# DBTITLE 1,Stream raw CDF into silver table (upserts and deletes)
# The full load deletes ids gone from the snapshot; those deletes propagate to silver.
stream_cdf_to_silver(
    spark, RAW_TABLE, SILVER_TABLE, "id",
    lambda df: transform_vulnerabilities(df, keep=["_change_type"]),
    CHECKPOINT, apply_deletes=True)
```

- [ ] **Step 8: Compile check and full test run**

Run: `poetry run python -m py_compile src/osv/*.py && poetry run pytest -q` — Expected: all pass.

- [ ] **Step 9: Commit**

```bash
git add vuln-info-ingest
git commit -m "vuln-info-ingest: OSV full/incremental loaders and silver transform" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 6: CVE v5 helpers and transform (rework)

**Files:**
- Create: `src/cve_v5/__init__.py` (empty), `src/cve_v5/helpers.py`, `src/cve_v5/transform.py`
- Test: `tests/unit/test_cve_v5.py`, `tests/data/cve_v5/log4shell.json`, `tests/data/cve_v5/rejected.json`, `tests/data/cve_v5/cvss31_adp_ssvc.json`

**Interfaces:**
- Consumes: `common.staging.to_single_line`
- Produces:
  - `cve_v5.helpers.parse_ts(s: str) -> datetime.datetime` (UTC-aware)
  - `cve_v5.helpers.raw_github_url(link: str) -> str`
  - `cve_v5.helpers.plan_incremental(delta_log: list[dict], watermark: datetime | None, threshold: int) -> tuple[dict[str, str], datetime | None, bool]` → (`{cve_id: github_link}`, newest fetchTime processed, needs_full_load)
  - `cve_v5.helpers.newest_fetch_time(delta_log: list[dict]) -> datetime | None`
  - `cve_v5.helpers.iter_tar_records(fileobj) -> Iterator[bytes]`
  - `cve_v5.helpers.extract_raw_data(df)` → columns `raw, data_type, data_version, cve_id, cve_year, date_published, date_updated` (same as the original)
  - `cve_v5.transform.transform_cves(df)` → silver columns in the order of the silver DDL

- [ ] **Step 1: Create fixtures** (trimmed, structurally real CVE JSON 5.1 records; hand-trimmed, values chosen for the tests)

`tests/data/cve_v5/log4shell.json` — CNA with CVSS 3.1 inside `metrics[0]`, English + German description, two CWEs across two problemTypes, CISA-ADP at `adp[1]` with SSVC, affected with extra fields (`collectionURL`, `packageName`):
```json
{"dataType":"CVE_RECORD","dataVersion":"5.1",
 "cveMetadata":{"cveId":"CVE-2021-44228","assignerOrgId":"f0158376-9dc2-43b6-827c-5f631a4d8d09","assignerShortName":"apache","state":"PUBLISHED","dateReserved":"2021-11-26T00:00:00.000Z","datePublished":"2021-12-10T00:00:00.000Z","dateUpdated":"2025-02-04T14:25:34.416Z"},
 "containers":{
  "cna":{"title":"Apache Log4j2 JNDI features do not protect against attacker controlled LDAP and other JNDI related endpoints",
   "descriptions":[{"lang":"de","value":"Deutsch"},{"lang":"en","value":"Apache Log4j2 2.0-beta9 through 2.15.0 JNDI features ..."}],
   "affected":[{"vendor":"Apache Software Foundation","product":"Apache Log4j2","collectionURL":"https://repo.maven.apache.org/maven2","packageName":"org.apache.logging.log4j:log4j-core","defaultStatus":"unaffected","platforms":["Java"],
     "versions":[{"version":"2.0-beta9","status":"affected","lessThan":"2.3.1","versionType":"maven"},{"version":"2.4","status":"affected","lessThanOrEqual":"2.12.1","versionType":"maven"}]}],
   "metrics":[{"cvssV3_1":{"version":"3.1","baseScore":10.0,"baseSeverity":"CRITICAL","vectorString":"CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"}}],
   "problemTypes":[{"descriptions":[{"type":"CWE","cweId":"CWE-502","description":"CWE-502 Deserialization of Untrusted Data","lang":"en"}]},
                   {"descriptions":[{"type":"CWE","cweId":"CWE-400","description":"CWE-400 Uncontrolled Resource Consumption","lang":"en"},{"type":"text","description":"no id","lang":"en"}]}],
   "references":[{"url":"https://logging.apache.org/log4j/2.x/security.html","name":"vendor","tags":["vendor-advisory"]}]},
  "adp":[{"providerMetadata":{"orgId":"af854a3a-2127-422b-91ae-364da2661108","shortName":"CVE"},"title":"CVE Program Container"},
         {"providerMetadata":{"orgId":"134c704f-9b21-4f2e-91b3-4a467353bcc0","shortName":"CISA-ADP"},
          "metrics":[{"other":{"type":"ssvc","content":{"options":[{"Exploitation":"active"},{"Automatable":"yes"},{"Technical Impact":"total"}]}}}]}]}}
```

`tests/data/cve_v5/cvss31_adp_ssvc.json` — CNA has no metrics; CISA-ADP at `adp[0]` provides CVSS 3.1 in `metrics[0]` (the SSVC fields are therefore absent in `metrics[0].other`):
```json
{"dataType":"CVE_RECORD","dataVersion":"5.1",
 "cveMetadata":{"cveId":"CVE-2024-0001","assignerOrgId":"00000000-0000-0000-0000-000000000001","assignerShortName":"example","state":"PUBLISHED","dateReserved":"2024-01-01T00:00:00Z","datePublished":"2024-01-02T00:00:00Z","dateUpdated":"2024-02-01T00:00:00Z"},
 "containers":{
  "cna":{"descriptions":[{"lang":"en-US","value":"Only en-US description"}],
   "affected":[{"vendor":"Example","product":"Widget","defaultStatus":"unknown","versions":[{"version":"1.0","status":"affected"}]}],
   "problemTypes":[{"descriptions":[{"type":"text","description":"n/a","lang":"en"}]}],
   "references":[{"url":"https://example.com/a"}]},
  "adp":[{"providerMetadata":{"orgId":"134c704f-9b21-4f2e-91b3-4a467353bcc0","shortName":"CISA-ADP"},
          "metrics":[{"cvssV3_1":{"version":"3.1","baseScore":7.5,"baseSeverity":"HIGH","vectorString":"CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:N/A:N"}}]}]}}
```

`tests/data/cve_v5/rejected.json`:
```json
{"dataType":"CVE_RECORD","dataVersion":"5.1",
 "cveMetadata":{"cveId":"CVE-2023-99999","assignerOrgId":"00000000-0000-0000-0000-000000000002","assignerShortName":"mitre","state":"REJECTED","dateReserved":"2023-01-01T00:00:00Z","dateRejected":"2023-06-01T00:00:00Z","dateUpdated":"2023-06-01T00:00:00Z"},
 "containers":{"cna":{"rejectedReasons":[{"lang":"en","value":"DO NOT USE THIS CANDIDATE NUMBER."}]}}}
```

- [ ] **Step 2: Write the failing tests** — `tests/unit/test_cve_v5.py`:

```python
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


def test_raw_github_url():
    assert helpers.raw_github_url(
        "https://github.com/CVEProject/cvelistV5/blob/main/cves/2024/1xxx/CVE-2024-1234.json") == \
        "https://raw.githubusercontent.com/CVEProject/cvelistV5/main/cves/2024/1xxx/CVE-2024-1234.json"
    with pytest.raises(ValueError):
        helpers.raw_github_url("https://example.com/x.json")


def _entry(ts, new=(), updated=()):
    def items(ids):
        return [{"cveId": i, "githubLink": f"https://github.com/o/r/blob/main/cves/{i}-{ts}.json"} for i in ids]
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
    assert changes == {"CVE-1": "https://github.com/o/r/blob/main/cves/CVE-1-2026-10-22T12:00:00.000Z.json",
                       "CVE-2": "https://github.com/o/r/blob/main/cves/CVE-2-2026-10-22T11:00:00.000Z.json"}


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
```

- [ ] **Step 3: Run to verify failure** — `poetry run pytest tests/unit/test_cve_v5.py -v` — Expected: FAIL, `ModuleNotFoundError: No module named 'cve_v5.helpers'`.

- [ ] **Step 4: Implement `src/cve_v5/helpers.py`**

```python
"""CVE v5 loader helpers. Pure Python except extract_raw_data (Spark; pyspark is
not imported at module level)."""
from __future__ import annotations

import datetime
import re
import tarfile
from typing import Iterator

from common.staging import to_single_line

_GITHUB_BLOB = re.compile(r"^https://github\.com/([^/]+)/([^/]+)/blob/(.+)$")
# Record files only: cves/<year>/<bucket>/CVE-<year>-<n>.json (skips deltaLog.json etc.)
_CVE_FILE = re.compile(r"(^|/)cves/\d{4}/[^/]+/CVE-\d{4}-\d+\.json$")


def parse_ts(s: str) -> datetime.datetime:
    """ISO 8601 timestamp from deltaLog.json ('...Z') -> UTC-aware datetime."""
    ts = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=datetime.timezone.utc)


def raw_github_url(link: str) -> str:
    """deltaLog githubLink (github.com/<o>/<r>/blob/<ref>/<path>) -> raw download URL."""
    m = _GITHUB_BLOB.match(link)
    if not m:
        raise ValueError(f"unexpected githubLink: {link}")
    return f"https://raw.githubusercontent.com/{m[1]}/{m[2]}/{m[3]}"


def newest_fetch_time(delta_log: list[dict]) -> datetime.datetime | None:
    return max((parse_ts(e["fetchTime"]) for e in delta_log), default=None)


def plan_incremental(delta_log: list[dict], watermark: datetime.datetime | None, threshold: int
                     ) -> tuple[dict[str, str], datetime.datetime | None, bool]:
    """Decide what an incremental run does.

    Returns ({cve_id: githubLink}, newest fetchTime processed, needs_full_load).
    A full load is needed when there is no watermark, when the log no longer reaches
    back to the watermark (deltaLog.json keeps a rolling window, so changes could be
    missing), or when more than `threshold` CVEs changed."""
    if watermark is None:
        return {}, None, True
    if not delta_log:
        return {}, None, False
    if min(parse_ts(e["fetchTime"]) for e in delta_log) > watermark:
        return {}, None, True
    entries = sorted((e for e in delta_log if parse_ts(e["fetchTime"]) > watermark),
                     key=lambda e: parse_ts(e["fetchTime"]))
    if not entries:
        return {}, None, False
    changes: dict[str, str] = {}
    for e in entries:   # oldest first, so the latest link wins
        for item in (e.get("new") or []) + (e.get("updated") or []):
            changes[item["cveId"]] = item["githubLink"]
    newest = parse_ts(entries[-1]["fetchTime"])
    return changes, newest, len(changes) > threshold


def iter_tar_records(fileobj) -> Iterator[bytes]:
    """Stream a cvelistV5 .tar.gz and yield each CVE record as one JSON line."""
    with tarfile.open(fileobj=fileobj, mode="r|gz") as tf:
        for member in tf:
            if member.isfile() and _CVE_FILE.search(member.name):
                yield to_single_line(tf.extractfile(member).read().strip())


def extract_raw_data(df):
    """Add raw-table id columns from the `raw` VARIANT (merge key + convenience)."""
    return df.selectExpr(
        "raw",
        r"try_variant_get(raw, '$.dataType', 'string') as data_type",
        r"try_variant_get(raw, '$.dataVersion', 'string') as data_version",
        r"try_variant_get(raw, '$.cveMetadata.cveId', 'string') as cve_id",
        r"try_cast(regexp_extract(cve_id, '^CVE-(\\d+)-.*$', 1) as int) as cve_year",
        r"try_variant_get(raw, '$.cveMetadata.datePublished', 'timestamp') as date_published",
        r"try_variant_get(raw, '$.cveMetadata.dateUpdated', 'timestamp') as date_updated",
    )
```

- [ ] **Step 5: Implement `src/cve_v5/transform.py`**

```python
"""Flatten raw CVE v5 VARIANT records into the typed silver schema. Every field is
read with try_variant_get (complex types are passed as the type hint directly).
CVSS: CNA metrics first, CISA-ADP as fallback; SSVC from the CISA-ADP container.

No module-level pyspark import, so the module imports without a Spark install."""

CVSS_VERSIONS = [("4.0", "cvssV4_0"), ("3.1", "cvssV3_1"), ("3.0", "cvssV3_0"), ("2.0", "cvssV2_0")]

AFFECTED_TYPE = """ARRAY<STRUCT<
    vendor: STRING, product: STRING, defaultStatus: STRING,
    platforms: ARRAY<STRING>,
    versions: ARRAY<STRUCT<version: STRING, status: STRING,
        lessThan: STRING, lessThanOrEqual: STRING, versionType: STRING>>
>>"""
REFERENCES_TYPE = "ARRAY<STRUCT<url: STRING, name: STRING, tags: ARRAY<STRING>>>"
DESCRIPTIONS_TYPE = "ARRAY<STRUCT<lang: STRING, value: STRING>>"
PROBLEM_TYPES_TYPE = "ARRAY<STRUCT<descriptions: ARRAY<STRUCT<cweId: STRING, description: STRING>>>>"
# CISA-ADP is located by providerMetadata.shortName among the first adp entries.
_ADP_SLOTS = 3


def _v(path: str, typ: str = "string"):
    from pyspark.sql import functions as F
    return F.try_variant_get(F.col("raw"), f"$.{path}", typ)


def _cisa_adp(path: str, typ: str = "string"):
    """Field from the CISA-ADP container, wherever it sits in containers.adp[]."""
    from pyspark.sql import functions as F
    return F.coalesce(*[
        F.when(_v(f"containers.adp[{i}].providerMetadata.shortName") == "CISA-ADP",
               _v(f"containers.adp[{i}].{path}", typ))
        for i in range(_ADP_SLOTS)])


def _cvss(field, alias: str):
    """CVSS columns from `<base>.metrics[0]`, newest CVSS version first. The version
    is the first one whose object carries a baseScore."""
    from pyspark.sql import functions as F
    base = "metrics[0]"
    return [
        F.coalesce(*[F.when(field(f"{base}.{key}.baseScore", "double").isNotNull(), F.lit(ver))
                     for ver, key in CVSS_VERSIONS]).alias(f"{alias}_cvss_version"),
        F.coalesce(*[field(f"{base}.{key}.baseScore", "double") for _, key in CVSS_VERSIONS]
                   ).alias(f"{alias}_cvss_base_score"),
        F.coalesce(*[field(f"{base}.{key}.baseSeverity") for _, key in CVSS_VERSIONS]
                   ).alias(f"{alias}_cvss_severity"),
        F.coalesce(*[field(f"{base}.{key}.vectorString") for _, key in CVSS_VERSIONS]
                   ).alias(f"{alias}_cvss_vector"),
    ]


def transform_cves(df):
    """Raw CVE v5 rows (raw VARIANT + extract_raw_data columns) -> silver rows."""
    from pyspark.sql import functions as F

    cna = lambda path, typ="string": _v(f"containers.cna.{path}", typ)  # noqa: E731
    descriptions = f"try_variant_get(raw, '$.containers.cna.descriptions', '{DESCRIPTIONS_TYPE}')"
    problem_types = f"try_variant_get(raw, '$.containers.cna.problemTypes', '{PROBLEM_TYPES_TYPE}')"
    return (
        df.select(
            F.col("cve_id"),
            F.col("cve_year"),
            _v("cveMetadata.state").alias("state"),
            F.col("date_published"),
            _v("cveMetadata.dateReserved", "timestamp").alias("date_reserved"),
            F.col("date_updated"),
            _v("cveMetadata.assignerOrgId").alias("assigner_org_id"),
            _v("cveMetadata.assignerShortName").alias("assigner_short_name"),
            cna("title").alias("title"),
            # English description preferred, else the first one.
            F.expr(f"coalesce(get(filter({descriptions}, x -> x.lang = 'en'), 0).value, "
                   f"get({descriptions}, 0).value)").alias("description"),
            cna("affected", AFFECTED_TYPE).alias("affected_products"),
            *_cvss(cna, "cna"),
            *_cvss(_cisa_adp, "adp"),
            # All CWE ids across every problemTypes[].descriptions[] entry.
            F.expr(f"array_distinct(filter(transform(flatten(transform("
                   f"coalesce({problem_types}, array()), pt -> coalesce(pt.descriptions, array()))), "
                   f"d -> d.cweId), x -> x IS NOT NULL))").alias("cwe_ids"),
            cna("problemTypes[0].descriptions[0].description").alias("cwe_description"),
            _cisa_adp("metrics[0].other.content.options[0].Exploitation").alias("ssvc_exploitation"),
            _cisa_adp("metrics[0].other.content.options[1].Automatable").alias("ssvc_automatable"),
            _cisa_adp("metrics[0].other.content.options[2]['Technical Impact']").alias("ssvc_technical_impact"),
            cna("references", REFERENCES_TYPE).alias("references"),
            F.col("raw"),
        )
        # get() is 0-indexed and returns NULL for an empty array (unlike element_at under ANSI).
        .withColumn("cwe_id", F.expr("get(cwe_ids, 0)"))
        .withColumn("cvss_version", F.coalesce("cna_cvss_version", "adp_cvss_version"))
        .withColumn("cvss_base_score", F.coalesce("cna_cvss_base_score", "adp_cvss_base_score"))
        .withColumn("cvss_severity", F.coalesce("cna_cvss_severity", "adp_cvss_severity"))
        .withColumn("cvss_vector", F.coalesce("cna_cvss_vector", "adp_cvss_vector"))
        .withColumn("cvss_source", F.when(F.col("cna_cvss_base_score").isNotNull(), F.lit("CNA"))
                    .when(F.col("adp_cvss_base_score").isNotNull(), F.lit("ADP")))
        .select("cve_id", "cve_year", "state", "date_published", "date_reserved", "date_updated",
                "assigner_org_id", "assigner_short_name", "title", "description", "affected_products",
                "cvss_version", "cvss_base_score", "cvss_severity", "cvss_vector", "cvss_source",
                "cwe_ids", "cwe_id", "cwe_description", "ssvc_exploitation", "ssvc_automatable",
                "ssvc_technical_impact", "references", "raw")
    )
```
- [ ] **Step 6: Run to verify pass** — `poetry run pytest tests/unit/test_cve_v5.py -v` — Expected: all pass. If only `ssvc_technical_impact` is wrong (the `['Technical Impact']` path segment is not supported), change that column to `F.element_at(F.get(_cisa_adp("metrics[0].other.content.options", "array<map<string,string>>"), 2), F.lit("Technical Impact"))` and keep the test unchanged.

- [ ] **Step 7: Commit**

```bash
git add vuln-info-ingest
git commit -m "vuln-info-ingest: CVE v5 helpers and VARIANT transform" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 7: CVE v5 notebooks

**Files:**
- Create: `src/cve_v5/Full load.py`, `src/cve_v5/Incremental load.py`, `src/cve_v5/CVE v5 Silver Transform.py`, `src/cve_v5/cves_v5_comments.sql`

**Interfaces:**
- Consumes: Task 6 helpers/transform; `common.*`
- Produces: tables `cves_v5_raw` (columns `raw VARIANT, data_type STRING, data_version STRING, cve_id STRING, cve_year INT, date_published TIMESTAMP, date_updated TIMESTAMP`, CDF on), `cves_v5_fetch_log` (`fetch_time timestamp, mode string, watermark timestamp, n_records long, n_unparsed long, n_rows_after long`), `cves_v5`; task value `needs_full_load`.

- [ ] **Step 1: Copy comments file** — `cp $ORIG/CVEv5Load/cves_v5_comments.sql src/cve_v5/`; `grep -niE "$FORBIDDEN" src/cve_v5/cves_v5_comments.sql` must return nothing.

- [ ] **Step 2: Write `src/cve_v5/Full load.py`**

```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # CVE v5 full load — cvelistV5 tarball snapshot, upsert cves_v5_raw
# MAGIC Streams the `CVEProject/cvelistV5` main-branch tarball (no git clone), writes
# MAGIC every CVE record as one JSON line into gzip parts on the scratch Volume, then
# MAGIC `MERGE`s into `cves_v5_raw` on `cve_id`, updating rows only when `date_updated`
# MAGIC advanced. Nothing is deleted: rejected CVEs stay with `state = 'REJECTED'`.
# MAGIC Used for the initial load and as the fallback of `Incremental load`.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "tarball_url": str, "delta_log_url": str,
                         "part_records": int})
FULL_STAGING = volume_path(p, "cve_v5") + "/full"
RAW_TABLE = table_name(p, "cves_v5_raw")
FETCH_LOG_TABLE = table_name(p, "cves_v5_fetch_log")

# COMMAND ----------

# DBTITLE 1,Watermark, then stream the tarball into gzip JSON-line parts
import datetime, json, urllib.request
from cve_v5.helpers import iter_tar_records, newest_fetch_time, extract_raw_data
from common.staging import write_text_parts, clear_parts
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log
from delta.tables import DeltaTable

# Taken before the download: everything in deltaLog up to here is in the tarball,
# later changes are re-fetched by the next incremental run (the MERGE is idempotent).
started = datetime.datetime.now(datetime.UTC)
with urllib.request.urlopen(p["delta_log_url"]) as r:
    watermark = newest_fetch_time(json.load(r)) or started
print(f"watermark={watermark}")

clear_parts(FULL_STAGING)
with urllib.request.urlopen(p["tarball_url"]) as r:
    n_records, n_parts = write_text_parts(iter_tar_records(r), FULL_STAGING, p["part_records"])
print(f"wrote {n_records} records into {n_parts} parts")

# COMMAND ----------

# DBTITLE 1,Parse, dedupe, create cves_v5_raw if needed (CDF on) and merge
parsed = extract_raw_data(spark.read.text(f"{FULL_STAGING}/part-*.jsonl.gz")
                          .selectExpr("try_parse_json(value) as raw"))
n_unparsed = parsed.filter("cve_id IS NULL").count()
if n_unparsed:
    print(f"WARNING: {n_unparsed} records did not parse and are skipped")
src = dedupe_latest(parsed.filter("cve_id IS NOT NULL"), "cve_id", "date_updated")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    raw VARIANT, data_type STRING, data_version STRING, cve_id STRING, cve_year INT,
    date_published TIMESTAMP, date_updated TIMESTAMP
  ) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
""")
(DeltaTable.forName(spark, RAW_TABLE).alias("t")
 .merge(src.alias("s"), "t.cve_id = s.cve_id")
 .whenMatchedUpdateAll(condition="t.date_updated IS NULL OR s.date_updated > t.date_updated")
 .whenNotMatchedInsertAll()
 .execute())

# COMMAND ----------

# DBTITLE 1,Fetch-log row
n_after = spark.table(RAW_TABLE).count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), "full", watermark, n_records, n_unparsed, n_after),
    "fetch_time timestamp, mode string, watermark timestamp, n_records long, n_unparsed long, n_rows_after long")
print(f"cves_v5_raw rows: {n_after}")
```

- [ ] **Step 3: Write `src/cve_v5/Incremental load.py`**

```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # CVE v5 incremental load — deltaLog.json since the watermark, upsert cves_v5_raw
# MAGIC Reads `cves/deltaLog.json`, takes the CVEs new/updated after the stored
# MAGIC watermark, downloads each record and `MERGE`s it (gated by `date_updated`).
# MAGIC Sets the task value `needs_full_load=true` instead when there is no watermark
# MAGIC yet, when deltaLog no longer reaches back to the watermark, or when more than
# MAGIC `full_reload_threshold` CVEs changed; the job then runs `Full load`.

# COMMAND ----------

# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path

p = get_params(dbutils, {**COMMON_PARAMS, "delta_log_url": str, "full_reload_threshold": int,
                         "fetch_threads": int, "part_records": int})
INCREMENTAL_STAGING = volume_path(p, "cve_v5") + "/incremental"
RAW_TABLE = table_name(p, "cves_v5_raw")
FETCH_LOG_TABLE = table_name(p, "cves_v5_fetch_log")
dbutils.jobs.taskValues.set(key="needs_full_load", value="false")

# COMMAND ----------

# DBTITLE 1,Plan: changed CVEs since the watermark
import datetime, json, urllib.request
from concurrent.futures import ThreadPoolExecutor
from cve_v5.helpers import plan_incremental, raw_github_url, extract_raw_data
from common.staging import to_single_line, write_text_parts, clear_parts
from common.http import fetch_with_retries
from common.merge import dedupe_latest
from common.fetch_log import append_fetch_log, read_watermark
from delta.tables import DeltaTable

watermark = read_watermark(spark, FETCH_LOG_TABLE)
with urllib.request.urlopen(p["delta_log_url"]) as r:
    delta_log = json.load(r)
changes, newest, needs_full = plan_incremental(delta_log, watermark, p["full_reload_threshold"])
print(f"watermark={watermark}: {len(changes)} changed CVEs, newest fetchTime={newest}, full={needs_full}")

if needs_full:
    dbutils.jobs.taskValues.set(key="needs_full_load", value="true")
    dbutils.notebook.exit("full load")
if not changes:
    dbutils.notebook.exit("no changes")

# COMMAND ----------

# DBTITLE 1,Fetch changed records and write gzip JSON-line parts
def fetch(link: str) -> bytes | None:
    data = fetch_with_retries(raw_github_url(link))
    return None if data is None else to_single_line(data)


clear_parts(INCREMENTAL_STAGING)
with ThreadPoolExecutor(p["fetch_threads"]) as pool:
    records = (x for x in pool.map(fetch, changes.values()) if x is not None)
    n_records, n_parts = write_text_parts(records, INCREMENTAL_STAGING, p["part_records"])
print(f"wrote {n_records} records into {n_parts} parts, {len(changes) - n_records} not found")

# COMMAND ----------

# DBTITLE 1,Parse and merge into cves_v5_raw
n_unparsed = 0
if n_records:
    parsed = extract_raw_data(spark.read.text(f"{INCREMENTAL_STAGING}/part-*.jsonl.gz")
                              .selectExpr("try_parse_json(value) as raw"))
    n_unparsed = parsed.filter("cve_id IS NULL").count()
    if n_unparsed:
        print(f"WARNING: {n_unparsed} records did not parse and are skipped")
    src = dedupe_latest(parsed.filter("cve_id IS NOT NULL"), "cve_id", "date_updated")
    (DeltaTable.forName(spark, RAW_TABLE).alias("t")
     .merge(src.alias("s"), "t.cve_id = s.cve_id")
     .whenMatchedUpdateAll(condition="t.date_updated IS NULL OR s.date_updated > t.date_updated")
     .whenNotMatchedInsertAll()
     .execute())

# COMMAND ----------

# DBTITLE 1,Fetch-log row
new_watermark = max(watermark, newest)   # never move the watermark back
n_after = spark.table(RAW_TABLE).count()
append_fetch_log(
    spark, FETCH_LOG_TABLE,
    (datetime.datetime.now(datetime.UTC), "incremental", new_watermark, n_records, n_unparsed, n_after),
    "fetch_time timestamp, mode string, watermark timestamp, n_records long, n_unparsed long, n_rows_after long")
print(f"cves_v5_raw rows: {n_after}, watermark -> {new_watermark}")
```

- [ ] **Step 4: Write `src/cve_v5/CVE v5 Silver Transform.py`**

```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Parameters
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, table_name, volume_path
from common.silver import stream_cdf_to_silver
from cve_v5.transform import transform_cves

p = get_params(dbutils, COMMON_PARAMS)
RAW_TABLE = table_name(p, "cves_v5_raw")
SILVER_TABLE = table_name(p, "cves_v5")
# Checkpoint lives outside the loaders' staging directory, so the staging cleanup
# never destroys the stream's offsets.
CHECKPOINT = volume_path(p, "cve_v5_checkpoints") + "/silver"
```
Then copy the original cell **"Create silver table"** from `$ORIG/CVEv5Load/CVE v5 Silver Transform.py` verbatim, replacing `{silver_table}` with `{SILVER_TABLE}` and `print(f"Table {silver_table} ready")` with `print(f"Table {SILVER_TABLE} ready")`. Then:
```python
# COMMAND ----------

# DBTITLE 1,Stream raw CDF into silver table
stream_cdf_to_silver(spark, RAW_TABLE, SILVER_TABLE, "cve_id", transform_cves, CHECKPOINT)
```
The DDL column order is the same as the final `.select(...)` in `transform.py` (pinned by `test_transform_columns`).

- [ ] **Step 5: Compile check and tests** — `poetry run python -m py_compile src/cve_v5/*.py && poetry run pytest -q` — Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add vuln-info-ingest
git commit -m "vuln-info-ingest: CVE v5 full/incremental loaders and silver transform" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 8: Bundle — variables, setup notebook, jobs

**Files:**
- Create: `databricks.yml`, `resources/variables.yml`, `src/common/Create volume.py`, `resources/vuln_kev.job.yml`, `resources/vuln_ghsa.job.yml`, `resources/vuln_osv.job.yml`, `resources/vuln_osv_initial_load.job.yml`, `resources/vuln_cve_v5.job.yml`, `resources/vuln_cve_v5_initial_load.job.yml`

**Interfaces:**
- Consumes: notebook paths from Tasks 3–7; task value `needs_full_load` from the incremental notebooks.

- [ ] **Step 1: `src/common/Create volume.py`**

```python
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Create the scratch Volume used for staging and checkpoints
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, volume_name

p = get_params(dbutils, COMMON_PARAMS)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name(p)}")
print(f"Volume {volume_name(p)} ready")
```

- [ ] **Step 2: `databricks.yml`**

```yaml
# Databricks Asset Bundle for ingestion of vulnerability data (CVE v5, GHSA, OSV, CISA KEV).
# See README.md for deployment instructions.
bundle:
  name: vuln_info_ingest

include:
  - resources/*.yml

sync:
  exclude:
    - tests/**
    - .venv/**

targets:
  dev:
    # Resources are prefixed with '[dev <user>]' and schedules are paused.
    mode: development
    default: true
    variables:
      catalog: cybersecurity
      schema: datasets

  prod:
    mode: production
    workspace:
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      catalog: cybersecurity
      schema: datasets
```

- [ ] **Step 3: `resources/variables.yml`**

```yaml
variables:
  catalog:
    description: Catalog for all tables and the scratch Volume
  schema:
    description: Schema for all tables and the scratch Volume
  volume:
    description: Scratch Volume (created if missing) for staging files and streaming checkpoints
    default: reference
  schedule:
    description: Default Quartz cron schedule of the periodic jobs
    default: "0 0 */6 * * ?"
  cve_v5_schedule:
    description: Schedule of the CVE v5 job
    default: ${var.schedule}
  ghsa_schedule:
    description: Schedule of the GHSA job
    default: ${var.schedule}
  osv_schedule:
    description: Schedule of the OSV job
    default: ${var.schedule}
  kev_schedule:
    description: Schedule of the KEV job
    default: ${var.schedule}
  timezone:
    description: Time zone of the schedules
    default: UTC
  pause_status:
    description: PAUSED or UNPAUSED (development mode always pauses)
    default: UNPAUSED
```

- [ ] **Step 4: `resources/vuln_kev.job.yml`**

```yaml
resources:
  jobs:
    vuln_kev:
      name: vuln_kev
      description: Load the CISA Known Exploited Vulnerabilities catalog into kev_raw and kev_vulnerabilities
      max_concurrent_runs: 1
      schedule:
        quartz_cron_expression: ${var.kev_schedule}
        timezone_id: ${var.timezone}
        pause_status: ${var.pause_status}
      parameters:
        - name: catalog
          default: ${var.catalog}
        - name: schema
          default: ${var.schema}
        - name: volume
          default: ${var.volume}
        - name: catalog_url
          default: https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json
      tasks:
        - task_key: setup
          notebook_task:
            notebook_path: "../src/common/Create volume.py"
        - task_key: load
          depends_on:
            - task_key: setup
          notebook_task:
            notebook_path: "../src/kev/Load KEV.py"
        - task_key: silver
          depends_on:
            - task_key: load
          notebook_task:
            notebook_path: "../src/kev/KEV Silver Transform.py"
```

- [ ] **Step 5: `resources/vuln_ghsa.job.yml`** — same shape as KEV:

```yaml
resources:
  jobs:
    vuln_ghsa:
      name: vuln_ghsa
      description: Load the GitHub Advisory Database into ghsa_raw and github_advisories
      max_concurrent_runs: 1
      schedule:
        quartz_cron_expression: ${var.ghsa_schedule}
        timezone_id: ${var.timezone}
        pause_status: ${var.pause_status}
      parameters:
        - name: catalog
          default: ${var.catalog}
        - name: schema
          default: ${var.schema}
        - name: volume
          default: ${var.volume}
        - name: tarball_url
          default: https://codeload.github.com/github/advisory-database/tar.gz/refs/heads/main
        - name: part_records
          default: "20000"
      tasks:
        - task_key: setup
          notebook_task:
            notebook_path: "../src/common/Create volume.py"
        - task_key: load
          depends_on:
            - task_key: setup
          notebook_task:
            notebook_path: "../src/ghsa/Load advisories.py"
        - task_key: silver
          depends_on:
            - task_key: load
          notebook_task:
            notebook_path: "../src/ghsa/GHSA Silver Transform.py"
```

- [ ] **Step 6: `resources/vuln_osv.job.yml`**

```yaml
resources:
  jobs:
    vuln_osv:
      name: vuln_osv
      description: Incremental load of the OSV database into osv_raw and osv_vulnerabilities (falls back to a full load)
      max_concurrent_runs: 1
      schedule:
        quartz_cron_expression: ${var.osv_schedule}
        timezone_id: ${var.timezone}
        pause_status: ${var.pause_status}
      parameters:
        - name: catalog
          default: ${var.catalog}
        - name: schema
          default: ${var.schema}
        - name: volume
          default: ${var.volume}
        - name: bucket_url
          default: https://storage.googleapis.com/osv-vulnerabilities
        - name: part_records
          default: "20000"
        - name: full_reload_threshold
          default: "100000"
        - name: watermark_overlap_hours
          default: "6"
        - name: fetch_threads
          default: "32"
        - name: min_snapshot_ratio
          default: "0.9"
      tasks:
        - task_key: setup
          notebook_task:
            notebook_path: "../src/common/Create volume.py"
        - task_key: incremental
          depends_on:
            - task_key: setup
          notebook_task:
            notebook_path: "../src/osv/Incremental load.py"
        - task_key: check_full_load
          depends_on:
            - task_key: incremental
          condition_task:
            op: EQUAL_TO
            left: "{{tasks.incremental.values.needs_full_load}}"
            right: "true"
        - task_key: full_load
          depends_on:
            - task_key: check_full_load
              outcome: "true"
          notebook_task:
            notebook_path: "../src/osv/Full load.py"
        - task_key: silver
          depends_on:
            - task_key: incremental
            - task_key: full_load
          run_if: NONE_FAILED
          notebook_task:
            notebook_path: "../src/osv/OSV Silver Transform.py"
```

- [ ] **Step 7: `resources/vuln_osv_initial_load.job.yml`**

```yaml
resources:
  jobs:
    vuln_osv_initial_load:
      name: vuln_osv_initial_load
      description: One-time full load of the OSV database (all.zip snapshot); safe to re-run
      max_concurrent_runs: 1
      parameters:
        - name: catalog
          default: ${var.catalog}
        - name: schema
          default: ${var.schema}
        - name: volume
          default: ${var.volume}
        - name: bucket_url
          default: https://storage.googleapis.com/osv-vulnerabilities
        - name: part_records
          default: "20000"
        - name: min_snapshot_ratio
          default: "0.9"
      tasks:
        - task_key: setup
          notebook_task:
            notebook_path: "../src/common/Create volume.py"
        - task_key: full_load
          depends_on:
            - task_key: setup
          notebook_task:
            notebook_path: "../src/osv/Full load.py"
        - task_key: silver
          depends_on:
            - task_key: full_load
          notebook_task:
            notebook_path: "../src/osv/OSV Silver Transform.py"
```

- [ ] **Step 8: `resources/vuln_cve_v5.job.yml`**

```yaml
resources:
  jobs:
    vuln_cve_v5:
      name: vuln_cve_v5
      description: Incremental load of CVE v5 records (cvelistV5 deltaLog) into cves_v5_raw and cves_v5 (falls back to a full load)
      max_concurrent_runs: 1
      schedule:
        quartz_cron_expression: ${var.cve_v5_schedule}
        timezone_id: ${var.timezone}
        pause_status: ${var.pause_status}
      parameters:
        - name: catalog
          default: ${var.catalog}
        - name: schema
          default: ${var.schema}
        - name: volume
          default: ${var.volume}
        - name: tarball_url
          default: https://codeload.github.com/CVEProject/cvelistV5/tar.gz/refs/heads/main
        - name: delta_log_url
          default: https://raw.githubusercontent.com/CVEProject/cvelistV5/refs/heads/main/cves/deltaLog.json
        - name: full_reload_threshold
          default: "5000"
        - name: fetch_threads
          default: "16"
        - name: part_records
          default: "20000"
      tasks:
        - task_key: setup
          notebook_task:
            notebook_path: "../src/common/Create volume.py"
        - task_key: incremental
          depends_on:
            - task_key: setup
          notebook_task:
            notebook_path: "../src/cve_v5/Incremental load.py"
        - task_key: check_full_load
          depends_on:
            - task_key: incremental
          condition_task:
            op: EQUAL_TO
            left: "{{tasks.incremental.values.needs_full_load}}"
            right: "true"
        - task_key: full_load
          depends_on:
            - task_key: check_full_load
              outcome: "true"
          notebook_task:
            notebook_path: "../src/cve_v5/Full load.py"
        - task_key: silver
          depends_on:
            - task_key: incremental
            - task_key: full_load
          run_if: NONE_FAILED
          notebook_task:
            notebook_path: "../src/cve_v5/CVE v5 Silver Transform.py"
```

- [ ] **Step 9: `resources/vuln_cve_v5_initial_load.job.yml`**

```yaml
resources:
  jobs:
    vuln_cve_v5_initial_load:
      name: vuln_cve_v5_initial_load
      description: One-time full load of CVE v5 records (cvelistV5 tarball); safe to re-run
      max_concurrent_runs: 1
      parameters:
        - name: catalog
          default: ${var.catalog}
        - name: schema
          default: ${var.schema}
        - name: volume
          default: ${var.volume}
        - name: tarball_url
          default: https://codeload.github.com/CVEProject/cvelistV5/tar.gz/refs/heads/main
        - name: delta_log_url
          default: https://raw.githubusercontent.com/CVEProject/cvelistV5/refs/heads/main/cves/deltaLog.json
        - name: part_records
          default: "20000"
      tasks:
        - task_key: setup
          notebook_task:
            notebook_path: "../src/common/Create volume.py"
        - task_key: full_load
          depends_on:
            - task_key: setup
          notebook_task:
            notebook_path: "../src/cve_v5/Full load.py"
        - task_key: silver
          depends_on:
            - task_key: full_load
          notebook_task:
            notebook_path: "../src/cve_v5/CVE v5 Silver Transform.py"
```

- [ ] **Step 10: Validate** — ask the user which Databricks CLI profile to use (never pick one), then:

Run: `cd vuln-info-ingest && databricks bundle validate --strict -t dev --profile <profile>` and the same with `-t prod`.
Expected: `Validation OK!`. If a `${var.schedule}` default in another variable is rejected, replace the four `<source>_schedule` defaults with the literal `"0 0 */6 * * ?"` and note it in the README. Fix any other reported error and re-run. Then `databricks bundle validate -t dev --profile <profile> -o json | jq '.resources.jobs.vuln_osv.schedule, .resources.jobs.vuln_osv.tasks[].task_key'` and check the schedule resolved to `0 0 */6 * * ?` and the five task keys.

- [ ] **Step 11: Commit**

```bash
git add vuln-info-ingest
git commit -m "vuln-info-ingest: bundle with periodic and initial-load jobs" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```

---

### Task 9: Documentation and final checks

**Files:**
- Create: `vuln-info-ingest/README.md`
- Modify: `README.md` (repo root, list under the top bullet list)

- [ ] **Step 1: Write `vuln-info-ingest/README.md`** with these sections:
  1. **Overview** — one paragraph + table of sources (CVE v5 → `cves_v5`, GHSA → `github_advisories`, OSV → `osv_vulnerabilities`, KEV → `kev_vulnerabilities`) and the join key `cve_id`.
  2. **Deployment** — prerequisites (catalog + schema exist; serverless enabled); `databricks bundle deploy -t dev --profile <p>`; variables table (copy from `resources/variables.yml`); overriding: `--var schedule="0 0 */3 * * ?"`, or per source `--var osv_schedule=...`, or in `targets.<t>.variables`; dev mode pauses schedules.
  3. **First run order** — run `vuln_cve_v5_initial_load` and `vuln_osv_initial_load` once (`databricks bundle run vuln_osv_initial_load -t <t> --profile <p>`); GHSA and KEV need none. Don't run an initial-load job at the same time as the matching periodic job (pause the periodic one first).
  4. **Jobs** — the job table from the spec (§1 Jobs), job parameters per job and the full-load fallback via `needs_full_load`.
  5. **Per-source sections** (CVE v5, GHSA, OSV, KEV): tables, data facts, notes. Take the content from the original READMEs (`$ORIG/{GHSALoad,OSVLoad,KEVLoad}/README.md`), dropping the "Run order", "Layout" and "Tests" subsections (replaced by sections 3, 7, 8), every internal catalog/workspace name (the parenthesized workspace/catalog qualifier after "Tables", and "Run on ..." sentences), and the private-repo references. For CVE v5 write: tables `cves_v5_raw` / `cves_v5` / `cves_v5_fetch_log`; full load = tarball snapshot; incremental = `deltaLog.json` (rolling window → full-load fallback); rejected CVEs stay with `state = 'REJECTED'`; CVSS CNA-first/CISA-ADP fallback; SSVC from CISA-ADP. Carry over the CVE TODO list from `$ORIG/CVEv5Load/README.md` as "Possible extensions".
  6. **Layout** — the `src/` tree with one line per file.
  7. **Tests** — `poetry env use "$(uv python find 3.12)" && poetry install --with test && poetry run pytest`.
  8. **Adding a source** — new `src/<source>/` package (`__init__.py`, `helpers.py`, `transform.py`, load notebook(s) with the Parameters cell pattern, silver notebook calling `stream_cdf_to_silver`), a `resources/vuln_<source>.job.yml` (copy `vuln_kev.job.yml`), a `<source>_schedule` variable, tests under `tests/unit/`.

- [ ] **Step 2: Root README** — add after the `uc-udfs` bullet:
```markdown
* [vuln-info-ingest](vuln-info-ingest/) - ingestion of vulnerability data (CVE v5, GitHub advisories, OSV, CISA KEV) with Databricks Asset Bundles.
```

- [ ] **Step 3: Private-name scan** — run from repo root:
`grep -rniE "$FORBIDDEN" vuln-info-ingest docs README.md`
Expected: no output. Fix anything found.

- [ ] **Step 4: Final verification** — `cd vuln-info-ingest && poetry run pytest -q` (all pass) and `databricks bundle validate --strict -t dev --profile <profile>` (OK).

- [ ] **Step 5: Commit**

```bash
git add README.md vuln-info-ingest
git commit -m "vuln-info-ingest: README and root README entry" -m "Co-authored-by: Isaac <no-reply@databricks.com>"
```
