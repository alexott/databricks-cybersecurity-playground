# vuln-info-ingest — design

Date: 2026-10-22

## Goal

Consolidate four existing vulnerability-data loaders (`CVEv5Load`, `GHSALoad`,
`OSVLoad`, `KEVLoad`) into a single project `vuln-info-ingest/` in this repository, deployed with a
Databricks Asset Bundle (DAB):

- periodic jobs, every 6 hours by default, configurable per target and per source;
- one-time initial-load jobs where a source needs one (CVE v5, OSV);
- easy to extend with more sources later.

Out of scope: modifying or removing the original loaders;
deploying or running jobs (validation only, deploy on explicit request).

## Decisions

| Topic | Decision |
|---|---|
| Job topology | One job per source; separate initial-load jobs for CVE v5 and OSV |
| CVE v5 | Rework to the pattern used by GHSA/OSV/KEV |
| Code structure | Notebooks + plain Python modules, shared `common/` package |
| Configuration | No `config.py`: every setting is a job parameter, read via widgets |
| Tests | Moved into the project with its own `pyproject.toml` (poetry) |
| Compute | Serverless (notebook `environment_version = "6"`) |

## 1. Bundle, jobs, parameters

### Layout

```
vuln-info-ingest/
  databricks.yml
  resources/
    variables.yml
    vuln_cve_v5.job.yml
    vuln_cve_v5_initial_load.job.yml
    vuln_ghsa.job.yml
    vuln_osv.job.yml
    vuln_osv_initial_load.job.yml
    vuln_kev.job.yml
  src/ ...            (section 2)
  tests/ ...          (section 3)
  pyproject.toml
  README.md
```

### Jobs

All jobs are serverless, `max_concurrent_runs: 1`.

| Job | Tasks | Schedule |
|---|---|---|
| `vuln_cve_v5` | `setup` → `incremental` → `check_full_load` (condition) → `full_load` (if true) → `silver` | `${var.cve_v5_schedule}` |
| `vuln_cve_v5_initial_load` | `setup` → `full_load` → `silver` | none |
| `vuln_osv` | `setup` → `incremental` → `check_full_load` (condition) → `full_load` (if true) → `silver` | `${var.osv_schedule}` |
| `vuln_osv_initial_load` | `setup` → `full_load` → `silver` | none |
| `vuln_ghsa` | `setup` → `load` → `silver` | `${var.ghsa_schedule}` |
| `vuln_kev` | `setup` → `load` → `silver` | `${var.kev_schedule}` |

- The incremental loaders set the task value `needs_full_load` (`"true"`/`"false"`).
  A `condition_task` compares `{{tasks.incremental.values.needs_full_load}}` with
  `"true"`; `full_load` depends on its `true` outcome. This replaces the current
  `dbutils.notebook.run("./Full load")` call.
- `silver` depends on `incremental` and `full_load` with `run_if: NONE_FAILED`, so it
  runs whether or not the full-load branch was taken, and not after a failure.
- The `setup` task (`src/common/setup` notebook) runs
  `CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}`. Catalog and schema must exist.
- Full loads are idempotent MERGEs, so initial-load jobs are safe to re-run.
- Running an initial-load job and the matching periodic job concurrently is not
  prevented by the platform; the README documents to pause/avoid overlap.

### Bundle variables (`resources/variables.yml`)

| Variable | Default | Notes |
|---|---|---|
| `catalog` | — | required, set per target |
| `schema` | — | required, set per target |
| `volume` | `reference` | scratch Volume for staging and checkpoints |
| `schedule` | `0 0 */6 * * ?` | Quartz cron, default for every source |
| `cve_v5_schedule`, `ghsa_schedule`, `osv_schedule`, `kev_schedule` | `${var.schedule}` | per-source override; if `bundle validate` rejects variable-to-variable defaults, fall back to literal defaults |
| `timezone` | `UTC` | |
| `pause_status` | `UNPAUSED` | |

Targets: `dev` (`mode: development`, default; schedules paused by the mode) and
`prod` (`mode: production`). No workspace host is hard-coded; the user picks the
workspace with `--profile`.

### Job parameters

Every job declares job-level `parameters` (pushed to all notebook tasks as widgets):

- common: `catalog: ${var.catalog}`, `schema: ${var.schema}`, `volume: ${var.volume}`;
- per source, defaults copied from today's constants:
  - CVE v5: `tarball_url` (`https://codeload.github.com/CVEProject/cvelistV5/tar.gz/refs/heads/main`),
    `delta_log_url` (`https://raw.githubusercontent.com/CVEProject/cvelistV5/refs/heads/main/cves/deltaLog.json`),
    `full_reload_threshold` (`5000`), `fetch_threads` (`16`), `part_records` (`20000`);
  - GHSA: `tarball_url` (`https://codeload.github.com/github/advisory-database/tar.gz/refs/heads/main`),
    `part_records` (`20000`);
  - OSV: `bucket_url` (`https://storage.googleapis.com/osv-vulnerabilities`),
    `full_reload_threshold` (`100000`), `watermark_overlap_hours` (`6`),
    `fetch_threads` (`32`), `min_snapshot_ratio` (`0.9`), `part_records` (`20000`);
  - KEV: `catalog_url` (`https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json`).

Defaults live only in the job YAML; notebooks have no in-code defaults and fail
fast on a missing parameter. Initial-load jobs declare the subset of parameters
their tasks use.

Derived in code (not parameters):

- tables: `{catalog}.{schema}.<name>` with the current names — `cves_v5_raw`,
  `cves_v5`, `cves_v5_fetch_log`, `ghsa_raw`, `github_advisories`, `ghsa_fetch_log`,
  `osv_raw`, `osv_vulnerabilities`, `osv_fetch_log`, `kev_raw`,
  `kev_vulnerabilities`, `kev_fetch_log`;
- paths: staging `/Volumes/{catalog}/{schema}/{volume}/<source>` and checkpoints
  `/Volumes/{catalog}/{schema}/{volume}/<source>_checkpoints` (outside staging, so
  staging cleanup never wipes stream offsets).

## 2. Code layout and shared code

```
src/
  common/
    __init__.py
    params.py      # get_params(dbutils, spec) -> dict; table_name(), volume_path()
    staging.py     # clear_parts(dir), write_text_parts(lines, dir, part_records)
    http.py        # fetch_with_retries(url, timeout, attempts) -> bytes | None
    merge.py       # dedupe_latest(df, key, order_col)
    silver.py      # stream_cdf_to_silver(spark, raw_table, silver_table, key,
                   #                      transform_fn, checkpoint, apply_deletes=False)
    fetch_log.py   # append_fetch_log(spark, table, row, schema); read_watermark(spark, table)
    setup.py       # notebook: CREATE VOLUME IF NOT EXISTS
  cve_v5/  helpers.py  transform.py  "Full load.py"  "Incremental load.py"
           "CVE v5 Silver Transform.py"  cves_v5_comments.sql
  ghsa/    helpers.py  transform.py  "Load advisories.py"
           "GHSA Silver Transform.py"  github_advisories_comments.sql
  osv/     helpers.py  transform.py  "Full load.py"  "Incremental load.py"
           "OSV Silver Transform.py"
  kev/     helpers.py  transform.py  "Load KEV.py"
           "KEV Silver Transform.py"  kev_vulnerabilities_comments.sql
```

### Rules

- Notebooks start with the serverless environment header
  (`environment_version = "6"`), read parameters with `common.params.get_params`,
  then call helpers with explicit arguments. Notebooks import `common` after
  appending the parent `src/` directory (resolved from the notebook's own path) to
  `sys.path`; source-local modules import directly.
- `get_params(dbutils, spec)`: `spec` maps parameter name → type (`str`, `int`,
  `float`); creates text widgets, reads them, converts, raises on missing/empty.
- `helpers.py` / `transform.py` / `common/*` (except `setup.py`) contain no widget
  access and no module-level pyspark import, so they import without Spark.
- `common.silver.stream_cdf_to_silver` holds the logic duplicated in all four
  silver notebooks: if silver is empty, MERGE the full raw snapshot and start the
  stream at the latest raw version; then stream raw CDF (`insert`,
  `update_postimage`, plus `delete` when `apply_deletes`) with `foreachBatch`
  MERGE on `key`, `trigger(availableNow=True)`, checkpoint at `checkpoint`.
  Each micro-batch keeps only the last change per `key` (highest
  `_commit_version`) before merging — as OSV does today. This also fixes a latent
  bug in the CVE/GHSA/KEV silver notebooks, whose MERGE fails when one batch
  holds several raw commits for the same key. With `apply_deletes` (OSV only), a
  last change of type `delete` deletes the silver row, matching OSV's current
  notebook.
- Silver notebooks keep their `CREATE TABLE` DDL (with column comments) and call
  the helper.
- Source-specific behaviour is preserved: GHSA `github_reviewed` from the tree
  path; KEV soft-delete via `date_removed` and catalog-version-gated refresh; OSV
  GCS `generation` pinning, `MIN_SNAPSHOT_RATIO` guard, delete of ids missing from
  the full snapshot, watermark overlap; all `modified`-gated MERGE conditions.

### CVE v5 rework

- **Full load**: record `max(fetchTime)` from `deltaLog.json` *before* the
  download (watermark); download the `cvelistV5` tarball to `/tmp`, extract,
  write every `cves/**/CVE-*.json` as one line into gzip JSONL parts on the
  staging Volume path; `extract_raw_data` (unchanged columns: `raw`, `data_type`,
  `data_version`, `cve_id`, `cve_year`, `date_published`, `date_updated`);
  `dedupe_latest` on `cve_id` by `date_updated`; `CREATE TABLE IF NOT EXISTS
  cves_v5_raw` with CDF on; MERGE on `cve_id` — update when
  `t.date_updated IS NULL OR s.date_updated > t.date_updated`, insert new, no
  deletes (rejected CVEs remain with `state = 'REJECTED'`); append fetch-log row
  (`fetch_time`, `mode='full'`, `watermark`, `n_records`, `n_unparsed`,
  `n_rows_after`).
- **Incremental**: read watermark from `cves_v5_fetch_log` (none → set
  `needs_full_load=true` and exit); fetch `deltaLog.json`; if the watermark is
  older than the oldest entry's `fetchTime` (log window no longer covers the gap)
  or the number of distinct changed CVEs exceeds `full_reload_threshold`, set
  `needs_full_load=true` and exit; otherwise collect changed CVEs (`new` +
  `updated`, latest wins), download each via its raw GitHub URL with
  `fetch_with_retries` on a thread pool, stage gzip parts, same gated MERGE,
  append fetch-log row (`mode='incremental'`, watermark = newest `fetchTime`
  processed, never moving backwards); set `needs_full_load=false`.
- **Silver**: `transform_cves` moves unchanged into `cve_v5/transform.py`;
  notebook keeps DDL and calls `stream_cdf_to_silver`.
- `cves_v5_delta` is no longer written; `cves_v5_fetch_log` replaces it.

### Adding a source

New `src/<source>/` folder (helpers, transform, load notebook(s), silver
notebook) + one `resources/vuln_<source>.job.yml` + optional
`<source>_schedule` variable. Documented in the README.

## 3. Tests, docs, verification

- `pyproject.toml` (poetry, `package-mode = false`): test deps pytest,
  pytest-spark, chispa, pyspark 4.x, delta-spark.
- `tests/conftest.py` puts `src/` on `sys.path` and configures the Spark session.
- `tests/unit/test_ghsa.py`, `test_osv.py`, `test_kev.py` and `tests/data/{ghsa,osv,kev}`
  carried over from the original loaders and repointed at `src/`.
- New: `tests/unit/test_cve_v5.py` (extract + transform on trimmed real CVE
  records in `tests/data/cve_v5/`, deltaLog filtering/threshold/window logic as a
  pure function in `cve_v5/helpers.py`), `tests/unit/test_common.py`
  (`get_params` with a fake dbutils, `write_text_parts`/`clear_parts`,
  `dedupe_latest`).
- `vuln-info-ingest/README.md`: merged content of the four source READMEs (tables,
  data facts, notes), plus deployment (variables, targets, initial-load order,
  schedule override) and "adding a source". One-line entry in the root `README.md`.
- Verification: `poetry run pytest` passes locally;
  `databricks bundle validate --strict -t dev --profile <user's choice>` passes.
  No deploy or run without explicit approval.
