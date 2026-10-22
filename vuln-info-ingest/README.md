# vuln-info-ingest — vulnerability data ingestion for Databricks

Loads public vulnerability data into Unity Catalog tables and keeps them up to date
with periodic [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/), deployed as a
[Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/).

| Source | Silver table | What it adds |
|---|---|---|
| [CVE List V5](https://github.com/CVEProject/cvelistV5) | `cves_v5` | The CVE records: description, affected products, CVSS, CWE, CISA SSVC |
| [GitHub Advisory Database](https://github.com/github/advisory-database) | `github_advisories` | Package-level dependency edges (ecosystem, package, version range) |
| [OSV](https://google.github.io/osv.dev/data/) | `osv_vulnerabilities` | Superset of GHSA: PyPI, Go, RustSec, npm malware, Linux distros, ... |
| [CISA KEV](https://www.cisa.gov/known-exploited-vulnerabilities-catalog) | `kev_vulnerabilities` | "Actively exploited in the wild" signal |

All silver tables carry `cve_id`, the join key between them. Every source has a
`<source>_raw` table (one row per record with the original JSON as `VARIANT` plus
id columns, Change Data Feed on) and a `<source>_fetch_log` table (one row per load
run). The silver tables are kept in sync from the raw tables by streaming their
change feed. Column comments are part of the silver `CREATE TABLE` statements.

## Deployment

Prerequisites: a Unity Catalog catalog and schema you can create tables and volumes
in, and serverless compute for jobs. The scratch Volume (`volume` variable) is a bundle
resource, created by `bundle deploy`. If a Volume with that name already exists, adopt it
with `databricks bundle deployment bind reference <catalog>.<schema>.<volume> -t <target>`.
Note that `bundle destroy` deletes the Volume, including the silver streaming
checkpoints in it.

```sh
databricks bundle validate -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
```

Bundle variables (`resources/variables.yml`):

| Variable | Default | Description |
|---|---|---|
| `catalog` | — (set per target) | Catalog for all tables and the scratch Volume |
| `schema` | — (set per target) | Schema for all tables and the scratch Volume |
| `volume` | `reference` | Scratch Volume for staging files and streaming checkpoints |
| `schedule` | `0 0 */6 * * ?` | Default Quartz cron schedule of the periodic jobs |
| `cve_v5_schedule`, `ghsa_schedule`, `osv_schedule`, `kev_schedule` | `${var.schedule}` | Per-source schedule override |
| `timezone` | `UTC` | Time zone of the schedules |
| `pause_status` | `UNPAUSED` | `PAUSED` or `UNPAUSED`; the `dev` target sets `PAUSED` |

Override them in `targets.<target>.variables` of `databricks.yml`, or on the command
line, e.g. `--var schedule="0 0 */3 * * ?"` for all sources or
`--var osv_schedule="0 30 2 * * ?"` for one. The `dev` target uses development mode
(job names prefixed with `[dev <user>]`) and pauses the schedules; `prod` runs them.

## First run

CVE v5 and OSV need a full load before the periodic job can work incrementally. Run
each initial-load job once:

```sh
databricks bundle run vuln_cve_v5_initial_load -t <target> --profile <profile>
databricks bundle run vuln_osv_initial_load -t <target> --profile <profile>
```

GHSA and KEV always load a full snapshot, so their periodic jobs need no preparation.
Full loads are idempotent `MERGE`s and safe to re-run, but don't run an initial-load
job while the matching periodic job is running (pause the periodic job first).

## Jobs

| Job | Tasks | Schedule |
|---|---|---|
| `vuln_cve_v5` | `incremental` → `check_full_load` → `full_load` (only if needed) → `silver` | `cve_v5_schedule` |
| `vuln_cve_v5_initial_load` | `full_load` → `silver` | none |
| `vuln_osv` | `incremental` → `check_full_load` → `full_load` (only if needed) → `silver` | `osv_schedule` |
| `vuln_osv_initial_load` | `full_load` → `silver` | none |
| `vuln_ghsa` | `load` → `silver` | `ghsa_schedule` |
| `vuln_kev` | `load` → `silver` | `kev_schedule` |

All settings are job parameters (no config files); defaults are in the job YAML files
and can be changed per run. Every job has `catalog`, `schema` and `volume`; besides
those:

- CVE v5: `tarball_url`, `delta_log_url`, `full_reload_threshold` (5000), `fetch_threads` (16), `watermark_overlap_minutes` (30), `part_records` (20000).
- GHSA: `tarball_url`, `part_records` (20000).
- OSV: `bucket_url`, `full_reload_threshold` (100000), `watermark_overlap_hours` (6), `fetch_threads` (32), `min_snapshot_ratio` (0.9), `part_records` (20000).
- KEV: `catalog_url`.

The incremental loaders set the task value `needs_full_load`. When it is `true`
(too many changes, or for CVE v5 also no watermark yet or a gap in `deltaLog.json`),
the condition task runs `full_load`; otherwise `full_load` is skipped. `silver` runs
in both cases.

## CVE v5

- `cves_v5_raw` — one row per CVE: `raw` VARIANT + `cve_id`, `cve_year`, `date_published`, `date_updated`. Change Data Feed on.
- `cves_v5` — typed silver: identifiers, lifecycle timestamps, state, title, description, `affected_products`, CVSS, CWE ids, CISA SSVC, references, `raw`.
- `cves_v5_fetch_log` — one row per load run (mode, watermark, counts).

**Full load** streams the `cvelistV5` main-branch tarball (no git clone), writes every
`cves/<year>/<bucket>/CVE-*.json` as one line into gzip parts on the Volume, and
`MERGE`s into `cves_v5_raw` on `cve_id`, updating rows only when `date_updated`
advanced. The watermark is the newest `fetchTime` of `cves/deltaLog.json`, read before
the download.

**Incremental load** reads `cves/deltaLog.json`, takes the CVEs new or updated after
the watermark minus `watermark_overlap_minutes`, downloads each record from GitHub and
merges it the same way. The overlap re-reads recent changes because
`raw.githubusercontent.com` is CDN-cached and can briefly return the previous version.
`deltaLog.json` only keeps a rolling window of changes, so when the watermark is older
than its oldest entry the job falls back to the full load.

Notes:

- Nothing is deleted: rejected CVEs stay with `state = 'REJECTED'`. Filter
  `state = 'PUBLISHED'` for actionable analysis.
- CVSS comes from the CNA container first and from CISA-ADP as a fallback
  (`cvss_source` = `CNA` / `ADP`), newest CVSS version first; SSVC fields come from
  the CISA-ADP container.
- `cwe_ids` holds every CWE across all problem types; `cwe_id` is its first element.

Possible extensions:

- Incorporate https://github.com/kaze-technologies/cvefree and enrich other data?
- Incorporate https://nvd.nist.gov/vuln/data-feeds? Most probably not necessary — CVSS metrics are already in the records.
- Build a data source on top of the NVD change history API: https://nvd.nist.gov/developers/vulnerabilities
- Add a table with CWE definitions (https://cwe.mitre.org/data/definitions/940.html), if available in machine-readable format.
- Decode attack vectors for the different CVSS versions.

## GitHub Advisory Database (GHSA)

The payoff over `cves_v5` is a real **package-level dependency edge**
(`affected_packages`: ecosystem, package, version range) plus an `aliases` → CVE link.
CVE v5's `affected_products` records only the upstream product; GHSA records the
affected packages (e.g. Log4Shell lists 10 Maven packages, not 1).

- `ghsa_raw` — one row per advisory: `raw` VARIANT + extracted ids + `github_reviewed`. Change Data Feed on.
- `github_advisories` — typed silver, kept in sync from `ghsa_raw`.
- `ghsa_fetch_log` — one row per load run (fetch time, source ref, counts).

The loader downloads the repository tarball to ephemeral `/tmp` (no git clone),
consolidates every `GHSA-*.json` (both the `github-reviewed/` and `unreviewed/` trees)
into JSONL parts on the Volume, then `MERGE`s into `ghsa_raw` on `ghsa_id`, updating
only rows whose `date_modified` advanced. A re-run with no upstream changes is a no-op.

Notes:

- `github_reviewed` comes from the source tree (authoritative); `unreviewed`
  advisories often have no curated ecosystem, so `affected_packages` can be empty —
  filter `github_reviewed = TRUE` for the curated package graph.
- `affected_packages` is one struct per (package, range); a package with only an
  explicit `versions` list keeps a struct with null version bounds; `raw` retains
  multi-segment ranges.
- Join `github_advisories.cve_id` to `cves_v5.cve_id` to attach package dependency
  edges to CVEs; `aliases` holds the complete id list.

## OSV (Open Source Vulnerabilities)

Ingests the full [OSV database](https://google.github.io/osv.dev/data/) from the public
bucket `gs://osv-vulnerabilities`. OSV joins many sources in one schema (GHSA, PyPI,
Go, RustSec, npm malware, CVE-derived GIT records, and Linux distros such as Debian,
Ubuntu, SUSE, Red Hat, Alpine, Chainguard, Wolfi). It is a superset of
`github_advisories`.

- `osv_raw` — one row per OSV id: `raw` VARIANT + extracted ids and timestamps. Change Data Feed on.
- `osv_vulnerabilities` — typed silver, kept in sync from `osv_raw` including deletes.
- `osv_fetch_log` — one row per load run (mode, all.zip generation, watermark, counts).

Data facts (checked 2026-10-05):

- `all.zip`: ~2.6 GB compressed, ~13.3 GB uncompressed, ~2.0M records, one
  `<ID>.json` per id. JSON, not protobuf.
- `all.zip` is **rebuilt about every 15 minutes**, so the full load pins one GCS
  `generation` for the download.
- `modified_id.csv`: ~130 MB, `<modified>,<ecosystem_dir>/<id>`, newest first.
- Normal churn is ~15K changed records per day, but OSV sometimes rewrites most of
  the database (1.5M changes in Sept 2026). That is why the incremental load falls
  back to the full load above `full_reload_threshold`.
- The largest record is ~20 MB (SUSE).

**Full load** pins and downloads `all.zip` to the Volume, writes each record as one
line into gzip parts, then `MERGE`s into `osv_raw` on `id`: update only when
`modified` advanced, insert new ids, **delete ids no longer in the snapshot**. It
refuses to merge if the snapshot has less than `min_snapshot_ratio` of the current
rows. The watermark is the snapshot's build time.

**Incremental load** streams `modified_id.csv` until `watermark - watermark_overlap_hours`,
fetches each changed `<ecosystem>/<id>.json` with a thread pool, and `MERGE`s (no deletes).

Notes:

- `cve_id` is the id itself (GIT-ecosystem records are `CVE-*`), else the first CVE
  alias, else the first CVE in `upstream` (distro records such as `UBUNTU-CVE-*`).
- Withdrawn records stay in the tables with `date_withdrawn` set; filter
  `date_withdrawn IS NULL` for active analysis.
- `affected_packages.ecosystem` keeps the full OSV value (`Debian:12`); `ecosystems`
  holds base names (`Debian`) for filtering.
- Linux-distro feeds are most of the rows. Filter on `ecosystems` or `id_prefix` for
  language-package analysis.

## CISA Known Exploited Vulnerabilities (KEV)

KEV is the authoritative "this CVE is being actively exploited in the wild" list.
Joining `cve_id` turns any CVE, package, or product finding into a prioritization
signal.

- `kev_raw` — one row per cveID: `raw` VARIANT + `cve_id`, `date_added`, the
  catalog-level `catalog_version` / `date_released`, and the soft-delete marker
  `date_removed`. Change Data Feed on.
- `kev_vulnerabilities` — typed silver, kept in sync from `kev_raw`.
- `kev_fetch_log` — one row per load run (fetch time, catalog version, release time,
  counts, active-row count).

Data facts (checked 2026-10-07):

- The catalog is a single ~1.8 MB JSON file: catalog-level `catalogVersion`
  (date-based, e.g. `2026.10.04`), `dateReleased`, `count`, and a
  `vulnerabilities[]` array (~1,734 records).
- Each record is flat: `cveID`, `vendorProject`, `product`, `vulnerabilityName`,
  `dateAdded`, `shortDescription`, `requiredAction`, `dueDate`,
  `knownRansomwareCampaignUse` (`Known`/`Unknown`), `forensicTriage`, `notes`
  (semicolon-separated URLs), and `cwes[]` (empty for ~10% of records).
- **There is no per-record modified timestamp.** The catalog version is the only
  freshness token, so the loader refreshes a matched row only when the catalog
  version advanced (or a previously removed cveID reappears).
- The catalog is **cumulative** — entries are not aged out when a vulnerability is
  patched. Removals are rare and are CISA **data-quality corrections** (a CVE added
  in error, or a CVE record later REJECTED/disputed), never de-exploitation.

The loader downloads the catalog, stages one JSON line per vulnerability on the
Volume, then `MERGE`s into `kev_raw` on `cve_id`. **Soft delete:** cveIDs absent from
the snapshot keep their row with `date_removed` set to the catalog's release time
(stamped once, on first disappearance); a reappearing cveID clears it back to NULL.
Nothing is ever physically deleted, so a truncated download cannot lose data.

Notes:

- `known_ransomware_campaign_use` keeps the source string (`Known` / `Unknown`);
  filter `= 'Known'` for the ransomware subset.
- Filter `date_removed IS NULL` for the active catalog; keep removed rows for a
  historical "ever on KEV" view.
- `LEFT JOIN kev_vulnerabilities` onto any CVE-keyed table on `cve_id` and treat a
  non-NULL match (with `date_removed IS NULL`) as "known-exploited".

## Layout

```
databricks.yml                  bundle definition and targets
resources/variables.yml         bundle variables
resources/vuln_*.job.yml        one file per job
resources/reference.volume.yml  the scratch Volume
src/common/
  params.py                     job parameters (widgets), table/Volume names
  staging.py                    gzip JSON-line staging parts on the Volume
  fetch.py                      single-record downloads with retries
  merge.py                      dedupe before MERGE, last change per key from CDF
  fetch_log.py                  fetch-log rows and incremental watermarks
  silver.py                     generic raw CDF -> silver streaming MERGE
src/<source>/                   cve_v5, ghsa, osv, kev
  helpers.py                    pure Python / raw-table id columns
  transform.py                  raw VARIANT -> typed silver columns
  *load*.py, Load *.py          loader notebooks
  * Silver Transform.py         silver notebook (CREATE TABLE with comments + sync)
tests/                          pytest + pytest-spark unit tests and sample records
```

## Tests

```sh
poetry env use "$(uv python find 3.12)"
poetry install --with test
poetry run pytest
```

The tests run locally on PySpark (no Databricks connection needed): pure-Python
helper tests plus Spark tests of the extract/transform functions against sample
records in `tests/data/`.

## Adding a source

1. Create `src/<source>/` with `__init__.py`, `helpers.py` (raw-table id columns),
   `transform.py` (raw VARIANT → silver columns; read fields with
   `try_variant_get`), a load notebook and a silver notebook. Start each notebook with
   the serverless environment header and a "Parameters" cell that calls
   `common.params.get_params`; the silver notebook creates the table (with column
   comments) and calls `common.silver.stream_cdf_to_silver`.
2. Add `resources/vuln_<source>.job.yml` (copy `vuln_kev.job.yml`) and a
   `<source>_schedule` variable defaulting to `${var.schedule}`.
3. Add tests under `tests/unit/` with sample records in `tests/data/<source>/`.
