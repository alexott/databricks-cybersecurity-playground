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
| [FIRST EPSS](https://www.first.org/epss/) | `epss_scores`, `epss_scores_history` | Daily probability of exploitation in the next 30 days |
| [MITRE CWE](https://cwe.mitre.org/) | `cwe_entries` | Weakness names, hierarchy, CAPEC attack patterns for the CWE ids of the other tables |
| [CAPEC](https://capec.mitre.org/) + [ATT&CK](https://attack.mitre.org/) | `cwe_capec_attack` | CWE → CAPEC → ATT&CK technique edges (one-time build) |
| [MITRE ATT&CK](https://attack.mitre.org/) | `attack_objects`, `attack_relationships` (+ views) | Techniques, tactics, groups, software, mitigations and how they relate |
| [MITRE ATLAS](https://atlas.mitre.org/) | `atlas_objects`, `atlas_relationships` (+ views) | ATT&CK-style matrix of attacks on AI systems, with case studies |

All silver tables carry `cve_id`, the join key between them. Every source except EPSS
has a `<source>_raw` table (one row per record with the original JSON as `VARIANT` plus
id columns, Change Data Feed on) and a `<source>_fetch_log` table (one row per load
run). The silver tables are kept in sync from the raw tables by streaming their
change feed. EPSS ships already typed CSV files, so its loader writes the typed
`epss_scores_history` table directly (see [EPSS](#first-epss-exploit-prediction-scoring-system)).
Column comments are part of the silver `CREATE TABLE` statements.

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

All jobs run on serverless compute in the `STANDARD` performance mode
(`performance_target` in the job YAML): startup is slower than
`PERFORMANCE_OPTIMIZED`, but runs cost less, and none of these jobs is latency
critical.

Bundle variables (`resources/variables.yml`):

| Variable | Default | Description |
|---|---|---|
| `catalog` | — (set per target) | Catalog for all tables and the scratch Volume |
| `schema` | — (set per target) | Schema for all tables and the scratch Volume |
| `volume` | `reference` | Scratch Volume for staging files and streaming checkpoints |
| `schedule` | `0 0 */6 * * ?` | Default Quartz cron schedule of the periodic jobs |
| `cve_v5_schedule`, `ghsa_schedule`, `osv_schedule`, `kev_schedule` | `${var.schedule}` | Per-source schedule override |
| `epss_schedule` | `0 0 15 * * ?` | Schedule of the EPSS job (daily, after the ~13:30 UTC publication) |
| `cwe_schedule` | `0 0 4 ? * MON` | Schedule of the CWE job (weekly; the catalog is released a few times a year) |
| `attack_version` | `latest` | ATT&CK version of the ATT&CK and `cwe_capec_attack` jobs, e.g. `19.1` to pin |
| `atlas_version` | `latest` | ATLAS release of the ATLAS job, e.g. `2026.10` to pin |
| `atlas_schedule` | `0 0 5 ? * MON` | Schedule of the ATLAS job (weekly; ATLAS is released about monthly) |
| `attack_schedule` | `0 30 4 ? * MON` | Schedule of the ATT&CK job (weekly; ATT&CK is released about twice a year) |
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

To build the `cwe_capec_attack` edge table, run its one-time job:

```sh
databricks bundle run vuln_cwe_capec_attack -t <target> --profile <profile>
```

For the EPSS score history since 2021, run the EPSS backfill once (the periodic job
only looks back `lookback_days`, 14 by default):

```sh
databricks bundle run vuln_epss_initial_load -t <target> --profile <profile>
```

GHSA, KEV, CWE, ATT&CK and ATLAS always load a full snapshot, so their periodic jobs need no preparation.
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
| `vuln_epss` | `load` → `current` | `epss_schedule` |
| `vuln_epss_initial_load` | `load` → `current` | none |
| `vuln_cwe` | `load` → `silver` | `cwe_schedule` |
| `vuln_cwe_capec_attack` | `build` | none |
| `vuln_attack` | `load` → `silver` | `attack_schedule` |
| `vuln_atlas` | `load` → `silver` | `atlas_schedule` |

All settings are job parameters (no config files); defaults are in the job YAML files
and can be changed per run. Every job has `catalog`, `schema` and `volume`; besides
those:

- CVE v5: `tarball_url`, `delta_log_url`, `full_reload_threshold` (5000), `fetch_threads` (16), `watermark_overlap_minutes` (30), `part_records` (20000).
- GHSA: `tarball_url`, `part_records` (20000).
- OSV: `bucket_url`, `full_reload_threshold` (100000), `watermark_overlap_hours` (6), `fetch_threads` (32), `min_snapshot_ratio` (0.9), `part_records` (20000).
- KEV: `catalog_url`.
- CWE: `catalog_url`.
- ATT&CK: `index_url`, `domains` (`enterprise-attack,mobile-attack,ics-attack`), `attack_version` (`${var.attack_version}`).
- ATLAS: `manifest_url`, `atlas_version` (`${var.atlas_version}`).
- CWE → CAPEC → ATT&CK: `capec_url`, `index_url`, `attack_version` (`${var.attack_version}`); no `volume`.
- EPSS: `base_url`, `lookback_days` (14; 3000 in the initial-load job), `batch_days` (90), `fetch_threads` (8), `min_snapshot_ratio` (0.9).

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

## FIRST EPSS (Exploit Prediction Scoring System)

[EPSS](https://www.first.org/epss/) estimates, for every published CVE, the
probability that it is exploited in the wild in the next 30 days. KEV says "is being
exploited"; EPSS says "how likely it is to be", for all CVEs, recomputed daily.

- `epss_scores_history` — one row per CVE per day since 2021-04-14: `cve_id`,
  `score_date`, `epss`, `percentile`, `model_version`. Clustered by `(score_date, cve_id)`.
- `epss_scores` — the newest day only, one row per scored CVE. Change Data Feed on.
- `epss_fetch_log` — one row per load run (dates missing/loaded, model version, rows).

Data facts (checked 2026-11-04):

- One gzip CSV per day, `epss_scores-YYYY-MM-DD.csv.gz` (~2.8 MB, ~400K CVEs today),
  published around 13:30 UTC on `https://epss.empiricalsecurity.com` (S3 behind
  CloudFront; `epss_scores-current.csv.gz` redirects to the newest file). The same
  files, history included, are in the GitHub repository
  [empiricalsec/epss_scores](https://github.com/empiricalsec/epss_scores); the loader
  uses the CDN to stay clear of GitHub's rate limits for unauthenticated downloads.
- Three formats: `cve,epss` (until 2021-08), `cve,epss,percentile` (from 2021-09), and
  since 2022-02-04 a first line `#model_version:<v>,score_date:<ts>`. From 2022-02-04
  to 2025-03-16 that line is a separate gzip member (a multi-member gzip file).
- A date that is not published yet answers HTTP 403 (S3 for a missing key).
- Scores shift sharply on model changes: v2 (`v2022.01.01`) from 2022-02-04, v3
  (`v2023.03.01`) from 2023-03-07, v4 (`v2025.03.14`) from 2025-03-17; the current model is
  `v2026.06.15`. The repository's `beta_scores/` (EPSS v5 beta) is not loaded.
- Every CVE can change score every day, so most rows change daily; there is no
  per-record modified time.

**Load** (`load` task) lists the dates in the last `lookback_days` (not before
2021-04-14) that are missing from `epss_scores_history`, downloads their files in
parallel to the Volume, `batch_days` dates at a time, and writes each batch with
`replaceWhere score_date IN (<dates>)`. A date that is not published yet is skipped and
loaded by a later run, so gaps fill themselves. The initial-load job is the same
notebook with `lookback_days=3000`; if it fails midway, a re-run loads only the dates
still missing. The column layout of every file is validated before loading.

**Current** (`current` task) `MERGE`s the newest score date of the history into
`epss_scores` on `cve_id` and deletes CVEs no longer scored (mostly REJECTED CVEs; the
history keeps them). It refuses to merge a day with fewer than `min_snapshot_ratio` of
the current rows.

Notes:

- `percentile` is NULL before 2021-09 and `model_version` is NULL before 2022-02-04
  (EPSS v1 files had no version line).
- Compare scores across `model_version` changes with care; for trends, compare days
  with the same `model_version`.
- `LEFT JOIN epss_scores` on `cve_id` to rank CVE, package or product findings, e.g.
  `epss >= 0.1` or `percentile >= 0.95`, and combine with `kev_vulnerabilities`.

## MITRE CWE (Common Weakness Enumeration)

[CWE](https://cwe.mitre.org/) is the catalog behind the `cwe_ids` / `cwes` columns of
the other tables. `cwe_entries` gives them names, descriptions, the weakness hierarchy,
MITRE's mapping guidance and the CAPEC attack patterns that exploit them.

- `cwe_raw` — one row per CWE id: `raw` VARIANT (JSON rendering of the XML entry) +
  `cwe_id`, `entry_type`, `catalog_version`, `catalog_date`, and the soft-delete
  marker `date_removed`. Change Data Feed on.
- `cwe_entries` — typed silver, kept in sync from `cwe_raw`.
- `cwe_fetch_log` — one row per load run (catalog version and date, counts).

Data facts (checked 2026-11-05):

- The catalog is one zipped XML file, `cwec_latest.xml.zip` (~2 MB, ~19 MB unzipped).
  CWE 5.0 (2026-10-28) uses schema v8, which renamed Categories to **Groupings**.
- 1,477 entries: 984 weaknesses, 431 groupings, 62 views. CWE ids are unique across
  the three kinds, and ids are not removed: obsolete entries stay with status
  `Deprecated` or `Obsolete`.
- In `cves_v5` (2026-11), 99.4% of the CWE references are weaknesses, 0.6% are
  groupings, and a handful are views — so all three kinds are loaded.
- New releases come a few times a year; entries have no reliable per-record modified
  time.

The loader downloads the zip to memory and converts every entry to JSON: attributes
and child elements become object keys, elements the CWE schema allows to repeat are
always arrays (so every VARIANT path has one type), and XHTML text (paragraphs, lists,
code) is rendered to plain text. It stages one JSON line per entry on the Volume and
`MERGE`s into `cwe_raw` on `cwe_id`, KEV-style: the catalog version is the freshness
token, and ids absent from the snapshot are soft-deleted with `date_removed`.

Notes:

- Filter `status NOT IN ('Deprecated', 'Obsolete')` for current entries;
  `mapping_usage` says whether CVEs should be mapped to the entry (`Allowed`,
  `Allowed-with-Review`, `Discouraged`, `Prohibited` — groupings and views are
  `Prohibited`).
- `related_weaknesses` holds the CWE graph; `view_id = 'CWE-1000'` with
  `nature = 'ChildOf'` is the research hierarchy, `ordinal = 'Primary'` the main parent.
- `related_capec_ids` links weaknesses to CAPEC attack patterns; `observed_examples`
  references real CVEs.
- `raw` keeps the fields not flattened (mitigations, detection methods, demonstrative
  examples, taxonomy mappings, references). The order between different tags is not
  kept, e.g. the interleaved `Body_Text` / `Example_Code` of a demonstrative example.

### CWE → CAPEC → ATT&CK edges

`cwe_capec_attack` links weaknesses to attacker behavior. Each row is one edge
(`src_type`, `src_id`, `rel`, `dst_type`, `dst_id`, `dst_name`, `source_version`):

- `CWE` → `related_attack_pattern` → `CAPEC`: the CAPEC attack patterns that exploit a
  weakness (`Related_Weaknesses` of each CAPEC attack pattern; 1,212 edges).
- `CAPEC` → `maps_to_technique` → `technique`: the ATT&CK techniques of an attack
  pattern (its `ATTACK` taxonomy mappings; 270 edges), named from the Enterprise
  ATT&CK STIX bundle of the `attack_version` bundle variable (the same as the ATT&CK job).

The `vuln_cwe_capec_attack` job downloads both files and rebuilds the table with
`CREATE OR REPLACE`. It has no schedule: CAPEC has not been released since 3.9
(2023-01-24). Re-run it after a CAPEC release or to move to another ATT&CK version
(ATT&CK 19.1 and 20.0 give the same edges today). With `attack_version=latest` a
later rebuild can pick up renamed techniques; `source_version` records the versions used.

Notes:

- Mappings to techniques that are not in Enterprise ATT&CK are dropped (CAPEC-648 →
  T1513, a Mobile technique); duplicate edges in CAPEC are dropped.
- CAPEC 3.9 maps to 14 technique ids that ATT&CK has since revoked (e.g. `T1562.002`);
  they are kept with their ATT&CK name.
- From a CVE to techniques: explode `cves_v5.cwe_ids`, join `src_id` of the CWE edges,
  then join their `dst_id` to `src_id` of the CAPEC edges.

## MITRE ATT&CK

[ATT&CK](https://attack.mitre.org/) describes adversary behavior: tactics, techniques,
the groups and software that use them, and mitigations. It is loaded from MITRE's
[attack-stix-data](https://github.com/mitre-attack/attack-stix-data) (STIX 2.1).

- `attack_raw` — one row per STIX object: `raw` VARIANT + `stix_id`, `type`,
  `attack_id` (T1059, G0016, S0154, ...), `modified`, `attack_version`, and the
  soft-delete marker `date_removed`. Change Data Feed on.
- `attack_objects` — typed silver for every object except relationships, kept in sync
  from `attack_raw`.
- `attack_relationships` — typed silver for the relationship objects, from the same
  change feed.
- Views over the silver tables: `attack_techniques`, `attack_tactics`, `attack_groups`,
  `attack_software`, `attack_mitigations`, `attack_campaigns`, and
  `attack_relationships_resolved` (source and target as ATT&CK ids and names).
- `attack_fetch_log` — one row per load run (domains, versions, counts).

Data facts (checked 2026-11-05, ATT&CK 20.0, released 2026-10-13):

- One STIX bundle per domain: Enterprise (~55 MB, 26,690 objects), Mobile (~6 MB,
  2,684), ICS (~5 MB, 2,566). `index.json` lists every version of each domain.
- Groups, software, data sources and some relationships are in several domains with
  the same STIX id and identical content; they are one row, `domains` lists the
  domains. All domains together are ~31.7K objects.
- Enterprise has 21,819 relationships: `uses` (groups, software and campaigns →
  techniques and software), `mitigates`, `detects`, `subtechnique-of`, `revoked-by`,
  `attributed-to`.
- Revoked and deprecated objects stay in the bundles with `revoked` /
  `x_mitre_deprecated` set (149 revoked Enterprise techniques).

The loader resolves the versions in `index.json` (`attack_version=latest` or a pinned
version) and stops if the fetch log already has them. Otherwise it downloads the
bundles, keeps one object per STIX id, stages one JSON line per object on the Volume,
and `MERGE`s into `attack_raw` on `stix_id`: update when `modified` advanced, insert
new objects, soft-delete objects that are no longer in the bundles.

Notes:

- Because updates are gated by `modified`, pinning an older version does not roll
  rows back; drop the tables (and the `attack_checkpoints` directory) to reload one.
  Removing a domain from `domains` soft-deletes its objects.
- Filter `NOT revoked AND NOT deprecated` for the current matrix; a revoked object's
  replacement is its `revoked-by` relationship.
- `attack_techniques.technique_id` joins to `cwe_capec_attack.dst_id`. That job
  downloads the Enterprise bundle itself (the same `attack_version`) and does not need
  these tables.
- The `attack_version` bundle variable sets the version of both jobs, e.g.
  `--var attack_version=19.1` to pin.

## MITRE ATLAS

[ATLAS](https://atlas.mitre.org/) (Adversarial Threat Landscape for AI Systems) is an
ATT&CK-style knowledge base of attacks on AI and ML systems, with case studies of
real incidents and exercises. It is loaded from MITRE's
[atlas-data](https://github.com/mitre-atlas/atlas-data) releases.

- `atlas_raw` — one row per object and per relationship: `raw` VARIANT + `atlas_id`,
  `object_type`, `modified`, `content_hash`, `atlas_release`, and the soft-delete
  marker `date_removed`. Change Data Feed on.
- `atlas_objects` — typed silver for tactics, techniques, mitigations, case studies,
  entities, software and the matrix, kept in sync from `atlas_raw`.
- `atlas_relationships` — typed silver for the relationships, from the same change feed.
- Views: `atlas_techniques` (with parent and tactics), `atlas_tactics` (in matrix
  order), `atlas_mitigations`, `atlas_case_studies`, `atlas_software`,
  `atlas_entities`, `atlas_relationships_resolved`.
- `atlas_fetch_log` — one row per load run (release, release date, counts).

Data facts (checked 2026-11-05, release 2026.10 of 2026-10-28):

- Releases are about monthly. `dist/manifest.yaml` lists every release with its
  file; the current format is 6 (`dist/v6/ATLAS-<release>.yaml`, ~1.1 MB, format
  6.1.0 since 2026.10).
- 2026.10 has 16 tactics, 221 techniques (incl. sub-techniques), 44 mitigations,
  77 case studies, 44 entities, 37 software and 1,750 relationships.
- Objects are keyed by ATLAS id (`AML.T0000`, `AML.T0000.003`, `AML.CS0002`, ...) and
  have created / modified dates. Relationships have neither an id nor a date; their
  key is `<source>|<type>|<target>|<step-id>` and changes are detected with a content
  hash. Technique tactics and parents are relationships (`achieves`, `specializes`).
- The STIX bundle in `atlas-navigator-data` and the format-5 `dist/ATLAS.yaml` lag
  behind (170 techniques), so the loader reads the format-6 YAML.

The loader resolves the release in the manifest (`atlas_version=latest` or pinned)
and stops if the fetch log already has it. Otherwise it downloads the release YAML,
flattens it into one record per object and relationship, stages them on the Volume,
and `MERGE`s into `atlas_raw` on `atlas_id`: update when the content hash changed,
insert new records, soft-delete records that are no longer in the release.

Notes:

- `attack_id` is the corresponding ATT&CK object (`attack-reference`), e.g. `T1596`
  for `AML.T0000`; join it to `attack_objects.attack_id` / `attack_techniques`.
- Case-study procedures are `employs` relationships, one row per step (`step_id`,
  `tactic_id`, `leads_to`).
- Pinning an older release overwrites changed rows with the older content (changes
  are detected by hash, not date).

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
src/<source>/                   cve_v5, ghsa, osv, kev, epss, cwe, capec, attack, atlas
  helpers.py                    pure Python / raw-table id columns
  transform.py                  raw VARIANT -> typed silver columns
  *load*.py, Load *.py          loader notebooks
  * Silver Transform.py         silver notebook (CREATE TABLE with comments + sync);
                                EPSS: EPSS Current Scores.py (newest day of the history)
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
