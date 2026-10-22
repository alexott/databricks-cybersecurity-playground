-- Comments for the GitHub Advisory (GHSA) silver table.
--
-- This file is the canonical source of table and column descriptions for the
-- github_advisories silver table. It is also embedded into the CREATE TABLE
-- statement in `GHSA Silver Transform.py` so new deployments get these comments
-- automatically.
--
-- To apply against an existing table in another workspace, replace the catalog and
-- schema below with your target and run the statements with a SQL warehouse.
-- Single-quotes in comments are escaped as ''.

USE CATALOG cybersecurity;
USE SCHEMA datasets;

-- =============================================================================
-- Table comment
-- =============================================================================
COMMENT ON TABLE github_advisories IS
  'Silver-layer Delta table of GitHub Security Advisories (GHSA), sourced from the github/advisory-database repo (OSV format). Each row is one advisory with structured metadata extracted from the OSV JSON: ids (ghsa_id, cve_id, aliases), free-text summary/details, GitHub severity label and CVSS version/vector, CWE classification (cwe_ids), the package-level affected_packages edge (ecosystem, package, purl, version range) that CVE v5 affected_products lacks, a convenience ecosystems array, references, the github_reviewed quality flag, lifecycle timestamps (published/modified/withdrawn), and the original raw VARIANT. Join cve_id to cves_v5 to enrich CVEs with package dependency edges. Filter github_reviewed = TRUE for curated package data and date_withdrawn IS NULL to exclude retracted advisories.';

-- =============================================================================
-- Column comments
-- =============================================================================

ALTER TABLE github_advisories ALTER COLUMN ghsa_id
  COMMENT 'GitHub Security Advisory identifier (GHSA-xxxx-xxxx-xxxx). Primary key.';

ALTER TABLE github_advisories ALTER COLUMN cve_id
  COMMENT 'First CVE id among aliases (NULL if none). Join key to cves_v5. See aliases for the complete list.';

ALTER TABLE github_advisories ALTER COLUMN aliases
  COMMENT 'All external ids this advisory aliases (CVE-*, other GHSA-*, etc.).';

ALTER TABLE github_advisories ALTER COLUMN related
  COMMENT 'Ids of related advisories (OSV `related`); NULL/empty when absent.';

ALTER TABLE github_advisories ALTER COLUMN summary
  COMMENT 'Short one-line advisory summary.';

ALTER TABLE github_advisories ALTER COLUMN details
  COMMENT 'Full Markdown advisory details.';

ALTER TABLE github_advisories ALTER COLUMN severity_label
  COMMENT 'GitHub qualitative severity: ''CRITICAL'', ''HIGH'', ''MODERATE'', ''LOW''. From database_specific.severity.';

ALTER TABLE github_advisories ALTER COLUMN cvss_version
  COMMENT 'CVSS metric type of severity[0]: ''CVSS_V3'' or ''CVSS_V4''.';

ALTER TABLE github_advisories ALTER COLUMN cvss_vector
  COMMENT 'CVSS vector string of severity[0] (OSV stores the vector, not a numeric base score).';

ALTER TABLE github_advisories ALTER COLUMN cwe_ids
  COMMENT 'CWE identifiers (CWE-NNN) from database_specific.cwe_ids. Use EXPLODE to filter/join.';

ALTER TABLE github_advisories ALTER COLUMN ecosystems
  COMMENT 'Distinct package ecosystems across affected_packages (e.g. Maven, npm, PyPI, Go). Convenience for filtering.';

ALTER TABLE github_advisories ALTER COLUMN affected_packages
  COMMENT 'Package-level affected edges, one struct per (package, version range). ecosystem+package identify the component; introduced/fixed/last_affected bound the vulnerable range (range_type ECOSYSTEM/SEMVER/GIT). Packages with only an explicit versions list keep a struct with null version bounds. Use LATERAL VIEW EXPLODE for the dependency graph. This is the package dependency edge CVE v5 affected_products lacks.';

ALTER TABLE github_advisories ALTER COLUMN `references`
  COMMENT 'Advisory reference URLs with their OSV type (ADVISORY, WEB, PACKAGE, FIX, ...).';

ALTER TABLE github_advisories ALTER COLUMN github_reviewed
  COMMENT 'TRUE when the advisory comes from the github-reviewed tree (curated ecosystem/version data); FALSE for the unreviewed CVE-mirror tree. Filter to TRUE for high-quality package data.';

ALTER TABLE github_advisories ALTER COLUMN date_published
  COMMENT 'OSV `published` timestamp.';

ALTER TABLE github_advisories ALTER COLUMN date_modified
  COMMENT 'OSV `modified` timestamp; the high-water mark used for incremental upserts.';

ALTER TABLE github_advisories ALTER COLUMN date_withdrawn
  COMMENT 'OSV `withdrawn` timestamp; non-NULL means the advisory was retracted. Exclude withdrawn advisories for active analysis.';

ALTER TABLE github_advisories ALTER COLUMN schema_version
  COMMENT 'OSV schema version of the source record.';

ALTER TABLE github_advisories ALTER COLUMN raw
  COMMENT 'Raw VARIANT of the original OSV/GHSA record. Use for fields not surfaced into the silver layer (ecosystem_specific, database_specific.*, multi-segment ranges).';
