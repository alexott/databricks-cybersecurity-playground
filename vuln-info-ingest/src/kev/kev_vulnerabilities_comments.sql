-- Comments for the CISA KEV silver table.
--
-- This file is the canonical source of table and column descriptions for the
-- kev_vulnerabilities silver table. It is also embedded into the CREATE TABLE
-- statement in `KEV Silver Transform.py` so new deployments get these comments
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
COMMENT ON TABLE kev_vulnerabilities IS
  'Silver-layer Delta table of the CISA Known Exploited Vulnerabilities (KEV) catalog (https://www.cisa.gov/known-exploited-vulnerabilities-catalog). Each row is one CVE CISA has confirmed is actively exploited in the wild, with the affected vendor/product, CISA''s required remediation action and due date, the ransomware-use assessment, forensic-triage recommendation, associated CWEs, and the original raw VARIANT. The catalog is cumulative (entries are not aged out when patched); removals are rare CISA data-quality corrections and are soft-deleted via date_removed rather than physically dropped. Join cve_id to cves_v5 / github_advisories / osv_vulnerabilities to flag which CVEs are known-exploited. Filter date_removed IS NULL for the active catalog.';

-- =============================================================================
-- Column comments
-- =============================================================================

ALTER TABLE kev_vulnerabilities ALTER COLUMN cve_id
  COMMENT 'CVE identifier (CVE-YYYY-NNNN). Primary key and the join key to cves_v5, github_advisories and osv_vulnerabilities.';

ALTER TABLE kev_vulnerabilities ALTER COLUMN vendor_project
  COMMENT 'Vendor or project that produces the affected product (CISA `vendorProject`).';

ALTER TABLE kev_vulnerabilities ALTER COLUMN product
  COMMENT 'Affected product name (CISA `product`).';

ALTER TABLE kev_vulnerabilities ALTER COLUMN vulnerability_name
  COMMENT 'CISA''s short name for the vulnerability (`vulnerabilityName`).';

ALTER TABLE kev_vulnerabilities ALTER COLUMN short_description
  COMMENT 'One-paragraph description of the vulnerability (`shortDescription`).';

ALTER TABLE kev_vulnerabilities ALTER COLUMN required_action
  COMMENT 'The remediation CISA requires for federal agencies (`requiredAction`).';

ALTER TABLE kev_vulnerabilities ALTER COLUMN date_added
  COMMENT 'Date CISA added this CVE to the KEV catalog (`dateAdded`).';

ALTER TABLE kev_vulnerabilities ALTER COLUMN due_date
  COMMENT 'Date by which the required action must be completed (`dueDate`).';

ALTER TABLE kev_vulnerabilities ALTER COLUMN known_ransomware_campaign_use
  COMMENT 'CISA''s assessment of ransomware use: ''Known'' or ''Unknown'' (faithful source value).';

ALTER TABLE kev_vulnerabilities ALTER COLUMN forensic_triage
  COMMENT 'Whether CISA recommends forensic triage for this CVE (`forensicTriage`, e.g. ''Yes'').';

ALTER TABLE kev_vulnerabilities ALTER COLUMN notes
  COMMENT 'Free-text notes, usually a semicolon-separated list of reference URLs (`notes`).';

ALTER TABLE kev_vulnerabilities ALTER COLUMN cwes
  COMMENT 'CWE identifiers (CWE-NNN) CISA associates with the CVE; empty array when none. Use EXPLODE to filter/join.';

ALTER TABLE kev_vulnerabilities ALTER COLUMN catalog_version
  COMMENT 'CISA catalog version this row was last refreshed from (`catalogVersion`, e.g. ''2026.10.04'').';

ALTER TABLE kev_vulnerabilities ALTER COLUMN date_released
  COMMENT 'Release timestamp of that catalog version (`dateReleased`).';

ALTER TABLE kev_vulnerabilities ALTER COLUMN date_removed
  COMMENT 'Catalog version''s release timestamp in which this cveID first disappeared from KEV; NULL means currently listed. Removals are rare CISA corrections (CVE added in error or later REJECTED), not de-exploitation. Filter date_removed IS NULL for the active catalog.';

ALTER TABLE kev_vulnerabilities ALTER COLUMN raw
  COMMENT 'Raw VARIANT of the original per-vulnerability KEV record.';
