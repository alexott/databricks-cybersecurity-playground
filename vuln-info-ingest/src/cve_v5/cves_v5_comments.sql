-- Comments for the CVE v5 silver table.
--
-- This file is the canonical source of table and column descriptions for the CVE v5
-- silver table. It is also embedded into the CREATE TABLE statement in
-- `CVE v5 Silver Transform.py` so new deployments get these comments automatically.
--
-- To apply against an existing table in another workspace, replace the table name
-- below with your target three-part identifier (catalog.schema.table) and run the
-- statements with a SQL warehouse. Single-quotes in comments are escaped as ''.

-- Replace the catalog and schema with your target:
USE CATALOG cybersecurity;
USE SCHEMA datasets;

-- =============================================================================
-- Table comment
-- =============================================================================
COMMENT ON TABLE cves_v5 IS
  'Silver-layer Delta table of CVE v5 (Common Vulnerabilities and Exposures version 5) records, sourced from MITRE/NVD and other CVE Numbering Authorities (CNAs). Each row is one CVE with structured metadata extracted from the original CVE v5 JSON: identifiers (cve_id, cve_year, assigner_*), lifecycle timestamps (date_reserved/published/updated), publication state, free-text title and description, an array of affected products (with vendors, products, platforms, and version ranges), CVSS scoring with CNA-preferred / CISA-ADP-fallback (cvss_version, cvss_base_score, cvss_severity, cvss_vector, cvss_source), CWE classification (cwe_ids array, with cwe_id/cwe_description kept for the first entry), SSVC priority signals (ssvc_exploitation, ssvc_automatable, ssvc_technical_impact), an array of references, and the original raw VARIANT record. Use the extracted columns directly for analytics; consult `raw` only for fields not surfaced into the silver layer. Always filter `state = ''PUBLISHED''` to exclude rejected/invalid CVEs.';

-- =============================================================================
-- Column comments
-- =============================================================================

ALTER TABLE cves_v5 ALTER COLUMN cve_id
  COMMENT 'CVE identifier in the format CVE-YYYY-NNNNN.';

ALTER TABLE cves_v5 ALTER COLUMN cve_year
  COMMENT 'Year component of the CVE ID (e.g., 2026 for CVE-2026-12345).';

ALTER TABLE cves_v5 ALTER COLUMN state
  COMMENT 'Publication state of the CVE record. Values: ''PUBLISHED'' (active), ''REJECTED'' (invalid / withdrawn). Filter to ''PUBLISHED'' for actionable analysis.';

ALTER TABLE cves_v5 ALTER COLUMN date_published
  COMMENT 'Timestamp when the CVE was first published. Use for recency filtering.';

ALTER TABLE cves_v5 ALTER COLUMN date_reserved
  COMMENT 'Timestamp when the CVE ID was reserved by a CNA (before publication details were available).';

ALTER TABLE cves_v5 ALTER COLUMN date_updated
  COMMENT 'Timestamp of the most recent update to the CVE record. Use to detect re-scored or re-classified vulnerabilities.';

ALTER TABLE cves_v5 ALTER COLUMN assigner_org_id
  COMMENT 'Internal organization ID of the CVE Numbering Authority (CNA) that assigned this CVE.';

ALTER TABLE cves_v5 ALTER COLUMN assigner_short_name
  COMMENT 'Short name of the assigning authority (e.g., ''mitre'', ''microsoft'', ''redhat''). Useful for filtering by issuer or grouping by ecosystem.';

ALTER TABLE cves_v5 ALTER COLUMN title
  COMMENT 'Short human-readable title of the CVE.';

ALTER TABLE cves_v5 ALTER COLUMN description
  COMMENT 'Full natural-language description of the vulnerability. Use full-text search (ILIKE / contains) when keyword filtering is needed.';

ALTER TABLE cves_v5 ALTER COLUMN affected_products
  COMMENT 'Array of structs describing affected products. Each entry has fields like vendor, product, defaultStatus, platforms, and versions. Use LATERAL VIEW EXPLODE to filter on vendor/product.';

ALTER TABLE cves_v5 ALTER COLUMN cvss_version
  COMMENT 'CVSS specification version used for scoring. Values: ''2.0'', ''3.0'', ''3.1'', ''4.0''.';

ALTER TABLE cves_v5 ALTER COLUMN cvss_base_score
  COMMENT 'Numeric CVSS base score (0.0-10.0). Higher is more severe.';

ALTER TABLE cves_v5 ALTER COLUMN cvss_severity
  COMMENT 'Qualitative CVSS severity. Values: ''CRITICAL'', ''HIGH'', ''MEDIUM'', ''LOW'', ''NONE''. Prefer this over cvss_base_score for human-friendly queries.';

ALTER TABLE cves_v5 ALTER COLUMN cvss_vector
  COMMENT 'Raw CVSS vector string encoding the attack characteristics.';

ALTER TABLE cves_v5 ALTER COLUMN cvss_source
  COMMENT 'Source that produced the CVSS score (''CNA'' = CVE Numbering Authority, ''ADP'' = Authorized Data Publisher, typically CISA-ADP).';

ALTER TABLE cves_v5 ALTER COLUMN cwe_ids
  COMMENT 'All Common Weakness Enumeration identifiers (CWE-NNN) classifying the underlying weaknesses, collected across every problemType/description entry. Use EXPLODE to filter or join on individual CWEs. A CVE can carry several CWEs.';

ALTER TABLE cves_v5 ALTER COLUMN cwe_id
  COMMENT 'First CWE identifier (= cwe_ids[0]), kept for backward compatibility. Prefer cwe_ids for complete weakness classification.';

ALTER TABLE cves_v5 ALTER COLUMN cwe_description
  COMMENT 'Human-readable description of the first CWE classification.';

ALTER TABLE cves_v5 ALTER COLUMN ssvc_exploitation
  COMMENT 'SSVC (Stakeholder-Specific Vulnerability Categorization) — exploitation maturity. Values: ''active'' (in-the-wild exploitation), ''poc'' (proof-of-concept exists), ''none'' (no public exploitation). Filter to ''active'' for highest-priority CVEs.';

ALTER TABLE cves_v5 ALTER COLUMN ssvc_automatable
  COMMENT 'SSVC — whether exploitation is automatable. Values: ''yes'', ''no''. ''yes'' means an attacker can script exploitation at scale.';

ALTER TABLE cves_v5 ALTER COLUMN ssvc_technical_impact
  COMMENT 'SSVC — severity of impact upon successful exploitation. Values: ''partial'', ''total''.';

ALTER TABLE cves_v5 ALTER COLUMN `references`
  COMMENT 'Array of structs containing reference URLs (vendor advisories, blog posts, exploit code). Each entry has url, name, and tags.';

ALTER TABLE cves_v5 ALTER COLUMN raw
  COMMENT 'Raw VARIANT JSON of the original CVE v5 record. Use for fields not extracted into the silver layer.';
