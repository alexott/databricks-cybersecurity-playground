"""The Genie space "Analysis of Vulnerabilities". Content: genie/analysis_of_vulnerabilities.geniespace.json."""
from __future__ import annotations

from pathlib import Path

from databricks.bundles.genie_spaces import GenieSpace

SPACE_FILE = Path(__file__).resolve().parents[1] / "genie" / "analysis_of_vulnerabilities.geniespace.json"

DESCRIPTION = """\
This space provides vulnerability intelligence for triage and patch prioritization, combining CVE v5 records with exploitation, exploit-prediction, open-source package and weakness data.

**Capabilities:**
- Analyze CVE trends, severity (CVSS), SSVC signals, assigners, and affected vendors/products (CVE v5).
- Identify vulnerabilities confirmed as exploited in the wild, ransomware use, and CISA remediation due dates (CISA KEV).
- Prioritize by probability of exploitation in the next 30 days and track how that probability changes over time (FIRST EPSS, current and daily history).
- Assess open-source package exposure by ecosystem, package and fixed version (GitHub Security Advisories, OSV).
- Classify weaknesses (CWE catalog) and relate them to CAPEC attack patterns and MITRE ATT&CK techniques.

**Limitations:**
- No asset inventory, so it cannot tell which vulnerabilities affect your environment.
- No patch or remediation status beyond CISA KEV required actions and due dates.
- CWE-to-ATT&CK links are weakness-based associations, not observed attacker behavior."""


def genie_space() -> GenieSpace:
    return GenieSpace(
        title="Analysis of Vulnerabilities",
        description=DESCRIPTION,
        warehouse_id="${var.warehouse_id}",
        parent_path="${var.genie_parent_path}",
        serialized_space=SPACE_FILE.read_text(),
    )
