"""Builder of the cwe_capec_attack edge table (pure Python, no Spark): CWE weakness ->
CAPEC attack pattern -> MITRE ATT&CK technique.

Both edge sets come from the CAPEC catalog (`Related_Weaknesses` and the `ATTACK`
`Taxonomy_Mappings` of each attack pattern). Technique names come from the Enterprise
ATT&CK STIX bundle; mappings to techniques that are not in Enterprise ATT&CK (Mobile
techniques such as T1513) are dropped. Revoked techniques are kept: CAPEC 3.9 (2023)
still maps to them."""
from __future__ import annotations

import xml.etree.ElementTree as ET

CapecData = tuple[str, str, list[tuple[str, str, str]], list[tuple[str, str]]]


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_capec(data: bytes) -> CapecData:
    """CAPEC catalog XML -> (Version, Date, [(cwe_id, capec_id, capec_name)],
    [(capec_id, technique_id)]), in catalog order, duplicates kept."""
    root = ET.fromstring(data)
    if _local(root.tag) != "Attack_Pattern_Catalog":
        raise ValueError(f"not a CAPEC catalog: root element {_local(root.tag)!r}")
    ns = root.tag[:-len(_local(root.tag))]
    cwe_edges, technique_edges = [], []
    for ap in root.iterfind(f"{ns}Attack_Patterns/{ns}Attack_Pattern"):
        capec_id = f"CAPEC-{ap.get('ID')}"
        for rw in ap.iterfind(f"{ns}Related_Weaknesses/{ns}Related_Weakness"):
            cwe_edges.append((f"CWE-{rw.get('CWE_ID')}", capec_id, ap.get("Name")))
        for tm in ap.iterfind(f"{ns}Taxonomy_Mappings/{ns}Taxonomy_Mapping"):
            entry_id = (tm.findtext(f"{ns}Entry_ID") or "").strip()
            if tm.get("Taxonomy_Name") == "ATTACK" and entry_id:
                technique_edges.append((capec_id, f"T{entry_id}"))
    return root.get("Version"), root.get("Date"), cwe_edges, technique_edges


def parse_attack_techniques(bundle: dict) -> tuple[str | None, dict[str, str]]:
    """Enterprise ATT&CK STIX bundle -> (ATT&CK version, {technique id: name})."""
    version, names = None, {}
    for o in bundle.get("objects", []):
        if o.get("type") == "x-mitre-collection":
            version = o.get("x_mitre_version")
        elif o.get("type") == "attack-pattern":
            for ref in o.get("external_references", []):
                if ref.get("source_name") == "mitre-attack" and ref.get("external_id"):
                    names[ref["external_id"]] = o["name"]
    return version, names


def build_rows(capec: CapecData, attack: tuple[str | None, dict[str, str]]) -> list[tuple]:
    """Rows (src_type, src_id, rel, dst_type, dst_id, dst_name, source_version) of
    cwe_capec_attack, without duplicates, in catalog order."""
    capec_version, _, cwe_edges, technique_edges = capec
    attack_version, technique_names = attack
    source = f"capec_{capec_version};attack_enterprise_{attack_version}"
    rows = [("CWE", cwe, "related_attack_pattern", "CAPEC", ap, name, source) for cwe, ap, name in cwe_edges]
    rows += [("CAPEC", ap, "maps_to_technique", "technique", t, technique_names[t], source)
             for ap, t in technique_edges if t in technique_names]
    return list(dict.fromkeys(rows))
