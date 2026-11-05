"""CWE loader helpers. The catalog is XML, so each Weakness / Grouping / View entry
is converted to JSON for the raw VARIANT (pure Python, ElementTree):

- an element without attributes or child elements becomes its text;
- other elements become objects: attributes as keys, child elements under their tag
  name, and their own text (if any) under `Text`;
- tags the CWE schema (cwe_schema_v8.0.xsd) allows to repeat are always arrays, even
  with one value, so a VARIANT path has the same type in every record;
- structured text (XHTML paragraphs, lists, code) is rendered to plain text with
  line breaks (`render_text`).

Element order between different tags is not kept (e.g. the interleaved Body_Text /
Example_Code of a Demonstrative_Example are two arrays). `extract_raw_data` is Spark
and takes the DataFrame passed in, so this module imports without a local Spark install."""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET

_XHTML = "{http://www.w3.org/1999/xhtml}"
# Elements with maxOccurs > 1 in cwe_schema_v8.0.xsd (CWE 5.0).
REPEATED = frozenset({
    "Affected_Resource", "Alternate_Term", "Architecture", "Author", "Background_Detail", "Body_Text",
    "Category", "Consequence", "Contribution", "Contribution_Name", "Custom_Phase", "Demonstrative_Example",
    "Detection_Method", "Dimension", "Example_Code", "Exploitation_Factor", "External_Reference",
    "Functional_Area", "Grouping", "Has_Member", "Impact", "Introduction", "Language", "Media_Type",
    "Member_Of", "Mitigation", "Modification", "Note", "Observed_Example", "Operating_System", "Phase",
    "Previous_Entry_Name", "Rationale", "Reason", "Reference", "Related_Attack_Pattern",
    "Related_Weakness", "Role", "Scope", "Stakeholder", "Submission", "Submission_Name", "Suggestion",
    "Taxonomy_Mapping", "Technology", "View", "Weakness", "Weakness_Ordinality",
})
_BLOCK = frozenset({"p", "div", "ul", "ol", "li", "table", "tr", "pre", "blockquote", "h1", "h2", "h3"})
# Catalog section -> entry type (the `entry_type` column).
ENTRY_SECTIONS = {"Weaknesses": "Weakness", "Groupings": "Grouping", "Views": "View"}


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def render_text(e: ET.Element) -> str:
    """Text of an element, XHTML rendered to lines: block elements and <br> start a new
    line, list items get "- ", a `margin-left` div (code nesting) indents by 2 spaces.
    Source whitespace (XML pretty-printing) is collapsed; empty lines are dropped."""
    lines: list[list] = [[0, ""]]

    def text(s: str | None) -> None:
        if s:
            lines[-1][1] += re.sub(r"\s+", " ", s)

    def walk(el: ET.Element, indent: int) -> None:
        text(el.text)
        for c in el:
            tag = _local(c.tag) if c.tag.startswith(_XHTML) else None
            if tag == "br":
                lines.append([indent, ""])
            elif tag in _BLOCK:
                inner = indent + (2 if tag == "div" and "margin-left" in c.get("style", "") else 0)
                lines.append([inner, "- " if tag == "li" else ""])
                walk(c, inner)
                lines.append([indent, ""])
            else:
                walk(c, indent)
            text(c.tail)

    walk(e, 0)
    return "\n".join(" " * ind + s.strip() for ind, s in lines if s.strip() and s.strip() != "-")


def element_to_json(e: ET.Element):
    """One CWE XML element -> JSON-able value (see the module docstring)."""
    children = [c for c in e if not c.tag.startswith(_XHTML)]
    if not e.attrib and not children:
        return render_text(e)
    out: dict = {_local(k): v for k, v in e.attrib.items()}
    groups: dict[str, list] = {}
    for c in children:
        groups.setdefault(_local(c.tag), []).append(element_to_json(c))
    for tag, values in groups.items():
        # A tag not known as repeatable still keeps every value when it repeats.
        out[tag] = values if tag in REPEATED or len(values) > 1 else values[0]
    # Text of an element with child elements is only the XML indentation (or XHTML).
    text = render_text(e) if len(children) < len(e) or not children else ""
    if text:
        out["Text"] = text
    return out


def parse_catalog(data: bytes) -> tuple[str, str, list[tuple[str, dict]]]:
    """CWE catalog XML -> (Version, Date, [(entry_type, entry JSON)]), entries in
    catalog order: Weaknesses, then Groupings, then Views."""
    root = ET.fromstring(data)
    if _local(root.tag) != "Weakness_Catalog":
        raise ValueError(f"not a CWE catalog: root element {_local(root.tag)!r}")
    entries = []
    for section in root:
        entry_type = ENTRY_SECTIONS.get(_local(section.tag))
        if entry_type:
            entries.extend((entry_type, element_to_json(e)) for e in section)
    return root.get("Version"), root.get("Date"), entries


def extract_raw_data(df):
    """Add the raw-table id column from the per-entry VARIANT.

    `df` is a pyspark DataFrame with a `raw` VARIANT column plus `entry_type` and the
    catalog-level `catalog_version` / `catalog_date` stamped at load time. `cwe_id`
    (CWE-<ID>, as in cves_v5.cwe_ids) is the MERGE key."""
    return df.selectExpr(
        "raw",
        "concat('CWE-', try_variant_get(raw, '$.ID', 'string')) as cwe_id",
        "entry_type",
        "catalog_version",
        "catalog_date",
    )
