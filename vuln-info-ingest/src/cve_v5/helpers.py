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
