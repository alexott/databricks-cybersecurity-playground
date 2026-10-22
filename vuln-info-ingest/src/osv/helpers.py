"""OSV loader helpers: pure Python (no Spark), unit-tested in tests/unit/test_osv.py.
Staging helpers (to_single_line, write_text_parts, clear_parts) are in common.staging.
The Spark parts (extract_raw_data, transform_vulnerabilities) are in transform.py."""
from __future__ import annotations

import datetime
import urllib.parse
from typing import Iterable, Iterator

from common.staging import to_single_line


def parse_osv_ts(s: str) -> datetime.datetime:
    """OSV RFC 3339 UTC timestamp (may have nanoseconds) -> aware datetime.
    Fractional seconds are truncated to microseconds, as Spark TIMESTAMP does."""
    s = s.strip().rstrip("Z")
    base, _, frac = s.partition(".")
    ts = datetime.datetime.fromisoformat(base).replace(tzinfo=datetime.timezone.utc)
    if frac:
        ts = ts.replace(microsecond=int(frac[:6].ljust(6, "0")))
    return ts


def changed_since(lines: Iterable[str], since: datetime.datetime, limit: int
                  ) -> tuple[dict[str, str], datetime.datetime | None, bool]:
    """Scan modified_id.csv lines ("<modified>,<ecosystem_dir>/<id>", newest first)
    until a line is older than `since`.

    Returns ({id: "<ecosystem_dir>/<id>"}, newest modified seen, exceeded). An id in
    several ecosystems is listed once per ecosystem; one path is enough. Stops early
    with exceeded=True once more than `limit` ids changed."""
    changes: dict[str, str] = {}
    newest = None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        ts_s, path = line.split(",", 1)
        ts = parse_osv_ts(ts_s)
        if ts < since:
            break
        if newest is None:
            newest = ts
        changes.setdefault(path.split("/", 1)[1], path)
        if len(changes) > limit:
            return changes, newest, True
    return changes, newest, False


def record_url(bucket_url: str, path: str) -> str:
    """URL of one record file; ecosystem dirs and ids need quoting ('[EMPTY]', 'Red Hat', ':')."""
    return f"{bucket_url}/{urllib.parse.quote(path, safe='/')}.json"


def pinned_url(url: str, generation: str) -> str:
    """Pin a GCS object version: all.zip is rebuilt every ~15 minutes."""
    return f"{url}?generation={generation}"




def iter_zip_records(zf) -> Iterator[bytes]:
    """Yield each *.json entry of an open zipfile.ZipFile as one JSON line (bytes).
    ZipFile checks each entry's CRC while reading."""
    for info in zf.infolist():
        if info.filename.endswith(".json"):
            yield to_single_line(zf.read(info).strip())
